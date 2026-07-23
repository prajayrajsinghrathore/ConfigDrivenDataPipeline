# DAG Bundles Rollback Strategy

## Overview
With DAG Bundles, rollback is straightforward: update the Git tag reference in Helm values and redeploy. Since you use a **single config bundle** with flat structure, all tenants roll back together to the previous version.

## Rollback Procedure

### Step 1: Identify Target Version
Check available versions in your config repo:
```bash
git tag --list 'prod-*' --sort=-version:refname
# Output:
# prod-v1.2.3  (current - has issues)
# prod-v1.2.2  (stable - rollback target)
# prod-v1.2.1
```

### Step 2: Query Impacted DAG Runs
Use Kafka events to identify which DAG runs used the problematic version:
```sql
-- Find DAG runs using current bundle version
SELECT 
    dag_id,
    task_id,
    metadata.bundle_version,
    metadata.tenant,
    timestamp
FROM kafka_pipeline_events
WHERE metadata.bundle_version = 'prod-v1.2.3'
  AND event_type = 'data_load'
ORDER BY timestamp DESC
LIMIT 100;
```

### Step 3: Update Helm Values
```bash
# Rollback to prod-v1.2.2
# Note: We need to update tracking_ref in the GitDagBundle kwargs
helm upgrade airflow apache-airflow/airflow \
  -f helm-values-aks.yaml \
  --set-json 'config.dag_processor.dag_bundle_config_list=[{"name":"framework","classpath":"airflow.dag_processing.bundles.local.LocalDagBundle","kwargs":{"path":"/opt/airflow/dags","refresh_interval":300}},{"name":"production_configs","classpath":"airflow.providers.git.bundles.git.GitDagBundle","kwargs":{"tracking_ref":"refs/tags/prod-v1.2.2","git_conn_id":"azure_devops_git","subdir":"","refresh_interval":60}}]' \
  --set env[0].value="production_configs" \
  --set env[1].value="prod-v1.2.2" \
  --namespace airflow \
  --timeout 10m \
  --wait

# Verify deployment
kubectl get pods -n airflow -l component=scheduler
kubectl logs -n airflow -l component=scheduler --tail=50 | grep "bundle"
```

### Step 4: Validate Rollback
1. **Check Airflow UI**: Verify DAGs refresh with old config
   ```bash
   # Port-forward to webserver
   kubectl port-forward -n airflow svc/airflow-webserver 8080:8080
   # Open http://localhost:8080
   ```

2. **Check DAG Processor Logs**:
   ```bash
   kubectl logs -n airflow -l component=dag-processor --tail=100 | grep "bundle_version"
   # Should show: bundle_version=prod-v1.2.2
   ```

3. **Verify Kafka Events**: Trigger a test DAG run and check events
   ```bash
   # Trigger DAG via Airflow CLI
   kubectl exec -n airflow -it deployment/airflow-scheduler -- \
     airflow dags trigger <tenant>_<source_name> --conf '{"test": "rollback"}'
   
   # Check Kafka events
   # Should see metadata.bundle_version = "prod-v1.2.2"
   ```

### Step 5: Re-run Failed DAG Runs (If Needed)
If DAG runs failed due to config issues, manually re-run them:
```bash
# List failed runs
kubectl exec -n airflow -it deployment/airflow-scheduler -- \
  airflow dags list-runs -d <dag_id> --state failed --output json

# Clear failed runs (will trigger retry)
kubectl exec -n airflow -it deployment/airflow-scheduler -- \
  airflow tasks clear <dag_id> \
    --start-date <execution_date> \
    --end-date <execution_date> \
    --yes
```

## Emergency Rollback (Fast Track)

For critical production issues, create a quick rollback values file:
```bash
# Create rollback-v1.2.2.yaml with just the changes
cat > rollback-v1.2.2.yaml <<EOF
config:
  dag_processor:
    dag_bundle_config_list: |
      [
        {
          "name": "framework",
          "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
          "kwargs": {"path": "/opt/airflow/dags", "refresh_interval": 300}
        },
        {
          "name": "production_configs",
          "classpath": "airflow.providers.git.bundles.git.GitDagBundle",
          "kwargs": {
            "tracking_ref": "refs/tags/prod-v1.2.2",
            "git_conn_id": "azure_devops_git",
            "subdir": "",
            "refresh_interval": 60
          }
        }
      ]
  
env:
  - name: AIRFLOW_DAG_BUNDLE_VERSION
    value: "prod-v1.2.2"
EOF

# Apply rollback
helm upgrade airflow apache-airflow/airflow \
  -f helm-values-aks.yaml \
  -f rollback-v1.2.2.yaml \
  --force \
  --wait
```

## Rollback Decision Matrix

| Scenario | Action | Impact |
|----------|--------|--------|
| **Bad transformation logic** | Rollback + re-run failed DAGs | All tenants affected |
| **Invalid destination config** | Rollback immediately | Data loading stops |
| **Schema validation too strict** | Rollback + adjust schema | DAG parse failures |
| **Performance degradation** | Rollback + optimize config | Slow task execution |
| **New config syntax error** | Rollback automatically via CI | No impact (caught in validation) |

## Post-Rollback Actions

1. **Document the issue**:
   ```markdown
   ## Rollback: prod-v1.2.3 → prod-v1.2.2
   - **Date**: 2026-02-03 14:30 UTC
   - **Reason**: Invalid Snowflake connection for reporting tenant
   - **Impacted DAGs**: reporting_finnhub, reporting_market_data
   - **Failed runs**: 12 (all reporting tenant)
   - **Resolution**: Fixed connection string in config, will deploy as prod-v1.2.4
   ```

2. **Fix the root cause**: Update config in Git, create new PR
3. **Test in dev**: Deploy to dev environment first
   ```bash
   helm upgrade airflow-dev apache-airflow/airflow \
     -f helm-values-aks-dev.yaml \
     --set config.dag_processor.dag_bundle_config_list[1].kwargs.ref="refs/heads/develop"
   ```

4. **Release new version**: Once validated, tag and deploy
   ```bash
   # CI/CD creates prod-v1.2.4
   helm upgrade airflow apache-airflow/airflow \
     -f helm-values-aks.yaml \
     --set config.dag_processor.dag_bundle_config_list[1].kwargs.ref="refs/tags/prod-v1.2.4"
   ```

## Monitoring Rollback Success

### Metrics to Watch
1. **DAG parse errors**: Should drop to 0 after rollback
   ```bash
   kubectl exec -n airflow deployment/airflow-scheduler -- \
     airflow dags list --output json | jq '.[] | select(.is_paused==true)'
   ```

2. **Task success rate**: Should return to baseline
   ```sql
   SELECT 
       DATE(timestamp) as date,
       COUNT(*) FILTER (WHERE status='success') / COUNT(*) as success_rate
   FROM kafka_pipeline_events
   WHERE metadata.bundle_version IN ('prod-v1.2.2', 'prod-v1.2.3')
   GROUP BY date
   ORDER BY date DESC;
   ```

3. **Bundle refresh logs**: Confirm new version loaded
   ```bash
   kubectl logs -n airflow -l component=dag-processor -f | grep "GitDagBundle"
   ```

## Preventing Rollbacks

1. **Pre-deployment validation**:
   - Azure Pipeline validates all schemas
   - Contract tests check tenant assignments
   - Integration tests in dev environment

2. **Staged rollout** (future enhancement):
   - Deploy to `dev` bundle first (separate GitDagBundle)
   - Monitor for 24 hours
   - Promote to `prod` bundle

3. **Canary configs** (future enhancement):
   - Use DAG params to test new configs on subset of runs
   - Gradually increase traffic to new config
   - Automatic rollback on error threshold

## Runbook Checklist

- [ ] Identify problematic bundle version
- [ ] Query Kafka events for impacted runs
- [ ] Update Helm values with rollback tag
- [ ] Deploy and wait for scheduler restart
- [ ] Verify bundle version in logs
- [ ] Check Airflow UI for DAG health
- [ ] Trigger test DAG run
- [ ] Validate Kafka event metadata
- [ ] Re-run failed DAGs if needed
- [ ] Document rollback in incident log
- [ ] Create fix and deploy new version

## Rerun Behavior (`rerun_with_latest_version`)

By default in Airflow 3.3.0, retrying a failed task or clearing a DAG Run executes the task against its original bound DAG version. This ensures deterministic behavior and reproducibility.

However, if a pipeline failed due to a bug in the code or a misconfiguration, you might want the retry to execute the *fixed* version rather than the original buggy version.

### How to use `rerun_with_latest_version`

You can control this behavior via the pipeline YAML configuration `metadata` block:

```yaml
metadata:
  tenant: marketing
  owner: data-eng
  rerun_with_latest_version: true
```

- `rerun_with_latest_version: false` (Default): Retries use the historical bound version.
- `rerun_with_latest_version: true`: Retries will "upgrade" the DAG Run to the latest DAG version and execute the new logic.

### Operational Guidelines

1. **For transient failures (API timeouts, DB locks):** Leave `rerun_with_latest_version: false`. The existing logic is correct, and the failure is environmental.
2. **For logic bugs (incorrect transformations, wrong schemas):**
   - Push a fix to the DAG bundle.
   - Update the configuration to set `rerun_with_latest_version: true`.
   - Clear the failed tasks. They will pick up the new logic.
   - Once the backfill or recovery is complete, it is recommended to revert `rerun_with_latest_version` back to `false` for stability.

## Airflow Metadata-DB Version Rollback (3.3.0 → 3.1.6)

**Rehearsed 2026-07-23** on a throwaway copy of the live 3.3.0 metadata DB
(Postgres, `pg_dump | psql` into `airflow_rollback_test`), per Phase 0 Task 0.9.

```bash
# 1. Clone the metadata DB (never downgrade the live DB directly)
docker exec postgres sh -c "createdb -U airflow airflow_rollback_test \
  && pg_dump -U airflow airflow | psql -q -U airflow airflow_rollback_test"

# 2. Downgrade the copy
docker exec -e AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=\
"postgresql+psycopg2://airflow:airflow@postgres/airflow_rollback_test" \
  airflow-scheduler airflow db downgrade --to-version 3.1.6 -y
```

**Observed result:**
- Alembic head moved `d2f4e1b3c5a7` (3.3.0) → `cc92b33c6709` (3.1.6).
- 3.2.x/3.3.0 objects dropped cleanly, including `task_state_store` and the
  `retry_delay_override`/`retry_reason` TI columns.
- Deadline JSON conversion reversed ("Total migrated: 0 deadline records" —
  no deadline rows existed at rehearsal time; a populated DB would convert them).
- Round trip verified: `airflow db migrate` on the downgraded copy returned to
  `d2f4e1b3c5a7` with `task_state_store` recreated.

**Caveats:**
- Downgrading discards all data stored in 3.2.x/3.3.0-only tables/columns
  (state-store watermarks/DQ provenance, retry reasons, HITL detail rows added
  after the downgrade point). Export anything you need first.
- Code and DB must move together: a 3.1.6 DB with 3.3.0 images (or vice versa)
  will fail at startup. Roll back the image tag and the DB in the same window.
- Nothing is deployed to production yet, so this remains a rehearsal artifact;
  re-run it against a production-shaped dump before the first real deployment.
