# Local Kubernetes Sanity Deploy (Docker Desktop) — findings & runbook

**Executed 2026-07-25** against Docker Desktop Kubernetes (single node, no Istio).
Result: the full Airflow **3.3.0** stack — api-server, scheduler, dag-processor,
triggerer, celery worker, postgres, redis, statsd — all pods **1/1 Running**;
`GET /api/v2/version` → `{"version":"3.3.0"}`; `airflow jobs check` → "Found one
alive job." This is the substitute for the (ACR/AKS-blocked) dev-AKS sanity
deploy, and it verified the apiServer schema against the real chart.

## Why Helm (not the image directly)

The image is one container (the Airflow binary). An Airflow *deployment* is a
distributed system of ~7 coordinated components + DB-migration job + user/pool/
connection seeding + services + RBAC + volumes. The official apache-airflow Helm
chart declares and wires all of that; it deploys **our** image via
`images.airflow`. Image = what runs; chart = how the whole system runs on k8s.
The mandated corp DHI base is baked into our image at build time (Dockerfile
`AIRFLOW_BASE_IMAGE`), so Helm sits cleanly on top of it.

## Findings (each is an AKS-readiness item)

1. **No official chart supports Airflow 3.3.0 yet.** Latest is chart **1.22.0**
   (appVersion 3.2.2). We deploy chart 1.22.0 and override the image to our
   3.3.0 build; `airflowVersion: "3.3.0"` tells the chart the real version for
   feature gating. Re-check for a 3.3.0 chart before the real AKS deploy.

2. **apiServer schema VERIFIED.** Chart 1.22.0 has `apiServer:` with
   `service`/`resources`/`replicas` — exactly what `production.yaml` uses. The
   Airflow-3 `webserver:`→`apiServer:` rename in `production.yaml` is correct.
   (`webserver:` still exists in the chart but only for the FAB
   `webserver_config.py` ConfigMap.)

3. **Image entrypoint had to become chart-native (fixed).** The old
   `ENTRYPOINT ["airflow"]` broke the chart: every chart container runs shell
   forms (`bash -c "exec airflow api-server"`) and the injected
   wait-for-airflow-migrations init container runs `airflow db check-migrations`
   — under `["airflow"]` these became `airflow bash …` / `airflow airflow …` and
   crashlooped the whole release. Fix: `docker/entrypoint.sh` installed at
   **`/entrypoint`** (the path the chart hardcodes for liveness/readiness
   probes; the DHI base ships none). It dispatches: leading `airflow`/shell/path
   → exec verbatim; otherwise prepend `airflow` (so compose `command: scheduler`
   still works). Verified both styles.

4. **DHI base lacks the official image's helper scripts.** Besides `/entrypoint`,
   the chart's logGroomer sidecars run `/clean-logs`, which the DHI base doesn't
   ship → sidecar crashloops. For the sanity deploy we disabled the groomers
   (non-essential log pruning). Production choice: add `/clean-logs` to the image
   or keep groomers off and rotate logs another way.

5. **statsd module absent (non-fatal).** The image lacks the `statsd` python
   package, so metrics fall back to NoStatsLogger with an error-level log line.
   Harmless; add `statsd` to requirements if StatsD/Grafana metrics are wanted.

6. **dags/ and config/ are NOT baked into the image** (same class as the former
   plugins gap). Core pods run without them, but DAGs won't appear until the
   framework `generate_dags.py` + configs arrive — via the two-bundle model
   (LocalDagBundle baked / GitDagBundle from the config repo, Task 3C.4). The
   Dockerfile still copies only `rlam_airflow_framework`.

## Runbook (reproduce locally)

```bash
# 1. Build the image (chart-native entrypoint baked in)
docker compose -f docker/docker-compose.yaml build

# 2. Load it into the Docker Desktop k8s node (separate containerd store)
docker save configdrivenpipeline:latest \
  | docker exec -i desktop-control-plane ctr -n k8s.io images import -

# 3. Deploy the official chart with the local override
helm repo add apache-airflow https://airflow.apache.org
helm upgrade --install airflow apache-airflow/airflow \
  --version 1.22.0 -n airflow --create-namespace \
  -f helm/values/local-docker-desktop.yaml --timeout 6m

# 4. Verify
kubectl get pods -n airflow                      # all 1/1 Running
kubectl port-forward svc/airflow-api-server 8080:8080 -n airflow &
curl -s http://localhost:8080/api/v2/version     # {"version":"3.3.0"}

# Teardown
helm uninstall airflow -n airflow && kubectl delete ns airflow
```

## Carry-over to the real AKS deploy (Task 3C.1)

- Keep the chart-native `/entrypoint` (production fix, not just local).
- Decide `/clean-logs` (add script) vs. groomers-off.
- `production.yaml` still assumes managed-premium storage, Istio, workload
  identity, ACR image — all real on AKS; only stripped here. apiServer block is
  now schema-verified.
- Pin a 3.3.0-supporting chart version once published; until then chart 1.22.0 +
  image override is the pattern.
