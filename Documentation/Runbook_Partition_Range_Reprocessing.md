# Runbook: Partition-Range Reprocessing & Backfills

This runbook outlines the operational steps to trigger partition-range reprocessing, backfill historical windows, and clear incremental watermarks in the Config-Driven Data Pipeline framework.

---

## 1. Overview of Partition-Scoped Reprocessing

When a pipeline is configured with partitioning enabled:
```yaml
partition:
  enabled: true
  granularity: day
  column: event_date
```

The system automatically limits the impact of database updates:
- **Snowflake Table Writes**: Using `mode: replace` does not drop the entire table. Instead, it executes an atomic transaction that deletes only the rows matching the target partition value (e.g., `DELETE FROM table WHERE "event_date" = '2026-07-22'`) before inserting the new partition slice.
- **Object Storage / Local File Sinks**: Files are saved to a directory structure segmented by partition (e.g., `.../event_date=2026-07-22/data.parquet`), preventing file name collisions and overwriting other partitions.

---

## 2. Triggering Backfills and Reprocessing

### A. Reprocessing via the Airflow UI
1. Navigate to the Airflow Web Server UI.
2. Select your pipeline DAG (e.g., `test_test_src`).
3. Click the **Grid View**.
4. Locate the DAG run corresponding to the partition date you wish to reprocess.
5. Click on the first task (`fetch_source_data`) or the task group.
6. Click **Clear** (select **Downstream** to clear all dependent tasks).
7. Confirm the action. Airflow will rerun the tasks for this specific partition date.

### B. Triggering a Range Backfill via Airflow CLI
To backfill a specific date range, use the Airflow 3.x backfill command:

```bash
# Run backfill for daily partitions from May 1st to May 10th, 2026
airflow dags backfill \
  --start-date 2026-05-01 \
  --end-date 2026-05-10 \
  test_test_src
```

This will spawn partition runs for each date in the range. The data quality checks and Snowflake writes will execute independently and concurrently (up to `max_active_runs`), with each run modifying only its own partition slice.

---

## 3. Resetting Incremental Load Watermarks

If a pipeline uses incremental watermarks:
```yaml
incremental:
  enabled: true
  watermark_column: updated_at
  initial_watermark: 2026-01-01
```

The high-water mark is persisted in the **Asset State Store** or **Task State Store**. To reprocess data older than the current watermark, you must reset it.

### Resetting State via Airflow CLI
To reset or force a new watermark, you can update the state store key via the state management command or by clearing the task instance state:

```bash
# Reset task state for the ingestion task (if task state store is used)
airflow tasks clear \
  --task-regex "fetch_source_data" \
  --start-date 2026-05-01 \
  --end-date 2026-05-01 \
  test_test_src
```

Alternatively, you can manually delete or update the state record in the Airflow metadata database or via the Airflow REST API for state stores.
