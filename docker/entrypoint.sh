#!/usr/bin/env bash
# Chart-native entrypoint (added 2026-07-23).
#
# Replicates the dual behavior of the official Apache Airflow image entrypoint so
# the SAME image works with BOTH invocation styles:
#   - docker-compose passes bare airflow subcommands: `scheduler`, `api-server`,
#     `celery worker`, `dag-processor`, `triggerer`  -> we prepend `airflow`.
#   - the official Helm chart passes shell forms for main AND injected init
#     containers: `bash -c "exec airflow ..."`, `bash -c "airflow db check-..."`
#     -> we exec them as-is.
#
# The previous `ENTRYPOINT ["airflow"]` broke the Helm chart: every chart
# container (incl. the wait-for-airflow-migrations init container on every
# component) runs `bash -c ...`, which became `airflow bash -c ...` -> airflow
# printed its help and exited non-zero -> cluster-wide Init:CrashLoopBackOff.
set -euo pipefail

case "${1:-}" in
  # Already-prefixed airflow command: run verbatim. The official Helm chart's
  # injected init containers (e.g. wait-for-airflow-migrations) pass
  # `airflow db check-migrations ...` directly — prepending again would double it.
  airflow)
    exec "$@"
    ;;
  # Shells / interpreters / explicit paths: run verbatim (Helm main containers
  # use `bash -c "exec airflow ..."`; also debugging).
  bash | sh | /bin/bash | /bin/sh | /usr/bin/bash | python | python3 | /*)
    exec "$@"
    ;;
  # Empty -> drop into airflow help (matches bare `airflow`).
  "")
    exec airflow
    ;;
  # Anything else is treated as an airflow subcommand (compose style).
  *)
    exec airflow "$@"
    ;;
esac
