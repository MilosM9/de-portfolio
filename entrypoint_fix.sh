#!/usr/bin/env bash
set -euo pipefail

mkdir -p /opt/airflow/dags /opt/airflow/logs /opt/airflow/plugins || true
chown -R "${AIRFLOW_UID:-50000}:${AIRFLOW_GID:-0}" /opt/airflow || true

# delegiraj baznom Airflow entrypoint-u
exec /usr/bin/dumb-init -- /entrypoint "$@"
