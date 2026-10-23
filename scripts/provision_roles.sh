#!/usr/bin/env bash
# 앱(worker) 전용 DB role을 만든다. 멱등. compose의 seed 서비스와 같은 SQL(seeds/00_app_worker_role.sql)을 쓴다.
#   APP_WORKER_PASSWORD (기본: worker)
set -euo pipefail

PSQL=${PSQL:-docker compose exec -T postgres psql -U go -d go -v ON_ERROR_STOP=1}
PASSWORD=${APP_WORKER_PASSWORD:-worker}
SQL_FILE="$(dirname "$0")/../seeds/00_app_worker_role.sql"

$PSQL -v worker_password="${PASSWORD}" < "${SQL_FILE}"
echo "app_worker role ready"
