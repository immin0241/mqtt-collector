#!/usr/bin/env bash
# 앱(worker) 전용 DB role을 만든다. 멱등.
#   APP_WORKER_PASSWORD (기본: worker)
set -euo pipefail

PSQL=${PSQL:-docker compose exec -T postgres psql -U go -d go -v ON_ERROR_STOP=1}
PASSWORD=${APP_WORKER_PASSWORD:-worker}

$PSQL <<SQL
DO \$\$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'app_worker') THEN
        CREATE ROLE app_worker LOGIN PASSWORD '${PASSWORD}';
    ELSE
        ALTER ROLE app_worker PASSWORD '${PASSWORD}';
    END IF;
END
\$\$;

GRANT SELECT ON sensor TO app_worker;
GRANT SELECT, INSERT, UPDATE ON sensor_status TO app_worker;
GRANT INSERT ON sensor_event TO app_worker;
GRANT INSERT ON sensor_data TO app_worker;
GRANT INSERT ON sensor_data_rejected TO app_worker;
SQL
echo "app_worker role ready"
