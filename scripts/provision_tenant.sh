#!/usr/bin/env bash
# tenant 전용 Grafana 조회 role을 만든다. 멱등.
#   usage: scripts/provision_tenant.sh <tenant_id>
#   GRAFANA_DB_PASSWORD (기본: grafana)
#   GRAFANA_URL / GRAFANA_ADMIN / GRAFANA_ORG_ID 를 주면 해당 org에 datasource도 등록한다.
set -euo pipefail

TENANT_ID=${1:?usage: provision_tenant.sh <tenant_id>}
[[ "$TENANT_ID" =~ ^[0-9]+$ ]] || { echo "tenant_id must be an integer" >&2; exit 1; }

PSQL=${PSQL:-docker compose exec -T postgres psql -U go -d go -v ON_ERROR_STOP=1}
PASSWORD=${GRAFANA_DB_PASSWORD:-grafana}
ROLE="grafana_tenant_${TENANT_ID}"

$PSQL <<SQL
DO \$\$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '${ROLE}') THEN
        CREATE ROLE ${ROLE} LOGIN PASSWORD '${PASSWORD}';
    ELSE
        ALTER ROLE ${ROLE} PASSWORD '${PASSWORD}';
    END IF;
END
\$\$;

-- role -> tenant 매핑 (ADR-0009). 뷰는 session_user 로 이 매핑을 조회하므로 role 이 세션에서 바꿀 수 없다.
INSERT INTO tenant_role (role_name, tenant_id) VALUES ('${ROLE}', ${TENANT_ID})
ON CONFLICT (role_name) DO UPDATE SET tenant_id = EXCLUDED.tenant_id;
-- 예전 방식(ALTER ROLE ... SET app.tenant_id)으로 만든 role 에 남은 설정 제거
ALTER ROLE ${ROLE} RESET app.tenant_id;

-- 뷰만 조회 가능. 기본 테이블 권한은 주지 않는다.
GRANT SELECT ON v_sensor, v_sensor_status, v_sensor_event, v_sensor_data, v_sensor_data_1m TO ${ROLE};
SQL
echo "${ROLE} role ready"

if [[ -n "${GRAFANA_URL:-}" ]]; then
    ORG_ID=${GRAFANA_ORG_ID:?GRAFANA_ORG_ID required with GRAFANA_URL}
    curl -fsS -X POST "${GRAFANA_URL}/api/datasources" \
        -u "${GRAFANA_ADMIN:-admin:admin}" \
        -H "Content-Type: application/json" \
        -H "X-Grafana-Org-Id: ${ORG_ID}" \
        -d "{
          \"name\": \"sensors\", \"uid\": \"sensors\", \"type\": \"grafana-postgresql-datasource\",
          \"access\": \"proxy\", \"url\": \"${GRAFANA_DB_HOST:-postgres:5432}\",
          \"user\": \"${ROLE}\", \"secureJsonData\": {\"password\": \"${PASSWORD}\"},
          \"jsonData\": {\"database\": \"go\", \"sslmode\": \"disable\", \"timescaledb\": true, \"postgresVersion\": 1800}
        }"
    echo
fi
