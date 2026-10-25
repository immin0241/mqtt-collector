#!/usr/bin/env bash
# 시연 스택의 Grafana 멀티 테넌트 구성을 만든다. 멱등.
#   테넌트 demo-b, demo-c 마다: Grafana org 생성 -> 전용 DB role + 데이터소스(provision_tenant.sh) -> 대시보드 등록
#   테넌트 dev 는 기본 org(1)이 쓴다. (compose 가 데이터소스와 대시보드를 provisioning 한다)
#
#   usage: scripts/demo_setup.sh
#   환경 변수: GRAFANA_URL (기본 http://localhost:3000), GRAFANA_ADMIN (user:password 형식, 기본은 .env 의 값),
#              GRAFANA_DB_PASSWORD (기본 grafana)
#   COMPOSE_FILE 에 오버레이가 들어 있어야 한다. (export COMPOSE_FILE=docker-compose.yml:docker-compose.demo.yml)
set -euo pipefail

cd "$(dirname "$0")/.."
GRAFANA_URL=${GRAFANA_URL:-http://localhost:3000}
# 관리자 계정: 환경 변수 GRAFANA_ADMIN(user:password) > .env 의 GRAFANA_ADMIN_USER/PASSWORD > admin:admin
env_get() { grep -m1 "^$1=" .env 2>/dev/null | cut -d= -f2- || true; }
if [[ -z "${GRAFANA_ADMIN:-}" ]]; then
    gf_user=$(env_get GRAFANA_ADMIN_USER); gf_pass=$(env_get GRAFANA_ADMIN_PASSWORD)
    GRAFANA_ADMIN="${gf_user:-admin}:${gf_pass:-admin}"
fi
TENANTS=(demo-b demo-c)
DASHBOARD=grafana/dashboards/sensor-overview.json

psql_go() { docker compose exec -T postgres psql -U go -d go -tA -v ON_ERROR_STOP=1 "$@"; }
gf() { curl -fsS -u "$GRAFANA_ADMIN" -H "Content-Type: application/json" "$@"; }

for name in "${TENANTS[@]}"; do
    tenant_id=$(psql_go -c "SELECT id FROM tenant WHERE name = '${name}'")
    [[ -n "$tenant_id" ]] || { echo "tenant ${name} not found (demo seed not applied?)" >&2; exit 1; }

    # org: 이미 있으면 그 id 를 쓴다.
    org_id=$(gf "${GRAFANA_URL}/api/orgs/name/${name}" 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin).get("id",""))' || true)
    if [[ -z "$org_id" ]]; then
        org_id=$(gf -X POST "${GRAFANA_URL}/api/orgs" -d "{\"name\":\"${name}\"}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["orgId"])')
    fi
    echo "tenant ${name}: tenant_id=${tenant_id} grafana_org_id=${org_id}"

    # 이미 같은 datasource 가 있으면 role 만 갱신한다. (provision_tenant.sh 의 POST 는 중복 시 실패한다)
    if gf -H "X-Grafana-Org-Id: ${org_id}" "${GRAFANA_URL}/api/datasources/uid/sensors" >/dev/null 2>&1; then
        GRAFANA_URL= ./scripts/provision_tenant.sh "$tenant_id"
        echo "  datasource already exists in org ${org_id}"
    else
        GRAFANA_URL="$GRAFANA_URL" GRAFANA_ADMIN="$GRAFANA_ADMIN" GRAFANA_ORG_ID="$org_id" ./scripts/provision_tenant.sh "$tenant_id"
    fi

    # 대시보드: 기본 org 와 같은 JSON 을 쓴다. 데이터소스는 변수(ds)로 그 org 의 postgres 데이터소스를 찾는다.
    python3 - "$DASHBOARD" <<'PY' | gf -X POST -H "X-Grafana-Org-Id: ${org_id}" "${GRAFANA_URL}/api/dashboards/db" -d @- >/dev/null
import json, sys
d = json.load(open(sys.argv[1], encoding="utf-8"))
d["id"] = None
print(json.dumps({"dashboard": d, "overwrite": True, "folderId": 0}))
PY
    echo "  dashboard imported into org ${org_id}"
done

cat <<'EOF'

Grafana org 와 테넌트 연결이 끝났다. Logto 로그인을 쓰려면 .env 의 GRAFANA_OAUTH_ORG_MAPPING 에
  "<Logto 조직 id>:<Grafana org id>:<역할>"  (여러 개는 공백으로 구분)
을 넣고 grafana 를 재시작한다. 테넌트 dev 는 Grafana org 1 이다.
EOF
