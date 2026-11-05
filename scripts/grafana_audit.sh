#!/usr/bin/env bash
# Grafana org 멤버십 감사 (TC-09 Grafana 로그인 경로).
# Logto 에서 사용자가 속한 조직을 읽고 GRAFANA_OAUTH_ORG_MAPPING 으로 "있어야 할 Grafana org" 를 계산해,
# 실제 Grafana 멤버십과 비교한다. 매핑에 없는 org 에 속해 있거나(격리 위반), 속해야 할 org 에 없으면 실패한다.
# 매핑은 로그인할 때만 반영되므로 매핑을 바꾼 뒤 로그인하지 않은 사용자는 이전 멤버십이 남는다. 이 스크립트가 그것을 잡는다.
#
# 사용: scripts/grafana_audit.sh        (저장소 루트, 스택 기동 중, .env 의 GRAFANA_ADMIN_*, GRAFANA_OAUTH_ORG_MAPPING 사용)
#   GRAFANA_URL (기본 http://localhost:3000)
# 로컬 Grafana 관리자(admin)는 감사 대상에서 뺀다. 비밀번호는 출력하지 않는다.
set -euo pipefail

GRAFANA_URL=${GRAFANA_URL:-http://localhost:3000}
env_get() { grep -m1 "^$1=" .env 2>/dev/null | cut -d= -f2- | sed -E 's/^"(.*)"$/\1/' || true; }

gf_user=$(env_get GRAFANA_ADMIN_USER); gf_pass=$(env_get GRAFANA_ADMIN_PASSWORD)
mapping=$(env_get GRAFANA_OAUTH_ORG_MAPPING)
[[ -n "$mapping" ]] || { echo "GRAFANA_OAUTH_ORG_MAPPING 이 비어 있다" >&2; exit 2; }

logto_rows=$(docker compose exec -T logto-postgres psql -U logto -d logto -tA -F '|' \
    -c "select u.username, r.organization_id from users u join organization_user_relations r on r.user_id = u.id where u.username is not null")

GF_AUTH="${gf_user:-admin}:${gf_pass:-admin}" GRAFANA_URL="$GRAFANA_URL" MAPPING="$mapping" LOGTO_ROWS="$logto_rows" \
python3 -I - <<'EOF'
import base64, json, os, sys, urllib.request

base = os.environ["GRAFANA_URL"]
auth = base64.b64encode(os.environ["GF_AUTH"].encode()).decode()

def get(path):
    req = urllib.request.Request(base + path, headers={"Authorization": "Basic " + auth})
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.load(r)

# "logtoOrg:grafanaOrg:Role ..."
mapping = {}
for item in os.environ["MAPPING"].split():
    org, gorg, _role = item.split(":")
    mapping[org] = int(gorg)

logto = {}
for line in os.environ["LOGTO_ROWS"].splitlines():
    if line.strip():
        user, org = line.split("|")
        logto.setdefault(user, set()).add(org)

bad = 0
for u in get("/api/users"):
    login = u["login"]
    if login == "admin":
        continue
    actual = {o["orgId"] for o in get(f"/api/users/{u['id']}/orgs")}
    orgs = logto.get(login)
    if orgs is None:
        print(f"FAIL {login}: Logto 에 없는 사용자인데 Grafana org {sorted(actual)} 에 속함")
        bad += 1
        continue
    expected = {mapping[o] for o in orgs if o in mapping}
    extra, missing = actual - expected, expected - actual
    if extra or missing:
        print(f"FAIL {login}: 기대 {sorted(expected)} 실제 {sorted(actual)}"
              + (f" (초과 {sorted(extra)})" if extra else "") + (f" (누락 {sorted(missing)})" if missing else ""))
        bad += 1
    else:
        print(f"OK   {login}: org {sorted(actual)}")
print("격리 위반 없음" if not bad else f"불일치 {bad}건")
sys.exit(1 if bad else 0)
EOF
