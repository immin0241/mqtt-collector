#!/usr/bin/env bash
# Logto M2M 앱으로 조직 토큰을 받아 MCP 서버를 호출해 본다. (ADR-0014)
#   usage: LOGTO_M2M_ID=... LOGTO_M2M_SECRET=... scripts/mcp_token.sh <logto organization id> [scope] [--print]
#
# Logto 콘솔(http://localhost:3002)에서 먼저 할 일:
#   1. Applications -> Create -> Machine-to-machine. App ID / Secret 을 복사한다.
#   2. Organizations -> 대상 조직 -> Machine-to-machine 탭에서 이 앱을 추가하고, 조직 역할(read:data 가 들어 있는 역할)을 준다.
# scope 기본값은 read:data (Logto 기본 조직 권한). compose 의 MCP_SCOPE_REPORTS_READ 와 같아야 한다.
# 토큰은 비밀이다. 출력된 값을 로그·이슈에 붙이지 않는다. --print 를 주면 토큰만 출력한다.
set -euo pipefail

ORG=${1:?usage: mcp_token.sh <logto organization id> [scope] [--print]}
SCOPE=${2:-read:data}
[[ "$SCOPE" == "--print" ]] && SCOPE=read:data
ID=${LOGTO_M2M_ID:?LOGTO_M2M_ID is required}
SECRET=${LOGTO_M2M_SECRET:?LOGTO_M2M_SECRET is required}
LOGTO=${LOGTO_URL:-http://localhost:3001}
MCP=${MCP_URL:-http://localhost:8090/mcp}

RESP=$(curl -fsS -u "$ID:$SECRET" "$LOGTO/oidc/token" \
    -d grant_type=client_credentials \
    -d "resource=urn:logto:organization:$ORG" \
    -d "scope=$SCOPE")
TOKEN=$(printf '%s' "$RESP" | python3 -c 'import sys,json; print(json.load(sys.stdin)["access_token"])')

if [[ " $* " == *" --print "* ]]; then
    printf '%s\n' "$TOKEN"
    exit 0
fi

# 토큰의 claims(서명 검증 없이 디코드)만 보여 준다. aud 가 조직인지, scope 가 맞는지 확인용이다.
printf '%s' "$TOKEN" | python3 -c '
import sys, json, base64
p = sys.stdin.read().split(".")[1]
c = json.loads(base64.urlsafe_b64decode(p + "=" * (-len(p) % 4)))
print({k: c.get(k) for k in ("iss", "sub", "aud", "scope", "exp")})'

echo "--- MCP initialize"
curl -sS -o /dev/null -w "HTTP %{http_code}\n" -X POST "$MCP" \
    -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
    -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"mcp_token.sh","version":"0"}}}'
