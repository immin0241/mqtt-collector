#!/usr/bin/env bash
# 테넌트의 MCP 리포트용 읽기 전용 DB role(mcp_tenant_<id>)을 만든다. 멱등. (ADR-0014)
#   usage: MCP_ROLE_SECRET=... scripts/provision_mcp_tenant.sh <tenant_id>
# 비밀번호 = HMAC-SHA256(MCP_ROLE_SECRET, role 이름)의 16진수. internal/report.RolePassword 와 같은 값이다.
set -euo pipefail

TENANT_ID=${1:?usage: provision_mcp_tenant.sh <tenant_id>}
[[ "$TENANT_ID" =~ ^[0-9]+$ ]] || { echo "tenant_id must be an integer" >&2; exit 1; }
SECRET=${MCP_ROLE_SECRET:?MCP_ROLE_SECRET is required}

ROLE="mcp_tenant_${TENANT_ID}"
PASSWORD=$(printf '%s' "$ROLE" | openssl dgst -sha256 -hmac "$SECRET" -hex | awk '{print $NF}')

PSQL=${PSQL:-docker compose exec -T postgres psql -U go -d go -v ON_ERROR_STOP=1}
DIR=$(cd "$(dirname "$0")" && pwd)

$PSQL -v tenant_id="$TENANT_ID" -v role="$ROLE" -v password="$PASSWORD" < "$DIR/mcp_tenant_role.sql"
echo "${ROLE} role ready"
