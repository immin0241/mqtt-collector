-- MCP 서비스가 토큰의 조직 id 로 테넌트를 찾을 때만 쓰는 role (멱등). 비밀번호는 psql 변수로 받는다: psql -v mcp_password=...
--   compose `seed` 서비스가 적용한다. (ADR-0014)
-- 이 role 은 tenant (id, idp_org_id) 외에는 아무것도 읽지 못한다. 센서 데이터는 테넌트별 role(mcp_tenant_<id>)이 뷰로만 읽는다.
SELECT format('CREATE ROLE app_mcp LOGIN PASSWORD %L', :'mcp_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'app_mcp')
\gexec

SELECT format('ALTER ROLE app_mcp PASSWORD %L', :'mcp_password')
\gexec

GRANT SELECT (id, idp_org_id) ON tenant TO app_mcp;
