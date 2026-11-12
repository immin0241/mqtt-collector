-- 테넌트 하나의 MCP 읽기 전용 role 을 만든다 (멱등). psql 변수: tenant_id, role, password. (ADR-0014)
--   scripts/provision_mcp_tenant.sh 와 통합 테스트가 이 파일을 그대로 실행한다.
\set ON_ERROR_STOP on
BEGIN;

SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'role', :'password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = :'role')
\gexec
SELECT format('ALTER ROLE %I PASSWORD %L', :'role', :'password')
\gexec
-- 쓰기 권한이 없으므로 이 설정은 안전망이다. role 이 스스로 풀 수는 있지만 풀어도 쓸 테이블이 없다.
SELECT format('ALTER ROLE %I SET default_transaction_read_only = on', :'role')
\gexec
SELECT format('ALTER ROLE %I SET statement_timeout = %L', :'role', '10s')
\gexec

-- 뷰는 session_user 로 이 매핑을 조회한다(ADR-0009). 매핑이 없는 role 은 어떤 행도 보지 못한다.
INSERT INTO tenant_role (role_name, tenant_id)
VALUES (:'role', :'tenant_id'::int)
ON CONFLICT (role_name) DO UPDATE SET tenant_id = EXCLUDED.tenant_id;

-- 리포트 도구가 읽는 뷰만. 기본 테이블 권한은 주지 않는다.
SELECT format('GRANT SELECT ON v_sensor, v_sensor_status, v_sensor_event, v_sensor_data_1m, v_alert_event TO %I', :'role')
\gexec

COMMIT;
