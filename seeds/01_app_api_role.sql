-- API 전용 DB role (멱등). 비밀번호는 psql 변수로 받는다: psql -v api_password=...
--   compose `seed` 서비스가 적용한다. (ADR-0010)
-- 이 role 은 테넌트를 구분하지 못한다. 테넌트 격리는 API 코드가 모든 쿼리에서 tenant_id 로 거르는 데 달려 있으므로
-- 권한은 API 가 하는 일에 필요한 만큼만 준다. 센서 데이터와 수집 상태 테이블은 읽지도 못한다.
SELECT format('CREATE ROLE app_api LOGIN PASSWORD %L', :'api_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'app_api')
\gexec

SELECT format('ALTER ROLE app_api PASSWORD %L', :'api_password')
\gexec

GRANT SELECT ON tenant TO app_api;
GRANT SELECT, INSERT, UPDATE ON alert_rule TO app_api;
GRANT SELECT ON alert_event TO app_api;
-- 경보 상태 전이에 쓰는 컬럼만 수정할 수 있다.
GRANT UPDATE (status, status_changed_at, changed_by) ON alert_event TO app_api;
