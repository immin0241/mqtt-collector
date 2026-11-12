-- 워커 전용 DB role (멱등). 비밀번호는 psql 변수로 받는다: psql -v worker_password=...
--   compose `seed` 서비스와 scripts/provision_roles.sh 가 같은 파일을 쓴다.
SELECT format('CREATE ROLE app_worker LOGIN PASSWORD %L', :'worker_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'app_worker')
\gexec

SELECT format('ALTER ROLE app_worker PASSWORD %L', :'worker_password')
\gexec

GRANT SELECT ON sensor TO app_worker;
-- 수집기가 테넌트별 MQTT 세션을 만들려고 tenant.id 만 읽는다. (ADR-0011)
GRANT SELECT (id) ON tenant TO app_worker;
GRANT SELECT, INSERT, UPDATE ON sensor_status TO app_worker;
GRANT INSERT ON sensor_event TO app_worker;
GRANT INSERT ON sensor_data TO app_worker;
-- INSERT ... RETURNING sensor_id, seq, time (새로 저장된 메시지 수 계산)에 필요한 최소 권한. 값 컬럼은 읽지 못한다.
GRANT SELECT (sensor_id, seq, time) ON sensor_data TO app_worker;
GRANT SELECT ON metric TO app_worker;
GRANT INSERT ON sensor_data_rejected TO app_worker;
GRANT SELECT ON alert_rule TO app_worker;
GRANT SELECT, INSERT, UPDATE ON alert_event TO app_worker;
GRANT INSERT ON ingest_event TO app_worker;
