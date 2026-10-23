-- 워커 전용 DB role (멱등). 비밀번호는 psql 변수로 받는다: psql -v worker_password=...
--   compose `seed` 서비스와 scripts/provision_roles.sh 가 같은 파일을 쓴다.
SELECT format('CREATE ROLE app_worker LOGIN PASSWORD %L', :'worker_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'app_worker')
\gexec

SELECT format('ALTER ROLE app_worker PASSWORD %L', :'worker_password')
\gexec

GRANT SELECT ON sensor TO app_worker;
GRANT SELECT, INSERT, UPDATE ON sensor_status TO app_worker;
GRANT INSERT ON sensor_event TO app_worker;
GRANT INSERT ON sensor_data TO app_worker;
GRANT INSERT ON sensor_data_rejected TO app_worker;
GRANT SELECT ON alert_rule TO app_worker;
GRANT SELECT, INSERT, UPDATE ON alert_event TO app_worker;
GRANT INSERT ON ingest_event TO app_worker;
