-- 개발용 Grafana 조회 role (멱등). compose 의 Grafana 기본 org(1) 데이터소스 계정이다.
-- role 이름과 tenant id 는 무관하다. 어느 tenant 를 보는지는 tenant_role 매핑이 정한다. (ADR-0009)
--   비밀번호: psql -v grafana_password=...  (compose seed 서비스가 GRAFANA_DB_PASSWORD 로 전달)
-- 다른 tenant 용 role 은 scripts/provision_tenant.sh <tenant_id> 로 만든다. (GRANT 목록을 같이 맞출 것)

-- ON CONFLICT DO NOTHING 은 identity 시퀀스를 소비하므로 존재하지 않을 때만 넣는다.
INSERT INTO tenant (idp_org_id, name)
SELECT 'dev-org', 'dev'
WHERE NOT EXISTS (SELECT FROM tenant WHERE idp_org_id = 'dev-org');

SELECT format('CREATE ROLE grafana_dev LOGIN PASSWORD %L', :'grafana_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'grafana_dev')
\gexec

SELECT format('ALTER ROLE grafana_dev PASSWORD %L', :'grafana_password')
\gexec

INSERT INTO tenant_role (role_name, tenant_id)
SELECT 'grafana_dev', t.id FROM tenant t WHERE t.idp_org_id = 'dev-org'
ON CONFLICT (role_name) DO UPDATE SET tenant_id = EXCLUDED.tenant_id;

GRANT SELECT ON v_sensor, v_sensor_status, v_sensor_event, v_sensor_data, v_sensor_data_1m, v_alert_rule, v_alert_event TO grafana_dev;
