-- 시나리오 실행기(mqtt_emulator/run_scenario.py)용 센서 201~210 (idempotent)
-- ON CONFLICT DO NOTHING 은 충돌해도 identity 시퀀스를 소비해 tenant id 가 건너뛴다. 존재하지 않을 때만 넣는다.
INSERT INTO tenant (idp_org_id, name)
SELECT 'dev-org', 'dev'
WHERE NOT EXISTS (SELECT FROM tenant WHERE idp_org_id = 'dev-org');

INSERT INTO sensor (id, tenant_id, facility_id, sensor_type, vendor)
OVERRIDING SYSTEM VALUE
SELECT 200 + g, t.id, 1, (ARRAY['tilt', 'crack', 'env'])[1 + (g % 3)], 'emulator'
FROM tenant t,
     generate_series(1, 10) AS g
WHERE t.idp_org_id = 'dev-org'
ON CONFLICT (id) DO NOTHING;
