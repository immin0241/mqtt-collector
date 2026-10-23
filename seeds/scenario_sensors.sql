-- 시나리오 실행기(mqtt_emulator/run_scenario.py)용 센서 201~210 (idempotent)
INSERT INTO tenant (idp_org_id, name)
VALUES ('dev-org', 'dev')
ON CONFLICT (idp_org_id) DO NOTHING;

INSERT INTO sensor (id, tenant_id, facility_id, sensor_type, vendor)
OVERRIDING SYSTEM VALUE
SELECT 200 + g, t.id, 1, (ARRAY['tilt', 'crack', 'env'])[1 + (g % 3)], 'emulator'
FROM tenant t,
     generate_series(1, 10) AS g
WHERE t.idp_org_id = 'dev-org'
ON CONFLICT (id) DO NOTHING;
