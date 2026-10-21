-- 개발용 시드: mqtt_emulator가 사용하는 센서 101~103 (idempotent)
INSERT INTO tenant (idp_org_id, name)
VALUES ('dev-org', 'dev')
ON CONFLICT (idp_org_id) DO NOTHING;

INSERT INTO sensor (id, tenant_id, facility_id, sensor_type, vendor)
OVERRIDING SYSTEM VALUE
SELECT v.id, t.id, 1, v.sensor_type, 'emulator'
FROM tenant t,
     (VALUES (101, 'tilt'), (102, 'crack'), (103, 'env')) AS v (id, sensor_type)
WHERE t.idp_org_id = 'dev-org'
ON CONFLICT (id) DO NOTHING;
