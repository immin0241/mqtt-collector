-- 개발용 시드: mqtt_emulator가 사용하는 센서 101~103 (idempotent)
-- ON CONFLICT DO NOTHING 은 충돌해도 identity 시퀀스를 소비해 tenant id 가 건너뛴다. 존재하지 않을 때만 넣는다.
INSERT INTO tenant (idp_org_id, name)
SELECT 'dev-org', 'dev'
WHERE NOT EXISTS (SELECT FROM tenant WHERE name = 'dev');

INSERT INTO sensor (id, tenant_id, facility_id, sensor_type, vendor)
OVERRIDING SYSTEM VALUE
SELECT v.id, t.id, 1, v.sensor_type, 'emulator'
FROM tenant t,
     (VALUES (101, 'tilt'), (102, 'crack'), (103, 'env')) AS v (id, sensor_type)
WHERE t.name = 'dev'
ON CONFLICT (id) DO NOTHING;
