-- 시연용 시드 (멱등): 테넌트 B·C, 센서 9개(테넌트당 tilt/crack/env), 테넌트별 경보 규칙.
-- 테넌트 A(dev) 센서 101~103 은 seeds/dev_sensors.sql 이 만든다.
-- idp_org_id 는 자리표시자다. Logto 조직 id 를 만든 뒤 docs/demo-runbook.md 의 UPDATE 로 바꾼다.
-- ON CONFLICT DO NOTHING 은 identity 시퀀스를 소비하므로 존재하지 않을 때만 넣는다.
INSERT INTO tenant (idp_org_id, name)
SELECT v.org, v.name
FROM (VALUES ('demo-org-b', 'demo-b'), ('demo-org-c', 'demo-c')) AS v (org, name)
WHERE NOT EXISTS (SELECT FROM tenant t WHERE t.name = v.name);

INSERT INTO sensor (id, tenant_id, facility_id, sensor_type, vendor)
OVERRIDING SYSTEM VALUE
SELECT v.id, t.id, v.facility_id, v.sensor_type, 'emulator'
FROM (VALUES (111, 'demo-b', 2, 'tilt'), (112, 'demo-b', 2, 'crack'), (113, 'demo-b', 2, 'env'),
             (121, 'demo-c', 3, 'tilt'), (122, 'demo-c', 3, 'crack'), (123, 'demo-c', 3, 'env')
     ) AS v (id, tenant, facility_id, sensor_type)
         JOIN tenant t ON t.name = v.tenant
ON CONFLICT (id) DO NOTHING;

-- 테넌트마다 같은 규칙 3개. 에뮬레이터 제어 화면의 "급증" 프리셋(crack_base 6.5, tilt_x_base 8, base_temp 45)이 넘기는 값이다.
-- 정상 운전 값은 균열 0.5~2.5mm, 기울기 ±0.3°, 온도 약 0~30°C 라서 평소에는 울리지 않는다.
INSERT INTO alert_rule (tenant_id, name, sensor_type, metric, kind, operator, threshold, severity)
SELECT t.id, r.name, r.sensor_type, r.metric, 'threshold', 'gt', r.threshold, r.severity
FROM tenant t,
     (VALUES ('균열 폭 5mm 초과', 'crack', 'crack_width', 5.0, 'critical'),
             ('기울기 X 5° 초과', 'tilt', 'tilt_x', 5.0, 'warning'),
             ('온도 40°C 초과', 'env', 'temperature', 40.0, 'warning')) AS r (name, sensor_type, metric, threshold, severity)
WHERE t.name IN ('dev', 'demo-b', 'demo-c')
  AND NOT EXISTS (SELECT FROM alert_rule a WHERE a.tenant_id = t.id AND a.name = r.name);
