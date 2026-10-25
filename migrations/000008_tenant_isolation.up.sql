-- ADR-0009: 테넌트 격리 강화
--   1) 테넌트를 세션 GUC(app.tenant_id, role 이 SET 으로 바꿀 수 있음)가 아니라 session_user -> tenant_role 매핑으로 정한다.
--   2) sensor_data.tenant_id 를 다시 도입한다 (복합 FK + (tenant_id, sensor_id, time DESC) 인덱스). 값은 트리거가 채운다.

-- ---- 1. role -> tenant 매핑 ----
CREATE TABLE tenant_role
(
    role_name TEXT PRIMARY KEY,
    tenant_id INT NOT NULL REFERENCES tenant (id)
);

-- 기존 scripts/provision_tenant.sh 로 만든 role(grafana_tenant_<id>)을 매핑으로 옮긴다.
INSERT INTO tenant_role (role_name, tenant_id)
SELECT r.rolname, substring(r.rolname FROM '^grafana_tenant_([0-9]+)$')::INT
FROM pg_roles r
         JOIN tenant t ON t.id = substring(r.rolname FROM '^grafana_tenant_([0-9]+)$')::INT
WHERE r.rolname ~ '^grafana_tenant_[0-9]+$'
ON CONFLICT (role_name) DO NOTHING;

-- session_user 기준이므로 SET ROLE 로도 바꿀 수 없다. SECURITY DEFINER 라 tenant role 에 tenant_role 권한을 주지 않는다.
-- 매핑이 없는 role 은 NULL -> 어떤 행도 보이지 않는다(fail closed).
-- 뷰 안의 함수 호출은 호출자 권한으로 EXECUTE 를 검사하므로 PUBLIC 실행을 막지 않는다. 호출자 자신의 tenant id 만 돌려준다.
CREATE FUNCTION app_tenant_by_role() RETURNS INT
    LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS
$$
SELECT tenant_id FROM tenant_role WHERE role_name = session_user
$$;

-- ---- 2. sensor_data.tenant_id ----
DROP INDEX IF EXISTS sensor_data_sensor_id_time_idx;
ALTER TABLE sensor_data DROP CONSTRAINT sensor_data_sensor_id_fkey;

ALTER TABLE sensor_data ADD COLUMN tenant_id INT;
UPDATE sensor_data d SET tenant_id = s.tenant_id FROM sensor s WHERE s.id = d.sensor_id;
ALTER TABLE sensor_data ALTER COLUMN tenant_id SET NOT NULL;
ALTER TABLE sensor_data ADD FOREIGN KEY (sensor_id, tenant_id) REFERENCES sensor (id, tenant_id);

CREATE INDEX ON sensor_data (tenant_id, sensor_id, time DESC);

-- 수집기는 payload 에 tenant 가 없으므로 sensor 에서 채운다. 없는 센서는 예전 단일 FK 와 같은 오류(23503)를 내
-- 영구 오류로 분류돼 격리 테이블로 간다. 호출자가 준 tenant_id 는 무시한다.
CREATE FUNCTION sensor_data_set_tenant() RETURNS trigger
    LANGUAGE plpgsql AS
$$
BEGIN
    SELECT s.tenant_id INTO NEW.tenant_id FROM sensor s WHERE s.id = NEW.sensor_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'sensor % does not exist', NEW.sensor_id USING ERRCODE = 'foreign_key_violation';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER sensor_data_set_tenant
    BEFORE INSERT
    ON sensor_data
    FOR EACH ROW
EXECUTE FUNCTION sensor_data_set_tenant();

-- ---- 3. 뷰: 새 매핑 함수 사용. (SELECT fn()) 형태는 쿼리당 한 번만 평가된다. ----
CREATE OR REPLACE VIEW v_sensor WITH (security_barrier = true) AS
SELECT s.id, s.facility_id, s.sensor_type, s.vendor, s.expected_interval_ms
FROM sensor s
WHERE s.tenant_id = (SELECT app_tenant_by_role());

CREATE OR REPLACE VIEW v_sensor_status WITH (security_barrier = true) AS
SELECT st.sensor_id, st.status, st.last_seen_at, st.last_seq, st.status_changed_at
FROM sensor_status st
         JOIN sensor s ON s.id = st.sensor_id
WHERE s.tenant_id = (SELECT app_tenant_by_role());

CREATE OR REPLACE VIEW v_sensor_event WITH (security_barrier = true) AS
SELECT e.time, e.sensor_id, e.event_type, e.source, e.message
FROM sensor_event e
         JOIN sensor s ON s.id = e.sensor_id
WHERE s.tenant_id = (SELECT app_tenant_by_role());

-- 조인 없이 tenant_id 로 바로 거른다.
CREATE OR REPLACE VIEW v_sensor_data WITH (security_barrier = true) AS
SELECT d.time, d.sensor_id, d.seq, d.tilt_x, d.tilt_y, d.crack_width, d.temperature, d.received_at
FROM sensor_data d
WHERE d.tenant_id = (SELECT app_tenant_by_role());

-- 연속 집계(sensor_data_1m)에는 tenant_id 가 없어 sensor 조인을 유지한다.
CREATE OR REPLACE VIEW v_sensor_data_1m WITH (security_barrier = true) AS
SELECT a.*, s.expected_interval_ms
FROM sensor_data_1m a
         JOIN sensor s ON s.id = a.sensor_id
WHERE s.tenant_id = (SELECT app_tenant_by_role());

-- 더는 쓰지 않는 GUC 기반 함수
DROP FUNCTION app_tenant_id();
