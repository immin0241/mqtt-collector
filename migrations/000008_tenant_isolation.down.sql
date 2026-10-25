-- 000008 되돌리기: sensor_data.tenant_id 제거(000002 up 상태), 매핑 제거, GUC 기반 함수·뷰(000005) 복원

CREATE FUNCTION app_tenant_id() RETURNS INT
    LANGUAGE sql STABLE AS
$$
SELECT NULLIF(current_setting('app.tenant_id', true), '')::INT
$$;

CREATE OR REPLACE VIEW v_sensor WITH (security_barrier = true) AS
SELECT s.id, s.facility_id, s.sensor_type, s.vendor, s.expected_interval_ms
FROM sensor s
WHERE s.tenant_id = app_tenant_id();

CREATE OR REPLACE VIEW v_sensor_status WITH (security_barrier = true) AS
SELECT st.sensor_id, st.status, st.last_seen_at, st.last_seq, st.status_changed_at
FROM sensor_status st
         JOIN sensor s ON s.id = st.sensor_id
WHERE s.tenant_id = app_tenant_id();

CREATE OR REPLACE VIEW v_sensor_event WITH (security_barrier = true) AS
SELECT e.time, e.sensor_id, e.event_type, e.source, e.message
FROM sensor_event e
         JOIN sensor s ON s.id = e.sensor_id
WHERE s.tenant_id = app_tenant_id();

CREATE OR REPLACE VIEW v_sensor_data WITH (security_barrier = true) AS
SELECT d.time, d.sensor_id, d.seq, d.tilt_x, d.tilt_y, d.crack_width, d.temperature, d.received_at
FROM sensor_data d
         JOIN sensor s ON s.id = d.sensor_id
WHERE s.tenant_id = app_tenant_id();

CREATE OR REPLACE VIEW v_sensor_data_1m WITH (security_barrier = true) AS
SELECT a.*, s.expected_interval_ms
FROM sensor_data_1m a
         JOIN sensor s ON s.id = a.sensor_id
WHERE s.tenant_id = app_tenant_id();

DROP TRIGGER IF EXISTS sensor_data_set_tenant ON sensor_data;
DROP FUNCTION IF EXISTS sensor_data_set_tenant();

DROP INDEX IF EXISTS sensor_data_tenant_id_sensor_id_time_idx;
ALTER TABLE sensor_data DROP CONSTRAINT sensor_data_sensor_id_tenant_id_fkey;
ALTER TABLE sensor_data DROP COLUMN tenant_id;
ALTER TABLE sensor_data ADD FOREIGN KEY (sensor_id) REFERENCES sensor (id);
CREATE INDEX ON sensor_data (sensor_id, time DESC);

DROP FUNCTION IF EXISTS app_tenant_by_role();
DROP TABLE IF EXISTS tenant_role;
