-- 세션 GUC(app.tenant_id)에서 tenant를 읽는다. 미설정이면 NULL -> 어떤 행도 보이지 않는다(fail closed).
CREATE FUNCTION app_tenant_id() RETURNS INT
    LANGUAGE sql STABLE AS
$$
SELECT NULLIF(current_setting('app.tenant_id', true), '')::INT
$$;

CREATE VIEW v_sensor WITH (security_barrier = true) AS
SELECT s.id, s.facility_id, s.sensor_type, s.vendor, s.expected_interval_ms
FROM sensor s
WHERE s.tenant_id = app_tenant_id();

CREATE VIEW v_sensor_status WITH (security_barrier = true) AS
SELECT st.sensor_id, st.status, st.last_seen_at, st.last_seq, st.status_changed_at
FROM sensor_status st
         JOIN sensor s ON s.id = st.sensor_id
WHERE s.tenant_id = app_tenant_id();

CREATE VIEW v_sensor_event WITH (security_barrier = true) AS
SELECT e.time, e.sensor_id, e.event_type, e.source, e.message
FROM sensor_event e
         JOIN sensor s ON s.id = e.sensor_id
WHERE s.tenant_id = app_tenant_id();

CREATE VIEW v_sensor_data WITH (security_barrier = true) AS
SELECT d.time, d.sensor_id, d.seq, d.tilt_x, d.tilt_y, d.crack_width, d.temperature, d.received_at
FROM sensor_data d
         JOIN sensor s ON s.id = d.sensor_id
WHERE s.tenant_id = app_tenant_id();

CREATE VIEW v_sensor_data_1m WITH (security_barrier = true) AS
SELECT a.*, s.expected_interval_ms
FROM sensor_data_1m a
         JOIN sensor s ON s.id = a.sensor_id
WHERE s.tenant_id = app_tenant_id();
