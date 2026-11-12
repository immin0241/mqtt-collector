-- 000011 되돌리기: 가로 컬럼 sensor_data(000010 시점의 구조)를 빈 테이블로 복원한다.
-- 세로 구조에 있던 측정 데이터는 복구되지 않는다. (up 에서 기존 데이터를 폐기했듯 되돌릴 때도 변환하지 않는다)

DROP VIEW v_sensor_data_1m;
DROP VIEW v_sensor_data;
DROP MATERIALIZED VIEW sensor_data_1m;
DROP TABLE sensor_data;
TRUNCATE sensor_data_rejected;
DROP TABLE metric;

CREATE TABLE sensor_data
(
    time        TIMESTAMPTZ      NOT NULL,
    tenant_id   INT              NOT NULL,
    sensor_id   INT              NOT NULL,
    seq         BIGINT           NOT NULL,
    tilt_x      DOUBLE PRECISION,
    tilt_y      DOUBLE PRECISION,
    crack_width DOUBLE PRECISION,
    temperature DOUBLE PRECISION,
    received_at TIMESTAMPTZ      NOT NULL,
    FOREIGN KEY (sensor_id, tenant_id) REFERENCES sensor (id, tenant_id),
    UNIQUE (sensor_id, seq, time)
);

CREATE INDEX ON sensor_data (tenant_id, sensor_id, time DESC);

SELECT create_hypertable('sensor_data', by_range('time', INTERVAL '1 day'));

CREATE TRIGGER sensor_data_set_tenant
    BEFORE INSERT
    ON sensor_data
    FOR EACH ROW
EXECUTE FUNCTION sensor_data_set_tenant();

ALTER TABLE sensor_data
    SET (timescaledb.compress,
        timescaledb.compress_segmentby = 'sensor_id',
        timescaledb.compress_orderby = 'time DESC, seq DESC');

SELECT add_compression_policy('sensor_data', INTERVAL '3 days');

CREATE MATERIALIZED VIEW sensor_data_1m
    WITH (timescaledb.continuous) AS
SELECT time_bucket('1 minute', time) AS bucket,
       sensor_id,
       avg(tilt_x)                   AS tilt_x_avg,
       min(tilt_x)                   AS tilt_x_min,
       max(tilt_x)                   AS tilt_x_max,
       avg(tilt_y)                   AS tilt_y_avg,
       min(tilt_y)                   AS tilt_y_min,
       max(tilt_y)                   AS tilt_y_max,
       avg(crack_width)              AS crack_width_avg,
       min(crack_width)              AS crack_width_min,
       max(crack_width)              AS crack_width_max,
       avg(temperature)              AS temperature_avg,
       min(temperature)              AS temperature_min,
       max(temperature)              AS temperature_max,
       count(*)                      AS sample_count,
       min(seq)                      AS seq_min,
       max(seq)                      AS seq_max
FROM sensor_data
GROUP BY bucket, sensor_id
WITH NO DATA;

SELECT add_continuous_aggregate_policy('sensor_data_1m',
                                       start_offset => INTERVAL '3 days',
                                       end_offset => INTERVAL '1 minute',
                                       schedule_interval => INTERVAL '1 minute');

CREATE VIEW v_sensor_data WITH (security_barrier = true) AS
SELECT d.time, d.sensor_id, d.seq, d.tilt_x, d.tilt_y, d.crack_width, d.temperature, d.received_at
FROM sensor_data d
WHERE d.tenant_id = (SELECT app_tenant_by_role());

CREATE VIEW v_sensor_data_1m WITH (security_barrier = true) AS
SELECT a.*, s.expected_interval_ms
FROM sensor_data_1m a
         JOIN sensor s ON s.id = a.sensor_id
WHERE s.tenant_id = (SELECT app_tenant_by_role());

DO
$$
    DECLARE
        r RECORD;
    BEGIN
        FOR r IN SELECT role_name FROM tenant_role WHERE EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name)
            LOOP
                EXECUTE format('GRANT SELECT ON v_sensor_data, v_sensor_data_1m TO %I', r.role_name);
            END LOOP;
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_worker') THEN
            GRANT INSERT ON sensor_data TO app_worker;
        END IF;
    END
$$;
