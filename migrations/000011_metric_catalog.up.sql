-- ADR-0012: 측정값을 가로 컬럼(tilt_x, tilt_y, crack_width, temperature)에서 세로 구조(metric, value)로 바꾼다.
--   - 단위와 이름은 metric 카탈로그에 둔다. sensor_data 는 metric_id 만 저장하고 단위는 뷰에서 join 으로 붙인다.
--   - 중복 판정 키는 (sensor_id, seq, metric_id, time) 이다. 메시지 1개가 측정 행 1~2개로 펼쳐진다.
--   - 사용자 결정으로 기존 측정 데이터(sensor_data, 1분 집계, 격리 테이블)는 폐기한다. 되돌려도 복구되지 않는다.
--   - tenant, sensor, 경보 규칙·이력, 센서 상태 이력은 유지한다.

DROP VIEW v_sensor_data_1m;
DROP VIEW v_sensor_data;
DROP MATERIALIZED VIEW sensor_data_1m;
DROP TABLE sensor_data;
TRUNCATE sensor_data_rejected;

CREATE TABLE metric
(
    id   SMALLINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    unit TEXT NOT NULL
);

INSERT INTO metric (name, unit)
VALUES ('tilt_x', '°'),
       ('tilt_y', '°'),
       ('crack_width', 'mm'),
       ('temperature', '°C');

CREATE TABLE sensor_data
(
    time        TIMESTAMPTZ      NOT NULL,                      -- 센서 측정 시각
    tenant_id   INT              NOT NULL,                      -- 트리거가 sensor 에서 채운다 (ADR-0009)
    sensor_id   INT              NOT NULL,
    seq         BIGINT           NOT NULL,                      -- 센서별 시퀀스, 유실 대조용. 한 메시지의 측정 행은 seq 를 공유한다
    metric_id   SMALLINT         NOT NULL REFERENCES metric (id),
    value       DOUBLE PRECISION NOT NULL,
    received_at TIMESTAMPTZ      NOT NULL,                      -- 워커가 MQTT로 받은 시각, 지연 측정용
    FOREIGN KEY (sensor_id, tenant_id) REFERENCES sensor (id, tenant_id),
    UNIQUE (sensor_id, seq, metric_id, time)                    -- 중복 판정 키, 시간 컬럼 포함 필수
);

CREATE INDEX ON sensor_data (tenant_id, sensor_id, time DESC);

SELECT create_hypertable('sensor_data', by_range('time', INTERVAL '1 day'));

CREATE TRIGGER sensor_data_set_tenant
    BEFORE INSERT
    ON sensor_data
    FOR EACH ROW
EXECUTE FUNCTION sensor_data_set_tenant();

-- 3일 지난 원본 chunk 압축
ALTER TABLE sensor_data
    SET (timescaledb.compress,
        timescaledb.compress_segmentby = 'sensor_id, metric_id',
        timescaledb.compress_orderby = 'time DESC, seq DESC');

SELECT add_compression_policy('sensor_data', INTERVAL '3 days');

-- 1분 단위 집계. sample_count 는 (센서, 지표)별 측정 수다.
CREATE MATERIALIZED VIEW sensor_data_1m
    WITH (timescaledb.continuous) AS
SELECT time_bucket('1 minute', time) AS bucket,
       sensor_id,
       metric_id,
       avg(value)                    AS value_avg,
       min(value)                    AS value_min,
       max(value)                    AS value_max,
       count(*)                      AS sample_count,
       min(seq)                      AS seq_min,
       max(seq)                      AS seq_max
FROM sensor_data
GROUP BY bucket, sensor_id, metric_id
WITH NO DATA;

SELECT add_continuous_aggregate_policy('sensor_data_1m',
                                       start_offset => INTERVAL '3 days',
                                       end_offset => INTERVAL '1 minute',
                                       schedule_interval => INTERVAL '1 minute');

-- 뷰: 단위는 카탈로그에서 join 한다. tenant role 은 metric 테이블을 직접 읽을 수 없고 뷰 소유자 권한으로만 읽힌다.
CREATE VIEW v_sensor_data WITH (security_barrier = true) AS
SELECT d.time, d.sensor_id, d.seq, m.name AS metric, m.unit, d.value, d.received_at
FROM sensor_data d
         JOIN metric m ON m.id = d.metric_id
WHERE d.tenant_id = (SELECT app_tenant_by_role());

CREATE VIEW v_sensor_data_1m WITH (security_barrier = true) AS
SELECT a.bucket, a.sensor_id, m.name AS metric, m.unit,
       a.value_avg, a.value_min, a.value_max, a.sample_count, a.seq_min, a.seq_max, s.expected_interval_ms
FROM sensor_data_1m a
         JOIN sensor s ON s.id = a.sensor_id
         JOIN metric m ON m.id = a.metric_id
WHERE s.tenant_id = (SELECT app_tenant_by_role());

-- 이미 만들어진 role 은 새 뷰·테이블 권한을 다시 받아야 한다. (테이블·뷰를 새로 만들면 기존 GRANT 가 사라진다)
-- 아직 role 이 없는 새 DB 에서는 건너뛰고, seeds 와 scripts/provision_tenant.sh 가 부여한다.
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
            GRANT SELECT ON metric TO app_worker;
            GRANT INSERT ON sensor_data TO app_worker;
        END IF;
    END
$$;
