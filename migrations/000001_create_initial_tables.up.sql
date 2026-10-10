CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE tenant (
                        id              INT  GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        zitadel_org_id  TEXT NOT NULL UNIQUE,
                        name            TEXT NOT NULL
);

CREATE TABLE sensor (
                        id           INT  GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        tenant_id    INT  NOT NULL REFERENCES tenant (id),
                        facility_id  INT  NOT NULL,               -- 시설물 테이블은 별도로
                        sensor_type  TEXT NOT NULL CHECK (sensor_type IN ('tilt', 'crack', 'env')),
                        vendor       TEXT NOT NULL,
                        UNIQUE (id, tenant_id)                    -- 아래 복합 FK용
);

CREATE TABLE sensor_data (
                             time         TIMESTAMPTZ      NOT NULL,   -- 센서 측정 시각
                             tenant_id    INT              NOT NULL,
                             sensor_id    INT              NOT NULL,
                             seq          BIGINT           NOT NULL,   -- 센서별 시퀀스, 유실 대조용
                             tilt_x       DOUBLE PRECISION,            -- 도(°)
                             tilt_y       DOUBLE PRECISION,            -- 도(°)
                             crack_width  DOUBLE PRECISION,            -- mm
                             temperature  DOUBLE PRECISION,            -- °C
                             received_at  TIMESTAMPTZ      NOT NULL,   -- 워커가 MQTT로 받은 시각, 지연 측정용
                             FOREIGN KEY (sensor_id, tenant_id) REFERENCES sensor (id, tenant_id),
                             UNIQUE (sensor_id, seq, time)             -- 중복 판정 키, 시간 컬럼 포함 필수
);

CREATE INDEX ON sensor_data (tenant_id, sensor_id, time DESC);

SELECT create_hypertable('sensor_data', by_range('time', INTERVAL '1 day'));
