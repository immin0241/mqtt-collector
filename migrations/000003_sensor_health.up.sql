-- IDP가 Logto로 변경되어 컬럼명을 일반화
ALTER TABLE tenant RENAME COLUMN zitadel_org_id TO idp_org_id;

-- health checker의 타임아웃 기준이 되는 기대 발행 주기
ALTER TABLE sensor
    ADD COLUMN expected_interval_ms INT NOT NULL DEFAULT 1000 CHECK (expected_interval_ms > 0);

-- 센서별 현재 상태 (센서당 1행)
CREATE TABLE sensor_status
(
    sensor_id         INT         NOT NULL PRIMARY KEY REFERENCES sensor (id),
    status            TEXT        NOT NULL CHECK (status IN ('unknown', 'online', 'offline')),
    last_seen_at      TIMESTAMPTZ,
    last_seq          BIGINT,
    status_changed_at TIMESTAMPTZ NOT NULL
);

-- 접속/단절 전이 이력 (Grafana 텍스트 로그, 상태 타임라인)
CREATE TABLE sensor_event
(
    time       TIMESTAMPTZ NOT NULL,
    sensor_id  INT         NOT NULL REFERENCES sensor (id),
    event_type TEXT        NOT NULL CHECK (event_type IN ('connected', 'disconnected', 'timeout', 'recovered')),
    source     TEXT        NOT NULL CHECK (source IN ('lwt', 'timeout', 'data')),
    message    TEXT        NOT NULL
);

SELECT create_hypertable('sensor_event', by_range('time', INTERVAL '7 days'));

CREATE INDEX ON sensor_event (sensor_id, time DESC);
