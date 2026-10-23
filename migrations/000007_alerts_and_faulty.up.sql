-- 센서 상태에 faulty(장애: flatline, 범위 밖 반복)를 추가한다. 통신 오류(offline)와 구분된다.
ALTER TABLE sensor_status DROP CONSTRAINT sensor_status_status_check;
ALTER TABLE sensor_status
    ADD CONSTRAINT sensor_status_status_check CHECK (status IN ('unknown', 'online', 'offline', 'faulty'));

ALTER TABLE sensor_event DROP CONSTRAINT sensor_event_event_type_check;
ALTER TABLE sensor_event
    ADD CONSTRAINT sensor_event_event_type_check
        CHECK (event_type IN ('connected', 'disconnected', 'timeout', 'recovered', 'fault', 'fault_cleared'));

-- 수집 경로 자체의 장애 (센서별 판정을 멈추고 한 건으로 기록한다)
CREATE TABLE ingest_event
(
    time       TIMESTAMPTZ NOT NULL DEFAULT now(),
    event_type TEXT        NOT NULL CHECK (event_type IN ('stalled', 'resumed', 'broker_down', 'broker_up')),
    message    TEXT        NOT NULL
);

CREATE INDEX ON ingest_event (time DESC);

-- 경보 규칙. 대상은 센서 1개(sensor_id) 또는 테넌트의 센서 타입 전체(sensor_type) 중 하나다.
CREATE TABLE alert_rule
(
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id      INT              NOT NULL REFERENCES tenant (id),
    name           TEXT             NOT NULL,
    sensor_id      INT,
    sensor_type    TEXT CHECK (sensor_type IN ('tilt', 'crack', 'env')),
    metric         TEXT             NOT NULL CHECK (metric IN ('tilt_x', 'tilt_y', 'crack_width', 'temperature')),
    kind           TEXT             NOT NULL CHECK (kind IN ('threshold', 'rate_of_change')),
    -- threshold: 값 > 임계치(gt) 또는 값 < 임계치(lt)
    -- rate_of_change: window_seconds 동안의 변화량 절대값 > 임계치 (operator 는 쓰지 않는다)
    operator       TEXT CHECK (operator IN ('gt', 'lt')),
    threshold      DOUBLE PRECISION NOT NULL,
    window_seconds INT CHECK (window_seconds > 0),
    severity       TEXT             NOT NULL DEFAULT 'warning' CHECK (severity IN ('info', 'warning', 'critical')),
    enabled        BOOLEAN          NOT NULL DEFAULT true,
    created_at     TIMESTAMPTZ      NOT NULL DEFAULT now(),
    FOREIGN KEY (sensor_id, tenant_id) REFERENCES sensor (id, tenant_id),
    CHECK ((sensor_id IS NOT NULL) <> (sensor_type IS NOT NULL)),
    CHECK ((kind = 'threshold' AND operator IS NOT NULL AND window_seconds IS NULL)
        OR (kind = 'rate_of_change' AND operator IS NULL AND window_seconds IS NOT NULL))
);

CREATE INDEX ON alert_rule (tenant_id) WHERE enabled;

-- 경보 이벤트. 같은 이벤트가 재전달돼도 (rule_id, sensor_id, seq) 로 한 번만 생성된다.
CREATE TABLE alert_event
(
    id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    rule_id           BIGINT           NOT NULL REFERENCES alert_rule (id),
    sensor_id         INT              NOT NULL REFERENCES sensor (id),
    seq               BIGINT           NOT NULL,
    data_time         TIMESTAMPTZ      NOT NULL, -- 센서 측정 시각
    triggered_at      TIMESTAMPTZ      NOT NULL, -- 수신 핸들러가 규칙을 평가한 시각 (received_at)
    created_at        TIMESTAMPTZ      NOT NULL DEFAULT now(), -- DB 기록 시각. created_at - triggered_at = 경보 지연
    value             DOUBLE PRECISION NOT NULL,
    status            TEXT             NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'in_progress', 'resolved')),
    status_changed_at TIMESTAMPTZ      NOT NULL DEFAULT now(),
    changed_by        TEXT,
    UNIQUE (rule_id, sensor_id, seq)
);

CREATE INDEX ON alert_event (status, created_at DESC);
CREATE INDEX ON alert_event (sensor_id, created_at DESC);
