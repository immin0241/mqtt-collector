-- 격리 테이블: 파싱 실패 또는 영구 오류(데이터 오류, 제약 위반)로 sensor_data에 넣지 못한 메시지
CREATE TABLE sensor_data_rejected
(
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    received_at   TIMESTAMPTZ NOT NULL,                -- MQTT 수신 핸들러에서 찍은 시각
    rejected_at   TIMESTAMPTZ NOT NULL DEFAULT now(),  -- 격리 테이블에 기록된 시각
    topic         TEXT        NOT NULL,
    payload       BYTEA       NOT NULL,                -- 원문 그대로 (유효하지 않은 UTF-8일 수 있어 BYTEA)
    sensor_id     INT,                                 -- 파싱된 경우만. FK 없음 (존재하지 않는 센서도 격리 대상)
    seq           BIGINT,
    error_code    TEXT        NOT NULL,                -- SQLSTATE 또는 PARSE_ERROR
    error_message TEXT        NOT NULL
);

CREATE INDEX ON sensor_data_rejected (received_at DESC);
