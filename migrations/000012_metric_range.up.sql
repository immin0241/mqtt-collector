-- ADR-0013: 지표별 정상 범위를 metric 카탈로그에 둔다. 수집 경로가 범위 밖 값을 격리(sensor_data_rejected)하고,
-- 센서 상태 판정(범위 밖 반복 → faulty)도 같은 값을 읽는다. NULL 이면 그쪽은 무제한이다.
ALTER TABLE metric
    ADD COLUMN min_value DOUBLE PRECISION,
    ADD COLUMN max_value DOUBLE PRECISION,
    ADD CONSTRAINT metric_range_order CHECK (min_value IS NULL OR max_value IS NULL OR min_value <= max_value);

COMMENT ON COLUMN metric.min_value IS '정상 최솟값 (단위는 unit). 이보다 작은 값은 수집 시 격리된다. NULL = 무제한';
COMMENT ON COLUMN metric.max_value IS '정상 최댓값 (단위는 unit). 이보다 큰 값은 수집 시 격리된다. NULL = 무제한';

-- 계측기 일반 사양 기준 기본값. workers.DefaultRanges 와 같아야 한다 (통합 테스트가 비교한다).
UPDATE metric SET min_value = -30, max_value = 30 WHERE name IN ('tilt_x', 'tilt_y');
UPDATE metric SET min_value = 0,   max_value = 50 WHERE name = 'crack_width';
UPDATE metric SET min_value = -40, max_value = 85 WHERE name = 'temperature';
