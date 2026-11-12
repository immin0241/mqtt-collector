-- 000012 되돌리기: 범위 컬럼 제거. 이미 격리된 행(sensor_data_rejected)은 그대로 남는다.
ALTER TABLE metric
    DROP CONSTRAINT metric_range_order,
    DROP COLUMN min_value,
    DROP COLUMN max_value;
