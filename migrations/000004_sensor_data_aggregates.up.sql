-- 1분 단위 집계 (Grafana 긴 구간 조회 / 데이터 완성도 계산용)
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
                                       start_offset => INTERVAL '1 hour',
                                       end_offset => INTERVAL '1 minute',
                                       schedule_interval => INTERVAL '1 minute');

-- 3일 지난 원본 chunk 압축
ALTER TABLE sensor_data
    SET (timescaledb.compress,
        timescaledb.compress_segmentby = 'sensor_id',
        timescaledb.compress_orderby = 'time DESC, seq DESC');

SELECT add_compression_policy('sensor_data', INTERVAL '3 days');
