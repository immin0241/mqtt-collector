-- 늦게 도착한 데이터(TC-15)도 1분 집계에 반영되도록 refresh 범위를 넓힌다.
-- 000004 는 start_offset 1시간이라 1시간보다 늦게 도착한 행은 집계에 영영 반영되지 않았다.
-- 3일은 원본 압축 정책(3일)과 맞춘 값이다. refresh 는 무효화된(변경된) 버킷만 다시 계산하므로 범위를 넓혀도 비용은 늘지 않는다.
SELECT remove_continuous_aggregate_policy('sensor_data_1m');

SELECT add_continuous_aggregate_policy('sensor_data_1m',
                                       start_offset => INTERVAL '3 days',
                                       end_offset => INTERVAL '1 minute',
                                       schedule_interval => INTERVAL '1 minute');
