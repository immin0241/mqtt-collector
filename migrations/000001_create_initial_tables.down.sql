-- 이 시점에는 000002~000006의 down이 먼저 적용되어 sensor_data에 tenant_id가 복원되어 있다.
-- timescaledb extension은 다른 DB 객체와 공유될 수 있어 지우지 않는다.
DROP TABLE IF EXISTS sensor_data;
DROP TABLE IF EXISTS sensor;
DROP TABLE IF EXISTS tenant;
