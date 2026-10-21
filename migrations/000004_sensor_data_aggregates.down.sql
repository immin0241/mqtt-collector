SELECT remove_compression_policy('sensor_data', if_exists => true);
ALTER TABLE sensor_data SET (timescaledb.compress = false);

SELECT remove_continuous_aggregate_policy('sensor_data_1m', if_exists => true);
DROP MATERIALIZED VIEW IF EXISTS sensor_data_1m;
