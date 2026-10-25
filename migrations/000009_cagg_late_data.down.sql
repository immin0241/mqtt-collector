SELECT remove_continuous_aggregate_policy('sensor_data_1m');

SELECT add_continuous_aggregate_policy('sensor_data_1m',
                                       start_offset => INTERVAL '1 hour',
                                       end_offset => INTERVAL '1 minute',
                                       schedule_interval => INTERVAL '1 minute');
