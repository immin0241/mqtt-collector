DROP INDEX IF EXISTS sensor_data_tenant_id_sensor_id_time_idx;

ALTER TABLE sensor_data DROP CONSTRAINT sensor_data_sensor_id_tenant_id_fkey;
ALTER TABLE sensor_data DROP COLUMN tenant_id;
ALTER TABLE sensor_data ADD FOREIGN KEY (sensor_id) REFERENCES sensor (id);

CREATE INDEX ON sensor_data (sensor_id, time DESC);
