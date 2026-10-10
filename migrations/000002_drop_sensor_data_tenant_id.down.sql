DROP INDEX IF EXISTS sensor_data_sensor_id_time_idx;

ALTER TABLE sensor_data DROP CONSTRAINT sensor_data_sensor_id_fkey;
ALTER TABLE sensor_data ADD COLUMN tenant_id INT;
UPDATE sensor_data d SET tenant_id = s.tenant_id FROM sensor s WHERE s.id = d.sensor_id;
ALTER TABLE sensor_data ALTER COLUMN tenant_id SET NOT NULL;
ALTER TABLE sensor_data ADD FOREIGN KEY (sensor_id, tenant_id) REFERENCES sensor (id, tenant_id);

CREATE INDEX ON sensor_data (tenant_id, sensor_id, time DESC);
