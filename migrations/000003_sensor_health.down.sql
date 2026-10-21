DROP TABLE IF EXISTS sensor_event;
DROP TABLE IF EXISTS sensor_status;
ALTER TABLE sensor DROP COLUMN IF EXISTS expected_interval_ms;
ALTER TABLE tenant RENAME COLUMN idp_org_id TO zitadel_org_id;
