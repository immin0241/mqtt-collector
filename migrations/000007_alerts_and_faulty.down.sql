DROP TABLE IF EXISTS alert_event;
DROP TABLE IF EXISTS alert_rule;
DROP TABLE IF EXISTS ingest_event;

-- 새 값을 쓰는 행은 되돌릴 수 없으므로 제약을 복원하기 전에 정리한다.
DELETE FROM sensor_event WHERE event_type IN ('fault', 'fault_cleared');
UPDATE sensor_status SET status = 'online' WHERE status = 'faulty';

ALTER TABLE sensor_event DROP CONSTRAINT sensor_event_event_type_check;
ALTER TABLE sensor_event
    ADD CONSTRAINT sensor_event_event_type_check
        CHECK (event_type IN ('connected', 'disconnected', 'timeout', 'recovered'));

ALTER TABLE sensor_status DROP CONSTRAINT sensor_status_status_check;
ALTER TABLE sensor_status
    ADD CONSTRAINT sensor_status_status_check CHECK (status IN ('unknown', 'online', 'offline'));
