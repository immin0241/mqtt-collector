-- 관제 대시보드용 경보 조회 뷰. alert_rule / alert_event 테이블은 tenant role 에 열지 않고 뷰로만 보여준다. (ADR-0009)
-- 테넌트는 alert_rule.tenant_id 로 정하고, 매핑 함수는 쿼리당 한 번만 평가된다.

CREATE VIEW v_alert_rule WITH (security_barrier = true) AS
SELECT r.id, r.name, r.sensor_id, r.sensor_type, r.metric, r.kind, r.operator, r.threshold, r.window_seconds,
       r.severity, r.enabled, r.created_at
FROM alert_rule r
WHERE r.tenant_id = (SELECT app_tenant_by_role());

CREATE VIEW v_alert_event WITH (security_barrier = true) AS
SELECT e.id, e.rule_id, r.name AS rule_name, r.severity, e.sensor_id, e.seq, e.data_time, e.triggered_at,
       e.created_at, e.value, e.status, e.status_changed_at, e.changed_by
FROM alert_event e
         JOIN alert_rule r ON r.id = e.rule_id
WHERE r.tenant_id = (SELECT app_tenant_by_role());
