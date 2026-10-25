// Package api 는 경보 규칙 등록과 경보 상태 변경용 HTTP API 다. (ADR-0010)
//
// 수집 바이너리와 분리된 프로세스로 돌아 수집 경로의 장애 경계를 건드리지 않는다.
// 인증은 IdP 가 발급한 JWT 를 JWKS 로 검증하고, 테넌트는 토큰의 조직 id 로 정한다.
// DB 접속은 API 전용 role 하나를 쓰므로 테넌트 격리는 이 패키지의 모든 쿼리가 tenant_id 로 거른다는 데 달려 있다.
package api

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/gorm"

	"github.com/immin0241/webframework-project-mqtt-collector/workers"
)

//go:embed openapi.yaml
var openapiSpec []byte

// Scopes는 권한에 대응하는 토큰 scope 이름이다. 관리자는 규칙을, 운영자는 경보 상태를 바꾼다.
type Scopes struct {
	RulesWrite       string
	AlertsTransition string
}

var DefaultScopes = Scopes{RulesWrite: "alert-rules:write", AlertsTransition: "alerts:transition"}

type Server struct {
	DB       *gorm.DB
	Verifier TokenVerifier
	Scopes   Scopes
	Log      *slog.Logger
}

// Handler는 라우팅이 끝난 http.Handler 다.
func (s *Server) Handler() http.Handler {
	if s.Log == nil {
		s.Log = slog.Default()
	}
	if s.Scopes == (Scopes{}) {
		s.Scopes = DefaultScopes
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /openapi.yaml", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/yaml")
		_, _ = w.Write(openapiSpec)
	})
	mux.HandleFunc("GET /docs", serveDocs)

	mux.Handle("GET /v1/alert-rules", s.auth("", s.listRules))
	mux.Handle("POST /v1/alert-rules", s.auth(s.Scopes.RulesWrite, s.createRule))
	mux.Handle("PATCH /v1/alert-rules/{id}", s.auth(s.Scopes.RulesWrite, s.patchRule))
	mux.Handle("GET /v1/alerts", s.auth("", s.listAlerts))
	mux.Handle("POST /v1/alerts/{id}/transition", s.auth(s.Scopes.AlertsTransition, s.transitionAlert))
	return mux
}

type principal struct {
	Claims   *Claims
	TenantID int
}

type authedHandler func(w http.ResponseWriter, r *http.Request, p principal)

// auth는 토큰을 검증하고, 조직 id 로 테넌트를 찾고, 필요한 scope 가 있는지 본다.
// 조직이 어떤 테넌트에도 대응하지 않으면 403 이다. (존재 여부를 알려 주지 않으려고 401 과 구분만 한다)
func (s *Server) auth(requiredScope string, h authedHandler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tok, ok := bearer(r)
		if !ok {
			w.Header().Set("WWW-Authenticate", `Bearer`)
			writeError(w, http.StatusUnauthorized, "missing bearer token")
			return
		}
		claims, err := s.Verifier.Verify(tok)
		if err != nil {
			s.Log.Info("token rejected", "error", err)
			w.Header().Set("WWW-Authenticate", `Bearer error="invalid_token"`)
			writeError(w, http.StatusUnauthorized, "invalid token")
			return
		}
		var tenantID int
		err = s.DB.WithContext(r.Context()).Raw(`SELECT id FROM tenant WHERE idp_org_id = ?`, claims.OrgID).Scan(&tenantID).Error
		if err != nil {
			s.Log.Error("tenant lookup failed", "error", err)
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}
		if tenantID == 0 {
			writeError(w, http.StatusForbidden, "organization is not registered")
			return
		}
		if requiredScope != "" && !claims.Scopes[requiredScope] {
			writeError(w, http.StatusForbidden, "missing scope "+requiredScope)
			return
		}
		h(w, r, principal{Claims: claims, TenantID: tenantID})
	})
}

// ---- 규칙 ----

type ruleRow struct {
	ID            int64   `json:"id"`
	Name          string  `json:"name"`
	SensorID      *int    `json:"sensor_id,omitempty"`
	SensorType    *string `json:"sensor_type,omitempty"`
	Metric        string  `json:"metric"`
	Kind          string  `json:"kind"`
	Operator      *string `json:"operator,omitempty"`
	Threshold     float64 `json:"threshold"`
	WindowSeconds *int    `json:"window_seconds,omitempty"`
	Severity      string  `json:"severity"`
	Enabled       bool    `json:"enabled"`
}

const ruleColumns = `id, name, sensor_id, sensor_type, metric, kind, operator, threshold, window_seconds, severity, enabled`

func (s *Server) listRules(w http.ResponseWriter, r *http.Request, p principal) {
	var rows []ruleRow
	err := s.DB.WithContext(r.Context()).Raw(
		`SELECT `+ruleColumns+` FROM alert_rule WHERE tenant_id = ? ORDER BY id`, p.TenantID).Scan(&rows).Error
	if err != nil {
		s.fail(w, err)
		return
	}
	if rows == nil {
		rows = []ruleRow{}
	}
	writeJSON(w, http.StatusOK, rows)
}

type createRuleReq struct {
	Name          string  `json:"name"`
	SensorID      *int    `json:"sensor_id"`
	SensorType    *string `json:"sensor_type"`
	Metric        string  `json:"metric"`
	Kind          string  `json:"kind"`
	Operator      *string `json:"operator"`
	Threshold     float64 `json:"threshold"`
	WindowSeconds *int    `json:"window_seconds"`
	Severity      string  `json:"severity"`
}

func (s *Server) createRule(w http.ResponseWriter, r *http.Request, p principal) {
	var req createRuleReq
	if !decode(w, r, &req) {
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusUnprocessableEntity, "name is required")
		return
	}
	if req.Severity == "" {
		req.Severity = "warning"
	}
	// 나머지 형식(허용 값, 센서·타입 택일, kind 별 필드)은 alert_rule 의 CHECK 와 FK 가 판정한다.
	// sensor_id 는 (sensor_id, tenant_id) 복합 FK 라서 다른 테넌트의 센서를 지정하면 거부된다.
	var row ruleRow
	err := s.DB.WithContext(r.Context()).Raw(
		`INSERT INTO alert_rule (tenant_id, name, sensor_id, sensor_type, metric, kind, operator, threshold, window_seconds, severity)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING `+ruleColumns,
		p.TenantID, req.Name, req.SensorID, req.SensorType, req.Metric, req.Kind, req.Operator, req.Threshold, req.WindowSeconds, req.Severity,
	).Scan(&row).Error
	if err != nil {
		s.fail(w, err)
		return
	}
	s.Log.Info("alert rule created", "rule_id", row.ID, "tenant_id", p.TenantID, "by", p.Claims.Subject)
	writeJSON(w, http.StatusCreated, row)
}

type patchRuleReq struct {
	Name      *string  `json:"name"`
	Threshold *float64 `json:"threshold"`
	Severity  *string  `json:"severity"`
	Enabled   *bool    `json:"enabled"`
}

func (s *Server) patchRule(w http.ResponseWriter, r *http.Request, p principal) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	var req patchRuleReq
	if !decode(w, r, &req) {
		return
	}
	// COALESCE 로 보낸 필드만 바꾼다. kind 등 규칙의 의미를 바꾸는 필드는 새 규칙으로 만들게 한다.
	var rows []ruleRow
	err := s.DB.WithContext(r.Context()).Raw(
		`UPDATE alert_rule SET name = COALESCE(?, name), threshold = COALESCE(?, threshold),
		        severity = COALESCE(?, severity), enabled = COALESCE(?, enabled)
		 WHERE id = ? AND tenant_id = ? RETURNING `+ruleColumns,
		req.Name, req.Threshold, req.Severity, req.Enabled, id, p.TenantID).Scan(&rows).Error
	if err != nil {
		s.fail(w, err)
		return
	}
	if len(rows) == 0 {
		writeError(w, http.StatusNotFound, "rule not found")
		return
	}
	s.Log.Info("alert rule updated", "rule_id", id, "tenant_id", p.TenantID, "by", p.Claims.Subject)
	writeJSON(w, http.StatusOK, rows[0])
}

// ---- 경보 ----

type alertRow struct {
	ID              int64   `json:"id"`
	RuleID          int64   `json:"rule_id"`
	RuleName        string  `json:"rule_name"`
	Severity        string  `json:"severity"`
	SensorID        int     `json:"sensor_id"`
	Seq             int64   `json:"seq"`
	Value           float64 `json:"value"`
	Status          string  `json:"status"`
	CreatedAt       string  `json:"created_at"`
	StatusChangedAt string  `json:"status_changed_at"`
	ChangedBy       *string `json:"changed_by,omitempty"`
}

func (s *Server) listAlerts(w http.ResponseWriter, r *http.Request, p principal) {
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 1000 {
			writeError(w, http.StatusBadRequest, "limit must be 1..1000")
			return
		}
		limit = n
	}
	status := r.URL.Query().Get("status") // 빈 값이면 전체
	var rows []alertRow
	err := s.DB.WithContext(r.Context()).Raw(
		`SELECT e.id, e.rule_id, r.name AS rule_name, r.severity, e.sensor_id, e.seq, e.value, e.status,
		        to_char(e.created_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS created_at,
		        to_char(e.status_changed_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS status_changed_at, e.changed_by
		 FROM alert_event e JOIN alert_rule r ON r.id = e.rule_id
		 WHERE r.tenant_id = ? AND (? = '' OR e.status = ?)
		 ORDER BY e.created_at DESC, e.id DESC LIMIT ?`, p.TenantID, status, status, limit).Scan(&rows).Error
	if err != nil {
		s.fail(w, err)
		return
	}
	if rows == nil {
		rows = []alertRow{}
	}
	writeJSON(w, http.StatusOK, rows)
}

// 허용되는 다음 상태와 그 직전 상태. 상태 전이 규칙의 원본은 workers.TransitionAlert 이고, 여기는 요청의 to 에서 from 을 정할 뿐이다.
var transitionFrom = map[string]string{"in_progress": "open", "resolved": "in_progress"}

func (s *Server) transitionAlert(w http.ResponseWriter, r *http.Request, p principal) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	var req struct {
		To string `json:"to"`
	}
	if !decode(w, r, &req) {
		return
	}
	from, ok := transitionFrom[req.To]
	if !ok {
		writeError(w, http.StatusUnprocessableEntity, `to must be "in_progress" or "resolved"`)
		return
	}

	// 다른 테넌트의 경보는 존재하지 않는 것처럼 404 로 답한다.
	var current string
	res := s.DB.WithContext(r.Context()).Raw(
		`SELECT e.status FROM alert_event e JOIN alert_rule r ON r.id = e.rule_id WHERE e.id = ? AND r.tenant_id = ?`,
		id, p.TenantID).Scan(&current)
	if res.Error != nil {
		s.fail(w, res.Error)
		return
	}
	if res.RowsAffected == 0 {
		writeError(w, http.StatusNotFound, "alert not found")
		return
	}

	changed, err := workers.TransitionAlert(r.Context(), s.DB, id, from, req.To, p.Claims.Subject)
	if err != nil {
		s.fail(w, err)
		return
	}
	if !changed {
		// 이미 다른 요청이 처리했거나 현재 상태가 from 이 아니다.
		var now string
		_ = s.DB.WithContext(r.Context()).Raw(`SELECT status FROM alert_event WHERE id = ?`, id).Scan(&now).Error
		writeJSON(w, http.StatusConflict, map[string]any{
			"error": "alert is not in state " + from, "current_status": now,
		})
		return
	}
	s.Log.Info("alert transitioned", "alert_id", id, "from", from, "to", req.To, "tenant_id", p.TenantID, "by", p.Claims.Subject)
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "status": req.To, "changed_by": p.Claims.Subject})
}

// ---- 공통 ----

// fail은 DB 오류를 HTTP 상태로 바꾼다. 형식·제약 위반(SQLSTATE 22, 23)은 요청 문제이고 나머지는 서버 문제다.
func (s *Server) fail(w http.ResponseWriter, err error) {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && len(pgErr.Code) >= 2 && (pgErr.Code[:2] == "22" || pgErr.Code[:2] == "23") {
		msg := "constraint violation"
		if pgErr.Code == "23503" {
			msg = "referenced sensor does not exist in this organization"
		}
		writeError(w, http.StatusUnprocessableEntity, msg)
		return
	}
	if errors.Is(err, workers.ErrInvalidAlertTransition) {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	if errors.Is(err, context.Canceled) {
		return
	}
	s.Log.Error("request failed", "error", err)
	writeError(w, http.StatusInternalServerError, "internal error")
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return false
	}
	return true
}

func pathID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id < 1 {
		writeError(w, http.StatusBadRequest, "invalid id")
		return 0, false
	}
	return id, true
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

// serveDocs는 Swagger UI 를 보여 준다. 정적 파일은 CDN(버전 고정)에서 불러온다.
func serveDocs(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(`<!doctype html>
<html lang="ko"><head><meta charset="utf-8"><title>StructWatch API</title>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/5.17.14/swagger-ui.min.css"></head>
<body><div id="ui"></div>
<script src="https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/5.17.14/swagger-ui-bundle.min.js"></script>
<script>SwaggerUIBundle({url: "/openapi.yaml", dom_id: "#ui"});</script>
</body></html>`))
}
