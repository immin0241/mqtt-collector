package integration

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/immin0241/webframework-project-mqtt-collector/internal/api"
	"github.com/immin0241/webframework-project-mqtt-collector/internal/testenv"
)

const (
	apiIssuer    = "http://idp.test/oidc"
	apiAudPrefix = "urn:test:organization:"
)

type apiFixture struct {
	h      *harness
	srv    *httptest.Server
	key    *ecdsa.PrivateKey
	tenant map[string]int // idp_org_id -> tenant.id
}

// newAPIFixture는 테넌트 org-a(센서 1, 2), org-b(센서 3)와 app_api role 로 접속하는 API 서버를 만든다.
func newAPIFixture(t *testing.T) *apiFixture {
	t.Helper()
	h := newHarness(t, 1, 2) // SeedSensors 가 만드는 test-org 를 org-a 로 쓴다
	ctx := context.Background()
	conn := h.pg.Conn(t)
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, `UPDATE tenant SET idp_org_id = 'org-a' WHERE idp_org_id = 'test-org'`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO tenant (idp_org_id, name) VALUES ('org-b', 'b')`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO sensor (id, tenant_id, facility_id, sensor_type, vendor)
		OVERRIDING SYSTEM VALUE SELECT 3, t.id, 1, 'crack', 'test' FROM tenant t WHERE t.idp_org_id = 'org-b'`); err != nil {
		t.Fatal(err)
	}
	f := &apiFixture{h: h, tenant: map[string]int{}}
	rows, err := conn.Query(ctx, `SELECT id, idp_org_id FROM tenant`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id int
		var org string
		_ = rows.Scan(&id, &org)
		f.tenant[org] = id
	}
	rows.Close()

	// 실제 시드 파일로 API 전용 role 을 만든다. (psql 의 \gexec 를 쓰므로 컨테이너 안의 psql 로 실행한다)
	seed := filepath.Join(testenv.RepoRoot(), "seeds/01_app_api_role.sql")
	cmd := exec.Command("bash", "-c", fmt.Sprintf(
		"docker exec -i %s psql -U go -d go -v ON_ERROR_STOP=1 -v api_password=api < %s", h.pg.Container.GetContainerID(), seed))
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("apply api role seed: %v\n%s", err, out)
	}

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	f.key = key

	db := openAsAPIRole(t, h)
	verifier := api.NewStaticVerifier(func(*jwt.Token) (any, error) { return &key.PublicKey, nil }, apiIssuer, apiAudPrefix)
	srv := &api.Server{DB: db, Verifier: verifier, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	f.srv = httptest.NewServer(srv.Handler())
	t.Cleanup(f.srv.Close)
	return f
}

func openAsAPIRole(t *testing.T, h *harness) *gorm.DB {
	t.Helper()
	dsn := strings.Replace(h.pg.DSN, "go:go@", "app_api:api@", 1)
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("connect as app_api: %v", err)
	}
	return db
}

type tokenOpt struct {
	org     string
	scope   string
	sub     string
	issuer  string
	expires time.Duration
	method  jwt.SigningMethod
	key     any
}

func (f *apiFixture) token(t *testing.T, o tokenOpt) string {
	t.Helper()
	if o.sub == "" {
		o.sub = "user-1"
	}
	if o.issuer == "" {
		o.issuer = apiIssuer
	}
	if o.expires == 0 {
		o.expires = time.Hour
	}
	if o.method == nil {
		o.method = jwt.SigningMethodES256
		o.key = f.key
	}
	claims := jwt.MapClaims{
		"iss": o.issuer, "sub": o.sub, "aud": apiAudPrefix + o.org, "scope": o.scope,
		"exp": time.Now().Add(o.expires).Unix(), "iat": time.Now().Unix(),
	}
	s, err := jwt.NewWithClaims(o.method, claims).SignedString(o.key)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func (f *apiFixture) adminToken(t *testing.T, org string) string {
	return f.token(t, tokenOpt{org: org, scope: "alert-rules:write", sub: "admin-" + org})
}
func (f *apiFixture) operatorToken(t *testing.T, org string) string {
	return f.token(t, tokenOpt{org: org, scope: "alerts:transition", sub: "operator-" + org})
}

func (f *apiFixture) do(t *testing.T, method, path, token string, body any) (int, map[string]any, []byte) {
	t.Helper()
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, f.srv.URL+path, rd)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	return resp.StatusCode, m, raw
}

func crackRule(sensorID int) map[string]any {
	return map[string]any{"name": "crack>5", "sensor_id": sensorID, "metric": "crack_width", "kind": "threshold", "operator": "gt", "threshold": 5, "severity": "critical"}
}

func TestAPI_Authentication(t *testing.T) {
	f := newAPIFixture(t)
	other, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)

	cases := []struct {
		name  string
		token string
		want  int
	}{
		{"no token", "", 401},
		{"garbage", "not-a-jwt", 401},
		{"signed by another key", f.token(t, tokenOpt{org: "org-a", method: jwt.SigningMethodES256, key: other}), 401},
		{"wrong issuer", f.token(t, tokenOpt{org: "org-a", issuer: "http://evil/oidc"}), 401},
		{"expired", f.token(t, tokenOpt{org: "org-a", expires: -time.Hour}), 401},
		{"HS256", f.token(t, tokenOpt{org: "org-a", method: jwt.SigningMethodHS256, key: []byte("x")}), 401},
		{"alg none", f.token(t, tokenOpt{org: "org-a", method: jwt.SigningMethodNone, key: jwt.UnsafeAllowNoneSignatureType}), 401},
		{"organization not registered", f.adminToken(t, "org-unknown"), 403},
		{"valid", f.adminToken(t, "org-a"), 200},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if code, _, raw := f.do(t, "GET", "/v1/alert-rules", c.token, nil); code != c.want {
				t.Fatalf("status = %d, want %d: %s", code, c.want, raw)
			}
		})
	}
}

func TestAPI_RolesSeparateRuleAdminFromOperator(t *testing.T) {
	f := newAPIFixture(t)
	op := f.operatorToken(t, "org-a")
	admin := f.adminToken(t, "org-a")

	if code, _, _ := f.do(t, "POST", "/v1/alert-rules", op, crackRule(1)); code != 403 {
		t.Errorf("operator creating a rule: %d, want 403", code)
	}
	code, rule, raw := f.do(t, "POST", "/v1/alert-rules", admin, crackRule(1))
	if code != 201 {
		t.Fatalf("admin creating a rule: %d %s", code, raw)
	}
	if code, _, _ := f.do(t, "PATCH", fmt.Sprintf("/v1/alert-rules/%v", rule["id"]), op, map[string]any{"enabled": false}); code != 403 {
		t.Errorf("operator patching a rule: %d, want 403", code)
	}

	alertID := insertAlert(t, f, int64(rule["id"].(float64)), 1, 100, 6.5)
	if code, _, _ := f.do(t, "POST", fmt.Sprintf("/v1/alerts/%d/transition", alertID), admin, map[string]any{"to": "in_progress"}); code != 403 {
		t.Errorf("admin (without operator scope) transitioning: %d, want 403", code)
	}
	// 읽기는 scope 없이 인증만으로 된다.
	reader := f.token(t, tokenOpt{org: "org-a"})
	if code, _, _ := f.do(t, "GET", "/v1/alerts", reader, nil); code != 200 {
		t.Errorf("reader listing alerts: %d, want 200", code)
	}
}

func TestAPI_RuleValidationAndTenantIsolation(t *testing.T) {
	f := newAPIFixture(t)
	adminA := f.adminToken(t, "org-a")
	adminB := f.adminToken(t, "org-b")

	code, ruleA, raw := f.do(t, "POST", "/v1/alert-rules", adminA, crackRule(1))
	if code != 201 {
		t.Fatalf("create: %d %s", code, raw)
	}
	idA := fmt.Sprintf("%v", ruleA["id"])

	t.Run("other organization's sensor is rejected", func(t *testing.T) {
		if code, _, raw := f.do(t, "POST", "/v1/alert-rules", adminA, crackRule(3)); code != 422 {
			t.Fatalf("status = %d, want 422: %s", code, raw)
		}
	})
	t.Run("constraint violations are 422", func(t *testing.T) {
		bad := []map[string]any{
			{"name": "both", "sensor_id": 1, "sensor_type": "crack", "metric": "crack_width", "kind": "threshold", "operator": "gt", "threshold": 1},
			{"name": "no operator", "sensor_id": 1, "metric": "crack_width", "kind": "threshold", "threshold": 1},
			{"name": "bad metric", "sensor_id": 1, "metric": "nope", "kind": "threshold", "operator": "gt", "threshold": 1},
			{"name": "roc without window", "sensor_id": 1, "metric": "crack_width", "kind": "rate_of_change", "threshold": 1},
		}
		for _, b := range bad {
			if code, _, raw := f.do(t, "POST", "/v1/alert-rules", adminA, b); code != 422 {
				t.Errorf("%v: status = %d, want 422: %s", b["name"], code, raw)
			}
		}
	})
	t.Run("a caller cannot name a tenant", func(t *testing.T) {
		if code, _, _ := f.do(t, "POST", "/v1/alert-rules", adminA, map[string]any{"name": "x", "tenant_id": 2}); code != 400 {
			t.Errorf("unknown field tenant_id: %d, want 400", code)
		}
	})
	t.Run("listing shows only own rules", func(t *testing.T) {
		_, _, rawB := f.do(t, "GET", "/v1/alert-rules", adminB, nil)
		if strings.TrimSpace(string(rawB)) != "[]" {
			t.Fatalf("org-b sees %s, want []", rawB)
		}
		_, _, rawA := f.do(t, "GET", "/v1/alert-rules", adminA, nil)
		var listA []map[string]any
		_ = json.Unmarshal(rawA, &listA)
		if len(listA) != 1 || listA[0]["name"] != "crack>5" {
			t.Fatalf("org-a does not see its own rule: %s", rawA)
		}
	})
	t.Run("patching another organization's rule is 404 and changes nothing", func(t *testing.T) {
		if code, _, _ := f.do(t, "PATCH", "/v1/alert-rules/"+idA, adminB, map[string]any{"enabled": false}); code != 404 {
			t.Fatalf("status = %d, want 404", code)
		}
		if n := countRows(t, f.h, `SELECT count(*) FROM alert_rule WHERE id = $1 AND enabled`, ruleA["id"]); n != 1 {
			t.Fatal("rule was changed by another organization")
		}
	})
	t.Run("owner can patch", func(t *testing.T) {
		code, got, raw := f.do(t, "PATCH", "/v1/alert-rules/"+idA, adminA, map[string]any{"threshold": 7.5, "enabled": false})
		if code != 200 || got["threshold"].(float64) != 7.5 || got["enabled"].(bool) {
			t.Fatalf("patch: %d %s", code, raw)
		}
	})
}

func insertAlert(t *testing.T, f *apiFixture, ruleID int64, sensorID int, seq int64, value float64) int64 {
	t.Helper()
	conn := f.h.pg.Conn(t)
	defer conn.Close(context.Background())
	var id int64
	err := conn.QueryRow(context.Background(),
		`INSERT INTO alert_event (rule_id, sensor_id, seq, data_time, triggered_at, value) VALUES ($1, $2, $3, now(), now(), $4) RETURNING id`,
		ruleID, sensorID, seq, value).Scan(&id)
	if err != nil {
		t.Fatalf("insert alert: %v", err)
	}
	return id
}

func TestAPI_AlertTransitions(t *testing.T) {
	f := newAPIFixture(t)
	adminA, adminB := f.adminToken(t, "org-a"), f.adminToken(t, "org-b")
	opA := f.operatorToken(t, "org-a")

	_, ruleA, _ := f.do(t, "POST", "/v1/alert-rules", adminA, crackRule(1))
	_, ruleB, _ := f.do(t, "POST", "/v1/alert-rules", adminB, crackRule(3))
	alertA := insertAlert(t, f, int64(ruleA["id"].(float64)), 1, 1, 6)
	alertB := insertAlert(t, f, int64(ruleB["id"].(float64)), 3, 1, 6)
	path := func(id int64) string { return fmt.Sprintf("/v1/alerts/%d/transition", id) }

	t.Run("listing is scoped to the organization", func(t *testing.T) {
		_, _, rawA := f.do(t, "GET", "/v1/alerts", opA, nil)
		var listA []map[string]any
		_ = json.Unmarshal(rawA, &listA)
		if len(listA) != 1 || int64(listA[0]["id"].(float64)) != alertA {
			t.Fatalf("org-a alerts = %s", rawA)
		}
	})
	t.Run("another organization's alert is 404 and unchanged", func(t *testing.T) {
		if code, _, _ := f.do(t, "POST", path(alertB), opA, map[string]any{"to": "in_progress"}); code != 404 {
			t.Fatalf("status = %d, want 404", code)
		}
		if n := countRows(t, f.h, `SELECT count(*) FROM alert_event WHERE id = $1 AND status = 'open'`, alertB); n != 1 {
			t.Fatal("alert of org-b was changed by org-a")
		}
	})
	t.Run("skipping a state is rejected", func(t *testing.T) {
		code, body, _ := f.do(t, "POST", path(alertA), opA, map[string]any{"to": "resolved"})
		if code != 409 || body["current_status"] != "open" {
			t.Fatalf("open -> resolved: %d %v, want 409 with current_status=open", code, body)
		}
		if code, _, _ := f.do(t, "POST", path(alertA), opA, map[string]any{"to": "open"}); code != 422 {
			t.Fatalf("to=open: %d, want 422", code)
		}
	})
	t.Run("concurrent requests: exactly one wins", func(t *testing.T) {
		var wins, conflicts int32
		var wg sync.WaitGroup
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				code, _, _ := f.do(t, "POST", path(alertA), opA, map[string]any{"to": "in_progress"})
				switch code {
				case 200:
					atomic.AddInt32(&wins, 1)
				case 409:
					atomic.AddInt32(&conflicts, 1)
				}
			}()
		}
		wg.Wait()
		if wins != 1 || conflicts != 19 {
			t.Fatalf("wins=%d conflicts=%d, want 1 and 19", wins, conflicts)
		}
	})
	t.Run("full flow records who changed it", func(t *testing.T) {
		if code, _, raw := f.do(t, "POST", path(alertA), opA, map[string]any{"to": "resolved"}); code != 200 {
			t.Fatalf("in_progress -> resolved: %d %s", code, raw)
		}
		var by string
		conn := f.h.pg.Conn(t)
		defer conn.Close(context.Background())
		_ = conn.QueryRow(context.Background(), `SELECT changed_by FROM alert_event WHERE id = $1`, alertA).Scan(&by)
		if by != "operator-org-a" {
			t.Fatalf("changed_by = %q, want the token subject", by)
		}
	})
}

// API 전용 DB role 은 센서 데이터를 읽거나 경보 이력을 지우지 못한다. 앱 코드의 tenant_id 필터가 뚫려도 피해 범위를 줄인다.
func TestAPI_DBRoleIsLeastPrivilege(t *testing.T) {
	f := newAPIFixture(t)
	db := openAsAPIRole(t, f.h)
	for _, q := range []string{
		`SELECT count(*) FROM sensor_data`,
		`SELECT count(*) FROM sensor_status`,
		`DELETE FROM alert_event`,
		`DELETE FROM alert_rule`,
		`UPDATE alert_event SET value = 0`,
		`INSERT INTO alert_event (rule_id, sensor_id, seq, data_time, triggered_at, value) VALUES (1, 1, 1, now(), now(), 1)`,
		`UPDATE tenant SET name = 'x'`,
	} {
		if err := db.Exec(q).Error; err == nil {
			t.Errorf("app_api must not be able to run: %s", q)
		}
	}
}

// 끝에서 끝까지: API 로 만든 규칙이 수집 경로에서 평가되어 경보가 생기고, API 로 그 경보를 처리한다.
func TestAPI_RuleCreatedViaAPIFiresAlertAndCanBeHandled(t *testing.T) {
	f := newAPIFixture(t)
	f.h.startAppWith(t, "it-api-e2e", fastHealth)
	f.h.waitForSubscribe(t)
	admin, op := f.adminToken(t, "org-a"), f.operatorToken(t, "org-a")

	code, rule, raw := f.do(t, "POST", "/v1/alert-rules", admin, crackRule(1))
	if code != 201 {
		t.Fatalf("create rule: %d %s", code, raw)
	}
	time.Sleep(1500 * time.Millisecond) // 규칙 로더 갱신(500ms) 대기

	pub := testenv.NewPublisher(t, f.h.mq.Addr(t), "it-api-e2e-pub")
	for i, crack := range []float64{1, 2, 6.5, 2} {
		if err := pub.PublishRaw(testenv.Topic(1), reading(1, int64(i), crack)); err != nil {
			t.Fatal(err)
		}
	}

	var alertID int64
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		_, _, raw := f.do(t, "GET", "/v1/alerts?status=open", op, nil)
		var list []map[string]any
		_ = json.Unmarshal(raw, &list)
		if len(list) == 1 {
			alertID = int64(list[0]["id"].(float64))
			if list[0]["rule_id"].(float64) != rule["id"].(float64) || list[0]["value"].(float64) != 6.5 {
				t.Fatalf("unexpected alert: %v", list[0])
			}
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	if alertID == 0 {
		t.Fatal("no alert appeared for a rule created through the API")
	}
	for _, to := range []string{"in_progress", "resolved"} {
		if code, _, raw := f.do(t, "POST", fmt.Sprintf("/v1/alerts/%d/transition", alertID), op, map[string]any{"to": to}); code != 200 {
			t.Fatalf("-> %s: %d %s", to, code, raw)
		}
	}
}
