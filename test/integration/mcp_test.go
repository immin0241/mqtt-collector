package integration

import (
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
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/immin0241/webframework-project-mqtt-collector/internal/api"
	"github.com/immin0241/webframework-project-mqtt-collector/internal/report"
	"github.com/immin0241/webframework-project-mqtt-collector/internal/testenv"
)

const mcpSecret = "test-mcp-role-secret"

type mcpFixture struct {
	api *apiFixture
	srv *httptest.Server
}

func newECKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func runPSQLFile(t *testing.T, f *apiFixture, file string, vars ...string) {
	t.Helper()
	args := ""
	for _, v := range vars {
		args += fmt.Sprintf(" -v %s", v)
	}
	cmd := exec.Command("bash", "-c", fmt.Sprintf("docker exec -i %s psql -U go -d go -v ON_ERROR_STOP=1%s < %s",
		f.h.pg.Container.GetContainerID(), args, filepath.Join(testenv.RepoRoot(), file)))
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("psql %s: %v\n%s", file, err, out)
	}
}

// newMCPFixture는 org-a(센서 1, 2)와 org-b(센서 3)에 오늘 날짜의 데이터·경보를 넣고, 실제 프로비저닝 SQL 로 테넌트별 role 과
// app_mcp role 을 만든 뒤 MCP 핸들러를 띄운다.
func newMCPFixture(t *testing.T) *mcpFixture {
	t.Helper()
	f := newAPIFixture(t)
	ctx := context.Background()
	conn := f.h.pg.Conn(t)
	defer conn.Close(ctx)

	// org-a 센서 1·2 는 crack_width 5.0, org-b 센서 3 은 눈에 띄는 값 777 (범위 검증은 수집기 쪽이라 DB 에 직접 넣는다)
	for _, d := range []struct {
		sensor int
		value  float64
	}{{1, 5.0}, {2, 5.0}, {3, 777}} {
		if _, err := conn.Exec(ctx, `
			INSERT INTO sensor_data (time, sensor_id, seq, metric_id, value, received_at)
			SELECT now() - make_interval(secs => g), $1, g, (SELECT id FROM metric WHERE name = 'crack_width'), $2, now()
			FROM generate_series(1, 30) g`, d.sensor, d.value); err != nil {
			t.Fatalf("data: %v", err)
		}
	}
	for _, r := range []struct {
		org    string
		sensor int
		name   string
	}{{"org-a", 1, "a-rule"}, {"org-b", 3, "b-rule"}} {
		if _, err := conn.Exec(ctx, `INSERT INTO alert_rule (tenant_id, name, sensor_id, metric, kind, operator, threshold, severity)
			SELECT id, $2, $3, 'crack_width', 'threshold', 'gt', 3, 'critical' FROM tenant WHERE idp_org_id = $1`, r.org, r.name, r.sensor); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(ctx, `INSERT INTO alert_event (rule_id, sensor_id, seq, data_time, triggered_at, value)
			SELECT id, $2, 1, now(), now(), 9 FROM alert_rule WHERE name = $1`, r.name, r.sensor); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := conn.Exec(ctx, `CALL refresh_continuous_aggregate('sensor_data_1m', NULL, NULL)`); err != nil {
		t.Fatalf("refresh aggregate: %v", err)
	}

	runPSQLFile(t, f, "seeds/02_app_mcp_role.sql", "mcp_password=mcp")
	for _, org := range []string{"org-a", "org-b"} {
		id := f.tenant[org]
		runPSQLFile(t, f, "scripts/mcp_tenant_role.sql",
			fmt.Sprintf("tenant_id=%d", id),
			fmt.Sprintf("role=%s", report.RoleName(id)),
			fmt.Sprintf("password=%s", report.RolePassword(mcpSecret, id)))
	}

	lookupDB, err := gorm.Open(postgres.Open(strings.Replace(f.h.pg.DSN, "go:go@", "app_mcp:mcp@", 1)),
		&gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("connect as app_mcp: %v", err)
	}
	host, port, _ := strings.Cut(f.h.pg.Addr, ":")
	pools := &report.Pools{Host: host, Port: port, DBName: "go", SSLMode: "disable", Secret: mcpSecret, Max: 8}
	t.Cleanup(pools.Close)

	svc := &report.Service{
		Tenants: report.GormTenants{DB: lookupDB},
		DB:      pools,
		Loc:     time.UTC,
		Log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	verifier := api.NewStaticVerifier(func(*jwt.Token) (any, error) { return &f.key.PublicKey, nil }, apiIssuer, apiAudPrefix)
	srv := httptest.NewServer(svc.Handler(verifier, "http://mcp.test/mcp", apiIssuer, "http://mcp.test/.well-known/oauth-protected-resource", report.DefaultScope))
	t.Cleanup(srv.Close)
	return &mcpFixture{api: f, srv: srv}
}

type bearerTransport struct{ token string }

func (b bearerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+b.token)
	return http.DefaultTransport.RoundTrip(r)
}

func (m *mcpFixture) session(t *testing.T, token string) *mcp.ClientSession {
	t.Helper()
	c := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "0"}, nil)
	cs, err := c.Connect(context.Background(), &mcp.StreamableClientTransport{
		Endpoint:             m.srv.URL + "/mcp",
		HTTPClient:           &http.Client{Transport: bearerTransport{token}},
		DisableStandaloneSSE: true,
		MaxRetries:           -1,
	}, nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs
}

func (m *mcpFixture) call(t *testing.T, cs *mcp.ClientSession, tool string, args map[string]any) (string, bool) {
	t.Helper()
	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{Name: tool, Arguments: args})
	if err != nil {
		t.Fatalf("%s: %v", tool, err)
	}
	if res.IsError {
		for _, c := range res.Content {
			if tc, ok := c.(*mcp.TextContent); ok {
				return tc.Text, true
			}
		}
		return "", true
	}
	b, _ := json.Marshal(res.StructuredContent)
	return string(b), false
}

func (m *mcpFixture) token(t *testing.T, org, scope string) string {
	return m.api.token(t, tokenOpt{org: org, scope: scope})
}

func today() string { return time.Now().UTC().Format("2006-01-02") }

// 같은 도구를 org-a, org-b 토큰으로 호출하면 각자 자기 데이터만 보인다. 상대 데이터의 표식(777, b-rule, 센서 3)은 나오지 않는다.
func TestMCPToolsReturnOnlyOwnTenantData(t *testing.T) {
	m := newMCPFixture(t)
	a := m.session(t, m.token(t, "org-a", report.DefaultScope))
	b := m.session(t, m.token(t, "org-b", report.DefaultScope))

	for _, tc := range []struct {
		name string
		sess *mcp.ClientSession
		own  []string
		leak []string
	}{
		{"a", a, []string{"a-rule", `"sensor_id":1`}, []string{"b-rule", "777", `"sensor_id":3`}},
		{"b", b, []string{"b-rule", `"sensor_id":3`}, []string{"a-rule", `"sensor_id":1`, `"sensor_id":2`}},
	} {
		alerts, isErr := m.call(t, tc.sess, "list_alerts", map[string]any{"date": today()})
		if isErr {
			t.Fatalf("%s list_alerts: %s", tc.name, alerts)
		}
		sensors, _ := m.call(t, tc.sess, "get_sensor_status", nil)
		summary, _ := m.call(t, tc.sess, "get_daily_summary", map[string]any{"date": today()})
		all := alerts + sensors + summary
		for _, want := range tc.own {
			if !strings.Contains(all, want) {
				t.Errorf("tenant %s: expected %q in output\n%s", tc.name, want, all)
			}
		}
		for _, bad := range tc.leak {
			if strings.Contains(all, bad) {
				t.Errorf("tenant %s sees other tenant's data %q\n%s", tc.name, bad, all)
			}
		}
	}

	// 요약의 지표 통계도 각자의 값이다: a 는 max 5, b 는 max 777
	sumA, _ := m.call(t, a, "get_daily_summary", map[string]any{"date": today()})
	sumB, _ := m.call(t, b, "get_daily_summary", map[string]any{"date": today()})
	if !strings.Contains(sumA, `"max":5`) || strings.Contains(sumA, "777") {
		t.Errorf("tenant a summary: %s", sumA)
	}
	if !strings.Contains(sumB, "777") {
		t.Errorf("tenant b summary: %s", sumB)
	}
}

// 다른 테넌트의 sensor_id 로 추세를 요청해도 빈 결과다(존재 여부를 알려주지 않는다).
func TestMCPTrendForForeignSensorIsEmpty(t *testing.T) {
	m := newMCPFixture(t)
	a := m.session(t, m.token(t, "org-a", report.DefaultScope))
	own, _ := m.call(t, a, "get_metric_trend", map[string]any{"sensor_id": 1, "metric": "crack_width", "date": today()})
	foreign, isErr := m.call(t, a, "get_metric_trend", map[string]any{"sensor_id": 3, "metric": "crack_width", "date": today()})
	if isErr {
		t.Fatalf("foreign sensor should not error: %s", foreign)
	}
	if !strings.Contains(own, `"hourly":[{`) {
		t.Errorf("own sensor should have points: %s", own)
	}
	if !strings.Contains(foreign, `"hourly":[]`) || strings.Contains(foreign, "777") {
		t.Errorf("foreign sensor leaked or errored: %s", foreign)
	}
}

// 인증: 토큰 없음·잘못된 서명·scope 없음·등록되지 않은 조직·타 발급자 토큰은 도구까지 가지 못한다.
func TestMCPAuthRejections(t *testing.T) {
	m := newMCPFixture(t)
	post := func(token string) (*http.Response, string) {
		req, _ := http.NewRequest(http.MethodPost, m.srv.URL+"/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t","version":"0"}}}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp, string(b)
	}

	other := newECKey(t)
	cases := []struct {
		name  string
		token string
		want  int
	}{
		{"no token", "", http.StatusUnauthorized},
		{"garbage", "not-a-jwt", http.StatusUnauthorized},
		{"wrong signing key", m.api.token(t, tokenOpt{org: "org-a", scope: report.DefaultScope, method: jwt.SigningMethodES256, key: other}), http.StatusUnauthorized},
		{"wrong issuer", m.api.token(t, tokenOpt{org: "org-a", scope: report.DefaultScope, issuer: "http://evil/oidc"}), http.StatusUnauthorized},
		{"expired", m.api.token(t, tokenOpt{org: "org-a", scope: report.DefaultScope, expires: -time.Hour}), http.StatusUnauthorized},
		{"missing scope", m.token(t, "org-a", "alerts:transition"), http.StatusForbidden},
		{"unregistered org", m.token(t, "org-zzz", report.DefaultScope), http.StatusForbidden},
	}
	for _, tc := range cases {
		resp, body := post(tc.token)
		if resp.StatusCode != tc.want {
			t.Errorf("%s: status %d, want %d (%s)", tc.name, resp.StatusCode, tc.want, body)
		}
	}

	resp, _ := post("")
	if got := resp.Header.Get("WWW-Authenticate"); !strings.Contains(got, "resource_metadata=") {
		t.Errorf("401 should point to protected resource metadata, got %q", got)
	}
	meta, err := http.Get(m.srv.URL + "/.well-known/oauth-protected-resource")
	if err != nil {
		t.Fatal(err)
	}
	defer meta.Body.Close()
	var doc map[string]any
	_ = json.NewDecoder(meta.Body).Decode(&doc)
	if doc["resource"] != "http://mcp.test/mcp" || fmt.Sprint(doc["authorization_servers"]) != "["+apiIssuer+"]" {
		t.Errorf("protected resource metadata = %v", doc)
	}
}

// DB 수준 격리: 테넌트 role 로 직접 접속하면 기본 테이블은 못 읽고, 뷰는 자기 테넌트만 보며, 쓰기는 모두 거부된다.
// (도구 코드가 아니라 role 만으로도 격리가 성립하는지를 본다.)
func TestMCPTenantRoleIsReadOnlyAndScoped(t *testing.T) {
	m := newMCPFixture(t)
	ctx := context.Background()
	open := func(org string) *gorm.DB {
		id := m.api.tenant[org]
		dsn := strings.Replace(m.api.h.pg.DSN, "go:go@", fmt.Sprintf("%s:%s@", report.RoleName(id), report.RolePassword(mcpSecret, id)), 1)
		db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
		if err != nil {
			t.Fatalf("connect as %s role: %v", org, err)
		}
		sqlDB, _ := db.DB()
		t.Cleanup(func() { _ = sqlDB.Close() })
		return db
	}
	dbA, dbB := open("org-a"), open("org-b")

	var sensors []int
	if err := dbA.WithContext(ctx).Raw(`SELECT id FROM v_sensor ORDER BY id`).Scan(&sensors).Error; err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(sensors) != "[1 2]" {
		t.Errorf("org-a role v_sensor = %v, want [1 2]", sensors)
	}
	for _, q := range []string{
		`SELECT * FROM sensor_data LIMIT 1`,
		`SELECT * FROM sensor LIMIT 1`,
		`SELECT * FROM alert_event LIMIT 1`,
		`SELECT * FROM tenant LIMIT 1`,
		`SELECT * FROM sensor_data_rejected LIMIT 1`,
		`INSERT INTO sensor_data (time, sensor_id, seq, metric_id, value, received_at) VALUES (now(), 1, 99999, 1, 0, now())`,
		`UPDATE alert_event SET status = 'resolved'`,
		`DELETE FROM alert_rule`,
	} {
		if err := dbA.WithContext(ctx).Exec(q).Error; err == nil {
			t.Errorf("tenant role should be denied: %s", q)
		}
	}
	var sensorsB []int
	if err := dbB.WithContext(ctx).Raw(`SELECT id FROM v_sensor`).Scan(&sensorsB).Error; err != nil || fmt.Sprint(sensorsB) != "[3]" {
		t.Errorf("org-b role sees %v (err %v), want [3]", sensorsB, err)
	}
}

// app_mcp 는 tenant 의 id, idp_org_id 외에는 읽지 못한다.
func TestMCPLookupRoleIsMinimal(t *testing.T) {
	m := newMCPFixture(t)
	db, err := gorm.Open(postgres.Open(strings.Replace(m.api.h.pg.DSN, "go:go@", "app_mcp:mcp@", 1)), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, _ := db.DB()
	defer sqlDB.Close()
	if err := db.Exec(`SELECT id, idp_org_id FROM tenant`).Error; err != nil {
		t.Errorf("lookup should work: %v", err)
	}
	for _, q := range []string{`SELECT name FROM tenant`, `SELECT * FROM v_sensor`, `SELECT * FROM sensor_data LIMIT 1`, `UPDATE tenant SET idp_org_id = 'x'`} {
		if err := db.Exec(q).Error; err == nil {
			t.Errorf("app_mcp should be denied: %s", q)
		}
	}
}

// 입력 검증: 형식이 틀린 날짜·미래 날짜·모르는 지표·잘못된 상태는 도구 오류로 돌려준다(DB 까지 가지 않는다).
func TestMCPToolRejectsBadInput(t *testing.T) {
	m := newMCPFixture(t)
	a := m.session(t, m.token(t, "org-a", report.DefaultScope))
	future := time.Now().UTC().AddDate(0, 0, 2).Format("2006-01-02")
	for _, tc := range []struct {
		tool string
		args map[string]any
	}{
		{"get_daily_summary", map[string]any{"date": "yesterday"}},
		{"get_daily_summary", map[string]any{"date": future}},
		{"get_daily_summary", map[string]any{"date": "1999-12-31"}},
		{"get_metric_trend", map[string]any{"sensor_id": 1, "metric": "'; DROP TABLE sensor_data; --"}},
		{"list_alerts", map[string]any{"status": "closed"}},
	} {
		out, isErr := m.call(t, a, tc.tool, tc.args)
		if !isErr {
			t.Errorf("%s %v should be an error, got %s", tc.tool, tc.args, out)
		}
	}
}
