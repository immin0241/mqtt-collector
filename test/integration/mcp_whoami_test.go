package integration

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/immin0241/webframework-project-mqtt-collector/internal/report"
)

// whoami 는 검증된 토큰이 가리키는 사용자·테넌트를 그대로 보여 준다. 테넌트 id, 매칭된 조직, 사용자 id, scope, 만료 시각,
// 이 테넌트 role 에 보이는 센서 수(org-a 는 센서 2개, org-b 는 1개)를 낸다. 다른 테넌트의 값은 나오지 않는다.
func TestMCPWhoami(t *testing.T) {
	m := newMCPFixture(t)

	type who struct {
		TenantID       int       `json:"tenant_id"`
		OrganizationID string    `json:"organization_id"`
		Subject        string    `json:"subject"`
		Scopes         []string  `json:"scopes"`
		ExpiresAt      time.Time `json:"token_expires_at"`
		OrgsInToken    int       `json:"organizations_in_token"`
		SensorCount    int64     `json:"sensor_count"`
	}
	whoami := func(token string) (who, string) {
		t.Helper()
		out, isErr := m.call(t, m.session(t, token), "whoami", nil)
		if isErr {
			t.Fatalf("whoami: %s", out)
		}
		var w who
		if err := json.Unmarshal([]byte(out), &w); err != nil {
			t.Fatalf("decode %q: %v", out, err)
		}
		return w, out
	}

	// 사용자 로그인 토큰: 후보 조직 중 등록되지 않은 것(org-zzz)은 무시하고 org-a 로 매칭된다.
	a, rawA := whoami(m.userToken(t, mcpResource, []string{"org-zzz", "org-a"}, report.DefaultScope))
	if a.OrganizationID != "org-a" || a.Subject != "user-42" || a.SensorCount != 2 || a.OrgsInToken != 2 || a.TenantID == 0 {
		t.Errorf("user token org-a: %+v", a)
	}
	if len(a.Scopes) != 1 || a.Scopes[0] != report.DefaultScope {
		t.Errorf("scopes = %v", a.Scopes)
	}
	if d := time.Until(a.ExpiresAt); d < 30*time.Minute || d > 2*time.Hour {
		t.Errorf("token_expires_at = %s (in %s), want about 1h from now", a.ExpiresAt, d)
	}
	// 다른 테넌트의 조직 id 는 나오지 않는다. 등록되지 않은 후보도 값으로는 나오지 않고 개수에만 반영된다.
	if strings.Contains(rawA, "org-b") || strings.Contains(rawA, "org-zzz") {
		t.Errorf("whoami leaks other organizations: %s", rawA)
	}

	b, _ := whoami(m.userToken(t, mcpResource, []string{"org-b"}, report.DefaultScope))
	if b.OrganizationID != "org-b" || b.SensorCount != 1 || b.TenantID == 0 || b.TenantID == a.TenantID {
		t.Errorf("user token org-b: %+v (org-a tenant %d)", b, a.TenantID)
	}

	// 조직 토큰(M2M)도 같은 방식으로 매칭된다.
	o, _ := whoami(m.token(t, "org-a", report.DefaultScope))
	if o.OrganizationID != "org-a" || o.TenantID != a.TenantID || o.SensorCount != 2 || o.OrgsInToken != 1 {
		t.Errorf("org token: %+v", o)
	}
}
