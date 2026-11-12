package report

import (
	"testing"
	"time"
)

// scripts/provision_mcp_tenant.sh 는 openssl 로 같은 값을 계산한다. 이 벡터는 다음 명령의 출력이다:
//
//	printf '%s' mcp_tenant_7 | openssl dgst -sha256 -hmac test-mcp-role-secret -hex
//
// 둘이 어긋나면 서비스가 role 에 접속하지 못한다.
func TestRolePasswordMatchesProvisioningScript(t *testing.T) {
	const want = "8ef4c96265c2ff72480f49a7747b7fd4b9d7113645b211972d7cbcef56823a06"
	if got := RolePassword("test-mcp-role-secret", 7); got != want {
		t.Fatalf("RolePassword = %s, want %s", got, want)
	}
	if RolePassword("a", 1) == RolePassword("b", 1) || RolePassword("a", 1) == RolePassword("a", 2) {
		t.Fatal("password must depend on both secret and tenant")
	}
	if RoleName(12) != "mcp_tenant_12" {
		t.Fatalf("RoleName = %s", RoleName(12))
	}
}

func TestParseDate(t *testing.T) {
	seoul, _ := time.LoadLocation("Asia/Seoul")
	now := time.Date(2026, 11, 13, 1, 30, 0, 0, seoul) // 서울 기준 11월 13일 새벽

	t.Run("default is yesterday in the report time zone", func(t *testing.T) {
		w, err := ParseDate("", seoul, now)
		if err != nil {
			t.Fatal(err)
		}
		if w.Date != "2026-11-12" || !w.Start.Equal(time.Date(2026, 11, 12, 0, 0, 0, 0, seoul)) || w.End.Sub(w.Start) != 24*time.Hour {
			t.Fatalf("window = %+v", w)
		}
		if w.Timezone != "Asia/Seoul" {
			t.Fatalf("timezone = %s", w.Timezone)
		}
	})
	t.Run("today is allowed", func(t *testing.T) {
		if _, err := ParseDate("2026-11-13", seoul, now); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("rejects", func(t *testing.T) {
		for _, s := range []string{"yesterday", "2026-13-01", "2026-11-14", "1999-12-31", "2026/11/12", "2026-11-12; DROP TABLE x"} {
			if _, err := ParseDate(s, seoul, now); err == nil {
				t.Errorf("%q should be rejected", s)
			}
		}
	})
	t.Run("day boundary follows the report time zone", func(t *testing.T) {
		// 서울 1월 1일 00:00 = UTC 12월 31일 15:00. 하루 구간은 서울 기준이어야 한다.
		w, err := ParseDate("2026-01-01", seoul, time.Date(2026, 1, 2, 0, 0, 0, 0, seoul))
		if err != nil {
			t.Fatal(err)
		}
		if got := w.Start.UTC(); !got.Equal(time.Date(2025, 12, 31, 15, 0, 0, 0, time.UTC)) {
			t.Fatalf("start (UTC) = %v", got)
		}
	})
}
