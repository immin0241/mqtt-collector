package workers

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func validStore() *GormStore {
	return &GormStore{ranges: DefaultRanges}
}

func sqlState(t *testing.T, err error) string {
	t.Helper()
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("want *pgconn.PgError, got %T: %v", err, err)
	}
	return pgErr.Code
}

func TestValidateRangeAndTime(t *testing.T) {
	now := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)
	base := SensorData{SensorID: 1, Seq: 1, Time: now, ReceivedAt: now}
	st := validStore()

	cases := []struct {
		name string
		mod  func(d *SensorData)
		want string // "" = 정상
	}{
		{"ok", func(d *SensorData) { d.TiltX = f(1.5); d.CrackWidth = f(2) }, ""},
		{"range edge is ok", func(d *SensorData) { d.TiltX = f(30); d.CrackWidth = f(0) }, ""},
		{"tilt too high", func(d *SensorData) { d.TiltX = f(45) }, OutOfRangeCode},
		{"tilt too low", func(d *SensorData) { d.TiltY = f(-45) }, OutOfRangeCode},
		{"crack negative", func(d *SensorData) { d.CrackWidth = f(-0.1) }, OutOfRangeCode},
		{"temperature too high", func(d *SensorData) { d.Temperature = f(9999) }, OutOfRangeCode},
		{"one bad value rejects the whole message", func(d *SensorData) { d.TiltX = f(1); d.TiltY = f(99) }, OutOfRangeCode},
		{"zero time", func(d *SensorData) { d.TiltX = f(1); d.Time = time.Time{} }, BadTimeCode},
		{"epoch time", func(d *SensorData) { d.TiltX = f(1); d.Time = time.Unix(0, 0).UTC() }, BadTimeCode},
		{"far future", func(d *SensorData) { d.TiltX = f(1); d.Time = now.Add(48 * time.Hour) }, BadTimeCode},
		{"late data is fine", func(d *SensorData) { d.TiltX = f(1); d.Time = now.Add(-30 * 24 * time.Hour) }, ""},
		{"slight clock skew is fine", func(d *SensorData) { d.TiltX = f(1); d.Time = now.Add(2 * time.Hour) }, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := base
			c.mod(&d)
			err := st.validate(d, d.Measurements())
			if c.want == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("want SQLSTATE %s, got nil", c.want)
			}
			if got := sqlState(t, err); got != c.want {
				t.Fatalf("SQLSTATE = %s, want %s (%v)", got, c.want, err)
			}
			if ClassifyDBError(err) != KindPermanent {
				t.Fatalf("validation error must be permanent (a retry loop would never end): %v", err)
			}
		})
	}
}

// 카탈로그의 NULL(무제한)은 어떤 값도 통과시킨다.
func TestValidateUnboundedRange(t *testing.T) {
	st := &GormStore{ranges: map[string]PhysicalRange{"tilt_x": {Min: math.Inf(-1), Max: math.Inf(1)}}}
	now := time.Now()
	d := SensorData{SensorID: 1, Seq: 1, Time: now, ReceivedAt: now, TiltX: f(1e9)}
	if err := st.validate(d, d.Measurements()); err != nil {
		t.Fatalf("unbounded range rejected a value: %v", err)
	}
}

// 검증은 DB에 가기 전에 끝난다. DB가 없어도(nil) 범위 밖 메시지는 영구 오류로 돌아와야 한다.
func TestInsertRowsRejectsInvalidBeforeTouchingDB(t *testing.T) {
	now := time.Now()
	st := validStore() // DB 가 nil 이라 검증을 통과하면 패닉한다
	_, err := st.InsertRows(context.Background(), []SensorData{{SensorID: 1, Seq: 1, Time: now, ReceivedAt: now, TiltX: f(45)}})
	if got := sqlState(t, err); got != OutOfRangeCode {
		t.Fatalf("SQLSTATE = %s, want %s", got, OutOfRangeCode)
	}
}

// Ranges는 복사본이다. 호출자가 바꿔도 저장소의 검증이 달라지지 않는다.
func TestRangesReturnsCopy(t *testing.T) {
	st := validStore()
	r := st.Ranges()
	r["tilt_x"] = PhysicalRange{Min: -1000, Max: 1000}
	now := time.Now()
	d := SensorData{SensorID: 1, Seq: 1, Time: now, ReceivedAt: now, TiltX: f(45)}
	if err := st.validate(d, d.Measurements()); err == nil {
		t.Fatal("mutating the copy changed the store's validation")
	}
}
