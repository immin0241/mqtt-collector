//go:build experiment

package experiments

import (
	"context"
	"sort"
	"testing"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/immin0241/webframework-project-mqtt-collector/internal/testenv"
	"github.com/immin0241/webframework-project-mqtt-collector/workers"
)

// GormStore.InsertRows 단독 처리량(수신 경로·브로커 제외). 배치 하나는 메시지 batch 건이고,
// 메시지마다 측정값이 metrics 개라 DB 행은 batch × metrics 개다.
//
//	go test -tags experiment -run StoreThroughput -v ./test/experiments
func TestStoreThroughput(t *testing.T) {
	testenv.RequireDocker(t)
	batches := envInt("EXP_INSERT_BATCHES", 200)
	const batch = 300
	ctx := context.Background()

	pg := testenv.StartPostgres(t)
	pg.SeedSensors(t, 1, 2, 3, 4, 5)
	db, err := gorm.Open(postgres.Open(pg.DSN), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	store, err := workers.NewGormStore(ctx, db)
	if err != nil {
		t.Fatal(err)
	}

	f := func(v float64) *float64 { return &v }
	for _, metrics := range []int{1, 2, 4} {
		var seq int64
		lat := make([]time.Duration, 0, batches)
		start := time.Now()
		for b := 0; b < batches; b++ {
			rows := make([]workers.SensorData, batch)
			now := time.Now()
			for i := range rows {
				seq++
				d := workers.SensorData{Time: now.Add(time.Duration(seq) * time.Microsecond), SensorID: 1 + int(seq)%5, Seq: seq + int64(metrics)*1_000_000, ReceivedAt: now}
				d.TiltX = f(0.1)
				if metrics >= 2 {
					d.TiltY = f(0.2)
				}
				if metrics >= 3 {
					d.CrackWidth = f(1.0)
					d.Temperature = f(20)
				}
				rows[i] = d
			}
			s := time.Now()
			if _, err := store.InsertRows(ctx, rows); err != nil {
				t.Fatal(err)
			}
			lat = append(lat, time.Since(s))
		}
		total := time.Since(start)
		sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
		msgs := batches * batch
		t.Logf("metrics/msg=%d batch=%d batches=%d: %.0f msg/s, %.0f rows/s, batch commit p50=%s p95=%s",
			metrics, batch, batches, float64(msgs)/total.Seconds(), float64(msgs*metrics)/total.Seconds(),
			lat[len(lat)/2], lat[len(lat)*95/100])
	}
}
