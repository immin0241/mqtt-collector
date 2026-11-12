package reconcile

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// Snapshot은 received_at 구간에서 DB가 가진 (sensor_id, seq) 현황이다.
type Snapshot struct {
	Stored   SeqCounts // sensor_data
	Rejected SeqCounts // sensor_data_rejected 중 sensor_id/seq를 아는 행
	// RejectedUnattributed는 sensor_id/seq를 알 수 없는 격리 행 수(파싱 실패 등)다.
	RejectedUnattributed int
}

// LoadSnapshot은 [since, until) 구간의 received_at을 가진 행을 읽는다. DB는 읽기만 한다.
func LoadSnapshot(ctx context.Context, conn *pgx.Conn, since, until time.Time) (Snapshot, error) {
	var snap Snapshot
	var err error
	snap.Stored, err = queryCounts(ctx, conn,
		// 한 메시지는 지표별로 여러 행이 되지만 time은 같다. time이 다른 같은 (sensor_id, seq)만 중복으로 센다.
		`SELECT sensor_id, seq, count(DISTINCT time) FROM sensor_data
		  WHERE received_at >= $1 AND received_at < $2 GROUP BY sensor_id, seq`, since, until)
	if err != nil {
		return snap, err
	}
	snap.Rejected, err = queryCounts(ctx, conn,
		`SELECT sensor_id, seq, count(*) FROM sensor_data_rejected
		  WHERE sensor_id IS NOT NULL AND seq IS NOT NULL AND received_at >= $1 AND received_at < $2
		  GROUP BY sensor_id, seq`, since, until)
	if err != nil {
		return snap, err
	}
	err = conn.QueryRow(ctx,
		`SELECT count(*) FROM sensor_data_rejected
		  WHERE (sensor_id IS NULL OR seq IS NULL) AND received_at >= $1 AND received_at < $2`,
		since, until).Scan(&snap.RejectedUnattributed)
	return snap, err
}

func queryCounts(ctx context.Context, conn *pgx.Conn, sql string, since, until time.Time) (SeqCounts, error) {
	rows, err := conn.Query(ctx, sql, since, until)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := SeqCounts{}
	for rows.Next() {
		var id int
		var seq, n int64
		if err := rows.Scan(&id, &seq, &n); err != nil {
			return nil, err
		}
		if out[id] == nil {
			out[id] = map[int64]int{}
		}
		out[id][seq] = int(n)
	}
	return out, rows.Err()
}

// FilterSensors는 keep에 있는 센서만 남긴다. keep이 nil이면 그대로 돌려준다.
func FilterSensors(c SeqCounts, keep map[int]bool) SeqCounts {
	if keep == nil || c == nil {
		return c
	}
	out := SeqCounts{}
	for id, m := range c {
		if keep[id] {
			out[id] = m
		}
	}
	return out
}
