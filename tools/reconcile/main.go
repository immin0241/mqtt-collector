// reconcile은 발행 로그와 DB를 비교해 누락, 중복, 예상 외 이벤트를 출력한다.
//
//	go run ./tools/reconcile --published run.jsonl --since 2026-10-23T01:00:00Z
//
// 모든 TC 판정에 공통으로 쓴다. 누락, 예상 외, DB 중복이 하나라도 있으면 종료 코드 1을 돌려준다.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

func main() {
	var (
		dsn       = flag.String("dsn", os.Getenv("POSTGRESQL_URL"), "읽기 가능한 Postgres 접속 문자열 (기본: $POSTGRESQL_URL)")
		published = flag.String("published", "", "발행 로그 JSONL 경로. 비우면 저장된 seq의 min~max를 기대로 삼는다(꼬리 유실은 못 찾음)")
		sinceStr  = flag.String("since", "", "received_at 하한 (RFC3339, 포함). 이전 실행 데이터를 제외하는 데 쓴다")
		untilStr  = flag.String("until", "", "received_at 상한 (RFC3339, 미포함). 기본: 현재")
		sensorsF  = flag.String("sensors", "", "대상 센서 id, 쉼표 구분. 비우면 전체")
		asJSON    = flag.Bool("json", false, "JSON으로 출력")
	)
	flag.Parse()

	if *dsn == "" {
		fatal("--dsn 또는 POSTGRESQL_URL 이 필요하다")
	}
	since, until := time.Unix(0, 0).UTC(), time.Now().Add(time.Minute)
	var err error
	if *sinceStr != "" {
		if since, err = time.Parse(time.RFC3339, *sinceStr); err != nil {
			fatal("--since: %v", err)
		}
	}
	if *untilStr != "" {
		if until, err = time.Parse(time.RFC3339, *untilStr); err != nil {
			fatal("--until: %v", err)
		}
	}
	filter, err := parseSensors(*sensorsF)
	if err != nil {
		fatal("--sensors: %v", err)
	}

	var pub SeqCounts
	if *published != "" {
		f, err := os.Open(*published)
		if err != nil {
			fatal("%v", err)
		}
		pub, err = ParsePublished(f)
		f.Close()
		if err != nil {
			fatal("%v", err)
		}
		pub = filterSensors(pub, filter)
	}

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, *dsn)
	if err != nil {
		fatal("db connect: %v", err)
	}
	defer conn.Close(ctx)

	stored, err := queryCounts(ctx, conn,
		`SELECT sensor_id, seq, count(*) FROM sensor_data
		  WHERE received_at >= $1 AND received_at < $2 GROUP BY sensor_id, seq`, since, until)
	if err != nil {
		fatal("query sensor_data: %v", err)
	}
	rejected, err := queryCounts(ctx, conn,
		`SELECT sensor_id, seq, count(*) FROM sensor_data_rejected
		  WHERE sensor_id IS NOT NULL AND seq IS NOT NULL AND received_at >= $1 AND received_at < $2
		  GROUP BY sensor_id, seq`, since, until)
	if err != nil {
		fatal("query sensor_data_rejected: %v", err)
	}
	var unattributed int
	if err := conn.QueryRow(ctx,
		`SELECT count(*) FROM sensor_data_rejected
		  WHERE (sensor_id IS NULL OR seq IS NULL) AND received_at >= $1 AND received_at < $2`,
		since, until).Scan(&unattributed); err != nil {
		fatal("query sensor_data_rejected: %v", err)
	}
	stored, rejected = filterSensors(stored, filter), filterSensors(rejected, filter)

	reports := Compare(pub, stored, rejected)
	ok := true
	for _, r := range reports {
		ok = ok && r.OK()
	}

	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(map[string]any{"ok": ok, "sensors": reports, "rejected_unattributed": unattributed})
	} else {
		printTable(reports, unattributed, *published == "")
	}
	if !ok {
		os.Exit(1)
	}
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

func parseSensors(s string) (map[int]bool, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	out := map[int]bool{}
	for _, part := range strings.Split(s, ",") {
		id, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil {
			return nil, err
		}
		out[id] = true
	}
	return out, nil
}

func filterSensors(c SeqCounts, keep map[int]bool) SeqCounts {
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

func printTable(reports []SensorReport, unattributed int, contiguousMode bool) {
	if contiguousMode {
		fmt.Println("(발행 로그 없음: 저장된 seq의 min~max를 기대로 삼았다. 꼬리 쪽 유실은 확인할 수 없다)")
	}
	fmt.Printf("%-8s %9s %8s %9s %8s %11s %8s %10s\n",
		"sensor", "published", "stored", "rejected", "missing", "unexpected", "db_dup", "pub_dup")
	for _, r := range reports {
		mark := "OK"
		if !r.OK() {
			mark = "FAIL"
		}
		fmt.Printf("%-8d %9d %8d %9d %8d %11d %8d %10d  %s\n",
			r.SensorID, r.Published, r.Stored, r.Rejected, r.Missing, r.Unexpected,
			r.DBDuplicates, r.PublishedDuplicates, mark)
		if len(r.MissingSeqs) > 0 {
			fmt.Printf("         missing seq (최대 %d개): %v\n", sampleLimit, r.MissingSeqs)
		}
		if len(r.UnexpectedSeqs) > 0 {
			fmt.Printf("         unexpected seq (최대 %d개): %v\n", sampleLimit, r.UnexpectedSeqs)
		}
	}
	if unattributed > 0 {
		fmt.Printf("격리 테이블에 sensor_id/seq를 알 수 없는 행(파싱 실패 등): %d건\n", unattributed)
	}
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "reconcile: "+format+"\n", args...)
	os.Exit(2)
}
