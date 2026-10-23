package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"sort"
)

// SeqCounts는 sensor_id -> seq -> 등장 횟수다.
type SeqCounts map[int]map[int64]int

func (c SeqCounts) add(sensorID int, seq int64) {
	if c[sensorID] == nil {
		c[sensorID] = map[int64]int{}
	}
	c[sensorID][seq]++
}

// SensorReport는 센서 하나의 대조 결과다.
type SensorReport struct {
	SensorID int `json:"sensor_id"`

	Published int `json:"published"` // 기대 seq 수 (고유)
	Stored    int `json:"stored"`    // sensor_data에 있는 고유 seq 수
	Rejected  int `json:"rejected"`  // 격리 테이블에 있는 기대 seq 수

	Missing    int `json:"missing"`    // 기대했지만 저장도 격리도 안 된 것
	Unexpected int `json:"unexpected"` // 저장되었지만 기대하지 않은 것
	// DBDuplicates: 같은 (sensor_id, seq)가 sensor_data에 2행 이상 (time이 다른 경우). 있으면 안 된다.
	DBDuplicates int `json:"db_duplicates"`
	// PublishedDuplicates: 발행 로그에서 같은 seq가 2회 이상 나온 수. 중복 전송 주입의 정답 값이며 실패가 아니다.
	PublishedDuplicates int `json:"published_duplicates"`

	MissingSeqs    []int64 `json:"missing_seqs,omitempty"`
	UnexpectedSeqs []int64 `json:"unexpected_seqs,omitempty"`
}

// OK는 누락, 예상 외, DB 중복이 모두 0인지 본다. 격리와 발행 중복은 판정에 포함하지 않는다.
func (r SensorReport) OK() bool {
	return r.Missing == 0 && r.Unexpected == 0 && r.DBDuplicates == 0
}

const sampleLimit = 20

// ParsePublished는 발행 로그(JSONL, 한 줄에 {"sensor_id":..,"seq":..})를 읽는다.
func ParsePublished(r io.Reader) (SeqCounts, error) {
	out := SeqCounts{}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	line := 0
	for sc.Scan() {
		line++
		b := sc.Bytes()
		if len(b) == 0 {
			continue
		}
		var rec struct {
			SensorID *int   `json:"sensor_id"`
			Seq      *int64 `json:"seq"`
		}
		if err := json.Unmarshal(b, &rec); err != nil {
			return nil, fmt.Errorf("published log line %d: %w", line, err)
		}
		if rec.SensorID == nil || rec.Seq == nil {
			return nil, fmt.Errorf("published log line %d: sensor_id and seq are required", line)
		}
		out.add(*rec.SensorID, *rec.Seq)
	}
	return out, sc.Err()
}

// Compare는 기대(published)와 저장(stored)/격리(rejected)를 센서별로 대조한다.
// published가 nil이면 기대를 알 수 없으므로 저장·격리된 seq의 최솟값~최댓값을 기대로 삼는다.
// 이 모드는 중간 구멍은 찾지만 꼬리 쪽 유실은 찾지 못한다.
func Compare(published, stored, rejected SeqCounts) []SensorReport {
	ids := map[int]bool{}
	for id := range published {
		ids[id] = true
	}
	for id := range stored {
		ids[id] = true
	}
	for id := range rejected {
		ids[id] = true
	}

	sorted := make([]int, 0, len(ids))
	for id := range ids {
		sorted = append(sorted, id)
	}
	sort.Ints(sorted)

	reports := make([]SensorReport, 0, len(sorted))
	for _, id := range sorted {
		expected := published[id]
		if published == nil {
			expected = contiguous(stored[id], rejected[id])
		}

		rep := SensorReport{SensorID: id, Published: len(expected), Stored: len(stored[id])}
		for seq, n := range expected {
			if n > 1 {
				rep.PublishedDuplicates++
			}
			switch {
			case stored[id][seq] > 0:
			case rejected[id][seq] > 0:
				rep.Rejected++
			default:
				rep.Missing++
				rep.MissingSeqs = append(rep.MissingSeqs, seq)
			}
		}
		for seq, n := range stored[id] {
			if n > 1 {
				rep.DBDuplicates++
			}
			if _, ok := expected[seq]; !ok {
				rep.Unexpected++
				rep.UnexpectedSeqs = append(rep.UnexpectedSeqs, seq)
			}
		}
		rep.MissingSeqs = sortedSample(rep.MissingSeqs)
		rep.UnexpectedSeqs = sortedSample(rep.UnexpectedSeqs)
		reports = append(reports, rep)
	}
	return reports
}

func contiguous(maps ...map[int64]int) map[int64]int {
	var lo, hi int64
	found := false
	for _, m := range maps {
		for seq := range m {
			if !found || seq < lo {
				lo = seq
			}
			if !found || seq > hi {
				hi = seq
			}
			found = true
		}
	}
	out := map[int64]int{}
	if !found {
		return out
	}
	for seq := lo; seq <= hi; seq++ {
		out[seq] = 1
	}
	return out
}

// sortedSample은 오름차순으로 정렬한 뒤 앞의 sampleLimit개만 남긴다. (건수는 별도 필드가 정확히 센다)
func sortedSample(s []int64) []int64 {
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	if len(s) > sampleLimit {
		s = s[:sampleLimit]
	}
	return s
}
