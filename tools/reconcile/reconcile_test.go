package main

import (
	"strings"
	"testing"
)

func counts(m map[int][]int64) SeqCounts {
	out := SeqCounts{}
	for id, seqs := range m {
		for _, s := range seqs {
			out.add(id, s)
		}
	}
	return out
}

func find(t *testing.T, reports []SensorReport, id int) SensorReport {
	t.Helper()
	for _, r := range reports {
		if r.SensorID == id {
			return r
		}
	}
	t.Fatalf("no report for sensor %d", id)
	return SensorReport{}
}

func TestCompare_AllPresent(t *testing.T) {
	pub := counts(map[int][]int64{1: {0, 1, 2}})
	got := find(t, Compare(pub, counts(map[int][]int64{1: {0, 1, 2}}), SeqCounts{}), 1)
	if !got.OK() || got.Missing != 0 || got.Stored != 3 {
		t.Fatalf("unexpected report: %+v", got)
	}
}

func TestCompare_MissingAndUnexpected(t *testing.T) {
	pub := counts(map[int][]int64{1: {0, 1, 2, 3}})
	stored := counts(map[int][]int64{1: {0, 2, 3, 99}})
	got := find(t, Compare(pub, stored, SeqCounts{}), 1)
	if got.Missing != 1 || got.MissingSeqs[0] != 1 {
		t.Fatalf("missing: %+v", got)
	}
	if got.Unexpected != 1 || got.UnexpectedSeqs[0] != 99 {
		t.Fatalf("unexpected: %+v", got)
	}
	if got.OK() {
		t.Fatal("report with missing/unexpected must not be OK")
	}
}

// 격리된 행은 누락이 아니라 격리로 센다. (TC-07)
func TestCompare_RejectedIsNotMissing(t *testing.T) {
	pub := counts(map[int][]int64{1: {0, 1}})
	got := find(t, Compare(pub, counts(map[int][]int64{1: {0}}), counts(map[int][]int64{1: {1}})), 1)
	if got.Missing != 0 || got.Rejected != 1 || !got.OK() {
		t.Fatalf("report: %+v", got)
	}
}

// 같은 이벤트를 3번 발행해도 DB에 1건이면 정상이고, 발행 중복으로만 센다. (TC-05)
func TestCompare_PublishedDuplicatesAreNotFailures(t *testing.T) {
	pub := counts(map[int][]int64{1: {5, 5, 5}})
	got := find(t, Compare(pub, counts(map[int][]int64{1: {5}}), SeqCounts{}), 1)
	if got.PublishedDuplicates != 1 || got.DBDuplicates != 0 || !got.OK() {
		t.Fatalf("report: %+v", got)
	}
}

// DB에 같은 seq가 2행 이상이면 실패다.
func TestCompare_DBDuplicateFails(t *testing.T) {
	pub := counts(map[int][]int64{1: {5}})
	stored := SeqCounts{1: {5: 2}}
	got := find(t, Compare(pub, stored, SeqCounts{}), 1)
	if got.DBDuplicates != 1 || got.OK() {
		t.Fatalf("report: %+v", got)
	}
}

func TestCompare_ContiguousModeFindsGapsOnly(t *testing.T) {
	stored := counts(map[int][]int64{1: {0, 1, 3, 4}})
	got := find(t, Compare(nil, stored, SeqCounts{}), 1)
	if got.Missing != 1 || got.MissingSeqs[0] != 2 {
		t.Fatalf("report: %+v", got)
	}
}

// 누락이 많아도 건수는 정확히 세고 샘플만 상한까지 보여준다.
func TestCompare_MissingSampleIsCappedButCountIsExact(t *testing.T) {
	var seqs []int64
	for i := int64(0); i < 100; i++ {
		seqs = append(seqs, i)
	}
	got := find(t, Compare(counts(map[int][]int64{1: seqs}), SeqCounts{}, SeqCounts{}), 1)
	if got.Missing != 100 || len(got.MissingSeqs) != sampleLimit || got.MissingSeqs[0] != 0 {
		t.Fatalf("report: missing=%d sample=%v", got.Missing, got.MissingSeqs)
	}
}

func TestParsePublished(t *testing.T) {
	in := "{\"sensor_id\":1,\"seq\":0}\n\n{\"sensor_id\":1,\"seq\":1,\"extra\":true}\n{\"sensor_id\":2,\"seq\":0}\n"
	got, err := ParsePublished(strings.NewReader(in))
	if err != nil {
		t.Fatal(err)
	}
	if len(got[1]) != 2 || len(got[2]) != 1 {
		t.Fatalf("parsed: %+v", got)
	}
}

func TestParsePublished_RejectsBadLines(t *testing.T) {
	for _, in := range []string{"{not json\n", "{\"sensor_id\":1}\n"} {
		if _, err := ParsePublished(strings.NewReader(in)); err == nil {
			t.Fatalf("expected error for %q", in)
		}
	}
}
