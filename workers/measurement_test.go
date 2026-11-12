package workers

import (
	"encoding/json"
	"reflect"
	"testing"
)

func f(v float64) *float64 { return &v }

func TestMeasurementsSkipsNullFieldsInCatalogOrder(t *testing.T) {
	cases := []struct {
		name string
		in   SensorData
		want []Measurement
	}{
		{"tilt", SensorData{TiltX: f(0.1), TiltY: f(-0.2)}, []Measurement{{"tilt_x", 0.1}, {"tilt_y", -0.2}}},
		{"crack", SensorData{CrackWidth: f(1.5)}, []Measurement{{"crack_width", 1.5}}},
		{"env", SensorData{Temperature: f(21)}, []Measurement{{"temperature", 21}}},
		{"zero is a value", SensorData{CrackWidth: f(0)}, []Measurement{{"crack_width", 0}}},
		{"all four", SensorData{TiltX: f(1), TiltY: f(2), CrackWidth: f(3), Temperature: f(4)},
			[]Measurement{{"tilt_x", 1}, {"tilt_y", 2}, {"crack_width", 3}, {"temperature", 4}}},
		{"none", SensorData{}, []Measurement{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := c.in.Measurements()
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("got %v, want %v", got, c.want)
			}
		})
	}
}

// 에뮬레이터 payload(null 포함)를 그대로 받아도 null 필드는 측정 행이 되지 않는다.
func TestMeasurementsFromEmulatorPayload(t *testing.T) {
	payload := `{"sensor_id": 1, "seq": 5, "time": "2026-11-05T00:00:00Z", "tilt_x": 0.12, "tilt_y": -0.5, "crack_width": null, "temperature": null}`
	var d SensorData
	if err := json.Unmarshal([]byte(payload), &d); err != nil {
		t.Fatal(err)
	}
	got := d.Measurements()
	if len(got) != 2 || got[0].Metric != "tilt_x" || got[1].Metric != "tilt_y" {
		t.Fatalf("got %v, want tilt_x and tilt_y only", got)
	}
}

// KnownMetrics는 Measurements가 내는 이름과 정확히 같아야 한다. (둘 중 하나만 고치면 기동 검사와 저장이 어긋난다)
func TestKnownMetricsMatchMeasurements(t *testing.T) {
	all := SensorData{TiltX: f(1), TiltY: f(1), CrackWidth: f(1), Temperature: f(1)}
	var names []string
	for _, m := range all.Measurements() {
		names = append(names, m.Metric)
	}
	if !reflect.DeepEqual(names, KnownMetrics) {
		t.Fatalf("Measurements names %v != KnownMetrics %v", names, KnownMetrics)
	}
}
