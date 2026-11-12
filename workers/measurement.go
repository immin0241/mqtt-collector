package workers

// Measurement는 메시지 하나에서 펼쳐진 측정값 하나다. Metric은 metric 카탈로그(metric.name)의 이름이다.
type Measurement struct {
	Metric string
	Value  float64
}

// KnownMetrics는 수집기가 payload에서 꺼내는 지표 이름이다. metric 카탈로그에 모두 있어야 기동한다. (NewGormStore)
// 새 센서 종류의 지표를 추가하려면 카탈로그에 행을 넣고 여기와 Measurements에 필드 대응을 추가한다.
var KnownMetrics = []string{"tilt_x", "tilt_y", "crack_width", "temperature"}

// Measurements는 payload의 측정 필드를 지표 이름-값으로 펼친다. (정규화 계층)
// 값이 없는(null) 필드는 건너뛰고, 순서는 KnownMetrics와 같다. 측정값이 하나도 없으면 빈 슬라이스다.
func (d SensorData) Measurements() []Measurement {
	out := make([]Measurement, 0, len(KnownMetrics))
	add := func(name string, v *float64) {
		if v != nil {
			out = append(out, Measurement{Metric: name, Value: *v})
		}
	}
	add("tilt_x", d.TiltX)
	add("tilt_y", d.TiltY)
	add("crack_width", d.CrackWidth)
	add("temperature", d.Temperature)
	return out
}
