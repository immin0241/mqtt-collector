# ADR-0012: 측정값을 세로 구조(metric, value)로 바꾸고 단위는 metric 카탈로그에 둔다

- 상태: **채택** (2026-11-05). 선택은 사용자 결정이다.
- 관련: ADR-0003(오류 분류), ADR-0009(테넌트 격리), TC-01·05·07·09, 10.12 발표의 To-Do("다른 센서를 추가하기 쉽게 테이블 구조 수정", "센서 출력값 정규화 레이어")

## 배경

`sensor_data`는 `tilt_x`, `tilt_y`, `crack_width`, `temperature` 컬럼이 가로로 펼쳐진 구조였다.
새 센서 종류가 생길 때마다 컬럼, 연속 집계, 뷰, 경보 엔진, 대시보드를 모두 고쳐야 했고(컬럼명이 약 25개 파일에 퍼져 있었다),
단위는 컬럼 주석에만 있었다.

## 선택지 (단위를 어디에 둘 것인가)

| | 내용 | 판단 |
| --- | --- | --- |
| **A** | **`metric(id, name, unit)` 카탈로그 + `sensor_data.metric_id`, 단위는 join** | **채택.** 저장 용량이 가장 작고 새 지표는 행 추가로 끝난다 |
| B | 행마다 `unit` 컬럼 | join이 없고 쓰기 시점 단위가 보존되지만 행마다 문자열이 늘고 오타로 단위가 섞일 수 있다 |
| C | `sensor_metric(sensor_id, metric, unit)` 센서별 단위 | 제조사별 단위 차이를 다룰 수 있지만 등록을 잊으면 저장이 막힌다 |

## 결정

- `sensor_data(time, tenant_id, sensor_id, seq, metric_id, value, received_at)`. 메시지 하나가 지표별 행으로 펼쳐지고 한 메시지의 행들은 `seq`와 `time`을 공유한다.
- 중복 판정 키는 `UNIQUE (sensor_id, seq, metric_id, time)`이다.
- 연속 집계 `sensor_data_1m`은 `(bucket, sensor_id, metric_id)`별 `value_avg/min/max`, `sample_count`다. 집계 안에서는 join하지 않고 단위는 뷰에서 붙인다.
- `v_sensor_data`, `v_sensor_data_1m`은 `metric`과 `unit` 컬럼을 낸다. tenant role은 `metric` 테이블을 직접 읽을 수 없고 뷰 소유자 권한으로만 읽힌다.
- **payload는 바꾸지 않는다.** 에뮬레이터는 가로형 JSON을 그대로 보내고, 수집기가 `SensorData.Measurements()`로 펼친다(정규화 계층). 지표와 JSON 필드의 대응은 `workers/measurement.go`의 `KnownMetrics`가 한 곳에서 정하고, `NewGormStore`가 기동할 때 카탈로그에 모두 있는지 확인한다.
- 기존 측정 데이터(`sensor_data`, 1분 집계, `sensor_data_rejected`)는 사용자 결정으로 폐기한다. tenant, sensor, 경보 규칙·이력, 센서 상태는 유지한다. migration 000011의 down은 가로 구조를 빈 테이블로 복원하며 데이터는 복구되지 않는다.

## 불변식에 미치는 영향

- **1·3(ack, 격리):** 배치·격리·ack는 계속 **메시지 단위**다. `Store.InsertRows`는 한 문장(트랜잭션)으로 메시지의 모든 측정 행을 넣고, 새로 저장된 메시지 수를 돌려준다. 그래서 메시지의 일부 행만 저장되고 ack되는 일이 없다.
- **측정값이 하나도 없는 메시지**(예전에는 모든 값이 NULL인 행으로 저장됐다)는 행이 0개라 흔적이 남지 않는다. `InsertRows`가 SQLSTATE `22023`(영구 오류)을 내서 이분법으로 그 메시지만 격리 테이블로 보내고, `sensor_id`와 `seq`를 남긴다.
- **대조(reconcile):** 같은 `(sensor_id, seq)`가 여러 행이 되므로 중복 판정을 `count(DISTINCT time)`으로 바꿨다. `time`이 다른 같은 `seq`만 DB 중복이다.
- **dedup 지표:** 메시지 단위 그대로다.

## 비용과 한계

- 행 수가 메시지당 1~2배(현재는 tilt 2, 나머지 1)로 늘어난다. 압축 설정은 `segmentby = sensor_id, metric_id`다. 쓰기 처리량과 저장 용량 변화는 Phase 7에서 가로 구조(이전 커밋)와 비교해 측정한다.
- `sample_count`가 메시지 수가 아니라 (센서, 지표)별 측정 수가 되었다. 완성도 패널은 지표 중 최댓값으로 계산한다.
- 단위를 카탈로그에서 바꾸면 과거 데이터의 해석도 함께 바뀐다. 단위 이력이 필요해지면 B나 C를 다시 검토한다.
- 정상 범위 검증(ADR-0003의 미결)은 이번에 하지 않았다. 카탈로그에 범위 컬럼을 두는 것이 후보지만 사용하지 않는 컬럼을 미리 만들지는 않았다.
- `test/experiments`의 실험(tenant_id, 트리거 오버헤드, RLS)은 `experiment` 빌드 태그 뒤에 있고 가로 구조를 전제로 한 기록이다. 새 구조에서 다시 돌리려면 SQL을 고쳐야 한다.
