# ADR-0003: 오류 분류와 문제 행 격리

- 상태: 채택 (2026-10-23, 사용자 결정 C, D)
- 관련: 불변식 3, TC-07, 마이그레이션 000006

## 결정

**분류 (C-a, SQLSTATE 클래스 기반)** — `workers/classify.go`

| 분류 | 조건 |
| --- | --- |
| permanent | SQLSTATE 클래스 `22`(데이터 오류), `23`(무결성 제약 위반) |
| transient | 그 외 전부: PgError의 다른 클래스(08, 40, 53, 57, 58, 42 …)와 PgError가 아닌 오류(네트워크, context) |

C-a의 세부로 "알 수 없으면 transient"를 택했다. 잘못 permanent로 분류하면 정상 행이 격리되지만, 잘못 transient로 분류하면 수신이 멈출 뿐 데이터는 잃지 않는다. `42501`(권한)처럼 설정 오류도 transient라서 무한 재시도하며 수신이 정지한다.

**격리 (D-a, 이분법)**

- permanent 오류가 나면 배치를 반으로 나눠 각각 재삽입하고, 1행까지 좁혀지면 `sensor_data_rejected`에 원문 payload, topic, SQLSTATE, 메시지, received_at, (가능하면) sensor_id/seq와 함께 기록한다.
- 한 문장이 실패하면 그 문장은 전부 롤백되므로 부분 커밋은 없다. 비용은 문제 행 1개당 약 2·log2(N) 회의 INSERT다.
- JSON 파싱 실패는 DB에 가지 않고 같은 테이블에 `PARSE_ERROR`로 기록한다.
- 격리 INSERT 자체는 오류 종류와 무관하게 성공할 때까지 재시도한다. 여기서 포기하면 메시지가 사라지기 때문이다.

## 결과 / 한계

- 같은 불량 메시지가 재전달되면 격리 테이블에 중복 행이 생긴다. 격리 테이블은 진단용이라 허용한다.
- "범위 밖 값"은 [ADR-0013](0013-value-range-validation.md)에서 해결했다. 지표별 범위(metric 카탈로그)를 수집기가 검증하고 그 메시지만 격리한다. (2026-11-12)
- 시간 값이 0인 payload도 [ADR-0013](0013-value-range-validation.md)에서 해결했다. 2000-01-01 이전이거나 `received_at`보다 24시간 넘게 미래면 격리한다. (2026-11-12)
