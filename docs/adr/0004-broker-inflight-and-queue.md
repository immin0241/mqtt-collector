# ADR-0004: Mosquitto inflight / queue 설정

- 상태: 채택 (초안값, 2026-10-23). 사용자가 결정 E를 위임했다. 실험으로 보정한다.
- 관련: TC-03, TC-06

## 결정

| 설정 | 값 | 근거 |
| --- | --- | --- |
| `max_inflight_messages` | 600 | 배치 크기(300)의 2배. ack 대기 중에도 다음 배치분을 브로커가 계속 보낼 수 있게 한다. 배치 크기보다 작으면 배치가 채워지지 않아 `flushInterval`에 의존한다. |
| `max_queued_messages` | **200,000** (2026-11-12 변경, 이전 60,000) | 목표 장애 시간 × 처리량. 60,000은 TC-06 5분 조건에서 넘쳤다(누락 45.5%). 200,000은 200초 × 1,000 msg/s이고 같은 조건의 부족분 약 14만 건을 받는다. **세션별 큐에 적용된다**(ADR-0011). 사용자 결정: 쓰기 경로는 유지하고 큐만 올린다 |
| `log_type` | error, warning, notice | `all`은 메시지 단위 로그로 처리량을 깎는다. `connection_messages`는 유지한다. |

## 확인 필요 (측정 전 주장 금지)

- `max_queued_messages` 기본값 1,000은 `eclipse-mosquitto:2`에서 실측했다(`docs/experiments/phase1-tc02-03-04.md`). `max_inflight_messages` 기본값 20은 문서 기준이며 아직 실측하지 않았다.
- `persistence true`이지만 큐된 메시지가 디스크에 보존되는 범위(`autosave_interval` 등)는 TC-04/TC-03에서 확인한다.
- 큐가 가득 차면 mosquitto는 새 메시지를 버린다(QoS1도 해당). 60,000을 넘는 장애는 유실이 가능하므로 장애 허용 시간의 상한을 이 값이 정한다.
- 메모리 영향은 [mosquitto-queue-memory](../notes/mosquitto-queue-memory.md)에 측정했다: 메시지(149 B payload) 약 19만 건이 큐에 쌓이면 브로커 RSS가 약 117 MiB 늘었다(메시지당 약 650 B). **큐가 가득 찬 세션 하나의 최악값은 약 130 MiB**이고 세션(테넌트) 수만큼 곱해진다.
- 200,000은 처리 용량이 입력보다 낮은 시간이 길면 여전히 넘친다. 큐 상향은 유실 시점을 늦출 뿐이며, 용량 자체를 올리는 방법은 [phase7-insert-strategies](../experiments/phase7-insert-strategies.md)에 있다(미적용).
