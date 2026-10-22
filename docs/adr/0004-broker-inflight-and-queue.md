# ADR-0004: Mosquitto inflight / queue 설정

- 상태: 채택 (초안값, 2026-10-23). 사용자가 결정 E를 위임했다. 실험으로 보정한다.
- 관련: TC-03, TC-06

## 결정

| 설정 | 값 | 근거 |
| --- | --- | --- |
| `max_inflight_messages` | 600 | 배치 크기(300)의 2배. ack 대기 중에도 다음 배치분을 브로커가 계속 보낼 수 있게 한다. 배치 크기보다 작으면 배치가 채워지지 않아 `flushInterval`에 의존한다. |
| `max_queued_messages` | 60,000 | 목표 장애 시간 60초 × 처리량 1,000 msg/s (TC-06 부하). |
| `log_type` | error, warning, notice | `all`은 메시지 단위 로그로 처리량을 깎는다. `connection_messages`는 유지한다. |

## 확인 필요 (측정 전 주장 금지)

- `max_queued_messages` 기본값 1,000은 `eclipse-mosquitto:2`에서 실측했다(`docs/experiments/phase1-tc02-03-04.md`). `max_inflight_messages` 기본값 20은 문서 기준이며 아직 실측하지 않았다.
- `persistence true`이지만 큐된 메시지가 디스크에 보존되는 범위(`autosave_interval` 등)는 TC-04/TC-03에서 확인한다.
- 큐가 가득 차면 mosquitto는 새 메시지를 버린다(QoS1도 해당). 60,000을 넘는 장애는 유실이 가능하므로 장애 허용 시간의 상한을 이 값이 정한다.
- 메모리 영향(브로커 큐 60,000건 × 메시지 크기)은 아직 측정하지 않았다.
