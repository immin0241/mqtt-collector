# ADR-0007: 정상 종료 시 구독을 해제하지 않는다

- 상태: 채택 (2026-10-23, 사용자 결정)
- 관련: 불변식 1, TC-16, ADR-0001, ADR-0002
- 대체: Phase 1 종료 순서 "구독 해제 → drain → flush → ack → 연결 종료"

## 배경

Phase 3 통합 테스트(`test/integration` TC-16)가 문제를 재현했다. 앱을 정상 종료(ctx 취소)한 뒤 같은 `MQTT_CLIENT_ID`로
재시작하면, **앱이 내려가 있는 동안 발행된 메시지가 전부 유실**된다.

지속 세션(`CleanSession=false`)에서 `UNSUBSCRIBE`는 브로커가 세션에 저장한 구독 정보를 지운다.
구독이 없으면 브로커는 그 세션의 큐에 메시지를 쌓지 않는다.

## 측정

조건: TimescaleDB(pg18)·Mosquitto 2 컨테이너(`max_inflight_messages 600`, `max_queued_messages 60000`, persistence on),
센서 3개 × 300건 발행 → 종료 신호 → 종료 중·종료 후 센서당 300건 추가 발행 → 같은 clientID로 재시작 → 센서당 100건 추가 발행.

| 종료 방식 | 센서당 발행 | 센서당 저장 | 센서당 누락 |
| --- | --- | --- | --- |
| Unsubscribe 후 종료 (기존) | 700 | 393 | 307 |
| Unsubscribe 없이 종료 (변경) | 700 | 700 | 0 |

(시험 환경: WSL2, Docker 29.8.0. 1회 실행 결과이며 통계적 반복은 아니다.)

## 선택지

| | 설명 | 장단점 |
| --- | --- | --- |
| **a (채택)** | 구독은 유지하고 핸들러만 수신을 멈춘 뒤 drain → 최종 flush → ack → Disconnect | 변경이 작고 종료 중 도착분은 미ack라 재전달된다. 영구 종료 시 브로커 큐가 쌓인다. |
| b | Unsubscribe 없이 즉시 Disconnect | 최종 flush 후 ack가 모두 무효화돼 재시작 시 재전달·중복이 늘어난다. |
| c | 기본은 구독 유지, 플래그로 Unsubscribe 선택 | 영구 종료·축소 시나리오까지 대응하나 분기가 늘어난다. |

## 결정

a. `SensorCollectionWorker.Run`은 ctx 취소 시 Unsubscribe하지 않고 `Unsubscribed` 채널(이름은 유지)만 닫아
`DbPushWorker`가 drain을 시작하게 한다. 이후 도착한 메시지는 핸들러가 큐에 넣지 않고 ack하지 않는다.

## 결과 / 위험

- 영구 종료(센서 수집 중단) 시 브로커 큐가 `max_queued_messages`까지 쌓인다. 운영 절차로 세션을 정리해야 한다.
- Phase 7 수평 확장(공유 구독)에서 인스턴스를 줄일 때는 이 결정을 다시 검토한다.
