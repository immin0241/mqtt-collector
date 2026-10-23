# paho.mqtt.golang 수동 ack 동작 확인

- 대상 버전: `github.com/eclipse/paho.mqtt.golang v1.5.1` (go.mod)
- 확인 방법: 모듈 소스(`router.go`, `net.go`, `message.go`, `options.go`) 정독. 브로커 실험은 TC-02/03/04 수동 실행 시 별도 기록.

## 확인된 사실

| 항목 | 결과 | 근거 |
| --- | --- | --- |
| 수동 ack 지원 | `ClientOptions.SetAutoAckDisabled(true)` 후 핸들러가 `msg.Ack()`를 직접 호출 | `options.go:465`, `router.go:171,186,199` |
| `Ack()` 중복 호출 | `sync.Once`로 보호되어 한 번만 전송 | `message.go:76-78` |
| QoS 1 ack | PUBACK 전송. QoS 0은 아무 동작 없음 | `net.go:449-470` |
| 핸들러 밖(다른 고루틴)에서 `Ack()` | 가능. `ackFunc`가 `sendAck` 채널로 보냄 | `router.go:150-153` |
| 연결 끊긴 뒤 `Ack()` | **ACK가 조용히 버려진다.** 에러 반환 없음 | `net.go:446-448` WARNING 주석 |
| `OrderMatters`(기본 true) | 핸들러가 블로킹하면 다음 메시지 처리에 영향. 채널 push가 막히면 수신이 멈춘다(백프레셔로 사용 가능) | `client.go:86` |

## 설계에 미치는 영향

1. 재연결 후 이전 연결의 메시지에 `Ack()`해도 브로커에 전달되지 않는다. 브로커는 미확인 메시지를 재전달(`DUP`)하므로 **중복 수신은 정상 경로**이며, `UNIQUE (sensor_id, seq, time)` + `ON CONFLICT DO NOTHING`이 흡수한다.
2. ack 실패를 감지할 방법이 없다. 유실이 아니라 중복 쪽으로만 틀어지는 구조임을 TC-03에서 검증한다.
3. `CleanSession(false)`와 고정 `ClientID`가 있어야 브로커가 미확인 메시지를 보관·재전달한다.
4. 핸들러가 블로킹 채널 push를 하는 현재 구조에서 채널이 가득 차면 paho 읽기 루프가 멈춘다. DB 장애가 keepalive(60s)·ping timeout(5s)보다 길면 연결이 끊길 수 있어 TC-02에서 확인한다.

## 미확인 (실험 필요)

- 브로커 inflight 한도(`max_inflight_messages`)에 걸려 ack 대기 중인 메시지가 배치 크기(300)보다 작으면 배치가 채워지지 않고 `flushInterval`에만 의존하게 되는지.
- mosquitto 기본 inflight/queue 값은 설정 변경 전에 실제 브로커로 재확인한다.
