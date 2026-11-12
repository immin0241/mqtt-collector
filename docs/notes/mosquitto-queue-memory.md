# Mosquitto 오프라인 세션 큐의 메모리 비용

- 날짜: 2026-11-13
- 목적: `max_queued_messages`를 60,000에서 200,000으로 올릴 때 브로커 메모리 영향을 확인한다(ADR-0004, ADR-0011).

## 조건

| 항목 | 값 |
| --- | --- |
| 브로커 | `eclipse-mosquitto:2` 단독 컨테이너, `max_inflight_messages 600`, `max_queued_messages 200000`, persistence on, 인증 없음(메모리 비용은 인증과 무관) |
| 장비 | AMD Ryzen 5 7500F, RAM 15.8 GB, WSL2 위의 Docker |
| 구독자 | 영속 세션(`-c`) 1개가 `sensors/1/#` QoS 1 구독 후 접속 종료(오프라인) |
| 발행 | QoS 1, payload 149 B(JSON, 측정 2개), 토픽 `sensors/1/tilt/1`, 190,000건. 500건마다 PUBACK 대기 |
| 측정 | `docker stats` 메모리(RSS), `$SYS/broker/store/messages/count` |

## 결과

| 상태 | 브로커 메모리 |
| --- | --- |
| 유휴 | 2.4 MiB |
| 190,000건 큐 적재 후 | 119.2 MiB |

- 증가 약 117 MiB / 190,000건 = **메시지당 약 650 B** (payload 149 B 대비 약 4.4배).
- 드롭 수 0, 저장 메시지 수 175,682(`$SYS`는 10초 주기 갱신이라 약간 뒤처짐).
- 큐가 가득 찬 세션 하나의 최악값: 200,000 × 650 B ≈ **약 130 MiB**. 세션(테넌트)마다 큐가 따로이므로 곱해진다.

## 실험 중 발견

- 처음 시도에서 `mosquitto_pub -l` 로 빠르게 넣으면 큐가 거의 쌓이지 않았다(저장 30,149건). 원인은 확인하지 않았다. 결과는 PUBACK을 기다리는 발행기로 다시 측정했다.
- paho-mqtt(Python)는 inflight 한도(기본 20) 초과 시 `ERR_QUEUE_SIZE`로 발행을 거부한다. 발행기에서 주기적으로 `wait_for_publish`가 필요하다.

## 한계

단일 세션, 단일 토픽, 단일 payload 크기 1회 측정이다. 토픽이 길거나 payload가 크면 메시지당 비용이 늘어난다.
