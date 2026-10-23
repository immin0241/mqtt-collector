# mqtt_emulator

센서(기울기·균열·환경)를 흉내 내 MQTT로 측정값을 발행하는 Python(FastAPI) 에뮬레이터다. 수집기 시험용이다.

두 가지로 쓴다.

- **시나리오 실행기** (`run_scenario.py`): 시드와 시나리오 파일로 결정론적 데이터를 발행하고 발행 로그·정답 파일을 남긴다. 테스트와 정합성 검증용.
- **FastAPI 서버** (`main.py`): 실행 중에 센서·계수를 바꾸며 보여주는 데모용. 같은 물리 모델을 쓴다.

## 실행

루트의 `docker-compose.yml` 로 실행한다. (`emulator` 서비스, 포트 8000)

```bash
docker compose up -d --build emulator
curl -X PUT localhost:8000/config -H 'Content-Type: application/json' -d '{
  "sensors": [{"id":101,"sensor_type":"tilt"},{"id":102,"sensor_type":"crack"},{"id":103,"sensor_type":"env"}],
  "interval_seconds": 1.0, "jitter_fraction": 0.1, "topic_prefix": "sensors", "qos": 1 }'
curl localhost:8000/stats
```

센서 id는 DB의 `sensor` 테이블에 있어야 한다. (`seeds/dev_sensors.sql` 이 101~103을 만든다. 없으면 FK 위반으로 격리된다)

환경 변수: `MQTT_HOST`(기본 localhost), `MQTT_PORT`(기본 1883), `SEED`(물리 모델 시드, 기본 0),
`SEQ_STATE`(seq 보존 파일, 선택), `PUBLISH_LOG`(PUBACK 받은 전송 로그, 선택).

## 물리 모델 (`model.py`)

값은 시각과 계수의 함수이며 난수는 `(seed, sensor_id, seq)` 로만 정해진다.

- 온도 = `base_temp + temp_amplitude × sin(2π (t − temp_phase) / 1일) + 노이즈`
- 균열 폭 = `crack_base + crack_temp_sensitivity × (온도 − crack_ref_temp) + crack_trend_per_day × 경과 일수 + 노이즈`
- 기울기(x, y) = 균열과 같은 구조 (`tilt_*` 계수)

센서마다 `(seed, id)` 로 정해진 기본 계수를 쓰며 시나리오의 `model`, 또는 `PUT /sensors/{id}/model` 로 일부를 덮어쓴다.

## 시나리오 실행

```bash
uv run python run_scenario.py scenarios/tc01.json --out-dir out --host localhost
go run ../tools/reconcile --dsn "$POSTGRESQL_URL" --published out/publish.jsonl --since <실행 직전 UTC 시각>
```

`scenarios/tc01.json` 은 센서 201~210 × 1,000건(TC-01)이고, `scenarios/faults-demo.json` 은 장애 주입 예시다.
센서 201~210 은 `seeds/scenario_sensors.sql` 이 만든다.

시나리오 필드:

| 필드 | 설명 |
| --- | --- |
| `seed`, `start_time`, `interval_seconds`, `count` | 모의 시각은 `start_time + seq × interval_seconds`. `count` 는 센서당 슬롯 수 |
| `rate` | 초당 발행 수. 0 이면 제한 없음 |
| `sensors[]` | `id`, `type`(tilt/crack/env), `model`(계수 덮어쓰기) |
| `faults.duplicate_ratio`, `duplicate_copies` | 같은 이벤트를 추가로 다시 전송 (TC-05) |
| `faults.malformed_ratio` | 깨진 JSON 전송 (TC-07) |
| `faults.out_of_range_ratio` | 범위 밖 값 (tilt 45°, crack 99mm, temp 120°C) |
| `faults.silence[]` | `{sensor_id, from, to}` seq 구간 동안 전송 안 함 |
| `faults.flatline[]` | 구간 동안 같은 값 반복 |
| `faults.vendor_b_sensors[]` | 제조사 포맷 B(`id`, `n`, `ts`, `v{tx,ty,cw,temp}`). 수집기가 아직 해석하지 않아 격리된다 (TC-14 예정) |

출력 (`--out-dir`):

- `publish.jsonl`: PUBACK 받은 전송마다 `{"sensor_id","seq"}`. 깨진 JSON·포맷 B는 저장되지 않는 것이 정상이므로 올리지 않는다.
  `tools/reconcile --published` 의 입력이다.
- `truth.jsonl`: 주입한 이상 내역(정답). `kind` = duplicate / malformed / out_of_range / flatline / silence / vendor_b / publisher_dropped.
- `stats.json`: 생성·확인·재시도·재접속·버퍼 넘침 집계.
- `seq_state.json`: 센서별 다음 seq.

재현성: 같은 시나리오·시드·시작 seq 에서는 발행 로그와 정답 파일이 바이트 단위로 같다 (`--dry-run` 으로 확인, 테스트 포함).

seq 연속성: 재시작하면 `seq_state.json` 에서 이어서 발행한다 (`--append` 로 로그도 이어쓴다). 상태는 PUBACK 뒤에 기록하므로
죽은 직후 같은 슬롯을 다시 만들 수 있지만, 시각과 값이 `(seed, sensor_id, seq)` 로만 정해져 payload 가 똑같아 DB 중복 키가 흡수한다.
`seq` 는 시간 슬롯 번호라 침묵 구간에도 흘러가며 seq 에 빈 구간이 생긴다 (발행 로그 기준 대조라 누락이 아니다).

브로커 단절: 전송하지 못한 이벤트는 버리지 않고 버퍼에 쌓았다가 재접속 후 순서대로 보낸다. 버퍼(`--max-buffer`, 기본 10만)가 찬 경우만
버리며(`rate>0`) 그 수는 `stats.json` 의 `publisher_dropped` 와 정답 파일에 남는다. 따라서 대조에서 나오는 누락은 수집 측 유실이다.

## 모드

`PUT /config` 의 `mode`:

| mode | 설명 |
| --- | --- |
| `shared` (기본) | MQTT 연결 1개로 모든 센서를 발행. 대량 부하용이며 status/LWT 없음 |
| `per_sensor` | 센서마다 연결과 LWT를 가짐. 접속·단절 테스트용 |

센서 1개 = 컨테이너 1개로 띄우려면 `docker compose --profile sensors up -d sensor-101 sensor-102 sensor-103`.
`docker kill` 은 LWT(offline)를, `docker stop` 은 정상 종료를 만든다. 컨테이너 센서는 `run_sensor.py` 를 쓰며
`SENSOR_ID`, `SENSOR_TYPE`, `INTERVAL_SECONDS`, `JITTER_FRACTION`, `QOS`, `TOPIC_PREFIX`, `KEEPALIVE_SECONDS` 를 읽는다.

## API

| 메서드 | 경로 | 설명 |
| --- | --- | --- |
| GET / PUT | `/config` | 발행 설정 조회 / 교체 (`sensors`, `interval_seconds`, `jitter_fraction`, `topic_prefix`, `qos`, `mode`) |
| GET | `/stats` | `publish_completed`, `publish_errors`, `dropped_queue_full`, `queue_depth`, 지연 통계 |
| GET | `/sensors` | 센서 목록과 상태 |
| POST | `/sensors/{id}/pause`, `/resume` | 센서 침묵 / 재개 (shared 모드는 발행만 멈춤) |
| GET / PUT | `/sensors/{id}/model` | 물리 모델 계수 조회 / 일부 변경 (예: `{"crack_trend_per_day": 0.5}`) |
| POST | `/sensors/{id}/kill`, `/start` | 호스팅 센서 강제 종료 / 재시작 |
| POST | `/sensors/{id}/interval?seconds=` | 발행 주기 변경 |

## 메시지

토픽 `sensors/{type}/{id}`, QoS 1, JSON:

```json
{"sensor_id": 101, "seq": 42, "time": "2026-10-23T01:02:03.456789+00:00",
 "tilt_x": 0.123456, "tilt_y": -0.5, "crack_width": null, "temperature": null}
```

- `seq` 는 센서별로 0부터 증가하며 DB의 중복 판정 키 `(sensor_id, seq, time)` 에 쓰인다. `SEQ_STATE` 를 주면 재시작 후에도 이어진다.
- 센서 타입별로 채워지는 필드: `tilt` → `tilt_x`, `tilt_y` / `crack` → `crack_width` / `env` → `temperature`. 나머지는 `null`. 값은 물리 모델이 만든다.
- 상태 토픽 `sensors/{type}/{id}/status` 는 `online` / `offline` (retained, LWT) 이다.

## 알려진 한계

- FastAPI 서버(데모 경로)는 브로커가 끊긴 동안 생성된 메시지를 버퍼링하지 않고 `publish_errors` 로만 집계한다.
  버퍼링과 발행 측/수집 측 유실 구분은 시나리오 실행기에서만 지원한다. 정합성 검증에는 시나리오 실행기를 쓴다.
- 시나리오 실행기는 PUBACK 을 순차로 기다리므로 처리량이 브로커 왕복 시간에 묶인다 (로컬에서 rate=500 까지 확인했고 상한은 측정하지 않았다).
- 시나리오는 JSON 만 지원한다 (YAML 은 의존성을 늘리지 않으려고 제외).
- 실행 방법: `uv run python -m unittest discover -s tests -t .`
