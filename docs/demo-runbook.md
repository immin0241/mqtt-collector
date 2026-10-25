# 시연 런북 (AWS EC2, 멀티 테넌트, 포트 공개)

센서 9개(테넌트 3 × 센서 3: tilt/crack/env)를 16시간 실시간 누적하고 시연 4시간 전(T-4h)에 멈춘다.
아래 명령은 로컬(WSL2)에서 같은 오버레이로 검증했다. **EC2 자체와 Logto 연동은 검증하지 못했다** (맨 아래 "검증 범위").

| 시점 | 할 일 |
| --- | --- |
| T-20h ~ T-18h | 인스턴스·스택 기동, Logto/Grafana 로그인 경로 검증 (2시간 안에 끝낼 것) |
| T-18h ~ T-4h | **16시간 실시간 누적** (발행 시작 시각을 기록해 둔다) |
| T-12h | 중간 점검 |
| T-4h | 발행 중지, 최종 대조, EBS 스냅샷 |
| T-4h ~ T-1h | 리허설 |
| T-1h | 발행 재개, 최종 확인 |

## 1. 인스턴스

- Ubuntu 24.04 x86_64 (Dockerfile 이 `GOARCH=amd64` 고정). EBS gp3 50 GiB 이상 (센서 9개 × 16시간은 약 52만 행, 압축 전 0.1 GiB 수준이라 데이터는 작다. 이미지 빌드·Logto DB 때문에 넉넉히).
- g5.xlarge 의 GPU 는 쓰지 않는다. NVIDIA 드라이버는 설치하지 않는다.
- Docker 는 공식 저장소 버전을 권장한다 (compose v2 최신).

```bash
sudo apt-get update && sudo apt-get install -y ca-certificates curl git python3
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker ubuntu     # 재로그인 필요
git clone https://github.com/immin0241/webframework-project-mqtt-collector.git
cd webframework-project-mqtt-collector && git checkout dev
```

### 보안 그룹 (포트 공개)

| 포트 | 서비스 | 열 대상 |
| --- | --- | --- |
| 22 | SSH | 내 IP 만 |
| 3000 | Grafana | 시연 참석자 IP |
| 3001 | Logto (로그인, OIDC) | 시연 참석자 IP (브라우저가 접근하므로 Grafana 와 같은 대상) |
| 3002 | Logto 관리 콘솔 | **내 IP 만** |
| 8080 | 경보 API (Swagger `/docs`) | 시연 참석자 IP |
| 8000 | 오류 주입 화면 `/control` | **내 IP 만** (토큰으로도 보호됨) |

**열지 않는 포트:** 5432(DB), 1883/1884/9001(브로커), 9091(Prometheus). compose 가 이들을 호스트 로컬(`127.0.0.1`)에만 바인딩한다.

**전송은 평문 HTTP 다.** 로그인 자격 증명과 토큰이 평문으로 오간다. 그래서 IP 제한이 사실상 유일한 방어선이다.
도메인이 있으면 앞단에 TLS 프록시를 두는 것이 맞지만, 이 구성은 시험하지 않았다 (Logto `ENDPOINT`, Grafana `root_url`, API issuer 를 모두 https 로 바꿔야 한다).

## 2. 설정

```bash
cp .env.example .env
```

`.env` 에서 바꿀 것 (값은 직접 정하고 저장소에 올리지 않는다):

| 변수 | 설명 |
| --- | --- |
| `COMPOSE_FILE` | `docker-compose.yml:docker-compose.demo.yml` (시연 오버레이를 항상 적용. compose 가 `.env` 에서 읽으므로 셸에 export 할 필요가 없다) |
| `PUBLIC_HOST` | EC2 의 공인 IP 또는 DNS. **Elastic IP 를 먼저 붙인다.** Logto 설정 뒤에 주소가 바뀌면 Logto·Grafana·API 설정을 전부 다시 맞춰야 한다 |
| `CONTROL_TOKEN` | 오류 주입 API 토큰. 없으면 에뮬레이터가 기동하지 않는다 |
| `GRAFANA_ADMIN_USER`, `GRAFANA_ADMIN_PASSWORD` | 기본값 `admin/admin` 을 쓰지 말 것 |
| `APP_WORKER_PASSWORD`, `GRAFANA_DB_PASSWORD`, `API_DB_PASSWORD` | DB role 비밀번호. `.env.example` 에는 이 변수들이 주석으로 없을 수 있으니 직접 추가한다. **`DATABASE_URL` 안의 워커 비밀번호(`password=...`)도 같은 값으로 직접 고친다** |
| `GRAFANA_OAUTH_CLIENT_ID`, `GRAFANA_OAUTH_CLIENT_SECRET` | Logto 앱에서 새로 발급 (이전에 노출된 시크릿은 폐기) |
| `GRAFANA_OAUTH_ENABLED=true`, `GRAFANA_OAUTH_ORG_MAPPING` | 4단계에서 채운다 |

**바꾸지 말 것:** `MQTT_USERNAME`/`MQTT_PASSWORD`(`viewer`/`viewer123`). 브로커 비밀번호 파일은 `init` 서비스가 이 값으로 매번 다시 만들기 때문에 `.env` 만 바꾸면 앱이 브로커에 접속하지 못한다. 1884 포트는 호스트 로컬에만 열려 있다.
DB 슈퍼유저(`go`/`go`)도 `127.0.0.1` 에서만 접근된다.

**`.env` 를 셸에서 `source` 하지 않는다.** 값에 `&` 등이 있어 셸이 잘못 해석한다. compose 는 `.env` 를 직접 읽는다. 셸에서 토큰이 필요한 명령에는 다음을 쓴다:
```bash
export CONTROL_TOKEN=$(grep -m1 '^CONTROL_TOKEN=' .env | cut -d= -f2-)
```

```bash
docker compose up -d --build
docker compose ps              # migrate, seed 는 Exited(0) 이 정상. 나머지는 Up
```

`seed` 가 만드는 것: DB role 3개, 테넌트 dev/demo-b/demo-c, 센서 101~103/111~113/121~123, 테넌트마다 경보 규칙 3개(균열 5mm, 기울기 X 5°, 온도 40°C 초과).
개발용 시나리오 센서 201~210 은 시연 대시보드에 섞이지 않도록 건너뛴다.

## 3. Grafana 멀티 테넌트 구성

```bash
scripts/demo_setup.sh        # Grafana 관리자 계정은 .env 의 GRAFANA_ADMIN_USER/PASSWORD 를 읽는다
```

테넌트 demo-b, demo-c 마다 Grafana org 생성 → 전용 DB role(`grafana_tenant_<id>`) → 데이터소스 → 대시보드를 만든다. 멱등이다.
출력의 `grafana_org_id` 를 적어 둔다 (4단계 매핑에 쓴다). 테넌트 dev 는 기본 org(1)이다.

## 4. Logto (로그인 경로) — 가장 먼저 검증할 것

`docs/logto-grafana-setup.md` 를 따른다. 공개 주소로 바꾸면서 추가로 필요한 것:

1. Logto 관리 콘솔: `http://<PUBLIC_HOST>:3002`
2. Grafana 용 앱의 Redirect URI 에 `http://<PUBLIC_HOST>:3000/login/generic_oauth` 추가.
3. 조직 3개(dev, demo-b, demo-c) 생성, 사용자를 조직 역할로 배정.
4. 조직 id 를 DB 에 반영 (자리표시자 `dev-org`, `demo-org-b`, `demo-org-c` 를 교체):
   ```bash
   docker compose exec -T postgres psql -U go -d go -c "UPDATE tenant SET idp_org_id = '<Logto 조직 id>' WHERE name = 'dev'"
   # demo-b, demo-c 도 같은 방식
   ```
5. `.env` 에 `GRAFANA_OAUTH_ORG_MAPPING="<dev 조직 id>:1:Editor <demo-b 조직 id>:<org id>:Editor <demo-c 조직 id>:<org id>:Editor"` 후 `docker compose up -d grafana`.
6. API 용: 조직 토큰의 `aud` 가 `urn:logto:organization:<조직 id>` 이고 scope 에 `alert-rules:write`(관리자) 또는 `alerts:transition`(운영자) 가 들어가는지 확인.
   이 두 가지는 가정이다 (ADR-0010). 안 맞으면 `internal/api/auth.go` 를 고친다. 토큰은 Swagger(`/docs`) Authorize 에 붙여 시험한다.
   Logto 의 기본 조직 권한(`read:data`, `write:data`, `manage:tenant` 등)만 있는 경우, 별도 권한을 만들지 않고 `.env` 에
   `API_SCOPE_RULES_WRITE=manage:tenant`(admin 역할만 가짐), `API_SCOPE_ALERTS_TRANSITION=write:data`(admin·collaborator 가 가짐)를 넣으면
   admin = 규칙 관리, collaborator = 경보 처리로 나뉜다. 이 머신의 시연은 이 설정이다. 토큰에 org 권한이 `scope` 로 실제 담기는지는 확인하지 못했다.

**로그인이 끝내 안 되면:** Grafana 의 로컬 `admin` 계정으로 org 를 전환해 보여 줄 수 있다. 다만 격리 시연은 사용자 로그인이 있어야 의미가 있으므로,
이 경우 psql 로 `grafana_tenant_2` 계정에 접속해 뷰만 보이는 것을 보여 주는 쪽을 준비한다.

## 5. 누적 시작 (T-18h)

에뮬레이터는 `docker compose up` 시점에 이미 발행을 시작한다. 시작 직후:

```bash
docker compose logs emulator | tail -3
curl -s -H "X-Control-Token: $CONTROL_TOKEN" localhost:8000/control/state | head -c 300
docker compose exec -T postgres psql -U go -d go -c "select sensor_id, count(*) from sensor_data group by 1 order by 1"
```

- 센서 9개가 모두 늘어나는지, `sensor_data_rejected` 가 0인지 확인한다.
- **발행 시작 시각을 기록한다** (대조의 `--since` 에 쓴다): `date -u +%Y-%m-%dT%H:%M:%SZ`
- 누적 중에는 앱·에뮬레이터·DB 를 재시작하지 않는다. 재시작이 불가피해도 seq 와 센서 구성이 유지된다(`emulator-data` 볼륨, `INITIAL_CONFIG`).
- **TC-17 폭주 시험은 돌리지 않는다** (알려진 미달, ADR-0011).
- 누적 중에도 `/control` 로 소수의 오류를 주입해 두면 시간축에 이상 사례가 생긴다. 주입 내역은 `truth.jsonl` 에 남는다.

## 6. 점검 (T-12h, T-4h)

발행이 진행 중이면 대조에 시차가 생기므로 **잠깐 멈추고** 대조한다:

```bash
SINCE=<5단계에서 기록한 시각>
docker compose pause emulator; sleep 6
docker compose cp emulator:/data/publish.jsonl /tmp/publish.jsonl
go run ./tools/reconcile --dsn "postgres://go:go@localhost:5432/go?sslmode=disable" \
   --published /tmp/publish.jsonl --since "$SINCE"
docker compose unpause emulator
```

- 모든 센서가 `OK` 이고 `missing`, `unexpected`, `db_dup` 가 0이어야 한다. 깨진 JSON 주입분은 "격리 테이블" 줄에 따로 나온다.
- 멈춘 동안 센서가 몇 초 침묵하므로 상태 이벤트(timeout/recovered)가 한두 건 생긴다. 시연 이력에 남는 것이 싫으면 T-4h 최종 대조에서만 한다.
- Go 가 EC2 에 없으면 `docker run --rm -v $PWD:/src -w /src --network host golang:1.27 go run ./tools/reconcile ...` 를 쓴다 (첫 실행은 모듈을 받느라 오래 걸린다. 미리 한 번 돌려 둘 것).
- 대시보드: `channel_depth` 는 0 근처, `ingest_lag_seconds` 는 안정적이어야 한다. 1분 집계 패널은 1~2분 늦게 채워진다.
- 디스크: `df -h`, `docker system df`.

**T-4h:** 최종 대조 후 `docker compose stop emulator`. EBS 스냅샷을 만든다 (리허설 후 복원 지점).

## 7. 시연 직전 (T-1h)

```bash
docker compose start emulator       # seq 가 이어지고 센서가 recovered 로 돌아온다
```

T-4h ~ T-1h 3시간은 그래프에 빈 구간으로 보인다. 센서 상태 이력에도 offline/전체 두절(stalled) 이벤트가 남는다.
빈 구간이 싫으면 중지하지 않고 계속 발행하면 된다 (그 경우 리허설 중 주입한 오류가 이력에 섞이니 구분해 둘 것).

## 8. 시연 흐름과 오류 주입 버튼

`http://<PUBLIC_HOST>:8000/control` → 토큰 입력. 버튼은 다음 발행부터 적용된다.

| 버튼 | 일어나는 일 | 어디서 보나 | 소요 |
| --- | --- | --- | --- |
| 균열 급증 / 기울기 급증 / 고온 | 임계치 초과 → 경보 (테넌트별 규칙) | 대시보드 "열린 경보", 경보 목록, API `/v1/alerts` | 수 초 |
| 침묵 | 센서 offline 판정 | 센서 상태 패널 | 약 2초 지연 후 (타임아웃 한계) |
| 복구 | recovered | 같은 곳 | 수 초 |
| flatline | 같은 값 15번 → faulty | 센서 상태 패널 | 약 10초 이상 |
| 범위 밖 값 | 범위 밖 반복(3회) → faulty, 값에 따라 임계치 경보 | 센서 상태, 경보 | 수 초 |
| 깨진 JSON | 격리 테이블에 원문 보존, 정상 데이터는 영향 없음 | 수집 경로 대시보드 `rows_rejected_total` | 즉시 |
| 중복 전송 | DB 1건, `dedup_total` 증가 | 수집 경로 대시보드 | 즉시 |

추천 순서: ① 수집 경로 대시보드(수신 = 적재) → ② 균열 급증 → 경보 → Swagger 로 조치 중/완료 → ③ 침묵/복구 → ④ 두 테넌트 계정으로 같은 화면을 보여 격리 → ⑤ 깨진 JSON·중복 → ⑥ `reconcile` 0/0/0.
균열 급증은 "정상값 복귀" 버튼으로 되돌린다 (경보는 edge trigger 라 한 번만 생긴다).

**테넌트 계정 대시보드에는 버튼이 없다.** 제어 화면은 모든 테넌트 센서를 건드리므로 내 IP 에서만 쓴다.

## 9. 알려진 한계 (솔직하게 말할 것)

- **TC-17 미달:** 한 테넌트가 브로커 큐 상한(60,000건)을 넘게 폭주하면 다른 테넌트도 유실될 수 있다. 해결 방향은 결정(테넌트별 구독 분리, ADR-0011)했고 구현은 아직이다.
- Logto 로그인·API 인증은 실물로 끝까지 검증하지 못했다. 평문 HTTP 이다.
- 센서 9개·초당 1건 규모의 결과이며 처리량 주장이 아니다.
- 제어 화면(`/control`)은 로컬에서 API 와 스크립트 문법만 확인했고 브라우저 렌더링은 확인하지 못했다. 리허설에서 눈으로 확인할 것.

## 10. 검증 범위 (로컬 WSL2, 2026-10-26)

확인함: 오버레이 기동, 센서 9개 수집, 토큰 없는 호출 거부(401), 주입 5종 동작(경보·offline·격리·중복 제거), `demo_setup.sh` 2회 실행(멱등), org 별 데이터소스가 자기 테넌트 센서만 조회, 발행 로그 대조 OK(누락·예상 외·DB 중복 0).
확인 못 함: EC2, 공개 IP 에서의 Logto 리다이렉트, 실제 Logto 토큰, 16시간 연속 가동, 브라우저 렌더링.

## 11. 정리

시연이 끝나면 인스턴스를 중지/삭제한다 (g5.xlarge 는 시간당 과금). 필요한 데이터는 먼저 EBS 스냅샷이나 `pg_dump` 로 보존한다.
