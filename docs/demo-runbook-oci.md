# 시연 런북: Oracle Cloud (OCI) 판

`docs/demo-runbook.md` 는 AWS EC2 기준이다. 스택(`docker-compose.yml` + `docker-compose.demo.yml`)은 클라우드에 종속되지 않으므로
**이 문서의 1~3단계로 인스턴스를 준비한 뒤에는 `demo-runbook.md` 의 2단계(설정)부터 그대로 따른다.**
이 문서는 런북에서 AWS 용어로 쓴 부분(인스턴스, 보안 그룹, Elastic IP, EBS 스냅샷, 정리)만 OCI 로 바꾼다.
런북 6단계 T-4h 의 스냅샷은 이 문서 4단계로, 런북 11단계 정리는 5단계로 바꿔 읽는다.

> **검증 범위.** 이 저장소에서 확인한 것: 두 compose 파일을 합친 설정이 유효하고, 바깥에 열리는 포트가 3000·3001·3002·8000·8080 뿐이라는 것,
> `Dockerfile` 의 `GOARCH=amd64` 가 아래 shape(x86_64)와 맞는다는 것.
> **OCI 에서는 확인하지 못했다.** 콘솔 메뉴 이름은 바뀔 수 있고, 인스턴스 내부 방화벽 동작(3단계)은 실제 인스턴스에서 한 번 확인해야 한다.

## 1. 인스턴스

| 항목 | 값 | 이유 |
| --- | --- | --- |
| Shape | `VM.Standard.E4.Flex`, 2 OCPU, 16 GB, 네트워크 2 Gbps | AMD x86_64. Always Free 대상이 아니라 유료다 |
| 이미지 | Canonical **Ubuntu 24.04** (x86_64. aarch64 이미지를 고르지 않는다) | 런북이 Ubuntu 기준이다. Oracle Linux 는 SELinux 때문에 bind mount(`./broker1/config`, `./seeds` 등)에 `:z` 가 필요해서 쓰지 않는다 |
| 부트 볼륨 | 최소 50 GB, 100 GB 권장 | 센서 9개 × 16시간 데이터는 작지만 이미지 빌드, Docker 레이어, Logto DB 때문에 넉넉히 잡는다 |
| 공인 IP | **Reserved public IP** 를 만들어 인스턴스의 주 VNIC 에 붙인다 | AWS 의 Elastic IP 에 해당한다. 임시 IP 로 시작했다면 주 VNIC 의 IPv4 주소 편집에서 "No public IP" 로 바꾼 뒤 예약 IP 를 붙인다 |
| SSH 키 | 인스턴스 생성 시 등록 | 기본 사용자는 `ubuntu` |

**`PUBLIC_HOST` 는 예약 IP 를 쓴다.** Logto 를 설정한 뒤 주소가 바뀌면 Logto·Grafana·API 설정을 전부 다시 맞춰야 한다 (런북 2단계 참고).

접속 후 Docker 설치와 저장소 준비는 런북 1단계와 같다:

```bash
sudo apt-get update && sudo apt-get install -y ca-certificates curl git python3
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker ubuntu     # 재로그인 필요
git clone https://github.com/immin0241/webframework-project-mqtt-collector.git
cd webframework-project-mqtt-collector && git checkout dev
```

이미지 빌드와 설치에는 인스턴스에서 바깥으로 나가는(egress) 연결이 필요하다: Docker Hub, `ghcr.io`(에뮬레이터 베이스 이미지),
`proxy.golang.org`(Go 모듈), `pypi.org`/`files.pythonhosted.org`(에뮬레이터 의존성), `get.docker.com`, `github.com`.
VCN 기본 설정은 egress 를 모두 허용한다. 서브넷에 egress 제한을 걸었다면 빌드가 실패한다.

## 2. 네트워크 (VCN 보안 규칙)

VCN 기본 Security List 는 22 번(SSH)과 ICMP 만 받는다. 아래 ingress 규칙을 추가한다 (Stateful, 프로토콜 TCP).
NSG(Network Security Group)를 만들어 인스턴스 VNIC 에 붙이는 방식을 권한다. 서브넷 Security List 에 넣어도 같다.

| 포트 | 서비스 | Source CIDR |
| --- | --- | --- |
| 22 | SSH | 내 IP `/32` 만 |
| 3000 | Grafana | 시연 참석자 IP |
| 3001 | Logto (로그인, OIDC) | 시연 참석자 IP (브라우저가 접근하므로 Grafana 와 같은 대상) |
| 3002 | Logto 관리 콘솔 | **내 IP `/32` 만** |
| 8080 | 경보 API (Swagger `/docs`) | 시연 참석자 IP |
| 8000 | 오류 주입 화면 `/control` | **내 IP `/32` 만** (토큰으로도 보호됨) |

**열지 않는 포트:** 5432(DB), 1883/1884/9001(브로커), 9091(Prometheus). compose 가 호스트의 `127.0.0.1` 에만 바인딩하므로
VCN 규칙을 실수로 열어도 바깥에서 닿지 않는다. 그래도 규칙에는 추가하지 않는다.

**전송은 평문 HTTP 다.** IP 제한이 사실상 유일한 방어선이므로 `0.0.0.0/0` 으로 열지 않는다 (런북 1단계의 같은 경고).

## 3. 인스턴스 내부 방화벽 확인 (OCI Ubuntu 이미지 특유)

OCI 의 Ubuntu 이미지는 호스트 iptables 가 22 번 외의 INPUT 을 거부하도록 미리 설정돼 있다.
Docker 가 공개한 포트는 NAT 를 거쳐 FORWARD 체인으로 가므로 보통 이 규칙에 걸리지 않지만, 이 저장소에서 OCI 인스턴스로 확인한 것은 아니다.
**스택을 올린 뒤 내 PC 에서 직접 확인한다.** HTTP 응답 코드가 찍히면 열린 것이고, 타임아웃이면 막힌 것이다:

```bash
H=<PUBLIC_HOST>
curl -sS -m 5 -o /dev/null -w 'grafana  %{http_code}\n' http://$H:3000/api/health
curl -sS -m 5 -o /dev/null -w 'logto    %{http_code}\n' http://$H:3001/api/status
curl -sS -m 5 -o /dev/null -w 'api      %{http_code}\n' http://$H:8080/healthz
curl -sS -m 5 -o /dev/null -w 'emulator %{http_code}\n' http://$H:8000/healthz
```

반대로 **닫혀 있어야 하는 포트**도 확인한다. TCP 연결이 되는지만 본다 (DB·브로커는 HTTP 가 아니라서 curl 로는 열려 있어도 실패로 보인다).
모두 `closed` 여야 하고, 하나라도 `OPEN` 이면 시연 전에 원인을 찾는다:

```bash
for p in 5432 1883 1884 9001 9091; do
  nc -z -w 3 $H $p && echo "OPEN $p (!)" || echo "closed $p"
done
```

VCN 규칙을 열었는데도 첫 번째 확인이 타임아웃이면 인스턴스 안에서 INPUT 규칙을 연다:

```bash
sudo iptables -I INPUT -p tcp -m multiport --dports 3000,3001,3002,8000,8080 -m conntrack --ctstate NEW -j ACCEPT
```

- 이 규칙은 재부팅하면 사라진다. 시연 기간 중 재부팅이 있을 수 있으면 `/etc/iptables/rules.v4` 에 같은 ACCEPT 줄을 직접 넣는다.
  Docker 가 떠 있는 상태에서 `netfilter-persistent save` 를 하면 Docker 가 만든 규칙까지 저장되어 재부팅 뒤 중복될 수 있으니 쓰지 않는다.
- `ufw` 는 설치하지 않는다. Docker 공개 포트와 규칙이 엇갈려 어느 쪽이 적용되는지 헷갈리게 된다.

## 4. 스냅샷 (런북 6단계 T-4h)

EBS 스냅샷 대신 **부트 볼륨 백업**을 만든다. Docker 볼륨(`postgres_data`, `emulator-data`, `grafana-data` 등)은 `/var/lib/docker` 아래, 즉 부트 볼륨에 있으므로 함께 보존된다.

1. 최종 대조(런북 6단계)를 마치고 `docker compose stop` 으로 스택 전체를 멈춘다. (`emulator` 만 멈추면 DB 가 쓰는 중인 상태로 백업된다)
2. 콘솔의 부트 볼륨 화면에서 Manual backup 을 만들고, 상태가 `AVAILABLE` 이 될 때까지 기다린다.
3. `docker compose start` 로 되살린다. 이 기록용 복원 지점은 리허설이 꼬였을 때 쓴다.

복원은 백업에서 새 부트 볼륨을 만든 뒤 그 볼륨으로 인스턴스를 만드는 방식이다. 이때 **공인 IP 가 바뀌지 않도록 예약 IP 를 새 인스턴스에 다시 붙이고**,
`PUBLIC_HOST` 가 같은지 확인한다. 이 복원 절차는 실제로 해보지 않았으니 리허설 때 한 번 시험한다.

## 5. 정리 (런북 11단계)

E4.Flex 는 켜져 있는 시간만큼 과금된다. 시연이 끝나면 필요한 데이터를 부트 볼륨 백업이나 `pg_dump` 로 먼저 보존한 뒤 다음을 정리한다:

- 인스턴스 종료. 종료 화면에서 부트 볼륨도 함께 삭제할지 정한다 (남기면 볼륨 요금이 계속 나온다).
- 쓰지 않을 Reserved public IP 삭제.
- 만들어 둔 NSG, 부트 볼륨 백업 중 필요 없는 것 삭제.
