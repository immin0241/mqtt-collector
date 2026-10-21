# Logto + Grafana 설정

```
브라우저 ──로그인──▶ Logto(:3001, 관리콘솔 :3002) ──organizations claim──▶ Grafana(:3000)
                                                    org_mapping: Logto org ─▶ Grafana org ─▶ datasource(tenant role) ─▶ v_* 뷰
```

## 1. 기동
```bash
docker compose up -d logto grafana
```
- Logto 로그인/OIDC: http://localhost:3001 , 관리 콘솔: http://localhost:3002
- Grafana: http://localhost:3000 (기본 admin / admin, `.env`의 `GRAFANA_ADMIN_*` 로 변경)

## 2. Logto 콘솔 작업 (http://localhost:3002)
1. 최초 접속 시 관리자 계정을 만든다.
2. **Applications → Create application → Traditional web** (이름: grafana)
   - Redirect URI: `http://localhost:3000/login/generic_oauth`
   - Post sign-out redirect URI: `http://localhost:3000/login`
   - 생성 후 **App ID / App Secret** 을 복사한다.
3. **Organizations → Create organization** : tenant 하나당 organization 하나.
   - 만든 organization의 **ID** 를 복사한다.
   - Members 탭에서 해당 tenant 사용자를 추가한다. (사용자는 User management 에서 먼저 만든다.)
4. (선택) Sign-in experience 에서 로그인 방식(이메일/사용자명 등)을 정한다.
   Grafana 는 `username || email || sub` 순으로 로그인 이름을 쓴다.

## 3. DB tenant 와 Logto organization 연결
`tenant.idp_org_id` 가 Logto organization ID 이다.
```bash
docker compose exec -T postgres psql -U go -d go \
  -c "update tenant set idp_org_id = '<Logto organization id>' where name = 'dev'"
```

## 4. Grafana org 와 데이터소스 (tenant 마다)
- dev tenant(id 2) ↔ Grafana 기본 org(1) 은 provisioning 으로 이미 연결되어 있다.
- 새 tenant:
  ```bash
  # 1) Grafana org 생성 -> orgId 확인
  curl -s -u admin:admin -X POST localhost:3000/api/orgs -H 'Content-Type: application/json' -d '{"name":"<tenant name>"}'
  # 2) tenant 전용 DB role + 해당 org 에 datasource 등록
  GRAFANA_URL=http://localhost:3000 GRAFANA_ORG_ID=<orgId> scripts/provision_tenant.sh <tenant_id>
  ```
  대시보드는 org 마다 필요하다 (Dashboards → Import → `grafana/dashboards/sensor-overview.json`, datasource uid = `sensors`).

## 5. Grafana 에 Logto 연결
`.env` (git 제외) 에 채우고 Grafana 를 다시 띄운다.
```
GRAFANA_OAUTH_ENABLED=true
GRAFANA_OAUTH_CLIENT_ID=<App ID>
GRAFANA_OAUTH_CLIENT_SECRET=<App Secret>
GRAFANA_OAUTH_ORG_MAPPING=<Logto organization id>:1:Viewer
```
```bash
docker compose up -d grafana
```
- 여러 tenant 는 공백으로 이어 쓴다: `orgA:1:Viewer orgB:2:Viewer`
- 매핑에 없는 사용자는 어떤 org 에도 들어가지 못한다 (`GF_USERS_AUTO_ASSIGN_ORG=false`).

## 6. 확인
1. http://localhost:3000/login 에 "Sign in with Logto" 버튼이 보인다.
2. tenant A 사용자로 로그인 → 센서 A 만 보인다. tenant B 사용자는 센서 B 만 보인다.
3. 안 보이면: `docker compose logs grafana | grep -i oauth`
   - 토큰에 `organizations` claim 이 있는지 확인한다. (scope `urn:logto:scope:organizations` 필요)
   - 직접 확인: 로그인 후 Grafana 의 Profile 에서 org 소속을 본다.

## 참고
- 브라우저용 URL(auth)은 `localhost`, Grafana 컨테이너용 URL(token/api)은 `logto` 이다 (compose 내부 주소).
- 운영에서는 `ENDPOINT`/`ADMIN_ENDPOINT`, Grafana `root_url`, 모든 redirect URI 를 실제 도메인(https)으로 바꾼다.
- tenant 격리의 실제 방어선은 Grafana 가 아니라 DB 이다. org 별 datasource 는 `grafana_tenant_<id>` role 로 접속하고,
  이 role 은 해당 tenant 의 `v_*` 뷰만 읽을 수 있다.
