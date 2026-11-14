# MCP 서버와 Logto 설정 (사용자 로그인: Claude Code 커넥터)

Claude Desktop 에 설치하는 방법은 [claude-desktop-install.md](claude-desktop-install.md) 를 본다.

ADR-0014의 "사용자 로그인 토큰" 경로를 쓰기 위한 Logto 콘솔 작업이다. 서버 쪽은 `MCP_RESOURCE_URL`과 `MCP_ORG_CLAIM`만 맞으면 된다.

## 왜 필요한가

Claude Code는 인가 요청에 `resource=<MCP 서버 URL>`(RFC 8707)을 보낸다. Logto는 등록되지 않은 리소스를 `invalid_target` 으로 거절한다(실측).
조직 토큰(`aud = urn:logto:organization:<id>`)은 인가 엔드포인트에서 받을 수 없고 refresh token 교환으로만 받으므로 커넥터가 쓰지 못한다.
그래서 MCP 서버 URL을 API 리소스로 등록하고, 사용자의 조직을 토큰의 커스텀 클레임으로 넣는다.

## 1. API 리소스

Logto 콘솔(http://localhost:3002) → API resources → Create.
- Indicator: `http://localhost:8090/mcp` (서버의 `MCP_RESOURCE_URL` 과 **정확히** 같아야 한다)
- 권한(permission) 하나: `read:data` (서버의 `MCP_SCOPE_REPORTS_READ` 와 같게)

## 2. 사용자에게 권한 주기

Roles → Create role (type: User role) → 위 API 리소스의 `read:data` 를 넣는다 → 사용자에게 이 역할을 준다.
조직 역할(viewer 등)과는 별개다. 이 권한이 없으면 토큰의 scope 가 비어 MCP 가 403 을 돌려준다.

## 3. 커스텀 토큰 클레임 (사용자 액세스 토큰)

Custom JWT → User access token → 아래 스크립트를 저장한다.

```js
const getCustomJwtClaims = async ({ token, context, environmentVariables }) => {
  const orgs = context.user?.organizations ?? [];
  return {
    organization_ids: orgs.map((o) => (typeof o === 'string' ? o : o.id)),
  };
};
```

- 이 클레임은 Logto 가 서명하므로 신뢰한다. 이름을 바꾸려면 서버의 `MCP_ORG_CLAIM` 도 바꾼다(기본 `organization_ids`).
- 사용자가 등록된 테넌트 둘 이상에 속하면 서버는 **403** 으로 거부한다(어느 것인지 정할 수 없다). 한 사용자는 하나의 테넌트 조직에만 넣는다.
- `context.user.organizations` 의 형태(id 문자열인지 객체인지)는 Logto 버전에 따라 다를 수 있어 둘 다 처리한다. 콘솔의 스크립트 테스트 기능으로 출력을 확인한다.

## 4. 애플리케이션 (PKCE)

Native 또는 SPA 앱. 시크릿 없음. Redirect URI: `http://localhost:8880/callback`.

## 5. Claude Code 에서 연결

```bash
claude mcp add --transport http --client-id <App ID> --callback-port 8880 structwatch http://localhost:8090/mcp
```
Claude Code 안에서 `/mcp` → structwatch → Authenticate. 브라우저에서 Logto 로그인 후 도구가 나타난다.
Logto 는 동적 클라이언트 등록(DCR)을 지원하지 않으므로 `--client-id` 가 필요하다.

## 확인하는 방법

- 토큰 없이: `curl -i -X POST localhost:8090/mcp` → 401 + `WWW-Authenticate: Bearer resource_metadata=...`
- 로그인 후 `get_sensor_status` 가 사용자 조직의 센서만 돌려주는지 본다.
- 안 되면: `docker compose logs mcp`. 403 이면 scope(2단계)나 조직 클레임(3단계), 401 이면 aud(1단계의 indicator)를 의심한다.
