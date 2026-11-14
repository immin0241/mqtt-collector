# Claude Desktop 에서 일일 리포트 쓰기 (MCP 서버 + 스킬 설치)

테넌트 사용자가 Claude Desktop(또는 claude.ai)에서 "어제 리포트 써줘"라고 하면 MCP 서버가 그 사용자 테넌트의 데이터만 돌려주고, 스킬이 리포트 형식을 잡는다.
둘은 따로 설치한다: **MCP 서버(커넥터)는 데이터를, 스킬은 쓰는 방법을 담당한다.** 스킬만으로는 데이터에 닿지 못한다.

> 출처: Anthropic 문서 [Add a connector that isn't in the directory](https://claude.com/docs/connectors/custom/add-unlisted.md),
> [Skills overview](https://claude.com/docs/skills/overview.md), [Create custom skills](https://claude.com/docs/skills/how-to.md) (2026-11-14 확인).
> UI 이름과 플랜 조건은 바뀔 수 있으니 막히면 최신 문서를 본다.

## 먼저 알아둘 제약 (중요)

| | Claude Code | Claude Desktop / claude.ai |
| --- | --- | --- |
| 서버 주소 | `http://localhost:8090/mcp` 가능 (실제로 동작 확인함) | **HTTPS 주소**가 필요하다 (문서: "enter the HTTPS address where the server accepts MCP requests") |
| 인가 서버(Logto) | localhost 가능 | 공개 HTTPS 여야 한다고 보는 것이 안전하다 (Desktop 3P 문서: authorization endpoint 는 `https`) |
| OAuth 클라이언트 | `--client-id` 로 지정 | **Use your own OAuth client** (Logto 는 동적 클라이언트 등록을 지원하지 않음) |
| 이 저장소에서의 검증 | **검증함** (Claude Code 로 두 테넌트 사용자 로그인, 도구 호출) | **검증하지 않았다.** 아래 절차는 문서 기준이며 실제 로그인은 시험하지 못했다 |

즉 **내 PC의 localhost 스택은 Claude Code 에서만** 쓸 수 있다. Desktop 에서 쓰려면 MCP 서버와 Logto 를 공개 HTTPS 로 노출해야 한다(아래 B).
사설 네트워크 안의 서버는 Anthropic 의 [MCP tunnels](https://claude.com/docs/connectors/mcp-tunnels/overview) 를 안내하는데, 이 저장소에서는 시험하지 않았다.

## 방법 A. Claude Code (localhost, 검증됨)

```bash
# 스킬: 홈 폴더의 skills 에 복사
mkdir -p ~/.claude/skills && cp -r skills/daily-report ~/.claude/skills/

# MCP 서버 (Logto 에 PKCE 앱이 있어야 한다: docs/mcp-logto-setup.md)
claude mcp add --transport http --client-id <App ID> --callback-port 8880 structwatch http://localhost:8090/mcp
```
Claude Code 안에서 `/mcp` → structwatch → Authenticate. 이후 "어제 센서 리포트 써줘" 처럼 요청하면 스킬이 도구를 순서대로 부른다.

## 방법 B. Claude Desktop (공개 HTTPS 필요, 미검증)

### B-1. 서버를 공개 HTTPS 로 노출

1. MCP 서버(`:8090`)와 Logto(`:3001`)를 TLS 프록시나 터널 뒤에 두고 각각 공개 `https://` 주소를 만든다. (예: `https://mcp.example.com`, `https://auth.example.com`)
2. `.env` 를 새 주소에 맞춘다. 모든 값이 **같은 주소를 가리켜야** 한다.

   | 변수 | 값 |
   | --- | --- |
   | `MCP_RESOURCE_URL` | `https://mcp.example.com/mcp` |
   | `MCP_METADATA_URL` | `https://mcp.example.com/.well-known/oauth-protected-resource` |
   | `MCP_ISSUER` | `https://auth.example.com/oidc` (Logto `ENDPOINT` 가 `https://auth.example.com` 이어야 한다. issuer 는 `ENDPOINT/oidc`) |
   | Logto API 리소스 indicator | `https://mcp.example.com/mcp` (`MCP_RESOURCE_URL` 과 정확히 같게) |

   Logto `ENDPOINT` 를 바꾸면 issuer 가 바뀌므로 `API_ISSUER`, Grafana 의 OAuth URL 도 함께 바뀐다. 이 구성(TLS 프록시 뒤 Logto)은 `docs/demo-runbook.md` 에서도 "시험하지 않았다"고 적혀 있다.
3. `docker compose up -d --build mcp logto api` 로 다시 띄우고 `curl https://mcp.example.com/.well-known/oauth-protected-resource` 가 `resource` 를 올바르게 돌려주는지 확인한다.

### B-2. Logto 설정

`docs/mcp-logto-setup.md` 의 1~3단계(API 리소스, 사용자 역할, Custom JWT 의 `organization_ids` 클레임)를 **공개 주소 기준으로** 다시 한다. 그리고 Desktop 용 앱을 하나 더 만든다.

- 앱 유형: **Single-page application** (시크릿 없는 공개 클라이언트)
- Redirect URI: `https://claude.ai/api/mcp/auth_callback` — Anthropic 지원 문서가 안내한 Claude 의 OAuth 콜백이며 `claude.com` 도메인으로 바뀔 수 있다고 한다. **이 값은 문서를 통한 간접 확인이므로, 로그인 중 `redirect_uri` 오류가 나면 오류에 찍힌 주소를 그대로 추가한다.**

### B-3. 커넥터 추가

Claude Desktop: **Customize → Connectors → Add → Add custom connector**
1. 이름(예: structwatch)과 MCP server URL `https://mcp.example.com/mcp` 입력 → Continue
2. Authentication: **Sign in now**
3. OAuth client: **Use your own OAuth client** → B-2 에서 만든 앱의 Client ID 입력, Secret 은 비워 둔다
   (Logto 는 DCR 을 지원하지 않으므로 *Register automatically* 는 쓸 수 없다. *Use Claude's published identity* 는 Logto 지원을 확인하지 못했다)
4. Add → Connect → 브라우저에서 Logto 로그인
5. 인증 설정은 추가 후 바꿀 수 없다. 바꾸려면 커넥터를 지우고 다시 추가한다.

플랜: Free 는 커스텀 커넥터 1개, Pro/Max/Ultra 는 본인 계정에 추가, **Team/Enterprise 는 Owner 가** Organization settings → Connectors 에서 추가하고 멤버가 각자 Connect 한다.

대화에서는 **+ → Connectors** 에서 이 커넥터를 켠다. Customize → Connectors → 커넥터 → Tool permissions 에서 도구별로 *Always allow / Needs approval / Blocked* 를 정할 수 있다 (이 서버의 도구는 모두 읽기 전용이다).

### B-4. 스킬 설치 (Desktop)

필요 조건: Pro/Max/Ultra/Team/Enterprise 플랜, **Settings → Capabilities 에서 Code execution and file creation 켜기**.

1. ZIP 을 만든다. **스킬 폴더 자체가 ZIP 의 최상위**여야 한다. `SKILL.md` 가 ZIP 루트에 있으면 인식되지 않는다.
   ```bash
   cd skills && zip -r daily-report.zip daily-report/
   unzip -l daily-report.zip     # 모든 항목이 daily-report/ 로 시작해야 한다
   ```
   폴더 이름은 `SKILL.md` 의 `name` (`daily-report`)과 같아야 한다.
2. **Customize → Skills** 에서 ZIP 을 업로드하고 스킬 스위치를 켠다. (업로드 버튼 이름은 UI 에 따라 다를 수 있다.)
   업로드한 스킬은 Anthropic 이 검토하지 않으므로 켜기 전에 `SKILL.md` 를 읽어 본다.
3. 대화에서 "어제 센서 리포트 써줘" 라고 하거나 `/` 를 눌러 daily-report 를 고른다.

스킬은 도구를 이름(`get_daily_summary` 등)으로 부르므로 커넥터 이름은 아무거나 괜찮다. 커넥터가 꺼져 있으면 스킬이 데이터를 못 가져온다.

## 확인 체크리스트

- [ ] `curl -i -X POST <MCP URL>` → 401 + `WWW-Authenticate: Bearer resource_metadata=...`
- [ ] 로그인 후 `get_sensor_status` 가 **본인 테넌트 센서만** 돌려준다 (다른 테넌트 계정으로도 확인)
- [ ] 스킬이 `get_daily_summary` → `list_alerts` → `get_sensor_status` 순으로 부르고 한 화면 분량의 리포트를 쓴다
- [ ] 안 되면 `docker compose logs mcp`: 403 이면 scope(사용자 역할)나 조직 클레임, 401 이면 aud (API 리소스 indicator 와 `MCP_RESOURCE_URL` 불일치), 로그인 단계 오류는 redirect URI 를 의심한다

## 알려진 한계

- Desktop 경로는 이 저장소에서 **시험하지 못했다.** Anthropic 문서와 Logto 동작을 근거로 쓴 절차다.
- 한 사용자가 등록된 테넌트 둘 이상에 속하면 서버가 403 으로 거부한다 (ADR-0014).
- 도구는 경보 규칙을 조회하지 못해, 리포트의 "경보 없음"이 "규칙 없음"과 구분되지 않는다.
- 공개 HTTPS, 비밀값(`MCP_ROLE_SECRET`) 관리, 접근 로그 같은 운영 요건은 다루지 않았다.
