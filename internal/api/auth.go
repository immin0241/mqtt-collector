package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
)

// Claims는 검증이 끝난 토큰에서 API 가 쓰는 값이다.
type Claims struct {
	Subject string
	OrgID   string          // 조직 토큰의 audience 에서 꺼낸 IdP 조직 id. tenant.idp_org_id 와 대응한다. API 리소스 토큰이면 빈 값이다.
	OrgIDs  []string        // 후보 조직 id 들. 조직 토큰은 [OrgID], API 리소스 토큰은 사용자 정의 클레임의 값이다.
	Scopes  map[string]bool // scope 클레임
	Expiry  time.Time       // 토큰 만료 시각(exp). Verify 가 이미 만료를 거부하며, MCP 미들웨어가 값을 요구해 돌려준다.
}

// TokenVerifier는 Bearer 토큰을 검증한다. 테스트는 키를 직접 만든 구현을, 운영은 JWKS 구현을 쓴다.
type TokenVerifier interface {
	Verify(token string) (*Claims, error)
}

// JWTVerifier는 IdP(Logto)의 JWKS 로 서명을 검증한다.
type JWTVerifier struct {
	keyfunc   jwt.Keyfunc
	issuer    string
	audPrefix string

	// 사용자 로그인(API 리소스) 토큰 지원. resource 가 비어 있으면 조직 토큰만 받는다.
	resource string // 토큰 aud 에 이 값이 있으면 API 리소스 토큰으로 본다 (RFC 8707 resource indicator)
	orgClaim string // 이 클레임(문자열 배열)에서 사용자의 조직 id 를 읽는다. IdP 의 커스텀 토큰 클레임으로 넣는다.
}

// WithResource는 API 리소스 토큰(사용자가 커넥터로 로그인해 받은 토큰)도 받는 검증기를 돌려준다.
// 이 토큰의 aud 는 resource 이고 조직은 aud 가 아니라 orgClaim 클레임에 있다.
// 클레임은 IdP 가 서명한 값이어야 한다(Logto: Custom JWT 스크립트). 조직 토큰 경로는 그대로 유지된다. (ADR-0014)
func (v *JWTVerifier) WithResource(resource, orgClaim string) *JWTVerifier {
	c := *v
	c.resource, c.orgClaim = resource, orgClaim
	return &c
}

// NewJWKSVerifier는 jwksURL 의 키를 주기적으로 갱신하며 토큰을 검증한다.
//
//	issuer:    토큰의 iss 와 정확히 같아야 한다.
//	audPrefix: 조직 토큰의 aud 접두어. 이 접두어 뒤가 조직 id 다. (Logto: "urn:logto:organization:")
func NewJWKSVerifier(ctx context.Context, jwksURL, issuer, audPrefix string) (*JWTVerifier, error) {
	k, err := keyfunc.NewDefaultCtx(ctx, []string{jwksURL})
	if err != nil {
		return nil, fmt.Errorf("load jwks %s: %w", jwksURL, err)
	}
	return &JWTVerifier{keyfunc: k.Keyfunc, issuer: issuer, audPrefix: audPrefix}, nil
}

// NewStaticVerifier는 주어진 Keyfunc 로 검증한다. 테스트와 고정 키 환경용이다.
func NewStaticVerifier(kf jwt.Keyfunc, issuer, audPrefix string) *JWTVerifier {
	return &JWTVerifier{keyfunc: kf, issuer: issuer, audPrefix: audPrefix}
}

// 서명 알고리즘을 비대칭 키로 제한한다. HS256 으로 공개키를 비밀키처럼 쓰게 하는 공격과 alg=none 을 막는다.
var allowedAlgs = []string{"ES256", "ES384", "ES512", "RS256", "RS384", "RS512", "PS256", "PS384", "PS512"}

func (v *JWTVerifier) Verify(token string) (*Claims, error) {
	parsed, err := jwt.Parse(token, v.keyfunc,
		jwt.WithValidMethods(allowedAlgs),
		jwt.WithIssuer(v.issuer),
		jwt.WithExpirationRequired(),
		jwt.WithLeeway(5*time.Second),
	)
	if err != nil {
		return nil, err
	}
	mc, ok := parsed.Claims.(jwt.MapClaims)
	if !ok {
		return nil, errors.New("unexpected claims type")
	}

	sub, _ := mc.GetSubject()
	if sub == "" {
		return nil, errors.New("token has no sub")
	}
	auds, err := mc.GetAudience()
	if err != nil {
		return nil, err
	}
	var org string
	for _, a := range auds {
		if rest, found := strings.CutPrefix(a, v.audPrefix); found && rest != "" {
			org = rest
			break
		}
	}
	var orgIDs []string
	switch {
	case org != "":
		orgIDs = []string{org}
	case v.resource != "" && slices.Contains(auds, v.resource):
		orgIDs = stringList(mc[v.orgClaim])
		if len(orgIDs) == 0 {
			return nil, fmt.Errorf("token has no %q organization claim", v.orgClaim)
		}
	default:
		return nil, fmt.Errorf("token audience has no %q organization", v.audPrefix)
	}

	scopes := map[string]bool{}
	if s, ok := mc["scope"].(string); ok {
		for _, f := range strings.Fields(s) {
			scopes[f] = true
		}
	}
	var expiry time.Time
	if exp, err := mc.GetExpirationTime(); err == nil && exp != nil {
		expiry = exp.Time
	}
	return &Claims{Subject: sub, OrgID: org, OrgIDs: orgIDs, Scopes: scopes, Expiry: expiry}, nil
}

// stringList는 JSON 클레임 값(문자열 또는 문자열 배열)을 문자열 목록으로 바꾼다. 다른 타입의 요소는 버린다.
func stringList(v any) []string {
	switch x := v.(type) {
	case string:
		if x != "" {
			return []string{x}
		}
	case []any:
		var out []string
		for _, e := range x {
			if s, ok := e.(string); ok && s != "" {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

func bearer(r *http.Request) (string, bool) {
	h := r.Header.Get("Authorization")
	tok, found := strings.CutPrefix(h, "Bearer ")
	return tok, found && tok != ""
}
