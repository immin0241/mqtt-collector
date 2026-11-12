package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
)

// Claims는 검증이 끝난 토큰에서 API 가 쓰는 값이다.
type Claims struct {
	Subject string
	OrgID   string          // 토큰의 audience 에서 꺼낸 IdP 조직 id. tenant.idp_org_id 와 대응한다.
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
	if org == "" {
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
	return &Claims{Subject: sub, OrgID: org, Scopes: scopes, Expiry: expiry}, nil
}

func bearer(r *http.Request) (string, bool) {
	h := r.Header.Get("Authorization")
	tok, found := strings.CutPrefix(h, "Bearer ")
	return tok, found && tok != ""
}
