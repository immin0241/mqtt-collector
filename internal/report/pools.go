// Package report는 테넌트 일일 리포트용 읽기 전용 조회와 MCP 도구를 제공한다. (ADR-0014)
//
// 격리는 코드 필터가 아니라 테넌트별 DB role 로 한다. 뷰가 session_user 로 테넌트를 정하므로(ADR-0009)
// 이 패키지의 쿼리에 버그가 있어도 다른 테넌트의 행은 나오지 않는다.
package report

import (
	"container/list"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// RoleName은 테넌트의 리포트용 DB role 이름이다. scripts/provision_mcp_tenant.sh 와 같아야 한다.
func RoleName(tenantID int) string { return fmt.Sprintf("mcp_tenant_%d", tenantID) }

// RolePassword는 role 비밀번호다: HMAC-SHA256(secret, role 이름)의 16진수.
// 서비스가 즉석에서 계산하므로 테넌트별 비밀을 저장하지 않는다. 프로비저닝 스크립트도 같은 값을 쓴다.
func RolePassword(secret string, tenantID int) string {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write([]byte(RoleName(tenantID)))
	return hex.EncodeToString(m.Sum(nil))
}

// TenantDB는 테넌트의 읽기 전용 연결을 돌려준다.
type TenantDB interface {
	For(ctx context.Context, tenantID int) (*gorm.DB, error)
}

// Pools는 테넌트별 연결 풀을 LRU 로 최대 Max 개까지 유지한다. 초과하면 가장 오래 안 쓴 풀을 닫는다.
// 한 연결을 SET ROLE 로 돌려쓸 수 없다(뷰가 session_user 를 본다). 그래서 role 마다 풀이 따로 있다.
type Pools struct {
	// Host, Port, DBName, SSLMode 는 접속 대상이다. 사용자·비밀번호는 테넌트마다 계산한다.
	Host, Port, DBName, SSLMode string
	Secret                      string
	Max                         int
	ConnsPerPool                int

	mu    sync.Mutex
	items map[int]*list.Element
	order *list.List // 앞쪽이 최근 사용
}

type pool struct {
	tenantID int
	db       *gorm.DB
}

func (p *Pools) dsn(tenantID int) string {
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		p.Host, p.Port, RoleName(tenantID), RolePassword(p.Secret, tenantID), p.DBName, p.SSLMode)
}

func (p *Pools) For(_ context.Context, tenantID int) (*gorm.DB, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.items == nil {
		p.items = map[int]*list.Element{}
		p.order = list.New()
	}
	if el, ok := p.items[tenantID]; ok {
		p.order.MoveToFront(el)
		return el.Value.(*pool).db, nil
	}
	db, err := gorm.Open(postgres.Open(p.dsn(tenantID)), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		return nil, fmt.Errorf("open tenant pool: %w", err)
	}
	if sqlDB, err := db.DB(); err == nil {
		n := p.ConnsPerPool
		if n <= 0 {
			n = 2
		}
		sqlDB.SetMaxOpenConns(n)
		sqlDB.SetMaxIdleConns(n)
	}
	p.items[tenantID] = p.order.PushFront(&pool{tenantID: tenantID, db: db})

	max := p.Max
	if max <= 0 {
		max = 50
	}
	for p.order.Len() > max {
		last := p.order.Back()
		old := last.Value.(*pool)
		p.order.Remove(last)
		delete(p.items, old.tenantID)
		if sqlDB, err := old.db.DB(); err == nil {
			_ = sqlDB.Close()
		}
	}
	return db, nil
}

// Close는 모든 풀을 닫는다.
func (p *Pools) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, el := range p.items {
		if sqlDB, err := el.Value.(*pool).db.DB(); err == nil {
			_ = sqlDB.Close()
		}
	}
	p.items = nil
	p.order = nil
}
