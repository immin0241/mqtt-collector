package workers

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"gorm.io/gorm"
)

// 테넌트 폭주 격리(ADR-0011): 테넌트마다 MQTT 세션(client id, 구독)과 브로커 큐를 따로 둔다.
// 한 테넌트가 브로커 큐 상한을 넘겨도 다른 테넌트의 큐는 영향을 받지 않는다.
//
// topic 구조: sensors/{tenant_id}/{type}/{sensor_id}  (상태는 뒤에 /status)

const (
	// 기본값은 ADR-0011 "구현 결정"에 근거를 적었다.
	defaultTenantRefresh     = 10 * time.Second
	defaultTenantChannelSize = 100
	defaultMaxTenants        = 50
)

// TenantSource는 수집 대상 테넌트 id 목록을 돌려준다.
type TenantSource interface {
	TenantIDs(ctx context.Context) ([]int, error)
}

// GormTenantSource는 tenant 테이블에서 id를 읽는다.
type GormTenantSource struct{ DB *gorm.DB }

func (g GormTenantSource) TenantIDs(ctx context.Context) ([]int, error) {
	var ids []int
	if err := g.DB.WithContext(ctx).Raw(`SELECT id FROM tenant ORDER BY id`).Scan(&ids).Error; err != nil {
		return nil, fmt.Errorf("list tenants: %w", err)
	}
	return ids, nil
}

func tenantDataFilter(tenantID int) string   { return fmt.Sprintf("sensors/%d/+/+", tenantID) }
func tenantStatusFilter(tenantID int) string { return fmt.Sprintf("sensors/%d/+/+/status", tenantID) }

// tenantClientID는 테넌트별 지속 세션의 client id다. 기본 ID가 같으면 항상 같은 값이어야 재시작 후 큐를 이어받는다.
func tenantClientID(base string, tenantID int) string { return fmt.Sprintf("%s-t%d", base, tenantID) }

// tenantSession은 한 테넌트의 연결, 작은 채널, 그 채널을 공용 채널로 옮기는 forwarder의 묶음이다.
type tenantSession struct {
	tenantID  int
	client    mqtt.Client
	ch        chan Envelope
	connected atomic.Bool
	cancel    context.CancelFunc
	done      chan struct{} // forwarder 종료
}
