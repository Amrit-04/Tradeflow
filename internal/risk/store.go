package risk

import (
	"context"
	"fmt"
	"sync"

	"github.com/redis/go-redis/v9"
	"tradeflow/internal/domain"
)

// RiskStateStore provides atomic access to fast risk counters and flags.
type RiskStateStore interface {
	CheckAndIncrementDailyNotional(ctx context.Context, account string, notional int64, limit int64) (allowed bool, current int64, err error)
	GetPosition(ctx context.Context, account string, symbol string) (domain.Qty, error)
	UpdatePosition(ctx context.Context, account string, symbol string, deltaQty domain.Qty) error
	GetLastPrice(ctx context.Context, symbol string) (domain.Price, error)
	SetLastPrice(ctx context.Context, symbol string, price domain.Price) error
	IsSymbolRestricted(ctx context.Context, symbol string) (bool, error)
	SetSymbolRestricted(ctx context.Context, symbol string, restricted bool) error
	IsKillSwitchActive(ctx context.Context, account string) (bool, error)
	SetKillSwitch(ctx context.Context, account string, active bool) error
}

// =====================================================================
// Redis Risk Store (Production Atomic Lua Scripts)
// =====================================================================

var checkAndIncrementLua = redis.NewScript(`
local key = KEYS[1]
local increment = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])

local current = redis.call('GET', key)
if not current then
    current = 0
else
    current = tonumber(current)
end

if (current + increment) > limit then
    return {0, current}
else
    local newVal = redis.call('INCRBY', key, increment)
    return {1, newVal}
end
`)

type RedisRiskStore struct {
	client *redis.Client
}

func NewRedisRiskStore(client *redis.Client) *RedisRiskStore {
	return &RedisRiskStore{client: client}
}

func (r *RedisRiskStore) CheckAndIncrementDailyNotional(ctx context.Context, account string, notional int64, limit int64) (bool, int64, error) {
	key := fmt.Sprintf("risk:notional:daily:%s", account)
	res, err := checkAndIncrementLua.Run(ctx, r.client, []string{key}, notional, limit).Slice()
	if err != nil {
		return false, 0, err
	}
	allowed := res[0].(int64) == 1
	currVal := res[1].(int64)
	return allowed, currVal, nil
}

func (r *RedisRiskStore) GetPosition(ctx context.Context, account string, symbol string) (domain.Qty, error) {
	key := fmt.Sprintf("risk:pos:%s:%s", account, symbol)
	val, err := r.client.Get(ctx, key).Int64()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return domain.Qty(val), nil
}

func (r *RedisRiskStore) UpdatePosition(ctx context.Context, account string, symbol string, deltaQty domain.Qty) error {
	key := fmt.Sprintf("risk:pos:%s:%s", account, symbol)
	return r.client.IncrBy(ctx, key, int64(deltaQty)).Err()
}

func (r *RedisRiskStore) GetLastPrice(ctx context.Context, symbol string) (domain.Price, error) {
	key := fmt.Sprintf("market:price:%s", symbol)
	val, err := r.client.Get(ctx, key).Int64()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return domain.Price(val), nil
}

func (r *RedisRiskStore) SetLastPrice(ctx context.Context, symbol string, price domain.Price) error {
	key := fmt.Sprintf("market:price:%s", symbol)
	return r.client.Set(ctx, key, int64(price), 0).Err()
}

func (r *RedisRiskStore) IsSymbolRestricted(ctx context.Context, symbol string) (bool, error) {
	return r.client.SIsMember(ctx, "risk:restricted_symbols", symbol).Result()
}

func (r *RedisRiskStore) SetSymbolRestricted(ctx context.Context, symbol string, restricted bool) error {
	if restricted {
		return r.client.SAdd(ctx, "risk:restricted_symbols", symbol).Err()
	}
	return r.client.SRem(ctx, "risk:restricted_symbols", symbol).Err()
}

func (r *RedisRiskStore) IsKillSwitchActive(ctx context.Context, account string) (bool, error) {
	// Check global kill switch
	global, err := r.client.Get(ctx, "risk:kill_switch:global").Int()
	if err == nil && global == 1 {
		return true, nil
	}
	if account != "" {
		acctKey := fmt.Sprintf("risk:kill_switch:%s", account)
		acctVal, err := r.client.Get(ctx, acctKey).Int()
		if err == nil && acctVal == 1 {
			return true, nil
		}
	}
	return false, nil
}

func (r *RedisRiskStore) SetKillSwitch(ctx context.Context, account string, active bool) error {
	val := 0
	if active {
		val = 1
	}
	if account == "" || account == "global" {
		return r.client.Set(ctx, "risk:kill_switch:global", val, 0).Err()
	}
	return r.client.Set(ctx, fmt.Sprintf("risk:kill_switch:%s", account), val, 0).Err()
}

// =====================================================================
// Memory Risk Store (Thread-Safe In-Memory for Tests and Standalone)
// =====================================================================

type MemoryRiskStore struct {
	mu            sync.RWMutex
	dailyNotional map[string]int64
	positions     map[string]domain.Qty // key: account|symbol
	lastPrices    map[string]domain.Price
	restricted    map[string]bool
	killSwitches  map[string]bool
}

func NewMemoryRiskStore() *MemoryRiskStore {
	return &MemoryRiskStore{
		dailyNotional: make(map[string]int64),
		positions:     make(map[string]domain.Qty),
		lastPrices:    make(map[string]domain.Price),
		restricted:    make(map[string]bool),
		killSwitches:  make(map[string]bool),
	}
}

func (m *MemoryRiskStore) CheckAndIncrementDailyNotional(_ context.Context, account string, notional int64, limit int64) (bool, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	curr := m.dailyNotional[account]
	if curr+notional > limit {
		return false, curr, nil
	}
	m.dailyNotional[account] = curr + notional
	return true, m.dailyNotional[account], nil
}

func (m *MemoryRiskStore) GetPosition(_ context.Context, account string, symbol string) (domain.Qty, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.positions[account+"|"+symbol], nil
}

func (m *MemoryRiskStore) UpdatePosition(_ context.Context, account string, symbol string, deltaQty domain.Qty) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := account + "|" + symbol
	m.positions[key] = m.positions[key] + deltaQty
	return nil
}

func (m *MemoryRiskStore) GetLastPrice(_ context.Context, symbol string) (domain.Price, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lastPrices[symbol], nil
}

func (m *MemoryRiskStore) SetLastPrice(_ context.Context, symbol string, price domain.Price) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastPrices[symbol] = price
	return nil
}

func (m *MemoryRiskStore) IsSymbolRestricted(_ context.Context, symbol string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.restricted[symbol], nil
}

func (m *MemoryRiskStore) SetSymbolRestricted(_ context.Context, symbol string, restricted bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.restricted[symbol] = restricted
	return nil
}

func (m *MemoryRiskStore) IsKillSwitchActive(_ context.Context, account string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.killSwitches["global"] {
		return true, nil
	}
	if account != "" && m.killSwitches[account] {
		return true, nil
	}
	return false, nil
}

func (m *MemoryRiskStore) SetKillSwitch(_ context.Context, account string, active bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if account == "" || account == "global" {
		m.killSwitches["global"] = active
	} else {
		m.killSwitches[account] = active
	}
	return nil
}
