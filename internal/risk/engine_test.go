package risk_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"tradeflow/internal/domain"
	"tradeflow/internal/events"
	"tradeflow/internal/risk"
)

func TestRiskChecksIndividual(t *testing.T) {
	ctx := context.Background()
	store := risk.NewMemoryRiskStore()
	limits := risk.DefaultLimits()
	limits.MaxOrderQty = 1000
	limits.MaxOrderNotional = 15000000 // $150,000 in cents
	limits.PriceBandPercent = 0.05     // 5%
	limits.MaxAccountPosition = 2000
	limits.MaxDailyNotional = 20000000 // $200,000

	engine := risk.NewEngine(limits, store)
	_ = store.SetLastPrice(ctx, "AAPL", 15000) // $150.00

	baseOrder := events.OrderInboundPayload{
		OrderID:   "ORD-1",
		ClOrdID:   "CL-1",
		Account:   "ACCT-A",
		Symbol:    "AAPL",
		Side:      domain.SideBuy,
		OrderType: domain.OrderTypeLimit,
		OrderQty:  100,
		Price:     15000,
		Timestamp: time.Now(),
	}

	// 1. Valid order passes
	res := engine.Evaluate(ctx, baseOrder)
	if !res.Allowed {
		t.Fatalf("expected order to pass risk, got: %s %s", res.RejectCode, res.Reason)
	}

	// 2. RC01: Max Qty
	qtyOrder := baseOrder
	qtyOrder.OrderQty = 1500
	res = engine.Evaluate(ctx, qtyOrder)
	if res.Allowed || res.RejectCode != risk.RejectCodeMaxQty {
		t.Fatalf("expected RC01, got allowed=%v code=%s", res.Allowed, res.RejectCode)
	}

	// 3. RC02: Max Notional
	notionalOrder := baseOrder
	notionalOrder.Price = 200000 // $2,000 * 100 = $200,000 > $150,000
	res = engine.Evaluate(ctx, notionalOrder)
	if res.Allowed || res.RejectCode != risk.RejectCodeMaxNotional {
		t.Fatalf("expected RC02, got allowed=%v code=%s", res.Allowed, res.RejectCode)
	}

	// 4. RC03: Price Band (Fat finger)
	bandOrder := baseOrder
	bandOrder.Price = 16000 // $160 vs $150 is 6.6% deviation > 5%
	res = engine.Evaluate(ctx, bandOrder)
	if res.Allowed || res.RejectCode != risk.RejectCodePriceBand {
		t.Fatalf("expected RC03, got allowed=%v code=%s", res.Allowed, res.RejectCode)
	}

	// 5. RC04: Position limit
	_ = store.UpdatePosition(ctx, "ACCT-A", "AAPL", 1950)
	posOrder := baseOrder
	posOrder.OrderQty = 100 // 1950 + 100 = 2050 > 2000
	res = engine.Evaluate(ctx, posOrder)
	if res.Allowed || res.RejectCode != risk.RejectCodePositionLimit {
		t.Fatalf("expected RC04, got allowed=%v code=%s", res.Allowed, res.RejectCode)
	}

	// 6. RC06: Restricted Symbol
	_ = store.SetSymbolRestricted(ctx, "SANCTIONED_EQUITY", true)
	restrOrder := baseOrder
	restrOrder.Symbol = "SANCTIONED_EQUITY"
	res = engine.Evaluate(ctx, restrOrder)
	if res.Allowed || res.RejectCode != risk.RejectCodeRestrictedSymbol {
		t.Fatalf("expected RC06, got allowed=%v code=%s", res.Allowed, res.RejectCode)
	}

	// 7. RC07: Kill Switch
	_ = store.SetKillSwitch(ctx, "global", true)
	res = engine.Evaluate(ctx, baseOrder)
	if res.Allowed || res.RejectCode != risk.RejectCodeKillSwitch {
		t.Fatalf("expected RC07, got allowed=%v code=%s", res.Allowed, res.RejectCode)
	}
}

// TestRiskConcurrentDailyLimit asserts that 50 concurrent goroutines racing
// to consume a shared daily limit cannot over-allocate or bypass limits.
func TestRiskConcurrentDailyLimit(t *testing.T) {
	ctx := context.Background()
	store := risk.NewMemoryRiskStore()
	limits := risk.DefaultLimits()
	limits.MaxOrderQty = 1000
	limits.MaxDailyNotional = 1000000 // $10,000 limit

	engine := risk.NewEngine(limits, store)

	const numGoroutines = 50
	const orderNotional = 100000 // $1,000 per order
	// At $1,000 per order and $10,000 limit, exactly 10 orders must be approved and 40 rejected.

	var wg sync.WaitGroup
	var mu sync.Mutex
	approvedCount := 0
	rejectedCount := 0

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			order := events.OrderInboundPayload{
				OrderID:   "CONCURR-ORD",
				Account:   "SHARED_ACCT",
				Symbol:    "AAPL",
				Side:      domain.SideBuy,
				OrderQty:  10,
				Price:     10000, // 10 * 10,000 = 100,000 cents = $1,000
				Timestamp: time.Now(),
			}

			res := engine.Evaluate(ctx, order)
			mu.Lock()
			if res.Allowed {
				approvedCount++
			} else {
				rejectedCount++
			}
			mu.Unlock()
		}(i)
	}

	wg.Wait()

	if approvedCount != 10 {
		t.Fatalf("CONCURRENCY BUG: expected exactly 10 approved orders, got %d", approvedCount)
	}
	if rejectedCount != 40 {
		t.Fatalf("CONCURRENCY BUG: expected exactly 40 rejected orders, got %d", rejectedCount)
	}
}
