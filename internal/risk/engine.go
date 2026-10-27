package risk

import (
	"context"
	"fmt"

	"tradeflow/internal/domain"
	"tradeflow/internal/events"
)

// CheckResult represents the outcome of the risk engine evaluation.
type CheckResult struct {
	Allowed    bool
	RejectCode string
	Reason     string
}

// Engine evaluates incoming orders against institutional risk limits.
type Engine struct {
	limits Limits
	store  RiskStateStore
}

func NewEngine(limits Limits, store RiskStateStore) *Engine {
	return &Engine{
		limits: limits,
		store:  store,
	}
}

// Evaluate performs all 7 pre-trade checks in strict order.
func (e *Engine) Evaluate(ctx context.Context, order events.OrderInboundPayload) CheckResult {
	// 1. Check RC07: Kill Switch
	killActive, err := e.store.IsKillSwitchActive(ctx, order.Account)
	if err == nil && killActive {
		return CheckResult{
			Allowed:    false,
			RejectCode: RejectCodeKillSwitch,
			Reason:     "Kill switch is currently active",
		}
	}

	// 2. Check RC06: Restricted Symbol
	isRestricted, err := e.store.IsSymbolRestricted(ctx, order.Symbol)
	if err == nil && isRestricted {
		return CheckResult{
			Allowed:    false,
			RejectCode: RejectCodeRestrictedSymbol,
			Reason:     fmt.Sprintf("Symbol %s is on the restricted list", order.Symbol),
		}
	}

	// 3. Check RC01: Single-Order Max Quantity
	if order.OrderQty > e.limits.MaxOrderQty {
		return CheckResult{
			Allowed:    false,
			RejectCode: RejectCodeMaxQty,
			Reason:     fmt.Sprintf("Order quantity %d exceeds maximum allowed limit %d", order.OrderQty, e.limits.MaxOrderQty),
		}
	}

	// 4. Check RC02: Single-Order Max Notional
	orderNotional := int64(order.OrderQty) * int64(order.Price)
	if orderNotional > e.limits.MaxOrderNotional {
		return CheckResult{
			Allowed:    false,
			RejectCode: RejectCodeMaxNotional,
			Reason:     fmt.Sprintf("Order notional %d cents exceeds maximum allowed limit %d cents", orderNotional, e.limits.MaxOrderNotional),
		}
	}

	// 5. Check RC03: Fat-Finger Price Band (Pure integer basis-point math)
	lastPrice, err := e.store.GetLastPrice(ctx, order.Symbol)
	if err == nil && lastPrice > 0 && e.limits.PriceBandBps > 0 {
		var diff int64
		if order.Price > lastPrice {
			diff = int64(order.Price - lastPrice)
		} else {
			diff = int64(lastPrice - order.Price)
		}
		// Integer comparison: diff / lastPrice > bps / 10000 <=> diff * 10000 > lastPrice * bps
		if diff*10000 > int64(lastPrice)*e.limits.PriceBandBps {
			return CheckResult{
				Allowed:    false,
				RejectCode: RejectCodePriceBand,
				Reason: fmt.Sprintf("Price %s deviates by more than %.1f%% (%d bps) from last price %s",
					order.Price, float64(e.limits.PriceBandBps)/100.0, e.limits.PriceBandBps, lastPrice),
			}
		}
	}

	// 6. Check RC04: Per-Account Position Limit
	currPos, err := e.store.GetPosition(ctx, order.Account, order.Symbol)
	if err == nil {
		delta := order.OrderQty
		if order.Side == domain.SideSell {
			delta = -delta
		}
		newPos := currPos + delta
		if newPos < 0 {
			newPos = -newPos
		}
		if newPos > e.limits.MaxAccountPosition {
			return CheckResult{
				Allowed:    false,
				RejectCode: RejectCodePositionLimit,
				Reason: fmt.Sprintf("Projected position %d shares breaches account limit %d",
					newPos, e.limits.MaxAccountPosition),
			}
		}
	}

	// 7. Check RC05: Per-Account Daily Cumulative Notional (Atomic Check & Increment)
	allowed, currNotional, err := e.store.CheckAndIncrementDailyNotional(ctx, order.Account, orderNotional, e.limits.MaxDailyNotional)
	if err != nil {
		return CheckResult{
			Allowed:    false,
			RejectCode: RejectCodeDailyNotional,
			Reason:     fmt.Sprintf("Failed to evaluate daily notional: %v", err),
		}
	}
	if !allowed {
		return CheckResult{
			Allowed:    false,
			RejectCode: RejectCodeDailyNotional,
			Reason: fmt.Sprintf("Order notional %d breaches daily notional limit (current %d / limit %d)",
				orderNotional, currNotional, e.limits.MaxDailyNotional),
		}
	}

	return CheckResult{Allowed: true}
}
