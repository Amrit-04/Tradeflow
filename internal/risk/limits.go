package risk

import "tradeflow/internal/domain"

const (
	RejectCodeMaxQty          = "RC01"
	RejectCodeMaxNotional     = "RC02"
	RejectCodePriceBand       = "RC03"
	RejectCodePositionLimit   = "RC04"
	RejectCodeDailyNotional   = "RC05"
	RejectCodeRestrictedSymbol= "RC06"
	RejectCodeKillSwitch      = "RC07"
)

// Limits holds configurable pre-trade risk thresholds.
type Limits struct {
	MaxOrderQty         domain.Qty   // e.g. 10,000 shares
	MaxOrderNotional    int64        // In cents, e.g. $1,000,000 = 100,000,000 cents
	PriceBandPercent    float64      // e.g. 0.05 (5%)
	MaxAccountPosition  domain.Qty   // e.g. 50,000 shares
	MaxDailyNotional    int64        // In cents, e.g. $5,000,000 = 500,000,000 cents
}

// DefaultLimits provides institutional standard equity default thresholds.
func DefaultLimits() Limits {
	return Limits{
		MaxOrderQty:        10000,
		MaxOrderNotional:   100000000, // $1,000,000.00
		PriceBandPercent:   0.05,      // 5.0%
		MaxAccountPosition: 50000,
		MaxDailyNotional:   500000000, // $5,000,000.00
	}
}
