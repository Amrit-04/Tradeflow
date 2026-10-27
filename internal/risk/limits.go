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

// Limits holds configurable pre-trade risk thresholds using pure integer arithmetic.
type Limits struct {
	MaxOrderQty         domain.Qty   // e.g. 10,000 shares
	MaxOrderNotional    int64        // In minor units (cents), e.g. $1,000,000 = 100,000,000 cents
	PriceBandBps        int64        // In basis points, e.g. 500 bps = 5.00% (eliminates float64 rounding)
	MaxAccountPosition  domain.Qty   // e.g. 50,000 shares
	MaxDailyNotional    int64        // In minor units (cents), e.g. $5,000,000 = 500,000,000 cents
}

// DefaultLimits provides institutional standard equity default thresholds.
func DefaultLimits() Limits {
	return Limits{
		MaxOrderQty:        10000,
		MaxOrderNotional:   100000000, // $1,000,000.00
		PriceBandBps:       500,        // 500 bps = 5.0%
		MaxAccountPosition: 50000,
		MaxDailyNotional:   500000000, // $5,000,000.00
	}
}
