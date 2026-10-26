package domain

import (
	"errors"
	"fmt"
)

// Common domain errors
var (
	ErrInvalidQuantity = errors.New("order quantity must be strictly positive")
	ErrInvalidPrice    = errors.New("price must be non-negative")
	ErrInvalidSide     = errors.New("side must be BUY or SELL")
	ErrEmptyAccount    = errors.New("account cannot be empty")
	ErrEmptySymbol     = errors.New("symbol cannot be empty")
)

// Side represents the order side: BUY or SELL.
type Side string

const (
	SideBuy  Side = "BUY"
	SideSell Side = "SELL"
)

func (s Side) IsValid() bool {
	return s == SideBuy || s == SideSell
}

// OrderType represents whether an order is LIMIT or MARKET.
type OrderType string

const (
	OrderTypeLimit  OrderType = "LIMIT"
	OrderTypeMarket OrderType = "MARKET"
)

// OrderState represents the current state of an order in the OMS lifecycle.
type OrderState string

const (
	StateNone           OrderState = ""
	StatePendingNew     OrderState = "PendingNew"
	StateNew            OrderState = "New"
	StatePartiallyFilled OrderState = "PartiallyFilled"
	StateFilled         OrderState = "Filled"
	StatePendingCancel  OrderState = "PendingCancel"
	StateCancelled      OrderState = "Cancelled"
	StatePendingReplace OrderState = "PendingReplace"
	StateReplaced       OrderState = "Replaced"
	StateRejected       OrderState = "Rejected"
)

// IsTerminal returns true if the order is in a final state and cannot undergo further changes.
func (s OrderState) IsTerminal() bool {
	return s == StateFilled || s == StateCancelled || s == StateRejected
}

// Price represents price in fixed-point minor units (e.g. cents).
// $150.25 is stored as int64(15025). This prevents IEEE-754 binary floating-point errors.
type Price int64

// ToDollars converts minor units to floating dollars for display/reporting only.
func (p Price) ToDollars() float64 {
	return float64(p) / 100.0
}

// DollarsToPrice converts a dollar float to integer cents.
func DollarsToPrice(d float64) Price {
	return Price(int64(d*100.0 + 0.5))
}

func (p Price) String() string {
	return fmt.Sprintf("$%.2f", p.ToDollars())
}

// Qty represents share quantity as an exact 64-bit integer.
type Qty int64

func (q Qty) Int64() int64 {
	return int64(q)
}
