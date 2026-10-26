package domain

import (
	"fmt"
	"time"
)

// Order represents an equity order in the Order Management System (OMS).
type Order struct {
	OrderID   string     `json:"order_id"`
	ClOrdID   string     `json:"cl_ord_id"`
	Account   string     `json:"account"`
	Symbol    string     `json:"symbol"`
	Side      Side       `json:"side"`
	OrderType OrderType  `json:"order_type"`
	OrderQty  Qty        `json:"order_qty"`
	Price     Price      `json:"price"` // In cents
	State     OrderState `json:"state"`
	CumQty    Qty        `json:"cum_qty"`
	LeavesQty Qty        `json:"leaves_qty"`
	AvgPrice  Price      `json:"avg_price"` // In cents
	Version   int        `json:"version"`
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
}

// NewOrder constructs a validated new Order in PendingNew state.
func NewOrder(
	orderID string,
	clOrdID string,
	account string,
	symbol string,
	side Side,
	orderType OrderType,
	qty Qty,
	price Price,
) (*Order, error) {
	if orderID == "" {
		return nil, fmt.Errorf("order_id cannot be empty")
	}
	if clOrdID == "" {
		return nil, fmt.Errorf("cl_ord_id cannot be empty")
	}
	if account == "" {
		return nil, ErrEmptyAccount
	}
	if symbol == "" {
		return nil, ErrEmptySymbol
	}
	if !side.IsValid() {
		return nil, ErrInvalidSide
	}
	if qty <= 0 {
		return nil, ErrInvalidQuantity
	}
	if price < 0 {
		return nil, ErrInvalidPrice
	}

	now := time.Now().UTC()
	return &Order{
		OrderID:   orderID,
		ClOrdID:   clOrdID,
		Account:   account,
		Symbol:    symbol,
		Side:      side,
		OrderType: orderType,
		OrderQty:  qty,
		Price:     price,
		State:     StatePendingNew,
		CumQty:    0,
		LeavesQty: qty,
		AvgPrice:  0,
		Version:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}, nil
}

// TotalNotional returns total order notional in minor units (cents).
func (o *Order) TotalNotional() int64 {
	return int64(o.OrderQty) * int64(o.Price)
}
