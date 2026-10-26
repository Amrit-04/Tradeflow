package storage

import (
	"encoding/json"
	"time"

	"tradeflow/internal/domain"
)

// OrderEventRecord represents an immutable row in order_events.
type OrderEventRecord struct {
	EventID   string          `json:"event_id"`
	OrderID   string          `json:"order_id"`
	Seq       int             `json:"seq"`
	EventType string          `json:"event_type"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt time.Time       `json:"created_at"`
}

// OrderRecord represents a row in the orders read-model projection.
type OrderRecord struct {
	OrderID   string           `json:"order_id"`
	ClOrdID   string           `json:"cl_ord_id"`
	Account   string           `json:"account"`
	Symbol    string           `json:"symbol"`
	Side      domain.Side      `json:"side"`
	OrderQty  domain.Qty       `json:"order_qty"`
	Price     domain.Price     `json:"price"` // In cents
	State     domain.OrderState`json:"state"`
	CumQty    domain.Qty       `json:"cum_qty"`
	LeavesQty domain.Qty       `json:"leaves_qty"`
	AvgPrice  domain.Price     `json:"avg_price"` // In cents
	Version   int              `json:"version"`
	CreatedAt time.Time        `json:"created_at"`
	UpdatedAt time.Time        `json:"updated_at"`
}

// TradeRecord represents an executed trade in trades table.
type TradeRecord struct {
	TradeID      string       `json:"trade_id"`
	Venue        string       `json:"venue"`
	ExecID       string       `json:"exec_id"`
	OrderID      string       `json:"order_id"`
	Account      string       `json:"account"`
	Symbol       string       `json:"symbol"`
	Side         domain.Side  `json:"side"`
	ExecQty      domain.Qty   `json:"exec_qty"`
	ExecPrice    domain.Price `json:"exec_price"` // In cents
	TransactTime time.Time    `json:"transact_time"`
	CreatedAt    time.Time    `json:"created_at"`
}

// PositionRecord represents current holdings in positions table.
type PositionRecord struct {
	Account       string       `json:"account"`
	Symbol        string       `json:"symbol"`
	NetQty        domain.Qty   `json:"net_qty"`
	AvgCost       domain.Price `json:"avg_cost"`       // In cents (VWAP)
	RealizedPnL   int64        `json:"realized_pnl"`   // In cents
	UnrealizedPnL int64        `json:"unrealized_pnl"` // In cents
	UpdatedAt     time.Time    `json:"updated_at"`
}

// OutboxRecord represents an outgoing message staged in outbox table.
type OutboxRecord struct {
	OutboxID     string          `json:"outbox_id"`
	Topic        string          `json:"topic"`
	PartitionKey string          `json:"partition_key"`
	Payload      json.RawMessage `json:"payload"`
	CreatedAt    time.Time       `json:"created_at"`
	PublishedAt  *time.Time      `json:"published_at"`
}

// PendingFillRecord represents an early fill staged in pending_fills table.
type PendingFillRecord struct {
	PendingID   string          `json:"pending_id"`
	Venue       string          `json:"venue"`
	ExecID      string          `json:"exec_id"`
	OrderID     string          `json:"order_id"`
	Payload     json.RawMessage `json:"payload"`
	RetryCount  int             `json:"retry_count"`
	Status      string          `json:"status"` // PENDING, PROCESSED, FAILED
	CreatedAt   time.Time       `json:"created_at"`
	LastRetryAt *time.Time      `json:"last_retry_at"`
}

// BreakRecord represents an EOD reconciliation mismatch.
type BreakRecord struct {
	BreakID     string     `json:"break_id"`
	ReconDate   string     `json:"recon_date"` // YYYY-MM-DD
	Account     string     `json:"account"`
	Symbol      string     `json:"symbol"`
	InternalQty domain.Qty `json:"internal_qty"`
	ClearingQty domain.Qty `json:"clearing_qty"`
	BreakType   string     `json:"break_type"` // QTY_MISMATCH, MISSING_TRADE, EXTRA_TRADE
	Status      string     `json:"status"`     // OPEN, RESOLVED
	CreatedAt   time.Time  `json:"created_at"`
}
