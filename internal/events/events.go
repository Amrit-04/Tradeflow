package events

import (
	"encoding/json"
	"time"

	"tradeflow/internal/domain"
)

const (
	TopicOrdersInbound  = "orders.inbound"
	TopicOrdersAccepted = "orders.accepted"
	TopicOrdersRejected = "orders.rejected"
	TopicOrdersRouted   = "orders.routed"
	TopicMarketFills    = "market.fills"
)

// EventType enumerates domain event types for event sourcing and messaging.
type EventType string

const (
	EventOrderSubmitted   EventType = "ORDER_SUBMITTED"
	EventOrderAccepted    EventType = "ORDER_ACCEPTED"
	EventOrderRejected    EventType = "ORDER_REJECTED"
	EventOrderRouted      EventType = "ORDER_ROUTED"
	EventExecutionFill    EventType = "EXECUTION_FILL"
	EventOrderCancelled   EventType = "ORDER_CANCELLED"
	EventCancelRejected   EventType = "CANCEL_REJECTED"
	EventOrderReplaced    EventType = "ORDER_REPLACED"
)

// EventEnvelope provides a standardized container for messages across Kafka and storage.
type EventEnvelope struct {
	EventID       string          `json:"event_id"`
	EventType     EventType       `json:"event_type"`
	OrderID       string          `json:"order_id"`
	PartitionKey  string          `json:"partition_key"`
	Sequence      int             `json:"seq"`
	Timestamp     time.Time       `json:"timestamp"`
	Payload       json.RawMessage `json:"payload"`
}

// OrderInboundPayload represents the raw order accepted at the FIX Gateway.
type OrderInboundPayload struct {
	OrderID   string           `json:"order_id"`
	ClOrdID   string           `json:"cl_ord_id"`
	Account   string           `json:"account"`
	Symbol    string           `json:"symbol"`
	Side      domain.Side      `json:"side"`
	OrderType domain.OrderType `json:"order_type"`
	OrderQty  domain.Qty       `json:"order_qty"`
	Price     domain.Price     `json:"price"` // Minor units (cents)
	Timestamp time.Time        `json:"timestamp"`
}

// OrderRejectedPayload represents an order rejected by the Pre-Trade Risk Engine.
type OrderRejectedPayload struct {
	OrderID    string    `json:"order_id"`
	ClOrdID    string    `json:"cl_ord_id"`
	Account    string    `json:"account"`
	Symbol     string    `json:"symbol"`
	RejectCode string    `json:"reject_code"`
	Reason     string    `json:"reason"`
	Timestamp  time.Time `json:"timestamp"`
}

// OrderRoutedPayload represents an approved order transmitted by OMS to execution venue.
type OrderRoutedPayload struct {
	OrderID   string           `json:"order_id"`
	ClOrdID   string           `json:"cl_ord_id"`
	Account   string           `json:"account"`
	Symbol    string           `json:"symbol"`
	Side      domain.Side      `json:"side"`
	OrderQty  domain.Qty       `json:"order_qty"`
	Price     domain.Price     `json:"price"`
	Timestamp time.Time        `json:"timestamp"`
}

// ExecutionFillPayload represents an execution report received from the venue simulator.
type ExecutionFillPayload struct {
	Venue        string       `json:"venue"`
	ExecID       string       `json:"exec_id"`
	OrderID      string       `json:"order_id"`
	Account      string       `json:"account"`
	Symbol       string       `json:"symbol"`
	Side         domain.Side  `json:"side"`
	FillQty      domain.Qty   `json:"fill_qty"`
	FillPrice    domain.Price `json:"fill_price"` // Minor units (cents)
	TransactTime time.Time    `json:"transact_time"`
}
