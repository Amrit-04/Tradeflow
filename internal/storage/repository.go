package storage

import (
	"context"
	"errors"
)

var (
	ErrOrderNotFound     = errors.New("order not found")
	ErrPositionNotFound  = errors.New("position not found")
	ErrDuplicateEventSeq = errors.New("duplicate order event sequence")
	ErrDuplicateTrade    = errors.New("duplicate trade execution")
)

// Repository encapsulates all database access for Tradeflow.
type Repository interface {
	// Order & Event Sourcing (Atomic Transaction)
	SaveOrderEventAndProject(ctx context.Context, event OrderEventRecord, order OrderRecord, outbox *OutboxRecord) error
	GetOrder(ctx context.Context, orderID string) (*OrderRecord, error)
	GetOrderEvents(ctx context.Context, orderID string) ([]OrderEventRecord, error)

	// Trades (Idempotent Execution Recording)
	SaveTrade(ctx context.Context, trade TradeRecord) (inserted bool, err error)
	GetTrades(ctx context.Context, account string, symbol string) ([]TradeRecord, error)
	GetAllTrades(ctx context.Context) ([]TradeRecord, error)

	// Positions (Current Real-Time Holdings)
	GetPosition(ctx context.Context, account string, symbol string) (*PositionRecord, error)
	SavePosition(ctx context.Context, pos PositionRecord) error
	GetAllPositions(ctx context.Context) ([]PositionRecord, error)

	// Transactional Outbox
	GetUnpublishedOutbox(ctx context.Context, limit int) ([]OutboxRecord, error)
	MarkOutboxPublished(ctx context.Context, outboxID string) error

	// Pending Fills (Staging for Early / Out-of-Order Execution Fills)
	SavePendingFill(ctx context.Context, fill PendingFillRecord) (inserted bool, err error)
	GetPendingFillsByOrder(ctx context.Context, orderID string) ([]PendingFillRecord, error)
	MarkPendingFillProcessed(ctx context.Context, pendingID string) error

	// Reconciliation Breaks
	SaveBreak(ctx context.Context, b BreakRecord) error
	GetBreaksByDate(ctx context.Context, date string) ([]BreakRecord, error)
}
