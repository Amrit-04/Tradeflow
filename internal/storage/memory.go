package storage

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// MemoryRepository provides a high-performance, thread-safe in-memory
// implementation of the Repository interface with strict transaction semantics.
type MemoryRepository struct {
	mu           sync.RWMutex
	orderEvents  map[string][]OrderEventRecord // key: order_id
	eventSeqs    map[string]bool               // key: order_id + ":" + seq (unique constraint)
	orders       map[string]OrderRecord        // key: order_id
	trades       map[string]TradeRecord        // key: trade_id
	venueExecIDs map[string]bool               // key: venue + ":" + exec_id (unique constraint)
	positions    map[string]PositionRecord     // key: account + ":" + symbol
	outbox       []OutboxRecord
	pendingFills map[string]PendingFillRecord  // key: venue + ":" + exec_id
	breaks       []BreakRecord
}

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{
		orderEvents:  make(map[string][]OrderEventRecord),
		eventSeqs:    make(map[string]bool),
		orders:       make(map[string]OrderRecord),
		trades:       make(map[string]TradeRecord),
		venueExecIDs: make(map[string]bool),
		positions:    make(map[string]PositionRecord),
		outbox:       make([]OutboxRecord, 0),
		pendingFills: make(map[string]PendingFillRecord),
		breaks:       make([]BreakRecord, 0),
	}
}

// SaveOrderEventAndProject atomically appends an order event, updates the order projection,
// and stages an outbox message in a single lock (simulating an ACID transaction).
func (m *MemoryRepository) SaveOrderEventAndProject(_ context.Context, event OrderEventRecord, order OrderRecord, outbox *OutboxRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Enforce UNIQUE(order_id, seq)
	seqKey := fmt.Sprintf("%s:%d", event.OrderID, event.Seq)
	if m.eventSeqs[seqKey] {
		return ErrDuplicateEventSeq
	}

	m.eventSeqs[seqKey] = true
	m.orderEvents[event.OrderID] = append(m.orderEvents[event.OrderID], event)
	m.orders[order.OrderID] = order

	if outbox != nil {
		m.outbox = append(m.outbox, *outbox)
	}
	return nil
}

func (m *MemoryRepository) GetOrder(_ context.Context, orderID string) (*OrderRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ord, exists := m.orders[orderID]
	if !exists {
		return nil, ErrOrderNotFound
	}
	return &ord, nil
}

func (m *MemoryRepository) GetOrderEvents(_ context.Context, orderID string) ([]OrderEventRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	events, exists := m.orderEvents[orderID]
	if !exists {
		return []OrderEventRecord{}, nil
	}
	copied := make([]OrderEventRecord, len(events))
	copy(copied, events)
	return copied, nil
}

func (m *MemoryRepository) SaveTrade(_ context.Context, trade TradeRecord) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Enforce UNIQUE(venue, exec_id)
	key := fmt.Sprintf("%s:%s", trade.Venue, trade.ExecID)
	if m.venueExecIDs[key] {
		return false, nil // Deduplicated, not inserted
	}

	m.venueExecIDs[key] = true
	m.trades[trade.TradeID] = trade
	return true, nil
}

func (m *MemoryRepository) GetTrades(_ context.Context, account string, symbol string) ([]TradeRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []TradeRecord
	for _, t := range m.trades {
		if t.Account == account && t.Symbol == symbol {
			result = append(result, t)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].TransactTime.Before(result[j].TransactTime)
	})
	return result, nil
}

func (m *MemoryRepository) GetAllTrades(_ context.Context) ([]TradeRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]TradeRecord, 0, len(m.trades))
	for _, t := range m.trades {
		result = append(result, t)
	}
	return result, nil
}

func (m *MemoryRepository) GetPosition(_ context.Context, account string, symbol string) (*PositionRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := account + ":" + symbol
	pos, exists := m.positions[key]
	if !exists {
		return nil, ErrPositionNotFound
	}
	return &pos, nil
}

func (m *MemoryRepository) SavePosition(_ context.Context, pos PositionRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := pos.Account + ":" + pos.Symbol
	m.positions[key] = pos
	return nil
}

func (m *MemoryRepository) GetAllPositions(_ context.Context) ([]PositionRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]PositionRecord, 0, len(m.positions))
	for _, p := range m.positions {
		result = append(result, p)
	}
	return result, nil
}

func (m *MemoryRepository) GetUnpublishedOutbox(_ context.Context, limit int) ([]OutboxRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []OutboxRecord
	for _, o := range m.outbox {
		if o.PublishedAt == nil {
			result = append(result, o)
			if limit > 0 && len(result) >= limit {
				break
			}
		}
	}
	return result, nil
}

func (m *MemoryRepository) MarkOutboxPublished(_ context.Context, outboxID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now().UTC()
	for i := range m.outbox {
		if m.outbox[i].OutboxID == outboxID {
			m.outbox[i].PublishedAt = &now
			return nil
		}
	}
	return nil
}

func (m *MemoryRepository) SavePendingFill(_ context.Context, fill PendingFillRecord) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := fmt.Sprintf("%s:%s", fill.Venue, fill.ExecID)
	if _, exists := m.pendingFills[key]; exists {
		return false, nil // Already staged
	}
	m.pendingFills[key] = fill
	return true, nil
}

func (m *MemoryRepository) GetPendingFillsByOrder(_ context.Context, orderID string) ([]PendingFillRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []PendingFillRecord
	for _, pf := range m.pendingFills {
		if pf.OrderID == orderID && pf.Status == "PENDING" {
			result = append(result, pf)
		}
	}
	return result, nil
}

func (m *MemoryRepository) MarkPendingFillProcessed(_ context.Context, pendingID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for k, pf := range m.pendingFills {
		if pf.PendingID == pendingID {
			pf.Status = "PROCESSED"
			m.pendingFills[k] = pf
			return nil
		}
	}
	return nil
}

func (m *MemoryRepository) SaveBreak(_ context.Context, b BreakRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.breaks = append(m.breaks, b)
	return nil
}

func (m *MemoryRepository) GetBreaksByDate(_ context.Context, date string) ([]BreakRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []BreakRecord
	for _, b := range m.breaks {
		if b.ReconDate == date {
			result = append(result, b)
		}
	}
	return result, nil
}
