package position

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"tradeflow/internal/domain"
	"tradeflow/internal/events"
	"tradeflow/internal/storage"
)

// Book computes real-time positions, average costs, and P&L.
type Book struct {
	mu         sync.Mutex
	repo       storage.Repository
	markPrices map[string]domain.Price // key: symbol
}

func NewBook(repo storage.Repository) *Book {
	return &Book{
		repo:       repo,
		markPrices: make(map[string]domain.Price),
	}
}

func (b *Book) SetMarkPrice(symbol string, price domain.Price) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.markPrices[symbol] = price
}

func (b *Book) GetMarkPrice(symbol string) domain.Price {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.markPrices[symbol]
}

// ProcessFill handles an incoming execution fill with strict deduplication and accounting.
func (b *Book) ProcessFill(ctx context.Context, fill events.ExecutionFillPayload) (processed bool, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// 1. Check if parent order exists (Fill-Before-Order handling)
	_, err = b.repo.GetOrder(ctx, fill.OrderID)
	if err == storage.ErrOrderNotFound {
		// Park early fill in pending_fills table
		payloadBytes, _ := json.Marshal(fill)
		pending := storage.PendingFillRecord{
			PendingID: fmt.Sprintf("PEND-%s-%s", fill.Venue, fill.ExecID),
			Venue:     fill.Venue,
			ExecID:    fill.ExecID,
			OrderID:   fill.OrderID,
			Payload:   payloadBytes,
			Status:    "PENDING",
			CreatedAt: time.Now().UTC(),
		}
		_, saveErr := b.repo.SavePendingFill(ctx, pending)
		return false, saveErr
	}

	// 2. Enforce Idempotency via trades table unique constraint
	trade := storage.TradeRecord{
		TradeID:      fmt.Sprintf("TRD-%s-%s", fill.Venue, fill.ExecID),
		Venue:        fill.Venue,
		ExecID:       fill.ExecID,
		OrderID:      fill.OrderID,
		Account:      fill.Account,
		Symbol:       fill.Symbol,
		Side:         fill.Side,
		ExecQty:      fill.FillQty,
		ExecPrice:    fill.FillPrice,
		TransactTime: fill.TransactTime,
		CreatedAt:    time.Now().UTC(),
	}

	inserted, err := b.repo.SaveTrade(ctx, trade)
	if err != nil {
		return false, fmt.Errorf("failed to record trade: %w", err)
	}
	if !inserted {
		// Duplicate execution detected! Idempotently ignored.
		return false, nil
	}

	// 3. Update Position & P&L from deduplicated trades
	trades, err := b.repo.GetTrades(ctx, fill.Account, fill.Symbol)
	if err != nil {
		return false, fmt.Errorf("failed to get trades: %w", err)
	}

	pos := storage.PositionRecord{
		Account:   fill.Account,
		Symbol:    fill.Symbol,
		UpdatedAt: time.Now().UTC(),
	}

	var netQty int64 = 0
	var totalCost int64 = 0
	var realizedPnL int64 = 0

	for _, t := range trades {
		tQty := int64(t.ExecQty)
		tPrice := int64(t.ExecPrice)

		if t.Side == domain.SideBuy {
			if netQty >= 0 {
				totalCost += tQty * tPrice
				netQty += tQty
			} else {
				coveredQty := tQty
				if coveredQty > -netQty {
					coveredQty = -netQty
				}
				avgCost := totalCost / (-netQty)
				realizedPnL += coveredQty * (avgCost - tPrice)
				netQty += tQty
				if netQty > 0 {
					totalCost = netQty * tPrice
				} else if netQty == 0 {
					totalCost = 0
				} else {
					totalCost = (-netQty) * avgCost
				}
			}
		} else {
			if netQty <= 0 {
				totalCost += tQty * tPrice
				netQty -= tQty
			} else {
				soldQty := tQty
				if soldQty > netQty {
					soldQty = netQty
				}
				avgCost := totalCost / netQty
				realizedPnL += soldQty * (tPrice - avgCost)
				netQty -= tQty
				if netQty < 0 {
					totalCost = (-netQty) * tPrice
				} else if netQty == 0 {
					totalCost = 0
				} else {
					totalCost = netQty * avgCost
				}
			}
		}
	}

	pos.NetQty = domain.Qty(netQty)
	if netQty != 0 {
		if netQty > 0 {
			pos.AvgCost = domain.Price(totalCost / netQty)
		} else {
			pos.AvgCost = domain.Price(totalCost / (-netQty))
		}
	} else {
		pos.AvgCost = 0
	}
	pos.RealizedPnL = realizedPnL

	// 4. Calculate Unrealized PnL against current mark price
	markPrice := int64(b.markPrices[fill.Symbol])
	if markPrice == 0 {
		markPrice = int64(fill.FillPrice)
	}
	if netQty > 0 {
		pos.UnrealizedPnL = netQty * (markPrice - int64(pos.AvgCost))
	} else if netQty < 0 {
		pos.UnrealizedPnL = (-netQty) * (int64(pos.AvgCost) - markPrice)
	} else {
		pos.UnrealizedPnL = 0
	}

	if err := b.repo.SavePosition(ctx, pos); err != nil {
		return false, fmt.Errorf("failed to save position: %w", err)
	}

	return true, nil
}

// ResolvePendingFills replays parked fills for a newly created order.
func (b *Book) ResolvePendingFills(ctx context.Context, orderID string) (int, error) {
	pendingList, err := b.repo.GetPendingFillsByOrder(ctx, orderID)
	if err != nil {
		return 0, err
	}

	resolvedCount := 0
	for _, p := range pendingList {
		var fill events.ExecutionFillPayload
		if err := json.Unmarshal(p.Payload, &fill); err != nil {
			continue
		}
		processed, err := b.ProcessFill(ctx, fill)
		if err == nil && processed {
			_ = b.repo.MarkPendingFillProcessed(ctx, p.PendingID)
			resolvedCount++
		}
	}
	return resolvedCount, nil
}
