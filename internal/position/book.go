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

	// 3. Update Position & P&L
	pos, err := b.repo.GetPosition(ctx, fill.Account, fill.Symbol)
	if err == storage.ErrPositionNotFound || pos == nil {
		pos = &storage.PositionRecord{
			Account:       fill.Account,
			Symbol:        fill.Symbol,
			NetQty:        0,
			AvgCost:       0,
			RealizedPnL:   0,
			UnrealizedPnL: 0,
			UpdatedAt:     time.Now().UTC(),
		}
	}

	prevNetQty := int64(pos.NetQty)
	fillQty := int64(fill.FillQty)
	fillPrice := int64(fill.FillPrice) // Cents

	if fill.Side == domain.SideBuy {
		if prevNetQty >= 0 {
			// Increasing long position: update VWAP
			totalPrevCost := prevNetQty * int64(pos.AvgCost)
			newNotional := fillQty * fillPrice
			newNetQty := prevNetQty + fillQty
			pos.NetQty = domain.Qty(newNetQty)
			pos.AvgCost = domain.Price((totalPrevCost + newNotional) / newNetQty)
		} else {
			// Covering short position: realize P&L
			coveredQty := fillQty
			if coveredQty > -prevNetQty {
				coveredQty = -prevNetQty
			}
			// Short realized PnL = coveredQty * (AvgCost - fillPrice)
			pnlDelta := coveredQty * (int64(pos.AvgCost) - fillPrice)
			pos.RealizedPnL += pnlDelta
			newNetQty := prevNetQty + fillQty
			pos.NetQty = domain.Qty(newNetQty)
			if newNetQty == 0 {
				pos.AvgCost = 0
			} else if newNetQty > 0 {
				// Flipped long
				pos.AvgCost = domain.Price(fillPrice)
			}
		}
	} else {
		// Sell Side
		if prevNetQty <= 0 {
			// Increasing short position: update short VWAP
			totalPrevCost := -prevNetQty * int64(pos.AvgCost)
			newNotional := fillQty * fillPrice
			newNetQty := prevNetQty - fillQty
			pos.NetQty = domain.Qty(newNetQty)
			pos.AvgCost = domain.Price((totalPrevCost + newNotional) / (-newNetQty))
		} else {
			// Selling from long: realize P&L
			soldQty := fillQty
			if soldQty > prevNetQty {
				soldQty = prevNetQty
			}
			// Long realized PnL = soldQty * (fillPrice - AvgCost)
			pnlDelta := soldQty * (fillPrice - int64(pos.AvgCost))
			pos.RealizedPnL += pnlDelta
			newNetQty := prevNetQty - fillQty
			pos.NetQty = domain.Qty(newNetQty)
			if newNetQty == 0 {
				pos.AvgCost = 0
			} else if newNetQty < 0 {
				// Flipped short
				pos.AvgCost = domain.Price(fillPrice)
			}
		}
	}

	// 4. Calculate Unrealized PnL against current mark price
	markPrice := int64(b.markPrices[fill.Symbol])
	if markPrice == 0 {
		markPrice = fillPrice
	}
	currentNet := int64(pos.NetQty)
	if currentNet > 0 {
		pos.UnrealizedPnL = currentNet * (markPrice - int64(pos.AvgCost))
	} else if currentNet < 0 {
		pos.UnrealizedPnL = (-currentNet) * (int64(pos.AvgCost) - markPrice)
	} else {
		pos.UnrealizedPnL = 0
	}
	pos.UpdatedAt = time.Now().UTC()

	if err := b.repo.SavePosition(ctx, *pos); err != nil {
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
