package position_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"tradeflow/internal/domain"
	"tradeflow/internal/events"
	"tradeflow/internal/position"
	"tradeflow/internal/storage"
)

func createTestParentOrder(ctx context.Context, repo storage.Repository, orderID string) {
	_ = repo.SaveOrderEventAndProject(ctx, storage.OrderEventRecord{
		OrderID:   orderID,
		Seq:       1,
		EventType: "ORDER_NEW",
		Payload:   json.RawMessage(`{}`),
		CreatedAt: time.Now().UTC(),
	}, storage.OrderRecord{
		OrderID:   orderID,
		ClOrdID:   "CL-" + orderID,
		Account:   "ACCT-A",
		Symbol:    "AAPL",
		Side:      domain.SideBuy,
		OrderQty:  1000,
		Price:     15000,
		State:     domain.StateNew,
		LeavesQty: 1000,
		Version:   1,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}, nil)
}

func TestPositionVWAPAndRealizedPnL(t *testing.T) {
	ctx := context.Background()
	repo := storage.NewMemoryRepository()
	book := position.NewBook(repo)

	createTestParentOrder(ctx, repo, "ORD-1")

	// 1. First Buy: 100 shares @ $150.00 (15000 cents)
	fill1 := events.ExecutionFillPayload{
		Venue:        "SIM_VENUE",
		ExecID:       "EX-01",
		OrderID:      "ORD-1",
		Account:      "ACCT-A",
		Symbol:       "AAPL",
		Side:         domain.SideBuy,
		FillQty:      100,
		FillPrice:    15000,
		TransactTime: time.Now().UTC(),
	}
	processed, err := book.ProcessFill(ctx, fill1)
	if err != nil || !processed {
		t.Fatalf("failed to process fill1: %v", err)
	}

	pos, _ := repo.GetPosition(ctx, "ACCT-A", "AAPL")
	if pos.NetQty != 100 || pos.AvgCost != 15000 {
		t.Fatalf("expected NetQty=100, AvgCost=15000, got NetQty=%d, AvgCost=%d", pos.NetQty, pos.AvgCost)
	}

	// 2. Second Buy: 100 shares @ $160.00 (16000 cents)
	fill2 := events.ExecutionFillPayload{
		Venue:        "SIM_VENUE",
		ExecID:       "EX-02",
		OrderID:      "ORD-1",
		Account:      "ACCT-A",
		Symbol:       "AAPL",
		Side:         domain.SideBuy,
		FillQty:      100,
		FillPrice:    16000,
		TransactTime: time.Now().UTC(),
	}
	processed, err = book.ProcessFill(ctx, fill2)
	if err != nil || !processed {
		t.Fatalf("failed to process fill2: %v", err)
	}

	// VWAP expected: (100*15000 + 100*16000)/200 = 15500 ($155.00)
	pos, _ = repo.GetPosition(ctx, "ACCT-A", "AAPL")
	if pos.NetQty != 200 || pos.AvgCost != 15500 {
		t.Fatalf("expected NetQty=200, AvgCost=15500, got NetQty=%d, AvgCost=%d", pos.NetQty, pos.AvgCost)
	}

	// 3. Sell 50 shares @ $170.00 (17000 cents)
	fill3 := events.ExecutionFillPayload{
		Venue:        "SIM_VENUE",
		ExecID:       "EX-03",
		OrderID:      "ORD-1",
		Account:      "ACCT-A",
		Symbol:       "AAPL",
		Side:         domain.SideSell,
		FillQty:      50,
		FillPrice:    17000,
		TransactTime: time.Now().UTC(),
	}
	processed, err = book.ProcessFill(ctx, fill3)
	if err != nil || !processed {
		t.Fatalf("failed to process fill3: %v", err)
	}

	// Realized PnL expected: 50 * (17000 - 15500) = 50 * 1500 = 75000 cents ($750.00)
	// Remaining NetQty: 150 shares @ AvgCost 15500
	pos, _ = repo.GetPosition(ctx, "ACCT-A", "AAPL")
	if pos.NetQty != 150 {
		t.Fatalf("expected NetQty=150, got %d", pos.NetQty)
	}
	if pos.AvgCost != 15500 {
		t.Fatalf("expected AvgCost=15500, got %d", pos.AvgCost)
	}
	if pos.RealizedPnL != 75000 {
		t.Fatalf("expected RealizedPnL=75000, got %d", pos.RealizedPnL)
	}
}

func TestPositionIdempotencyUnderDuplicates(t *testing.T) {
	ctx := context.Background()
	repo := storage.NewMemoryRepository()
	book := position.NewBook(repo)

	createTestParentOrder(ctx, repo, "ORD-IDEM")

	fill := events.ExecutionFillPayload{
		Venue:        "SIM_VENUE",
		ExecID:       "EX-DUP-1",
		OrderID:      "ORD-IDEM",
		Account:      "ACCT-A",
		Symbol:       "NVDA",
		Side:         domain.SideBuy,
		FillQty:      200,
		FillPrice:    90000,
		TransactTime: time.Now().UTC(),
	}

	// Process 5 times with exact same venue and exec_id
	for i := 0; i < 5; i++ {
		_, _ = book.ProcessFill(ctx, fill)
	}

	pos, _ := repo.GetPosition(ctx, "ACCT-A", "NVDA")
	if pos.NetQty != 200 {
		t.Fatalf("IDEMPOTENCY BREACH: expected NetQty=200, got %d after 5 duplicate fills", pos.NetQty)
	}
	if pos.AvgCost != 90000 {
		t.Fatalf("IDEMPOTENCY BREACH: expected AvgCost=90000, got %d", pos.AvgCost)
	}

	trades, _ := repo.GetTrades(ctx, "ACCT-A", "NVDA")
	if len(trades) != 1 {
		t.Fatalf("expected exactly 1 trade record in DB, got %d", len(trades))
	}
}

func TestFillBeforeOrderResilience(t *testing.T) {
	ctx := context.Background()
	repo := storage.NewMemoryRepository()
	book := position.NewBook(repo)

	fill := events.ExecutionFillPayload{
		Venue:        "SIM_VENUE",
		ExecID:       "EX-EARLY-1",
		OrderID:      "ORD-UNREGISTERED",
		Account:      "ACCT-B",
		Symbol:       "GOOGL",
		Side:         domain.SideBuy,
		FillQty:      50,
		FillPrice:    17500,
		TransactTime: time.Now().UTC(),
	}

	// Fill arrives BEFORE order is recorded
	processed, err := book.ProcessFill(ctx, fill)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if processed {
		t.Fatalf("fill should NOT have been processed immediately without parent order")
	}

	// Assert fill is parked in pending_fills
	pending, _ := repo.GetPendingFillsByOrder(ctx, "ORD-UNREGISTERED")
	if len(pending) != 1 {
		t.Fatalf("expected 1 staged pending fill, got %d", len(pending))
	}

	// Parent order is now created and saved
	createTestParentOrder(ctx, repo, "ORD-UNREGISTERED")

	// Trigger pending resolution worker
	resolved, err := book.ResolvePendingFills(ctx, "ORD-UNREGISTERED")
	if err != nil || resolved != 1 {
		t.Fatalf("expected 1 resolved fill, got resolved=%d err=%v", resolved, err)
	}

	// Position should now reflect the fill
	pos, _ := repo.GetPosition(ctx, "ACCT-B", "GOOGL")
	if pos.NetQty != 50 || pos.AvgCost != 17500 {
		t.Fatalf("expected resolved NetQty=50, AvgCost=17500, got NetQty=%d, AvgCost=%d", pos.NetQty, pos.AvgCost)
	}
}
