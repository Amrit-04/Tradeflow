package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"tradeflow/internal/domain"
	"tradeflow/internal/events"
	"tradeflow/internal/position"
	"tradeflow/internal/risk"
	"tradeflow/internal/statemachine"
	"tradeflow/internal/storage"
)

func BenchmarkStateMachineTransitions(b *testing.B) {
	ord, _ := domain.NewOrder("ORD-BENCH", "CL-BENCH", "ACCT-A", "AAPL", domain.SideBuy, domain.OrderTypeLimit, 1000000, 15000)
	_, _ = statemachine.ApplyTransition(ord, statemachine.TriggerRiskPassed, nil)

	fill := statemachine.FillEvent{
		Venue:     "SIM_VENUE",
		ExecID:    "EXEC-B",
		Qty:       1,
		Price:     15000,
		Timestamp: time.Now(),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fill.ExecID = fmt.Sprintf("EX-%d", i)
		if ord.LeavesQty <= 0 {
			ord.LeavesQty = 1000000
			ord.CumQty = 0
			ord.State = domain.StateNew
		}
		_, _ = statemachine.ApplyTransition(ord, statemachine.TriggerFill, fill)
	}
}

func BenchmarkRiskEngineEvaluate(b *testing.B) {
	ctx := context.Background()
	store := risk.NewMemoryRiskStore()
	limits := risk.DefaultLimits()
	engine := risk.NewEngine(limits, store)
	_ = store.SetLastPrice(ctx, "AAPL", 15000)

	order := events.OrderInboundPayload{
		OrderID:   "ORD-B",
		ClOrdID:   "CL-B",
		Account:   "ACCT-BENCH",
		Symbol:    "AAPL",
		Side:      domain.SideBuy,
		OrderType: domain.OrderTypeLimit,
		OrderQty:  100,
		Price:     15000,
		Timestamp: time.Now(),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = engine.Evaluate(ctx, order)
	}
}

func BenchmarkPositionBookIdempotentFills(b *testing.B) {
	ctx := context.Background()
	repo := storage.NewMemoryRepository()
	book := position.NewBook(repo)

	orderID := "ORD-BENCH-POS"
	_ = repo.SaveOrderEventAndProject(ctx, storage.OrderEventRecord{
		OrderID:   orderID,
		Seq:       1,
		EventType: "ORDER_NEW",
		CreatedAt: time.Now().UTC(),
	}, storage.OrderRecord{
		OrderID:   orderID,
		ClOrdID:   "CL-BENCH-POS",
		Account:   "ACCT-BENCH",
		Symbol:    "AAPL",
		Side:      domain.SideBuy,
		OrderQty:  10000000,
		Price:     15000,
		State:     domain.StateNew,
		LeavesQty: 10000000,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}, nil)

	fill := events.ExecutionFillPayload{
		Venue:        "SIM_VENUE",
		ExecID:       "EX-01",
		OrderID:      orderID,
		Account:      "ACCT-BENCH",
		Symbol:       "AAPL",
		Side:         domain.SideBuy,
		FillQty:      10,
		FillPrice:    15000,
		TransactTime: time.Now().UTC(),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fill.ExecID = fmt.Sprintf("EX-%d", i)
		_, _ = book.ProcessFill(ctx, fill)
	}
}
