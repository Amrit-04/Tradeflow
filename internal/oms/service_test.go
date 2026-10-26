package oms_test

import (
	"context"
	"testing"
	"time"

	"tradeflow/internal/domain"
	"tradeflow/internal/events"
	"tradeflow/internal/oms"
	"tradeflow/internal/storage"
)

func TestOMSLifecycleAndEventSourcing(t *testing.T) {
	ctx := context.Background()
	repo := storage.NewMemoryRepository()
	service := oms.NewService(repo)

	orderID := "ORD-TEST-99"

	// 1. Accept Order
	inbound := events.OrderInboundPayload{
		OrderID:   orderID,
		ClOrdID:   "CL-99",
		Account:   "ACCT-A",
		Symbol:    "AAPL",
		Side:      domain.SideBuy,
		OrderType: domain.OrderTypeLimit,
		OrderQty:  500,
		Price:     15000,
		Timestamp: time.Now().UTC(),
	}

	err := service.HandleAcceptedOrder(ctx, inbound)
	if err != nil {
		t.Fatalf("failed to handle accepted order: %v", err)
	}

	// Verify order projected as New
	ord, err := repo.GetOrder(ctx, orderID)
	if err != nil || ord.State != domain.StateNew {
		t.Fatalf("expected state New, got %s, err: %v", ord.State, err)
	}

	// Verify Outbox staged
	outbox, _ := repo.GetUnpublishedOutbox(ctx, 10)
	if len(outbox) != 1 || outbox[0].Topic != events.TopicOrdersRouted {
		t.Fatalf("expected 1 outbox record for orders.routed, got %d", len(outbox))
	}

	// 2. First Execution Fill (200 shares)
	fill1 := events.ExecutionFillPayload{
		Venue:        "SIM_VENUE",
		ExecID:       "EX-01",
		OrderID:      orderID,
		Account:      "ACCT-A",
		Symbol:       "AAPL",
		Side:         domain.SideBuy,
		FillQty:      200,
		FillPrice:    15000,
		TransactTime: time.Now().UTC(),
	}
	res, err := service.HandleExecutionFill(ctx, fill1)
	if err != nil || res.NewState != domain.StatePartiallyFilled {
		t.Fatalf("expected PartiallyFilled, got %v, err: %v", res.NewState, err)
	}

	// 3. Second Execution Fill (300 shares completing order)
	fill2 := events.ExecutionFillPayload{
		Venue:        "SIM_VENUE",
		ExecID:       "EX-02",
		OrderID:      orderID,
		Account:      "ACCT-A",
		Symbol:       "AAPL",
		Side:         domain.SideBuy,
		FillQty:      300,
		FillPrice:    15100,
		TransactTime: time.Now().UTC(),
	}
	res, err = service.HandleExecutionFill(ctx, fill2)
	if err != nil || res.NewState != domain.StateFilled {
		t.Fatalf("expected Filled, got %v, err: %v", res.NewState, err)
	}

	// 4. Verify Event Sourcing Sequence: exactly 3 events (Seq 1, 2, 3)
	eventsList, err := repo.GetOrderEvents(ctx, orderID)
	if err != nil || len(eventsList) != 3 {
		t.Fatalf("expected exactly 3 event sourcing rows, got %d", len(eventsList))
	}
	if eventsList[0].Seq != 1 || eventsList[1].Seq != 2 || eventsList[2].Seq != 3 {
		t.Fatalf("event sequence broken: %d, %d, %d", eventsList[0].Seq, eventsList[1].Seq, eventsList[2].Seq)
	}
}
