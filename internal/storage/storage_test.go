package storage_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"tradeflow/internal/domain"
	"tradeflow/internal/storage"
)

func TestStorageEventSourcingAndIdempotency(t *testing.T) {
	ctx := context.Background()
	repo := storage.NewMemoryRepository()

	// 1. Save initial order event, projection, and outbox atomically
	payload, _ := json.Marshal(map[string]any{"action": "submit", "qty": 1000})
	event1 := storage.OrderEventRecord{
		OrderID:   "ORD-101",
		Seq:       1,
		EventType: "ORDER_SUBMITTED",
		Payload:   payload,
		CreatedAt: time.Now().UTC(),
	}

	orderProj := storage.OrderRecord{
		OrderID:   "ORD-101",
		ClOrdID:   "CL-101",
		Account:   "ACCT-1",
		Symbol:    "MSFT",
		Side:      domain.SideBuy,
		OrderQty:  1000,
		Price:     40000,
		State:     domain.StatePendingNew,
		LeavesQty: 1000,
		Version:   1,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	outboxPayload, _ := json.Marshal(map[string]any{"order_id": "ORD-101"})
	outbox1 := &storage.OutboxRecord{
		OutboxID:     "OUT-1",
		Topic:        "orders.inbound",
		PartitionKey: "ORD-101",
		Payload:      outboxPayload,
		CreatedAt:    time.Now().UTC(),
	}

	err := repo.SaveOrderEventAndProject(ctx, event1, orderProj, outbox1)
	if err != nil {
		t.Fatalf("failed to save initial order event: %v", err)
	}

	// Verify order can be retrieved
	savedOrder, err := repo.GetOrder(ctx, "ORD-101")
	if err != nil || savedOrder.OrderID != "ORD-101" {
		t.Fatalf("failed to retrieve projected order: %v", err)
	}

	// Verify outbox message is unpublished
	pendingOutbox, err := repo.GetUnpublishedOutbox(ctx, 10)
	if err != nil || len(pendingOutbox) != 1 {
		t.Fatalf("expected 1 unpublished outbox record, got %d", len(pendingOutbox))
	}

	// Mark outbox published
	err = repo.MarkOutboxPublished(ctx, "OUT-1")
	if err != nil {
		t.Fatalf("failed to mark outbox published: %v", err)
	}
	pendingOutbox, _ = repo.GetUnpublishedOutbox(ctx, 10)
	if len(pendingOutbox) != 0 {
		t.Fatalf("expected 0 unpublished outbox records after publish, got %d", len(pendingOutbox))
	}

	// 2. Enforce UNIQUE(order_id, seq) - duplicate event seq must fail
	err = repo.SaveOrderEventAndProject(ctx, event1, orderProj, nil)
	if err != storage.ErrDuplicateEventSeq {
		t.Fatalf("expected ErrDuplicateEventSeq, got %v", err)
	}

	// 3. Trade Deduplication on (venue, exec_id)
	trade1 := storage.TradeRecord{
		TradeID:      "TRD-1",
		Venue:        "SIM_VENUE",
		ExecID:       "EXEC-999",
		OrderID:      "ORD-101",
		Account:      "ACCT-1",
		Symbol:       "MSFT",
		Side:         domain.SideBuy,
		ExecQty:      500,
		ExecPrice:    40000,
		TransactTime: time.Now().UTC(),
		CreatedAt:    time.Now().UTC(),
	}

	// First insert must succeed
	inserted, err := repo.SaveTrade(ctx, trade1)
	if err != nil || !inserted {
		t.Fatalf("expected trade to be inserted, got inserted=%v, err=%v", inserted, err)
	}

	// Second insert of same venue + exec_id must be deduplicated
	inserted, err = repo.SaveTrade(ctx, trade1)
	if err != nil {
		t.Fatalf("unexpected error on duplicate trade: %v", err)
	}
	if inserted {
		t.Fatalf("IDEMPOTENCY FAILURE: duplicate trade (venue, exec_id) was inserted twice!")
	}
}
