package outbox_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"tradeflow/internal/bus"
	"tradeflow/internal/domain"
	"tradeflow/internal/outbox"
	"tradeflow/internal/storage"
)

func TestOutboxPublisherDrain(t *testing.T) {
	ctx := context.Background()
	repo := storage.NewMemoryRepository()
	msgBus := bus.NewMemoryBus()
	defer msgBus.Close()

	// Stage 5 orders in outbox
	for i := 1; i <= 5; i++ {
		orderID := fmt.Sprintf("ORD-%03d", i)
		event := storage.OrderEventRecord{
			OrderID:   orderID,
			Seq:       1,
			EventType: "ORDER_ROUTED",
			Payload:   json.RawMessage(`{"routed": true}`),
			CreatedAt: time.Now().UTC(),
		}
		order := storage.OrderRecord{
			OrderID:   orderID,
			ClOrdID:   fmt.Sprintf("CL-%03d", i),
			Account:   "ACCT-A",
			Symbol:    "AAPL",
			Side:      domain.SideBuy,
			OrderQty:  100,
			Price:     15000,
			State:     domain.StateNew,
			LeavesQty: 100,
			Version:   1,
			CreatedAt: time.Now().UTC(),
			UpdatedAt: time.Now().UTC(),
		}
		outboxRec := &storage.OutboxRecord{
			OutboxID:     fmt.Sprintf("OUT-%03d", i),
			Topic:        "orders.routed",
			PartitionKey: orderID,
			Payload:      json.RawMessage(fmt.Sprintf(`{"order_id": "%s"}`, orderID)),
			CreatedAt:    time.Now().UTC(),
		}
		_ = repo.SaveOrderEventAndProject(ctx, event, order, outboxRec)
	}

	publisher := outbox.NewPublisher(repo, msgBus, 10*time.Millisecond, 100)

	// First drain should publish all 5
	count := publisher.DrainBatch(ctx)
	if count != 5 {
		t.Fatalf("expected 5 published outbox messages, got %d", count)
	}

	// Second drain should find 0
	count = publisher.DrainBatch(ctx)
	if count != 0 {
		t.Fatalf("expected 0 published outbox messages on second drain, got %d", count)
	}
}
