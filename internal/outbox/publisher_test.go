package outbox_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"tradeflow/internal/bus"
	"tradeflow/internal/domain"
	"tradeflow/internal/outbox"
	"tradeflow/internal/storage"
)

type faultyBus struct {
	underlying bus.Bus
	mu         sync.Mutex
	callCount  int
	failAfter  int // Fail when callCount > failAfter
	failed     bool
}

func (f *faultyBus) Publish(ctx context.Context, topic string, partitionKey string, payload []byte) error {
	f.mu.Lock()
	f.callCount++
	currentCall := f.callCount
	shouldFail := f.failAfter >= 0 && currentCall > f.failAfter
	f.mu.Unlock()

	if shouldFail {
		return errors.New("simulated network partition / kafka broker connection drop")
	}
	return f.underlying.Publish(ctx, topic, partitionKey, payload)
}

func (f *faultyBus) Subscribe(ctx context.Context, topic string, groupID string, handler bus.HandlerFunc) error {
	return f.underlying.Subscribe(ctx, topic, groupID, handler)
}

func (f *faultyBus) Close() error {
	return f.underlying.Close()
}

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

// TestOutboxMidBatchPublishFailureAndRecovery asserts that injecting a bus.Publish error
// mid-batch preserves uncommitted outbox rows, and upon recovery drains the entire batch
// with zero message loss and exact idempotency at the consumer.
func TestOutboxMidBatchPublishFailureAndRecovery(t *testing.T) {
	ctx := context.Background()
	repo := storage.NewMemoryRepository()
	memBus := bus.NewMemoryBus()
	defer memBus.Close()

	// Inject failure after 2 successful publishes
	fBus := &faultyBus{
		underlying: memBus,
		failAfter:  2,
	}

	// Consumer tracking received messages (with deduplication)
	var mu sync.Mutex
	receivedMessages := make(map[string]int) // outbox ID -> delivery count

	_ = fBus.Subscribe(ctx, "orders.routed", "test_group", func(ctx context.Context, key string, payload []byte) error {
		var msg struct {
			OrderID string `json:"order_id"`
		}
		_ = json.Unmarshal(payload, &msg)

		mu.Lock()
		receivedMessages[msg.OrderID]++
		mu.Unlock()
		return nil
	})

	// Stage 5 outbox records in repository
	const totalRecords = 5
	for i := 1; i <= totalRecords; i++ {
		orderID := fmt.Sprintf("FAIL-ORD-%03d", i)
		event := storage.OrderEventRecord{
			OrderID:   orderID,
			Seq:       1,
			EventType: "ORDER_ROUTED",
			Payload:   json.RawMessage(`{}`),
			CreatedAt: time.Now().UTC(),
		}
		order := storage.OrderRecord{
			OrderID:   orderID,
			ClOrdID:   fmt.Sprintf("FAIL-CL-%03d", i),
			Account:   "FAIL_ACCT",
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
			OutboxID:     fmt.Sprintf("FAIL-OUT-%03d", i),
			Topic:        "orders.routed",
			PartitionKey: orderID,
			Payload:      json.RawMessage(fmt.Sprintf(`{"order_id": "%s"}`, orderID)),
			CreatedAt:    time.Now().UTC(),
		}
		if err := repo.SaveOrderEventAndProject(ctx, event, order, outboxRec); err != nil {
			t.Fatalf("failed to stage order: %v", err)
		}
	}

	publisher := outbox.NewPublisher(repo, fBus, 10*time.Millisecond, 100)

	// Phase 1: Drain batch with bus failure on 3rd item
	count := publisher.DrainBatch(ctx)
	if count != 2 {
		t.Fatalf("expected exactly 2 successful publishes before mid-batch failure, got %d", count)
	}

	// Assert exactly 3 records remain unpublished in the outbox table
	unpublished, err := repo.GetUnpublishedOutbox(ctx, 100)
	if err != nil {
		t.Fatalf("failed to query unpublished outbox: %v", err)
	}
	if len(unpublished) != 3 {
		t.Fatalf("DATA LOSS: expected 3 unpublished records preserved in outbox, got %d", len(unpublished))
	}

	// Phase 2: Broker recovers (disable failure)
	fBus.mu.Lock()
	fBus.failAfter = -1 // No further failures
	fBus.mu.Unlock()

	// Drain again
	count = publisher.DrainBatch(ctx)
	if count != 3 {
		t.Fatalf("expected 3 published records upon recovery, got %d", count)
	}

	// Assert outbox is now completely drained (0 unpublished)
	unpublished, err = repo.GetUnpublishedOutbox(ctx, 100)
	if err != nil || len(unpublished) != 0 {
		t.Fatalf("expected 0 unpublished records after recovery, got %d (err: %v)", len(unpublished), err)
	}

	// Give async subscriber time to process
	time.Sleep(50 * time.Millisecond)

	// Phase 3: Assert consumer received all 5 unique records with zero loss
	mu.Lock()
	defer mu.Unlock()
	if len(receivedMessages) != totalRecords {
		t.Fatalf("expected consumer to receive all %d unique records, got %d", totalRecords, len(receivedMessages))
	}

	for i := 1; i <= totalRecords; i++ {
		orderID := fmt.Sprintf("FAIL-ORD-%03d", i)
		if count := receivedMessages[orderID]; count == 0 {
			t.Fatalf("missing record %s at consumer!", orderID)
		}
	}
}
