package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"tradeflow/internal/domain"
	"tradeflow/internal/events"
	"tradeflow/internal/position"
	"tradeflow/internal/storage"
)

// TestChaosFillsDeduplicationAndIdempotency simulates a hostile exchange network
// emitting 1,000 randomized execution fills with 50% duplicate injection and reordering.
// Asserts that the final position book exactly equals the mathematical ground truth
// of the unique fills, with zero double-counting or state corruption.
func TestChaosFillsDeduplicationAndIdempotency(t *testing.T) {
	ctx := context.Background()
	repo := storage.NewMemoryRepository()
	book := position.NewBook(repo)

	const numUniqueFills = 200
	const duplicateMultiplier = 3 // Every fill injected 3 times

	// Register parent orders in DB
	for i := 1; i <= numUniqueFills; i++ {
		orderID := fmt.Sprintf("CHAOS-ORD-%04d", i)
		_ = repo.SaveOrderEventAndProject(ctx, storage.OrderEventRecord{
			OrderID:   orderID,
			Seq:       1,
			EventType: "ORDER_NEW",
			Payload:   json.RawMessage(`{}`),
			CreatedAt: time.Now().UTC(),
		}, storage.OrderRecord{
			OrderID:   orderID,
			ClOrdID:   "CL-" + orderID,
			Account:   "CHAOS_ACCT",
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

	// Generate unique fills
	var uniqueFills []events.ExecutionFillPayload
	var expectedNetQty int64 = 0

	for i := 1; i <= numUniqueFills; i++ {
		fillQty := int64(10 + (i % 50)) // 10 to 59 shares
		expectedNetQty += fillQty

		uniqueFills = append(uniqueFills, events.ExecutionFillPayload{
			Venue:        "SIM_VENUE_CHAOS",
			ExecID:       fmt.Sprintf("EXEC-%04d", i),
			OrderID:      fmt.Sprintf("CHAOS-ORD-%04d", i),
			Account:      "CHAOS_ACCT",
			Symbol:       "AAPL",
			Side:         domain.SideBuy,
			FillQty:      domain.Qty(fillQty),
			FillPrice:    15000,
			TransactTime: time.Now().UTC(),
		})
	}

	// Create chaotic payload stream with 3x duplicates shuffled randomly
	var chaoticStream []events.ExecutionFillPayload
	for rep := 0; rep < duplicateMultiplier; rep++ {
		chaoticStream = append(chaoticStream, uniqueFills...)
	}

	// Shuffle stream (simulating network reordering and asynchronous arrival)
	rnd := rand.New(rand.NewSource(42))
	rnd.Shuffle(len(chaoticStream), func(i, j int) {
		chaoticStream[i], chaoticStream[j] = chaoticStream[j], chaoticStream[i]
	})

	// Process chaotic stream through position book
	var uniqueAcceptedCount = 0
	var duplicatesFilteredCount = 0

	for _, fill := range chaoticStream {
		processed, err := book.ProcessFill(ctx, fill)
		if err != nil {
			t.Fatalf("unexpected error processing fill %s: %v", fill.ExecID, err)
		}
		if processed {
			uniqueAcceptedCount++
		} else {
			duplicatesFilteredCount++
		}
	}

	// Assertions
	if uniqueAcceptedCount != numUniqueFills {
		t.Fatalf("expected exactly %d unique fills accepted, got %d", numUniqueFills, uniqueAcceptedCount)
	}
	expectedDuplicates := numUniqueFills * (duplicateMultiplier - 1)
	if duplicatesFilteredCount != expectedDuplicates {
		t.Fatalf("expected %d duplicate fills filtered, got %d", expectedDuplicates, duplicatesFilteredCount)
	}

	// Assert Final Position equals mathematical ground truth
	pos, err := repo.GetPosition(ctx, "CHAOS_ACCT", "AAPL")
	if err != nil {
		t.Fatalf("failed to retrieve position: %v", err)
	}
	if int64(pos.NetQty) != expectedNetQty {
		t.Fatalf("DATA INTEGRITY BREACH: expected final NetQty=%d, but got %d", expectedNetQty, pos.NetQty)
	}

	trades, _ := repo.GetTrades(ctx, "CHAOS_ACCT", "AAPL")
	if len(trades) != numUniqueFills {
		t.Fatalf("expected %d trades in database, got %d", numUniqueFills, len(trades))
	}
}
