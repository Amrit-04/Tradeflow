package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"pgregory.net/rapid"
	"tradeflow/internal/domain"
	"tradeflow/internal/events"
	"tradeflow/internal/position"
	"tradeflow/internal/storage"
)

// TestPositionInvarianceUnderPermutationAndDuplication asserts that for ANY
// generated set of execution fills, any arbitrary permutation, duplicate injection,
// and early-arrival (fill arriving before parent order registration),
// the final position (NetQty, AvgCost, RealizedPnL) is 100% identical.
func TestPositionInvarianceUnderPermutationAndDuplication(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		ctx := context.Background()

		account := rapid.StringMatching(`ACCT-[A-Z]{3}`).Draw(t, "account")
		symbol := rapid.SampledFrom([]string{"AAPL", "MSFT", "GOOGL", "NVDA"}).Draw(t, "symbol")

		// Number of distinct orders
		numOrders := rapid.IntRange(1, 4).Draw(t, "numOrders")
		type orderDef struct {
			orderID  string
			side     domain.Side
			orderQty domain.Qty
			price    domain.Price
		}

		orders := make([]orderDef, numOrders)
		var uniqueFills []events.ExecutionFillPayload

		for i := 0; i < numOrders; i++ {
			orderID := fmt.Sprintf("ORD-%s-%02d", symbol, i+1)
			orders[i] = orderDef{
				orderID:  orderID,
				side:     domain.SideBuy,
				orderQty: 1000,
				price:    15000,
			}

			// Generate 1 to 4 partial fills for this order
			numFills := rapid.IntRange(1, 4).Draw(t, fmt.Sprintf("numFills_%d", i))
			for f := 0; f < numFills; f++ {
				qty := rapid.Int64Range(10, 200).Draw(t, fmt.Sprintf("fillQty_%d_%d", i, f))
				price := rapid.Int64Range(10000, 30000).Draw(t, fmt.Sprintf("fillPrice_%d_%d", i, f))

				uniqueFills = append(uniqueFills, events.ExecutionFillPayload{
					Venue:        "SIM_VENUE",
					ExecID:       fmt.Sprintf("EXEC-%s-%d-%d", orderID, f, rapid.IntRange(1000, 9999).Draw(t, "id")),
					OrderID:      orderID,
					Account:      account,
					Symbol:       symbol,
					Side:         domain.SideBuy,
					FillQty:      domain.Qty(qty),
					FillPrice:    domain.Price(price),
					TransactTime: time.Now().UTC(),
				})
			}
		}

		// Helper to save order in repo
		registerOrder := func(repo storage.Repository, o orderDef) {
			_ = repo.SaveOrderEventAndProject(ctx, storage.OrderEventRecord{
				OrderID:   o.orderID,
				Seq:       1,
				EventType: "ORDER_NEW",
				Payload:   json.RawMessage(`{}`),
				CreatedAt: time.Now().UTC(),
			}, storage.OrderRecord{
				OrderID:   o.orderID,
				ClOrdID:   "CL-" + o.orderID,
				Account:   account,
				Symbol:    symbol,
				Side:      o.side,
				OrderQty:  o.orderQty,
				Price:     o.price,
				State:     domain.StateNew,
				LeavesQty: o.orderQty,
				Version:   1,
				CreatedAt: time.Now().UTC(),
				UpdatedAt: time.Now().UTC(),
			}, nil)
		}

		// =====================================================================
		// Run 1: Canonical in-order processing (all orders registered first, unique fills in order)
		// =====================================================================
		repo1 := storage.NewMemoryRepository()
		book1 := position.NewBook(repo1)

		for _, o := range orders {
			registerOrder(repo1, o)
		}
		for _, fill := range uniqueFills {
			_, err := book1.ProcessFill(ctx, fill)
			if err != nil {
				t.Fatalf("run1 process fill error: %v", err)
			}
		}
		pos1, err := repo1.GetPosition(ctx, account, symbol)
		if err != nil {
			t.Fatalf("failed to get position run1: %v", err)
		}

		// =====================================================================
		// Run 2: Chaotic processing (Fill-Before-Order, random permutations, duplicates)
		// =====================================================================
		repo2 := storage.NewMemoryRepository()
		book2 := position.NewBook(repo2)

		// Split orders into early vs late registration
		// Half the orders will have fills arrive BEFORE the order is registered
		lateRegisteredOrders := make(map[string]orderDef)
		for i, o := range orders {
			if i%2 == 1 {
				lateRegisteredOrders[o.orderID] = o
			} else {
				registerOrder(repo2, o)
			}
		}

		// Build chaotic fill stream with duplicates
		var chaoticFills []events.ExecutionFillPayload
		for _, fill := range uniqueFills {
			dupFactor := rapid.IntRange(1, 4).Draw(t, "dupFactor")
			for d := 0; d < dupFactor; d++ {
				chaoticFills = append(chaoticFills, fill)
			}
		}

		// Permute the chaotic fills
		seed := rapid.Int64().Draw(t, "shuffleSeed")
		rnd := rand.New(rand.NewSource(seed))
		rnd.Shuffle(len(chaoticFills), func(i, j int) {
			chaoticFills[i], chaoticFills[j] = chaoticFills[j], chaoticFills[i]
		})

		for _, fill := range chaoticFills {
			_, err := book2.ProcessFill(ctx, fill)
			if err != nil {
				t.Fatalf("run2 process fill error: %v", err)
			}
		}

		// Now register the late orders
		for orderID, o := range lateRegisteredOrders {
			registerOrder(repo2, o)
			_, err := book2.ResolvePendingFills(ctx, orderID)
			if err != nil {
				t.Fatalf("failed to resolve pending fills for %s: %v", orderID, err)
			}
		}

		// Replay more duplicate fills after order registration to test late deduplication
		for _, fill := range uniqueFills {
			if rapid.Bool().Draw(t, "replayLate") {
				_, err := book2.ProcessFill(ctx, fill)
				if err != nil {
					t.Fatalf("run2 late replay fill error: %v", err)
				}
			}
		}

		pos2, err := repo2.GetPosition(ctx, account, symbol)
		if err != nil {
			t.Fatalf("failed to get position run2: %v", err)
		}

		// =====================================================================
		// Invariant Assertions
		// =====================================================================
		if pos2.NetQty != pos1.NetQty {
			t.Fatalf("NetQty mismatch under permutation/duplication: baseline=%d, chaotic=%d", pos1.NetQty, pos2.NetQty)
		}
		if pos2.AvgCost != pos1.AvgCost {
			t.Fatalf("AvgCost mismatch under permutation/duplication: baseline=%d, chaotic=%d", pos1.AvgCost, pos2.AvgCost)
		}
		if pos2.RealizedPnL != pos1.RealizedPnL {
			t.Fatalf("RealizedPnL mismatch: baseline=%d, chaotic=%d", pos1.RealizedPnL, pos2.RealizedPnL)
		}

		// Check trade counts in DB
		trades1, _ := repo1.GetTrades(ctx, account, symbol)
		trades2, _ := repo2.GetTrades(ctx, account, symbol)
		if len(trades2) != len(trades1) {
			t.Fatalf("Trade count mismatch: baseline has %d trades, chaotic has %d (duplicates leaked!)", len(trades1), len(trades2))
		}
		if len(trades1) != len(uniqueFills) {
			t.Fatalf("Expected exactly %d trades, got %d", len(uniqueFills), len(trades1))
		}
	})
}
