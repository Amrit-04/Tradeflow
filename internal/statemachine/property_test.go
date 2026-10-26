package statemachine_test

import (
	"fmt"
	"testing"
	"time"

	"pgregory.net/rapid"
	"tradeflow/internal/domain"
	"tradeflow/internal/statemachine"
)

// TestStateMachineInvariantsProperty uses property-based testing to verify that
// for ANY sequence of random actions, the state machine invariants hold:
// 1. cum_qty <= order_qty at all times (no overfilling)
// 2. Once in a terminal state (Filled, Cancelled, Rejected), no further mutations succeed
// 3. leaves_qty + cum_qty == order_qty (or leaves_qty == 0 if cancelled)
func TestStateMachineInvariantsProperty(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate an initial random order
		qtyVal := rapid.Int64Range(1, 100000).Draw(t, "qty")
		priceVal := rapid.Int64Range(1, 500000).Draw(t, "price") // in cents
		orderID := rapid.StringMatching(`ORD-[0-9]{3,6}`).Draw(t, "orderID")

		order, err := domain.NewOrder(
			orderID,
			"CL-"+orderID,
			"ACCT-1",
			"AAPL",
			domain.SideBuy,
			domain.OrderTypeLimit,
			domain.Qty(qtyVal),
			domain.Price(priceVal),
		)
		if err != nil {
			t.Fatalf("failed to create order: %v", err)
		}

		numSteps := rapid.IntRange(1, 50).Draw(t, "numSteps")
		wasTerminal := false
		terminalState := domain.StateNone

		for i := 0; i < numSteps; i++ {
			triggerType := rapid.IntRange(0, 7).Draw(t, fmt.Sprintf("trigger_%d", i))
			var trigger statemachine.Trigger
			var payload any

			switch triggerType {
			case 0:
				trigger = statemachine.TriggerRiskPassed
			case 1:
				trigger = statemachine.TriggerRiskRejected
			case 2:
				trigger = statemachine.TriggerFill
				// Generate random fill quantity (could be valid or intentionally overfill)
				fillQty := rapid.Int64Range(-10, qtyVal+50).Draw(t, fmt.Sprintf("fill_qty_%d", i))
				fillPrice := rapid.Int64Range(0, 1000000).Draw(t, fmt.Sprintf("fill_price_%d", i))
				payload = statemachine.FillEvent{
					Venue:     "SIM_VENUE",
					ExecID:    fmt.Sprintf("EXEC-%d-%d", i, fillQty),
					Qty:       domain.Qty(fillQty),
					Price:     domain.Price(fillPrice),
					Timestamp: time.Now(),
				}
			case 3:
				trigger = statemachine.TriggerCancelRequest
			case 4:
				trigger = statemachine.TriggerCancelAck
			case 5:
				trigger = statemachine.TriggerCancelReject
			case 6:
				trigger = statemachine.TriggerReplaceRequest
			case 7:
				trigger = statemachine.TriggerReplaceAck
			}

			cumBefore := order.CumQty

			_, err := statemachine.ApplyTransition(order, trigger, payload)

			// Invariant Checks
			if wasTerminal {
				// Once in terminal state, transitions must fail
				if err == nil {
					t.Fatalf("expected error when applying %s to terminal state %s, but got none", trigger, terminalState)
				}
				if order.State != terminalState {
					t.Fatalf("terminal state changed from %s to %s", terminalState, order.State)
				}
				if order.CumQty != cumBefore {
					t.Fatalf("cum_qty changed from %d to %d after terminal state reached", cumBefore, order.CumQty)
				}
			}

			// Invariant 1: cum_qty <= order_qty
			if order.CumQty > order.OrderQty {
				t.Fatalf("OVERFILL INVARIANT VIOLATED: cum_qty (%d) > order_qty (%d)", order.CumQty, order.OrderQty)
			}

			// Invariant 2: cum_qty >= 0 and leaves_qty >= 0
			if order.CumQty < 0 || order.LeavesQty < 0 {
				t.Fatalf("NEGATIVE QUANTITY INVARIANT VIOLATED: cum_qty=%d, leaves_qty=%d", order.CumQty, order.LeavesQty)
			}

			// Invariant 3: if not cancelled, leaves + cum == order_qty
			if order.State != domain.StateCancelled && order.CumQty+order.LeavesQty != order.OrderQty {
				t.Fatalf("QUANTITY CONSERVATION VIOLATED: cum (%d) + leaves (%d) != order_qty (%d)",
					order.CumQty, order.LeavesQty, order.OrderQty)
			}

			if order.State.IsTerminal() {
				wasTerminal = true
				terminalState = order.State
			}
		}
	})
}
