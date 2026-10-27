package statemachine_test

import (
	"errors"
	"testing"
	"time"

	"tradeflow/internal/domain"
	"tradeflow/internal/statemachine"
)

func createTestOrder(t *testing.T, qty domain.Qty, price domain.Price) *domain.Order {
	t.Helper()
	ord, err := domain.NewOrder(
		"ORD-001",
		"CLORD-001",
		"ACCT-ALPHA",
		"AAPL",
		domain.SideBuy,
		domain.OrderTypeLimit,
		qty,
		price,
	)
	if err != nil {
		t.Fatalf("failed to create test order: %v", err)
	}
	return ord
}

func TestNormalFillLifecycle(t *testing.T) {
	ord := createTestOrder(t, 1000, 15000) // 1000 shares @ $150.00

	// 1. Initial state
	if ord.State != domain.StatePendingNew {
		t.Fatalf("expected state PendingNew, got %s", ord.State)
	}

	// 2. Risk Passed -> New
	res, err := statemachine.ApplyTransition(ord, statemachine.TriggerRiskPassed, nil)
	if err != nil {
		t.Fatalf("unexpected error on RiskPassed: %v", err)
	}
	if ord.State != domain.StateNew || res.NewState != domain.StateNew {
		t.Fatalf("expected state New, got %s", ord.State)
	}

	// 3. First Partial Fill (400 shares @ $150.00)
	res, err = statemachine.ApplyTransition(ord, statemachine.TriggerFill, statemachine.FillEvent{
		Venue:     "SIM_VENUE",
		ExecID:    "EXEC-001",
		Qty:       400,
		Price:     15000,
		Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatalf("unexpected error on partial fill: %v", err)
	}
	if ord.State != domain.StatePartiallyFilled {
		t.Fatalf("expected state PartiallyFilled, got %s", ord.State)
	}
	if ord.CumQty != 400 || ord.LeavesQty != 600 {
		t.Fatalf("expected CumQty=400, LeavesQty=600, got CumQty=%d, LeavesQty=%d", ord.CumQty, ord.LeavesQty)
	}
	if ord.AvgPrice != 15000 {
		t.Fatalf("expected AvgPrice=15000, got %d", ord.AvgPrice)
	}

	// 4. Second Fill completing the order (600 shares @ $151.00)
	res, err = statemachine.ApplyTransition(ord, statemachine.TriggerFill, statemachine.FillEvent{
		Venue:     "SIM_VENUE",
		ExecID:    "EXEC-002",
		Qty:       600,
		Price:     15100,
		Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatalf("unexpected error on final fill: %v", err)
	}
	if ord.State != domain.StateFilled {
		t.Fatalf("expected state Filled, got %s", ord.State)
	}
	if ord.CumQty != 1000 || ord.LeavesQty != 0 {
		t.Fatalf("expected CumQty=1000, LeavesQty=0, got CumQty=%d, LeavesQty=%d", ord.CumQty, ord.LeavesQty)
	}
	// Expected avg price: (400*15000 + 600*15100) / 1000 = (6,000,000 + 9,060,000) / 1000 = 15060
	if ord.AvgPrice != 15060 {
		t.Fatalf("expected AvgPrice=15060, got %d", ord.AvgPrice)
	}

	// 5. Attempting to fill an already filled order must fail with ErrLateFillOnTerminal
	_, err = statemachine.ApplyTransition(ord, statemachine.TriggerFill, statemachine.FillEvent{
		Venue:     "SIM_VENUE",
		ExecID:    "EXEC-003",
		Qty:       100,
		Price:     15000,
		Timestamp: time.Now(),
	})
	if !errors.Is(err, statemachine.ErrLateFillOnTerminal) {
		t.Fatalf("expected ErrLateFillOnTerminal, got %v", err)
	}
}

func TestCancellationLifecycle(t *testing.T) {
	ord := createTestOrder(t, 500, 20000)

	// Risk Passed
	_, _ = statemachine.ApplyTransition(ord, statemachine.TriggerRiskPassed, nil)

	// In-flight cancel request
	res, err := statemachine.ApplyTransition(ord, statemachine.TriggerCancelRequest, nil)
	if err != nil {
		t.Fatalf("unexpected error on CancelRequest: %v", err)
	}
	if ord.State != domain.StatePendingCancel {
		t.Fatalf("expected state PendingCancel, got %s", ord.State)
	}

	// Cancel Ack from venue
	res, err = statemachine.ApplyTransition(ord, statemachine.TriggerCancelAck, nil)
	if err != nil {
		t.Fatalf("unexpected error on CancelAck: %v", err)
	}
	if ord.State != domain.StateCancelled || res.NewState != domain.StateCancelled {
		t.Fatalf("expected state Cancelled, got %s", ord.State)
	}
	if ord.LeavesQty != 0 {
		t.Fatalf("expected LeavesQty=0 after cancellation, got %d", ord.LeavesQty)
	}

	// Attempting to cancel again or fill must fail
	_, err = statemachine.ApplyTransition(ord, statemachine.TriggerCancelRequest, nil)
	if !errors.Is(err, statemachine.ErrOrderInTerminal) {
		t.Fatalf("expected ErrOrderInTerminal, got %v", err)
	}
}

func TestRiskRejectionLifecycle(t *testing.T) {
	ord := createTestOrder(t, 500, 20000)

	res, err := statemachine.ApplyTransition(ord, statemachine.TriggerRiskRejected, nil)
	if err != nil {
		t.Fatalf("unexpected error on RiskRejected: %v", err)
	}
	if ord.State != domain.StateRejected || res.NewState != domain.StateRejected {
		t.Fatalf("expected state Rejected, got %s", ord.State)
	}
	if !ord.State.IsTerminal() {
		t.Fatalf("rejected state must be terminal")
	}
}

func TestOverfillPrevention(t *testing.T) {
	ord := createTestOrder(t, 100, 10000)
	_, _ = statemachine.ApplyTransition(ord, statemachine.TriggerRiskPassed, nil)

	// Attempting to fill 101 shares on a 100-share order
	_, err := statemachine.ApplyTransition(ord, statemachine.TriggerFill, statemachine.FillEvent{
		Venue:     "SIM_VENUE",
		ExecID:    "EXEC-001",
		Qty:       101,
		Price:     10000,
		Timestamp: time.Now(),
	})
	if !errors.Is(err, statemachine.ErrOverfill) {
		t.Fatalf("expected ErrOverfill, got %v", err)
	}
}

func TestReplaceLifecycle(t *testing.T) {
	ord := createTestOrder(t, 500, 20000)

	// Risk Passed -> New
	_, _ = statemachine.ApplyTransition(ord, statemachine.TriggerRiskPassed, nil)

	// In-flight replace request
	res, err := statemachine.ApplyTransition(ord, statemachine.TriggerReplaceRequest, nil)
	if err != nil {
		t.Fatalf("unexpected error on ReplaceRequest: %v", err)
	}
	if ord.State != domain.StatePendingReplace {
		t.Fatalf("expected state PendingReplace, got %s", ord.State)
	}

	// Case 1: Replace Reject reverts to StateNew
	res, err = statemachine.ApplyTransition(ord, statemachine.TriggerReplaceReject, nil)
	if err != nil {
		t.Fatalf("unexpected error on ReplaceReject: %v", err)
	}
	if ord.State != domain.StateNew {
		t.Fatalf("expected state New after replace reject, got %s", ord.State)
	}

	// In-flight replace request again
	_, _ = statemachine.ApplyTransition(ord, statemachine.TriggerReplaceRequest, nil)

	// Case 2: Replace Ack -> Replaced
	res, err = statemachine.ApplyTransition(ord, statemachine.TriggerReplaceAck, nil)
	if err != nil {
		t.Fatalf("unexpected error on ReplaceAck: %v", err)
	}
	if ord.State != domain.StateReplaced || res.NewState != domain.StateReplaced {
		t.Fatalf("expected state Replaced, got %s", ord.State)
	}
}
