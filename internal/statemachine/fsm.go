package statemachine

import (
	"errors"
	"fmt"
	"time"

	"tradeflow/internal/domain"
)

var (
	ErrIllegalTransition  = errors.New("illegal state transition")
	ErrOrderInTerminal    = errors.New("order is in terminal state")
	ErrOverfill           = errors.New("execution fill quantity exceeds remaining leaves quantity")
	ErrInvalidFillQty     = errors.New("fill quantity must be strictly positive")
	ErrInvalidFillPrice   = errors.New("fill price must be non-negative")
	ErrLateFillOnTerminal = errors.New("fill received after order cancellation or closure (late fill)")
)

// Trigger represents an event that attempts to advance an order's state.
type Trigger string

const (
	TriggerSubmitNew      Trigger = "SubmitNew"
	TriggerRiskPassed     Trigger = "RiskPassed"
	TriggerRiskRejected   Trigger = "RiskRejected"
	TriggerFill           Trigger = "Fill"
	TriggerCancelRequest  Trigger = "CancelRequest"
	TriggerCancelAck      Trigger = "CancelAck"
	TriggerCancelReject   Trigger = "CancelReject"
	TriggerReplaceRequest Trigger = "ReplaceRequest"
	TriggerReplaceAck     Trigger = "ReplaceAck"
	TriggerReplaceReject  Trigger = "ReplaceReject"
)

// FillEvent carries execution fill details into the state machine.
type FillEvent struct {
	Venue     string
	ExecID    string
	Qty       domain.Qty
	Price     domain.Price
	Timestamp time.Time
}

// TransitionResult contains the outcome of a state transition.
type TransitionResult struct {
	PrevState domain.OrderState
	NewState  domain.OrderState
	Trigger   Trigger
	Timestamp time.Time
}

// ApplyTransition executes a state machine transition on an Order.
// Returns an error if the transition is illegal according to the OMS state graph.
func ApplyTransition(order *domain.Order, trigger Trigger, payload any) (*TransitionResult, error) {
	if order == nil {
		return nil, errors.New("cannot apply transition to nil order")
	}

	prevState := order.State
	if prevState.IsTerminal() {
		if trigger == TriggerFill {
			return nil, fmt.Errorf("%w: cannot fill order %s in terminal state %s", ErrLateFillOnTerminal, order.OrderID, prevState)
		}
		return nil, fmt.Errorf("%w: order %s is already in terminal state %s", ErrOrderInTerminal, order.OrderID, prevState)
	}

	now := time.Now().UTC()
	var newState domain.OrderState

	switch trigger {
	case TriggerRiskPassed:
		if prevState != domain.StatePendingNew {
			return nil, fmt.Errorf("%w: RiskPassed only allowed from PendingNew, currently %s", ErrIllegalTransition, prevState)
		}
		newState = domain.StateNew

	case TriggerRiskRejected:
		if prevState != domain.StatePendingNew {
			return nil, fmt.Errorf("%w: RiskRejected only allowed from PendingNew, currently %s", ErrIllegalTransition, prevState)
		}
		newState = domain.StateRejected

	case TriggerFill:
		fill, ok := payload.(FillEvent)
		if !ok {
			return nil, errors.New("fill trigger requires FillEvent payload")
		}
		if fill.Qty <= 0 {
			return nil, ErrInvalidFillQty
		}
		if fill.Price < 0 {
			return nil, ErrInvalidFillPrice
		}
		if prevState != domain.StateNew && prevState != domain.StatePartiallyFilled && prevState != domain.StatePendingCancel && prevState != domain.StatePendingReplace {
			return nil, fmt.Errorf("%w: cannot apply fill to state %s", ErrIllegalTransition, prevState)
		}

		// Check for overfill
		if fill.Qty > order.LeavesQty {
			return nil, fmt.Errorf("%w: fill qty %d exceeds leaves qty %d", ErrOverfill, fill.Qty, order.LeavesQty)
		}

		// Calculate new cumulative metrics with integer minor units
		prevCumNotional := int64(order.CumQty) * int64(order.AvgPrice)
		newFillNotional := int64(fill.Qty) * int64(fill.Price)
		newCumQty := order.CumQty + fill.Qty
		newLeavesQty := order.LeavesQty - fill.Qty
		newAvgPrice := domain.Price((prevCumNotional + newFillNotional) / int64(newCumQty))

		order.CumQty = newCumQty
		order.LeavesQty = newLeavesQty
		order.AvgPrice = newAvgPrice

		if newLeavesQty == 0 {
			newState = domain.StateFilled
		} else {
			newState = domain.StatePartiallyFilled
		}

	case TriggerCancelRequest:
		if prevState != domain.StateNew && prevState != domain.StatePartiallyFilled {
			return nil, fmt.Errorf("%w: CancelRequest only allowed from New or PartiallyFilled, currently %s", ErrIllegalTransition, prevState)
		}
		newState = domain.StatePendingCancel

	case TriggerCancelAck:
		if prevState != domain.StatePendingCancel {
			return nil, fmt.Errorf("%w: CancelAck only allowed from PendingCancel, currently %s", ErrIllegalTransition, prevState)
		}
		order.LeavesQty = 0
		newState = domain.StateCancelled

	case TriggerCancelReject:
		if prevState != domain.StatePendingCancel {
			return nil, fmt.Errorf("%w: CancelReject only allowed from PendingCancel, currently %s", ErrIllegalTransition, prevState)
		}
		// Revert to prior active state
		if order.CumQty > 0 {
			newState = domain.StatePartiallyFilled
		} else {
			newState = domain.StateNew
		}

	case TriggerReplaceRequest:
		if prevState != domain.StateNew && prevState != domain.StatePartiallyFilled {
			return nil, fmt.Errorf("%w: ReplaceRequest only allowed from New or PartiallyFilled, currently %s", ErrIllegalTransition, prevState)
		}
		newState = domain.StatePendingReplace

	case TriggerReplaceAck:
		if prevState != domain.StatePendingReplace {
			return nil, fmt.Errorf("%w: ReplaceAck only allowed from PendingReplace, currently %s", ErrIllegalTransition, prevState)
		}
		newState = domain.StateReplaced

	case TriggerReplaceReject:
		if prevState != domain.StatePendingReplace {
			return nil, fmt.Errorf("%w: ReplaceReject only allowed from PendingReplace, currently %s", ErrIllegalTransition, prevState)
		}
		// Revert to prior active state
		if order.CumQty > 0 {
			newState = domain.StatePartiallyFilled
		} else {
			newState = domain.StateNew
		}

	default:
		return nil, fmt.Errorf("%w: unrecognized trigger %s", ErrIllegalTransition, trigger)
	}

	order.State = newState
	order.Version++
	order.UpdatedAt = now

	return &TransitionResult{
		PrevState: prevState,
		NewState:  newState,
		Trigger:   trigger,
		Timestamp: now,
	}, nil
}
