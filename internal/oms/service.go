package oms

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"tradeflow/internal/domain"
	"tradeflow/internal/events"
	"tradeflow/internal/statemachine"
	"tradeflow/internal/storage"
)

// Service coordinates the OMS lifecycle, state machine, event sourcing, and outbox.
type Service struct {
	repo storage.Repository
}

func NewService(repo storage.Repository) *Service {
	return &Service{repo: repo}
}

// HandleAcceptedOrder records an approved order and stages it in the transactional outbox.
func (s *Service) HandleAcceptedOrder(ctx context.Context, p events.OrderInboundPayload) error {
	now := time.Now().UTC()

	order, err := domain.NewOrder(p.OrderID, p.ClOrdID, p.Account, p.Symbol, p.Side, p.OrderType, p.OrderQty, p.Price)
	if err != nil {
		return fmt.Errorf("invalid order: %w", err)
	}

	// Transition PendingNew -> New
	if _, err := statemachine.ApplyTransition(order, statemachine.TriggerRiskPassed, nil); err != nil {
		return fmt.Errorf("state transition failed: %w", err)
	}

	eventPayload, _ := json.Marshal(p)
	event := storage.OrderEventRecord{
		OrderID:   order.OrderID,
		Seq:       order.Version,
		EventType: string(events.EventOrderAccepted),
		Payload:   eventPayload,
		CreatedAt: now,
	}

	orderRecord := storage.OrderRecord{
		OrderID:   order.OrderID,
		ClOrdID:   order.ClOrdID,
		Account:   order.Account,
		Symbol:    order.Symbol,
		Side:      order.Side,
		OrderQty:  order.OrderQty,
		Price:     order.Price,
		State:     order.State,
		CumQty:    order.CumQty,
		LeavesQty: order.LeavesQty,
		AvgPrice:  order.AvgPrice,
		Version:   order.Version,
		CreatedAt: now,
		UpdatedAt: now,
	}

	routedPayload, _ := json.Marshal(events.OrderRoutedPayload{
		OrderID:   order.OrderID,
		ClOrdID:   order.ClOrdID,
		Account:   order.Account,
		Symbol:    order.Symbol,
		Side:      order.Side,
		OrderQty:  order.OrderQty,
		Price:     order.Price,
		Timestamp: now,
	})

	outboxRecord := &storage.OutboxRecord{
		OutboxID:     uuid.New().String(),
		Topic:        events.TopicOrdersRouted,
		PartitionKey: order.OrderID,
		Payload:      routedPayload,
		CreatedAt:    now,
	}

	// Single atomic ACID transaction
	return s.repo.SaveOrderEventAndProject(ctx, event, orderRecord, outboxRecord)
}

// HandleRejectedOrder records an order rejected by risk.
func (s *Service) HandleRejectedOrder(ctx context.Context, p events.OrderRejectedPayload) error {
	now := time.Now().UTC()

	eventPayload, _ := json.Marshal(p)
	event := storage.OrderEventRecord{
		OrderID:   p.OrderID,
		Seq:       1,
		EventType: string(events.EventOrderRejected),
		Payload:   eventPayload,
		CreatedAt: now,
	}

	orderRecord := storage.OrderRecord{
		OrderID:   p.OrderID,
		ClOrdID:   p.ClOrdID,
		Account:   p.Account,
		Symbol:    p.Symbol,
		Side:      domain.SideBuy, // Default if not parsed
		OrderQty:  0,
		Price:     0,
		State:     domain.StateRejected,
		CumQty:    0,
		LeavesQty: 0,
		AvgPrice:  0,
		Version:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}

	return s.repo.SaveOrderEventAndProject(ctx, event, orderRecord, nil)
}

// HandleExecutionFill updates order state machine upon fill reception from venue.
func (s *Service) HandleExecutionFill(ctx context.Context, fill events.ExecutionFillPayload) (*statemachine.TransitionResult, error) {
	ordRecord, err := s.repo.GetOrder(ctx, fill.OrderID)
	if err != nil {
		return nil, err
	}

	order := &domain.Order{
		OrderID:   ordRecord.OrderID,
		ClOrdID:   ordRecord.ClOrdID,
		Account:   ordRecord.Account,
		Symbol:    ordRecord.Symbol,
		Side:      ordRecord.Side,
		OrderQty:  ordRecord.OrderQty,
		Price:     ordRecord.Price,
		State:     ordRecord.State,
		CumQty:    ordRecord.CumQty,
		LeavesQty: ordRecord.LeavesQty,
		AvgPrice:  ordRecord.AvgPrice,
		Version:   ordRecord.Version,
		CreatedAt: ordRecord.CreatedAt,
		UpdatedAt: ordRecord.UpdatedAt,
	}

	fillEvent := statemachine.FillEvent{
		Venue:     fill.Venue,
		ExecID:    fill.ExecID,
		Qty:       fill.FillQty,
		Price:     fill.FillPrice,
		Timestamp: fill.TransactTime,
	}

	res, err := statemachine.ApplyTransition(order, statemachine.TriggerFill, fillEvent)
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	fillPayload, _ := json.Marshal(fill)
	event := storage.OrderEventRecord{
		OrderID:   order.OrderID,
		Seq:       order.Version,
		EventType: string(events.EventExecutionFill),
		Payload:   fillPayload,
		CreatedAt: now,
	}

	ordRecord.State = order.State
	ordRecord.CumQty = order.CumQty
	ordRecord.LeavesQty = order.LeavesQty
	ordRecord.AvgPrice = order.AvgPrice
	ordRecord.Version = order.Version
	ordRecord.UpdatedAt = now

	// Save to DB
	if err := s.repo.SaveOrderEventAndProject(ctx, event, *ordRecord, nil); err != nil {
		return nil, err
	}
	return res, nil
}
