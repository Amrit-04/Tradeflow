package execsim

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"tradeflow/internal/bus"
	"tradeflow/internal/domain"
	"tradeflow/internal/events"
)

// ChaosConfig controls deliberate anomaly injection in execution simulator.
type ChaosConfig struct {
	Enabled           bool
	DuplicateRate     float64 // 0.0 to 1.0 (e.g. 0.20 = 20% duplicate fills)
	OutOrderRate      float64 // 0.0 to 1.0 (e.g. 0.15 = 15% out-of-order delivery)
	LatencyDelayMinMs int
	LatencyDelayMaxMs int
}

// Simulator generates simulated venue executions for routed orders.
type Simulator struct {
	venueName string
	bus       bus.Bus
	chaos     ChaosConfig
	rnd       *rand.Rand
}

func NewSimulator(venueName string, bus bus.Bus, chaos ChaosConfig) *Simulator {
	if venueName == "" {
		venueName = "SIM_VENUE_X"
	}
	return &Simulator{
		venueName: venueName,
		bus:       bus,
		chaos:     chaos,
		rnd:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// ProcessRoutedOrder takes an approved order and produces partial and full fills.
func (s *Simulator) ProcessRoutedOrder(ctx context.Context, order events.OrderRoutedPayload) ([]events.ExecutionFillPayload, error) {
	var fills []events.ExecutionFillPayload

	orderQty := int64(order.OrderQty)
	if orderQty <= 100 {
		// Single full fill
		execID := fmt.Sprintf("EX-%s-1", order.OrderID)
		fills = append(fills, events.ExecutionFillPayload{
			Venue:        s.venueName,
			ExecID:       execID,
			OrderID:      order.OrderID,
			Account:      order.Account,
			Symbol:       order.Symbol,
			Side:         order.Side,
			FillQty:      order.OrderQty,
			FillPrice:    order.Price,
			TransactTime: time.Now().UTC(),
		})
	} else {
		// Multi-stage partial fills (e.g., 40% partial, 60% remaining)
		partQty := orderQty * 4 / 10
		remQty := orderQty - partQty

		fills = append(fills, events.ExecutionFillPayload{
			Venue:        s.venueName,
			ExecID:       fmt.Sprintf("EX-%s-1", order.OrderID),
			OrderID:      order.OrderID,
			Account:      order.Account,
			Symbol:       order.Symbol,
			Side:         order.Side,
			FillQty:      domain.Qty(partQty),
			FillPrice:    order.Price,
			TransactTime: time.Now().UTC(),
		})

		fills = append(fills, events.ExecutionFillPayload{
			Venue:        s.venueName,
			ExecID:       fmt.Sprintf("EX-%s-2", order.OrderID),
			OrderID:      order.OrderID,
			Account:      order.Account,
			Symbol:       order.Symbol,
			Side:         order.Side,
			FillQty:      domain.Qty(remQty),
			FillPrice:    order.Price,
			TransactTime: time.Now().UTC().Add(5 * time.Millisecond),
		})
	}

	// Apply Chaos Injections if enabled
	if s.chaos.Enabled {
		// 1. Injected Out-of-Order: swap fills[0] and fills[1]
		if len(fills) > 1 && s.rnd.Float64() < s.chaos.OutOrderRate {
			fills[0], fills[1] = fills[1], fills[0]
		}

		// 2. Injected Latency
		if s.chaos.LatencyDelayMaxMs > 0 {
			delay := s.chaos.LatencyDelayMinMs + s.rnd.Intn(s.chaos.LatencyDelayMaxMs-s.chaos.LatencyDelayMinMs+1)
			time.Sleep(time.Duration(delay) * time.Millisecond)
		}
	}

	// Publish fills to market.fills (Partition key = Account|Symbol)
	for _, fill := range fills {
		partitionKey := fmt.Sprintf("%s|%s", fill.Account, fill.Symbol)
		payloadBytes, _ := json.Marshal(fill)
		if err := s.bus.Publish(ctx, events.TopicMarketFills, partitionKey, payloadBytes); err != nil {
			return nil, err
		}

		// 3. Injected Duplicates: republish exact same fill if chaos triggered
		if s.chaos.Enabled && s.rnd.Float64() < s.chaos.DuplicateRate {
			_ = s.bus.Publish(ctx, events.TopicMarketFills, partitionKey, payloadBytes)
		}
	}

	return fills, nil
}
