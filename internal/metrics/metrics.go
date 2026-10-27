package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	InboundOrdersTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tradeflow_orders_inbound_total",
		Help: "Total count of inbound orders received at the FIX Gateway",
	})

	AcceptedOrdersTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tradeflow_orders_accepted_total",
		Help: "Total count of orders approved by Pre-Trade Risk Engine",
	})

	RejectedOrdersTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "tradeflow_orders_rejected_total",
		Help: "Total count of orders rejected by Pre-Trade Risk Engine partitioned by reject code",
	}, []string{"reject_code"})

	FillsProcessedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tradeflow_fills_processed_total",
		Help: "Total count of unique execution fills processed into positions",
	})

	DuplicateFillsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tradeflow_duplicate_fills_total",
		Help: "Total count of duplicate execution fills deduplicated and ignored",
	})

	GatewayLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "tradeflow_gateway_to_ack_latency_seconds",
		Help:    "Latency from FIX gateway receipt to order ack/reject execution report",
		Buckets: []float64{0.0005, 0.001, 0.002, 0.005, 0.010, 0.025, 0.050, 0.100},
	})

	PendingFillsCurrent = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "tradeflow_pending_fills_current",
		Help: "Current count of early fills parked in pending_fills waiting for parent orders",
	})

	PendingFillsResolvedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tradeflow_pending_fills_resolved_total",
		Help: "Total count of pending early fills successfully resolved upon parent order arrival",
	})

	PendingFillsOrphanedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tradeflow_pending_fills_orphaned_total",
		Help: "Total count of orphaned pending fills that exceeded TTL without parent order arriving (ALERT)",
	})
)
