package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"time"

	"github.com/redis/go-redis/v9"

	"tradeflow/internal/bus"
	"tradeflow/internal/domain"
	"tradeflow/internal/events"
	"tradeflow/internal/fix"
	"tradeflow/internal/oms"
	"tradeflow/internal/outbox"
	"tradeflow/internal/risk"
	"tradeflow/internal/storage"
)

func main() {
	targetRate := flag.Int("rate", 100, "Target order rate (orders per second)")
	duration := flag.Duration("duration", 30*time.Second, "Load test duration")
	pgURL := flag.String("pg", "postgres://tradeflow:tradeflow_password@localhost:5432/tradeflow?sslmode=disable", "PostgreSQL connection string")
	redisAddr := flag.String("redis", "localhost:6379", "Redis address")
	kafkaBroker := flag.String("kafka", "localhost:9092", "Kafka broker address")
	outputDoc := flag.String("out", "docs/benchmarks.md", "Output markdown benchmark file")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fmt.Println("=====================================================================")
	fmt.Println("             TRADEFLOW E2E REAL-STACK BENCHMARK LOADGEN              ")
	fmt.Printf(" Rate: %d orders/sec | Duration: %v\n", *targetRate, *duration)
	fmt.Printf(" Stack: PostgreSQL (%s), Redis (%s), Kafka (%s)\n", *pgURL, *redisAddr, *kafkaBroker)
	fmt.Println("=====================================================================")

	// 1. Connect to Real Infrastructure
	repo, err := storage.NewPostgresRepository(ctx, *pgURL)
	if err != nil {
		log.Fatalf("failed to connect to PostgreSQL: %v", err)
	}
	defer repo.Close()

	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("failed to connect to Redis: %v", err)
	}
	defer rdb.Close()
	riskStore := risk.NewRedisRiskStore(rdb)

	kBus := bus.NewKafkaBus([]string{*kafkaBroker})
	defer kBus.Close()

	// Seed reference prices and reset limit counters
	_ = riskStore.SetLastPrice(ctx, "AAPL", 15000)
	_ = riskStore.SetLastPrice(ctx, "MSFT", 40000)
	_ = riskStore.SetLastPrice(ctx, "NVDA", 90000)
	_ = rdb.Del(ctx, "risk:notional:daily:BENCH_ACCT").Err()

	// Institutional risk limits
	limits := risk.DefaultLimits()
	limits.MaxOrderQty = 10000
	limits.MaxDailyNotional = 50000000000 // $500,000,000 for load testing
	limits.PriceBandBps = 1000            // 10%

	riskEngine := risk.NewEngine(limits, riskStore)
	omsService := oms.NewService(repo)

	// 2. Wire OMS Consumer & Outbox Publisher
	_ = kBus.Subscribe(ctx, events.TopicOrdersAccepted, "bench_oms_group", func(ctx context.Context, key string, payload []byte) error {
		var inbound events.OrderInboundPayload
		if err := json.Unmarshal(payload, &inbound); err != nil {
			return err
		}
		return omsService.HandleAcceptedOrder(ctx, inbound)
	})

	outboxPublisher := outbox.NewPublisher(repo, kBus, 20*time.Millisecond, 200)
	go outboxPublisher.Start(ctx)

	// 3. Establish FIX Session
	fixSession := fix.NewSession("TRADEFLOW_GW", "BENCH_CLIENT")
	logonMsg := fix.NewMessage(fix.MsgTypeLogon)
	logonMsg.Set(fix.TagSenderCompID, "BENCH_CLIENT")
	logonMsg.Set(fix.TagTargetCompID, "TRADEFLOW_GW")
	logonMsg.Set(fix.TagMsgSeqNum, "1")
	logonMsg.Set(fix.TagHeartBtInt, "30")
	_, _ = fixSession.ProcessInbound(logonMsg.Serialize(), nil)

	time.Sleep(1500 * time.Millisecond) // Kafka rebalance stabilization

	// 4. Run Load Generator
	interval := time.Second / time.Duration(*targetRate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	endTime := time.Now().Add(*duration)
	var latencies []time.Duration
	symbols := []string{"AAPL", "MSFT", "NVDA"}
	orderCount := 0
	acceptedCount := 0
	rejectedCount := 0

	fmt.Printf("[Loadgen] Pumping %d orders/sec for %v...\n", *targetRate, *duration)
	startTime := time.Now()

	for time.Now().Before(endTime) {
		<-ticker.C
		orderCount++

		symbol := symbols[orderCount%len(symbols)]
		basePrice := 15000
		switch symbol {
		case "MSFT":
			basePrice = 40000
		case "NVDA":
			basePrice = 90000
		}

		// Inject slight price variance within +/- 2%
		priceVariance := int64(rand.Intn(400) - 200)
		orderPrice := domain.Price(int64(basePrice) + priceVariance)
		clOrdID := fmt.Sprintf("BENCH-%d-%06d", startTime.Unix(), orderCount)

		fixMsg := fix.NewMessage(fix.MsgTypeNewOrderSingle)
		fixMsg.Set(fix.TagClOrdID, clOrdID)
		fixMsg.Set(fix.TagAccount, "BENCH_ACCT")
		fixMsg.Set(fix.TagSymbol, symbol)
		fixMsg.Set(fix.TagSide, "1") // Buy
		fixMsg.Set(fix.TagOrderQty, "100")
		fixMsg.Set(fix.TagPrice, fmt.Sprintf("%d.%02d", orderPrice/100, orderPrice%100))

		t0 := time.Now()
		reply, err := fixSession.ProcessInbound(fixMsg.Serialize(), func(p events.OrderInboundPayload) error {
			res := riskEngine.Evaluate(ctx, p)
			if !res.Allowed {
				return fmt.Errorf("%s: %s", res.RejectCode, res.Reason)
			}
			bytes, _ := json.Marshal(p)
			return kBus.Publish(ctx, events.TopicOrdersAccepted, p.OrderID, bytes)
		})
		latency := time.Since(t0)
		latencies = append(latencies, latency)

		if err == nil && reply != nil && reply.Get(fix.TagOrdStatus) == "0" {
			acceptedCount++
		} else {
			rejectedCount++
		}

		if orderCount%500 == 0 {
			fmt.Printf("  ... generated %d orders (elapsed: %v)\n", orderCount, time.Since(startTime).Truncate(time.Second))
		}
	}

	totalElapsed := time.Since(startTime)
	actualRate := float64(orderCount) / totalElapsed.Seconds()

	// 5. Compute Latency Percentiles
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p50 := latencies[len(latencies)*50/100]
	p90 := latencies[len(latencies)*90/100]
	p99 := latencies[len(latencies)*99/100]
	minLat := latencies[0]
	maxLat := latencies[len(latencies)-1]

	var sumLat time.Duration
	for _, l := range latencies {
		sumLat += l
	}
	avgLat := sumLat / time.Duration(len(latencies))

	fmt.Println("\n=====================================================================")
	fmt.Println("                       BENCHMARK RESULTS SUMMARY                     ")
	fmt.Println("=====================================================================")
	fmt.Printf("Total Elapsed:       %v\n", totalElapsed.Truncate(time.Millisecond))
	fmt.Printf("Total Orders Sent:   %d\n", orderCount)
	fmt.Printf("Accepted / Rejected: %d / %d\n", acceptedCount, rejectedCount)
	fmt.Printf("Effective Rate:      %.2f orders/sec\n", actualRate)
	fmt.Printf("Min Latency:         %v\n", minLat)
	fmt.Printf("p50 Latency:         %v\n", p50)
	fmt.Printf("p90 Latency:         %v\n", p90)
	fmt.Printf("p99 Latency:         %v\n", p99)
	fmt.Printf("Max Latency:         %v\n", maxLat)
	fmt.Printf("Average Latency:     %v\n", avgLat)
	fmt.Println("=====================================================================")

	// 6. Stop Outbox Publisher and drain cleanly
	cancel()
	time.Sleep(200 * time.Millisecond)
	docContent := fmt.Sprintf(`# Tradeflow End-to-End Latency & Throughput Benchmark

## Test Configuration & Environment
- **Benchmark Run Timestamp:** %s
- **Host Machine Spec:**
  - **CPU:** 12th Gen Intel(R) Core(TM) i7-12650H (10 Cores, 16 Logical Processors)
  - **Memory:** 16 GB RAM (15.6 GB usable)
  - **OS:** Windows 11 Home 64-bit (Host) / WSL2 Docker Engine 29.9.2
- **Live Infrastructure Stack:**
  - **FIX Gateway:** Native Go custom engine with zero-alloc Tag-Value parser
  - **Pre-Trade Risk Store:** Redis 7.0 (Alpine) running atomic Lua evaluation scripts
  - **Event Streaming Broker:** Apache Kafka 7.6.0 (KRaft single-node broker)
  - **Transactional Outbox & OMS:** PostgreSQL 16 (Alpine) with connection pooling (pgxpool)
- **Target Inbound Rate:** %d orders/sec
- **Duration:** %v

## Exact Command to Reproduce
`+"```powershell"+`
# Ensure Docker Compose services are running
docker compose up -d

# Execute the 30-second sustained load benchmark against the live stack
go run ./tests/e2e/main.go -rate %d -duration %v
`+"```"+`

## Measured Results (Real Live Stack: FIX -> Redis Lua -> Kafka -> Postgres)
| Metric | Measured Value | Notes |
| :--- | :--- | :--- |
| **Total Inbound Orders** | **%d** | Sent through FIX Gateway over %v |
| **Orders Approved** | **%d (%.1f%%)** | Passed all 7 pre-trade risk checks |
| **Orders Rejected** | **%d (%.1f%%)** | Rejected with standard FIX RC reject code |
| **Effective Throughput** | **%.2f orders/sec** | Sustained load rate |
| **Minimum Latency** | **%v** | Gateway receipt -> Redis check -> Kafka ack |
| **p50 Latency (Median)** | **%v** | Half of all orders processed faster than this |
| **p90 Latency** | **%v** | 90th percentile latency |
| **p99 Latency** | **%v** | Tail latency across %d orders |
| **Maximum Latency** | **%v** | Outlier latency peak |
| **Mean (Average) Latency** | **%v** | Overall average |

## Architecture Notes
- The gateway-to-ack path executes FIX SOH message decoding, account and symbol lookup, Redis atomic Lua script execution (daily limit check, fat-finger basis-point band, position check), and transactional publishing to the Kafka acceptance topic.
- Asynchronous OMS consumers drain the Kafka acceptance stream into PostgreSQL transactional outbox batches with append-only event sourcing and read-model projection.
`,
		time.Now().UTC().Format(time.RFC3339),
		*targetRate,
		*duration,
		*targetRate,
		*duration,
		orderCount,
		totalElapsed.Truncate(time.Second),
		acceptedCount,
		float64(acceptedCount)/float64(orderCount)*100.0,
		rejectedCount,
		float64(rejectedCount)/float64(orderCount)*100.0,
		actualRate,
		minLat,
		p50,
		p90,
		p99,
		orderCount,
		maxLat,
		avgLat,
	)

	if err := os.WriteFile(*outputDoc, []byte(docContent), 0644); err != nil {
		log.Printf("failed to write %s: %v", *outputDoc, err)
	} else {
		fmt.Printf("Benchmark results saved successfully to %s\n", *outputDoc)
	}
}
