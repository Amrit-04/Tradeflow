//go:build integration

package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"

	"tradeflow/internal/bus"
	"tradeflow/internal/domain"
	"tradeflow/internal/events"
	"tradeflow/internal/outbox"
	"tradeflow/internal/position"
	"tradeflow/internal/risk"
	"tradeflow/internal/storage"
)

func setupTestPostgres(ctx context.Context, t *testing.T) (*storage.PostgresRepository, func()) {
	// If POSTGRES_URL is explicitly set, use it
	if connStr := os.Getenv("POSTGRES_URL"); connStr != "" {
		repo, err := storage.NewPostgresRepository(ctx, connStr)
		if err != nil {
			t.Fatalf("failed to connect to postgres: %v", err)
		}
		return repo, func() { repo.Close() }
	}

	// Find migration file
	migrationPath, err := filepath.Abs("../sql/migrations/001_init_schema.sql")
	if err != nil {
		t.Fatalf("failed to resolve migration path: %v", err)
	}

	pgContainer, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithInitScripts(migrationPath),
		tcpostgres.WithDatabase("tradeflow_test"),
		tcpostgres.WithUsername("testuser"),
		tcpostgres.WithPassword("testpass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("failed to start postgres testcontainer: %v", err)
	}

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		_ = pgContainer.Terminate(ctx)
		t.Fatalf("failed to get postgres connection string: %v", err)
	}

	repo, err := storage.NewPostgresRepository(ctx, connStr)
	if err != nil {
		_ = pgContainer.Terminate(ctx)
		t.Fatalf("failed to create postgres repo: %v", err)
	}

	cleanup := func() {
		repo.Close()
		_ = pgContainer.Terminate(ctx)
	}
	return repo, cleanup
}

func setupTestRedis(ctx context.Context, t *testing.T) (*redis.Client, func()) {
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		rdb := redis.NewClient(&redis.Options{Addr: addr})
		if err := rdb.Ping(ctx).Err(); err != nil {
			t.Fatalf("failed to connect to redis: %v", err)
		}
		return rdb, func() { _ = rdb.Close() }
	}

	redisContainer, err := tcredis.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatalf("failed to start redis testcontainer: %v", err)
	}

	connStr, err := redisContainer.ConnectionString(ctx)
	if err != nil {
		_ = redisContainer.Terminate(ctx)
		t.Fatalf("failed to get redis connection string: %v", err)
	}

	opts, err := redis.ParseURL(connStr)
	if err != nil {
		_ = redisContainer.Terminate(ctx)
		t.Fatalf("failed to parse redis url: %v", err)
	}

	rdb := redis.NewClient(opts)
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = redisContainer.Terminate(ctx)
		t.Fatalf("failed to ping redis: %v", err)
	}

	cleanup := func() {
		_ = rdb.Close()
		_ = redisContainer.Terminate(ctx)
	}
	return rdb, cleanup
}

// TestRealRiskConcurrency asserts that 50 concurrent goroutines racing to consume
// a shared daily limit against a real Redis Lua script cannot over-allocate or bypass limits.
func TestRealRiskConcurrency(t *testing.T) {
	ctx := context.Background()
	rdb, cleanup := setupTestRedis(ctx, t)
	defer cleanup()

	riskStore := risk.NewRedisRiskStore(rdb)
	_ = rdb.Del(ctx, "risk:notional:daily:SHARED_ACCT_REAL").Err()
	_ = riskStore.SetLastPrice(ctx, "AAPL", 10000)

	limits := risk.DefaultLimits()
	limits.MaxOrderQty = 1000
	limits.MaxDailyNotional = 1000000 // $10,000 limit

	engine := risk.NewEngine(limits, riskStore)

	const numGoroutines = 50
	const orderNotional = 100000 // $1,000 per order
	// At $1,000 per order and $10,000 limit, exactly 10 orders must be approved and 40 rejected.

	var wg sync.WaitGroup
	var mu sync.Mutex
	approvedCount := 0
	rejectedCount := 0

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			order := events.OrderInboundPayload{
				OrderID:   fmt.Sprintf("CONCURR-ORD-%03d", id),
				Account:   "SHARED_ACCT_REAL",
				Symbol:    "AAPL",
				Side:      domain.SideBuy,
				OrderQty:  10,
				Price:     10000, // 10 * 10,000 = 100,000 cents = $1,000
				Timestamp: time.Now(),
			}

			res := engine.Evaluate(ctx, order)
			mu.Lock()
			if res.Allowed {
				approvedCount++
			} else {
				rejectedCount++
				if res.RejectCode != risk.RejectCodeDailyNotional {
					t.Errorf("expected reject code %s, got %s: %s", risk.RejectCodeDailyNotional, res.RejectCode, res.Reason)
				}
			}
			mu.Unlock()
		}(i)
	}

	wg.Wait()

	if approvedCount != 10 {
		t.Fatalf("RACE CONDITION DETECTED IN REDIS LUA: expected exactly 10 approvals, got %d", approvedCount)
	}
	if rejectedCount != 40 {
		t.Fatalf("expected exactly 40 rejections, got %d", rejectedCount)
	}

	consumedStr, err := rdb.Get(ctx, "risk:notional:daily:SHARED_ACCT_REAL").Result()
	if err != nil {
		t.Fatalf("failed to retrieve consumed daily notional: %v", err)
	}
	if consumedStr != "1000000" {
		t.Fatalf("expected exactly 1000000 consumed in Redis, got %s", consumedStr)
	}
}

// TestRealOutboxDrain asserts transactional outbox persistence in PostgreSQL
// and reliable draining to the message bus.
func TestRealOutboxDrain(t *testing.T) {
	ctx := context.Background()
	repo, cleanup := setupTestPostgres(ctx, t)
	defer cleanup()

	msgBus := bus.NewMemoryBus()
	defer msgBus.Close()

	// Stage 5 orders in PostgreSQL transaction with Outbox
	for i := 1; i <= 5; i++ {
		orderID := fmt.Sprintf("REAL-ORD-%03d", i)
		event := storage.OrderEventRecord{
			OrderID:   orderID,
			Seq:       1,
			EventType: "ORDER_ROUTED",
			Payload:   json.RawMessage(`{"routed": true}`),
			CreatedAt: time.Now().UTC(),
		}
		order := storage.OrderRecord{
			OrderID:   orderID,
			ClOrdID:   fmt.Sprintf("REAL-CL-%03d", i),
			Account:   "ACCT-REAL",
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
			OutboxID:     uuid.New().String(),
			Topic:        "orders.routed",
			PartitionKey: orderID,
			Payload:      json.RawMessage(fmt.Sprintf(`{"order_id": "%s"}`, orderID)),
			CreatedAt:    time.Now().UTC(),
		}
		if err := repo.SaveOrderEventAndProject(ctx, event, order, outboxRec); err != nil {
			t.Fatalf("failed to save order event and outbox: %v", err)
		}
	}

	publisher := outbox.NewPublisher(repo, msgBus, 10*time.Millisecond, 100)

	// First drain should publish all 5
	count := publisher.DrainBatch(ctx)
	if count != 5 {
		t.Fatalf("expected 5 published outbox messages from PostgreSQL, got %d", count)
	}

	// Second drain should find 0 pending
	count = publisher.DrainBatch(ctx)
	if count != 0 {
		t.Fatalf("expected 0 published outbox messages on second drain, got %d", count)
	}
}

// TestRealIdempotency runs a chaotic execution fill stream with 50% duplicate injection
// against real PostgreSQL to assert strict database-level deduplication.
func TestRealIdempotency(t *testing.T) {
	ctx := context.Background()
	repo, cleanup := setupTestPostgres(ctx, t)
	defer cleanup()

	book := position.NewBook(repo)

	const numUniqueFills = 50
	const duplicateMultiplier = 3 // Every fill injected 3 times

	// Register parent orders in PostgreSQL
	for i := 1; i <= numUniqueFills; i++ {
		orderID := fmt.Sprintf("REAL-CHAOS-ORD-%04d", i)
		err := repo.SaveOrderEventAndProject(ctx, storage.OrderEventRecord{
			OrderID:   orderID,
			Seq:       1,
			EventType: "ORDER_NEW",
			Payload:   json.RawMessage(`{}`),
			CreatedAt: time.Now().UTC(),
		}, storage.OrderRecord{
			OrderID:   orderID,
			ClOrdID:   "CL-" + orderID,
			Account:   "REAL_CHAOS_ACCT",
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
		if err != nil {
			t.Fatalf("failed to register order in postgres: %v", err)
		}
	}

	var uniqueFills []events.ExecutionFillPayload
	var expectedNetQty int64 = 0

	for i := 1; i <= numUniqueFills; i++ {
		fillQty := int64(10 + (i % 20))
		expectedNetQty += fillQty

		uniqueFills = append(uniqueFills, events.ExecutionFillPayload{
			Venue:        "SIM_VENUE",
			ExecID:       fmt.Sprintf("REAL-EXEC-%04d", i),
			OrderID:      fmt.Sprintf("REAL-CHAOS-ORD-%04d", i),
			Account:      "REAL_CHAOS_ACCT",
			Symbol:       "AAPL",
			Side:         domain.SideBuy,
			FillQty:      domain.Qty(fillQty),
			FillPrice:    15000,
			TransactTime: time.Now().UTC(),
		})
	}

	// Duplicate and shuffle
	var chaoticStream []events.ExecutionFillPayload
	for rep := 0; rep < duplicateMultiplier; rep++ {
		chaoticStream = append(chaoticStream, uniqueFills...)
	}

	rnd := rand.New(rand.NewSource(42))
	rnd.Shuffle(len(chaoticStream), func(i, j int) {
		chaoticStream[i], chaoticStream[j] = chaoticStream[j], chaoticStream[i]
	})

	var uniqueAcceptedCount = 0
	var duplicatesFilteredCount = 0

	for _, fill := range chaoticStream {
		processed, err := book.ProcessFill(ctx, fill)
		if err != nil {
			t.Fatalf("unexpected error processing fill %s in postgres: %v", fill.ExecID, err)
		}
		if processed {
			uniqueAcceptedCount++
		} else {
			duplicatesFilteredCount++
		}
	}

	if uniqueAcceptedCount != numUniqueFills {
		t.Fatalf("expected exactly %d unique fills accepted in postgres, got %d", numUniqueFills, uniqueAcceptedCount)
	}
	expectedDuplicates := numUniqueFills * (duplicateMultiplier - 1)
	if duplicatesFilteredCount != expectedDuplicates {
		t.Fatalf("expected %d duplicate fills filtered, got %d", expectedDuplicates, duplicatesFilteredCount)
	}

	pos, err := repo.GetPosition(ctx, "REAL_CHAOS_ACCT", "AAPL")
	if err != nil {
		t.Fatalf("failed to retrieve position: %v", err)
	}
	if int64(pos.NetQty) != expectedNetQty {
		t.Fatalf("DATA INTEGRITY BREACH IN POSTGRES: expected final NetQty=%d, got %d", expectedNetQty, pos.NetQty)
	}

	trades, _ := repo.GetTrades(ctx, "REAL_CHAOS_ACCT", "AAPL")
	if len(trades) != numUniqueFills {
		t.Fatalf("expected %d trades in postgres, got %d", numUniqueFills, len(trades))
	}
}

// TestRealKafkaOutboxBrokerRestart asserts end-to-end outbox durability and recovery
// against real PostgreSQL and Apache Kafka across a container stop/pause and restart.
func TestRealKafkaOutboxBrokerRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	repo, cleanup := setupTestPostgres(ctx, t)
	defer cleanup()

	kafkaBrokers := []string{"localhost:9092"}
	if envBrokers := os.Getenv("KAFKA_BROKERS"); envBrokers != "" {
		kafkaBrokers = []string{envBrokers}
	}

	kBus := bus.NewKafkaBus(kafkaBrokers)
	defer kBus.Close()

	topic := events.TopicOrdersRouted
	received := make(map[string]int)

	// 1. Stage 6 orders with outbox records in PostgreSQL
	const totalOrders = 6
	for i := 1; i <= totalOrders; i++ {
		orderID := fmt.Sprintf("KAFKA-ORD-%d-%03d", time.Now().UnixNano(), i)
		event := storage.OrderEventRecord{
			OrderID:   orderID,
			Seq:       1,
			EventType: "ORDER_ROUTED",
			Payload:   json.RawMessage(`{}`),
			CreatedAt: time.Now().UTC(),
		}
		order := storage.OrderRecord{
			OrderID:   orderID,
			ClOrdID:   fmt.Sprintf("KAFKA-CL-%03d", i),
			Account:   "KAFKA_ACCT",
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
		payloadBytes, _ := json.Marshal(map[string]string{"order_id": orderID})
		outboxRec := &storage.OutboxRecord{
			OutboxID:     uuid.New().String(),
			Topic:        topic,
			PartitionKey: orderID,
			Payload:      payloadBytes,
			CreatedAt:    time.Now().UTC(),
		}
		if err := repo.SaveOrderEventAndProject(ctx, event, order, outboxRec); err != nil {
			t.Fatalf("failed to stage outbox record: %v", err)
		}
	}

	// 2. Publish initial 2 records by draining batch of 2
	publisher := outbox.NewPublisher(repo, kBus, 50*time.Millisecond, 2)
	count := publisher.DrainBatch(ctx)
	if count != 2 {
		t.Fatalf("expected 2 published messages in first batch, got %d", count)
	}

	// 3. Stop / Pause the Kafka broker container to inject network/broker outage
	containerName := "tradeflow-kafka"
	if err := exec.Command("docker", "pause", containerName).Run(); err != nil {
		t.Logf("skipping container pause if docker CLI unavailable: %v", err)
	} else {
		t.Log("[Integration] Kafka container PAUSED. Attempting outbox drain during outage...")

		// Attempt to drain remaining outbox messages during outage with short timeout
		outageCtx, outageCancel := context.WithTimeout(ctx, 500*time.Millisecond)
		_ = publisher.DrainBatch(outageCtx)
		outageCancel()

		// Verify unpublished rows remain intact in PostgreSQL
		unpublished, err := repo.GetUnpublishedOutbox(ctx, 100)
		if err != nil {
			t.Fatalf("failed to query unpublished outbox: %v", err)
		}
		if len(unpublished) < 4 {
			t.Fatalf("expected at least 4 unpublished outbox rows preserved during outage, got %d", len(unpublished))
		}
		t.Logf("[Integration] Preserved %d unpublished outbox rows safely in PostgreSQL during broker failure.", len(unpublished))

		// 4. Recover the Kafka broker container
		t.Log("[Integration] Unpausing Kafka container to simulate broker recovery...")
		if err := exec.Command("docker", "unpause", containerName).Run(); err != nil {
			t.Fatalf("failed to unpause kafka container: %v", err)
		}
		time.Sleep(2 * time.Second) // wait for Kafka readiness
	}

	// 5. Drain the recovered outbox
	recoveredPublisher := outbox.NewPublisher(repo, kBus, 50*time.Millisecond, 10)
	for i := 0; i < 10; i++ {
		_ = recoveredPublisher.DrainBatch(ctx)
		unpub, _ := repo.GetUnpublishedOutbox(ctx, 100)
		if len(unpub) == 0 {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}

	unpub, err := repo.GetUnpublishedOutbox(ctx, 100)
	if err != nil || len(unpub) != 0 {
		t.Fatalf("expected 0 unpublished outbox records after broker recovery, got %d", len(unpub))
	}

	// 6. Read and assert all 6 messages directly from Kafka topic from offset 0
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     kafkaBrokers,
		Topic:       topic,
		StartOffset: kafka.FirstOffset,
		MinBytes:    1,
		MaxBytes:    10e6,
	})
	defer reader.Close()

	readCtx, readCancel := context.WithTimeout(ctx, 10*time.Second)
	defer readCancel()

	for len(received) < totalOrders {
		m, err := reader.ReadMessage(readCtx)
		if err != nil {
			t.Fatalf("failed reading from kafka topic %s: %v", topic, err)
		}
		var msg map[string]string
		if err := json.Unmarshal(m.Value, &msg); err == nil {
			received[msg["order_id"]]++
		}
	}

	if len(received) != totalOrders {
		t.Fatalf("expected %d unique orders received from Kafka, got %d", totalOrders, len(received))
	}
}
