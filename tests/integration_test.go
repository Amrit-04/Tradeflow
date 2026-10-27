//go:build integration

package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
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
