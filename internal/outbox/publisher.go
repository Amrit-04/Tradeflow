package outbox

import (
	"context"
	"log"
	"time"

	"tradeflow/internal/bus"
	"tradeflow/internal/storage"
)

// Publisher polls unpublished records from outbox table and relays them to the message bus.
type Publisher struct {
	repo          storage.Repository
	bus           bus.Bus
	pollInterval  time.Duration
	batchSize     int
}

func NewPublisher(repo storage.Repository, bus bus.Bus, pollInterval time.Duration, batchSize int) *Publisher {
	if pollInterval <= 0 {
		pollInterval = 50 * time.Millisecond
	}
	if batchSize <= 0 {
		batchSize = 100
	}
	return &Publisher{
		repo:         repo,
		bus:          bus,
		pollInterval: pollInterval,
		batchSize:    batchSize,
	}
}

// Start begins the outbox drain loop until context cancellation.
func (p *Publisher) Start(ctx context.Context) {
	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.DrainBatch(ctx)
		}
	}
}

// DrainBatch fetches and publishes a single batch of unpublished outbox records.
func (p *Publisher) DrainBatch(ctx context.Context) int {
	records, err := p.repo.GetUnpublishedOutbox(ctx, p.batchSize)
	if err != nil {
		log.Printf("[Outbox] error querying unpublished records: %v", err)
		return 0
	}

	publishedCount := 0
	for _, rec := range records {
		if err := p.bus.Publish(ctx, rec.Topic, rec.PartitionKey, rec.Payload); err != nil {
			log.Printf("[Outbox] failed to publish record %s to %s: %v", rec.OutboxID, rec.Topic, err)
			continue // Will retry on next tick
		}
		if err := p.repo.MarkOutboxPublished(ctx, rec.OutboxID); err != nil {
			log.Printf("[Outbox] failed to mark record %s published: %v", rec.OutboxID, err)
		} else {
			publishedCount++
		}
	}
	return publishedCount
}
