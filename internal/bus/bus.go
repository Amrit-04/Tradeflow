package bus

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// Message represents a serialized message in the system.
type Message struct {
	Topic     string
	Key       string
	Value     []byte
	Timestamp time.Time
}

// HandlerFunc processes incoming messages from a subscribed topic.
type HandlerFunc func(ctx context.Context, key string, payload []byte) error

// Bus abstracts message publishing and subscription.
type Bus interface {
	Publish(ctx context.Context, topic string, key string, payload []byte) error
	Subscribe(ctx context.Context, topic string, groupID string, handler HandlerFunc) error
	Close() error
}

// =====================================================================
// Memory Bus (Thread-Safe Channel Bus for Standalone & Tests)
// =====================================================================

type MemoryBus struct {
	mu          sync.RWMutex
	subscribers map[string][]chan Message
	closed      bool
}

func NewMemoryBus() *MemoryBus {
	return &MemoryBus{
		subscribers: make(map[string][]chan Message),
	}
}

func (m *MemoryBus) Publish(_ context.Context, topic string, key string, payload []byte) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return fmt.Errorf("memory bus is closed")
	}

	msg := Message{
		Topic:     topic,
		Key:       key,
		Value:     payload,
		Timestamp: time.Now().UTC(),
	}

	for _, ch := range m.subscribers[topic] {
		select {
		case ch <- msg:
		default:
			// Buffer full, drop or block in non-buffered mode
		}
	}
	return nil
}

func (m *MemoryBus) Subscribe(ctx context.Context, topic string, _ string, handler HandlerFunc) error {
	m.mu.Lock()
	ch := make(chan Message, 1000)
	m.subscribers[topic] = append(m.subscribers[topic], ch)
	m.mu.Unlock()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				_ = handler(ctx, msg.Key, msg.Value)
			}
		}
	}()
	return nil
}

func (m *MemoryBus) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	for _, chList := range m.subscribers {
		for _, ch := range chList {
			close(ch)
		}
	}
	return nil
}

// =====================================================================
// Kafka Bus (Production Segmentio Kafka Driver)
// =====================================================================

type KafkaBus struct {
	brokers []string
	writers map[string]*kafka.Writer
	mu      sync.Mutex
}

func NewKafkaBus(brokers []string) *KafkaBus {
	return &KafkaBus{
		brokers: brokers,
		writers: make(map[string]*kafka.Writer),
	}
}

func (k *KafkaBus) getWriter(topic string) *kafka.Writer {
	k.mu.Lock()
	defer k.mu.Unlock()

	if w, ok := k.writers[topic]; ok {
		return w
	}
	w := &kafka.Writer{
		Addr:         kafka.TCP(k.brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafka.RequireAll,
	}
	k.writers[topic] = w
	return w
}

func (k *KafkaBus) Publish(ctx context.Context, topic string, key string, payload []byte) error {
	w := k.getWriter(topic)
	return w.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: payload,
		Time:  time.Now().UTC(),
	})
}

func (k *KafkaBus) Subscribe(ctx context.Context, topic string, groupID string, handler HandlerFunc) error {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        k.brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10e6, // 10MB
		CommitInterval: time.Second,
	})

	go func() {
		defer r.Close()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				m, err := r.FetchMessage(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					time.Sleep(100 * time.Millisecond)
					continue
				}
				if err := handler(ctx, string(m.Key), m.Value); err == nil {
					_ = r.CommitMessages(ctx, m)
				}
			}
		}
	}()
	return nil
}

func (k *KafkaBus) Close() error {
	k.mu.Lock()
	defer k.mu.Unlock()
	for _, w := range k.writers {
		_ = w.Close()
	}
	return nil
}
