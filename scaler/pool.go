package scaler

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/singleflight"
)

type ClientPool struct {
	clients   sync.Map
	logger    *slog.Logger
	closing   atomic.Bool
	group     singleflight.Group
	newClient func(ctx context.Context, meta *ScalerMetadata) (KubeMQClient, error)
}

func NewClientPool(logger *slog.Logger) *ClientPool {
	return &ClientPool{
		logger:    logger,
		newClient: NewKubeMQClient,
	}
}

func (p *ClientPool) GetOrCreate(ctx context.Context, meta *ScalerMetadata) (KubeMQClient, error) {
	if meta == nil {
		return nil, fmt.Errorf("kubemq: scaler metadata is nil")
	}
	if p.closing.Load() {
		return nil, fmt.Errorf("kubemq: client pool is shutting down")
	}

	key := meta.PoolKey()

	if existing, ok := p.clients.Load(key); ok {
		return existing.(KubeMQClient), nil
	}

	val, err, _ := p.group.Do(key, func() (any, error) {
		if existing, ok := p.clients.Load(key); ok {
			return existing, nil
		}
		client, err := p.newClient(ctx, meta)
		if err != nil {
			return nil, err
		}
		actual, loaded := p.clients.LoadOrStore(key, client)
		if loaded {
			if closeErr := client.Close(); closeErr != nil {
				p.logger.Warn("failed to close superseded client", "address", meta.KubeMQAddress, "error", closeErr)
			}
			return actual, nil
		}
		p.logger.Info("new KubeMQ connection created", "address", meta.KubeMQAddress)
		return client, nil
	})
	if err != nil {
		return nil, err
	}
	return val.(KubeMQClient), nil
}

// Evict removes and closes the connection for a specific metadata configuration.
// Called when a connection is determined to be broken beyond SDK auto-reconnect.
func (p *ClientPool) Evict(meta *ScalerMetadata) {
	key := meta.PoolKey()
	if val, loaded := p.clients.LoadAndDelete(key); loaded {
		if client, ok := val.(KubeMQClient); ok {
			if err := client.Close(); err != nil {
				p.logger.Warn("error closing evicted client", "address", meta.KubeMQAddress, "error", err)
			}
			p.logger.Info("evicted broken KubeMQ connection", "address", meta.KubeMQAddress)
		}
	}
}

// Shutdown marks the pool as closing, preventing new connections.
// Call before gRPC server GracefulStop.
func (p *ClientPool) Shutdown() {
	p.closing.Store(true)
}

func (p *ClientPool) CloseAll() {
	p.closing.Store(true)
	p.clients.Range(func(key, value any) bool {
		p.clients.Delete(key)
		if client, ok := value.(KubeMQClient); ok {
			if err := client.Close(); err != nil {
				p.logger.Warn("error closing KubeMQ client", "key", key, "error", err)
			}
		}
		return true
	})
}
