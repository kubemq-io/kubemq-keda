package scaler

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	kubemq "github.com/kubemq-io/kubemq-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockKubeMQClient struct {
	channels []*kubemq.ChannelInfo
	err      error
	closed   bool
}

func (m *mockKubeMQClient) ListQueuesChannels(_ context.Context, _ string) ([]*kubemq.ChannelInfo, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.channels, nil
}

func (m *mockKubeMQClient) Close() error {
	m.closed = true
	return nil
}

func TestPool_NewClientPool(t *testing.T) {
	pool := NewClientPool(testLogger())
	require.NotNil(t, pool)
	assert.NotNil(t, pool.newClient)
	assert.False(t, pool.closing.Load())
}

func TestPool_GetOrCreate_New(t *testing.T) {
	mock := &mockKubeMQClient{}
	pool := &ClientPool{
		logger: testLogger(),
		newClient: func(_ context.Context, _ *ScalerMetadata) (KubeMQClient, error) {
			return mock, nil
		},
	}

	meta := &ScalerMetadata{KubeMQAddress: "localhost:50000"}
	client, err := pool.GetOrCreate(context.Background(), meta)
	require.NoError(t, err)
	assert.Equal(t, mock, client)
}

func TestPool_GetOrCreate_Reuse(t *testing.T) {
	var createCount int
	mock := &mockKubeMQClient{}
	pool := &ClientPool{
		logger: testLogger(),
		newClient: func(_ context.Context, _ *ScalerMetadata) (KubeMQClient, error) {
			createCount++
			return mock, nil
		},
	}

	meta := &ScalerMetadata{KubeMQAddress: "localhost:50000"}
	c1, err := pool.GetOrCreate(context.Background(), meta)
	require.NoError(t, err)
	c2, err := pool.GetOrCreate(context.Background(), meta)
	require.NoError(t, err)
	assert.Equal(t, c1, c2)
	assert.Equal(t, 1, createCount)
}

func TestPool_GetOrCreate_DifferentAddress(t *testing.T) {
	var createCount int
	pool := &ClientPool{
		logger: testLogger(),
		newClient: func(_ context.Context, meta *ScalerMetadata) (KubeMQClient, error) {
			createCount++
			return &mockKubeMQClient{channels: []*kubemq.ChannelInfo{{Name: meta.KubeMQAddress}}}, nil
		},
	}

	c1, err := pool.GetOrCreate(context.Background(), &ScalerMetadata{KubeMQAddress: "h1:50000"})
	require.NoError(t, err)
	c2, err := pool.GetOrCreate(context.Background(), &ScalerMetadata{KubeMQAddress: "h2:50000"})
	require.NoError(t, err)
	assert.Equal(t, 2, createCount)
	assert.True(t, c1 != c2, "different addresses should return different clients")
}

func TestPool_GetOrCreate_DifferentAuth(t *testing.T) {
	var createCount int
	pool := &ClientPool{
		logger: testLogger(),
		newClient: func(_ context.Context, meta *ScalerMetadata) (KubeMQClient, error) {
			createCount++
			return &mockKubeMQClient{channels: []*kubemq.ChannelInfo{{Name: meta.AuthToken}}}, nil
		},
	}

	c1, err := pool.GetOrCreate(context.Background(), &ScalerMetadata{KubeMQAddress: "h:50000", AuthToken: "tok1"})
	require.NoError(t, err)
	c2, err := pool.GetOrCreate(context.Background(), &ScalerMetadata{KubeMQAddress: "h:50000", AuthToken: "tok2"})
	require.NoError(t, err)
	assert.Equal(t, 2, createCount)
	assert.True(t, c1 != c2, "different auth tokens should return different clients")
}

func TestPool_GetOrCreate_Concurrent(t *testing.T) {
	var createCount atomic.Int32
	pool := &ClientPool{
		logger: testLogger(),
		newClient: func(_ context.Context, _ *ScalerMetadata) (KubeMQClient, error) {
			createCount.Add(1)
			return &mockKubeMQClient{}, nil
		},
	}

	meta := &ScalerMetadata{KubeMQAddress: "localhost:50000"}
	const goroutines = 50
	var wg sync.WaitGroup
	errs := make([]error, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, errs[idx] = pool.GetOrCreate(context.Background(), meta)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		assert.NoError(t, err, "goroutine %d failed", i)
	}
}

func TestPool_GetOrCreate_FactoryError(t *testing.T) {
	pool := &ClientPool{
		logger: testLogger(),
		newClient: func(_ context.Context, _ *ScalerMetadata) (KubeMQClient, error) {
			return nil, fmt.Errorf("connection refused")
		},
	}

	meta := &ScalerMetadata{KubeMQAddress: "localhost:50000"}
	_, err := pool.GetOrCreate(context.Background(), meta)
	require.Error(t, err)
}

func TestPool_GetOrCreate_NilMeta(t *testing.T) {
	pool := &ClientPool{
		logger: testLogger(),
		newClient: func(_ context.Context, _ *ScalerMetadata) (KubeMQClient, error) {
			return &mockKubeMQClient{}, nil
		},
	}
	_, err := pool.GetOrCreate(context.Background(), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil")
}

func TestPool_GetOrCreate_Closing(t *testing.T) {
	pool := &ClientPool{
		logger: testLogger(),
		newClient: func(_ context.Context, _ *ScalerMetadata) (KubeMQClient, error) {
			return &mockKubeMQClient{}, nil
		},
	}
	pool.Shutdown()

	_, err := pool.GetOrCreate(context.Background(), &ScalerMetadata{KubeMQAddress: "h:50000"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "shutting down")
}

func TestPool_CloseAll(t *testing.T) {
	mock1 := &mockKubeMQClient{}
	mock2 := &mockKubeMQClient{}
	mocks := []*mockKubeMQClient{mock1, mock2}
	var idx atomic.Int32
	pool := &ClientPool{
		logger: testLogger(),
		newClient: func(_ context.Context, _ *ScalerMetadata) (KubeMQClient, error) {
			i := idx.Add(1) - 1
			return mocks[i], nil
		},
	}

	_, err := pool.GetOrCreate(context.Background(), &ScalerMetadata{KubeMQAddress: "h1:50000"})
	require.NoError(t, err)
	_, err = pool.GetOrCreate(context.Background(), &ScalerMetadata{KubeMQAddress: "h2:50000"})
	require.NoError(t, err)

	pool.CloseAll()
	assert.True(t, mock1.closed)
	assert.True(t, mock2.closed)
	assert.True(t, pool.closing.Load())
}

func TestPool_Evict(t *testing.T) {
	mock := &mockKubeMQClient{}
	pool := &ClientPool{
		logger: testLogger(),
		newClient: func(_ context.Context, _ *ScalerMetadata) (KubeMQClient, error) {
			return mock, nil
		},
	}

	meta := &ScalerMetadata{KubeMQAddress: "h:50000"}
	_, err := pool.GetOrCreate(context.Background(), meta)
	require.NoError(t, err)

	pool.Evict(meta)
	assert.True(t, mock.closed)

	mock2 := &mockKubeMQClient{}
	pool.newClient = func(_ context.Context, _ *ScalerMetadata) (KubeMQClient, error) {
		return mock2, nil
	}
	client, err := pool.GetOrCreate(context.Background(), meta)
	require.NoError(t, err)
	assert.Equal(t, mock2, client)
}

func TestPool_Evict_NotPresent(t *testing.T) {
	pool := &ClientPool{logger: testLogger()}
	pool.Evict(&ScalerMetadata{KubeMQAddress: "nonexistent:50000"})
}

func TestPool_Shutdown(t *testing.T) {
	pool := NewClientPool(testLogger())
	assert.False(t, pool.closing.Load())
	pool.Shutdown()
	assert.True(t, pool.closing.Load())
}
