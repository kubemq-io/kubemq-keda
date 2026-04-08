//go:build integration

package scaler

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	kubemq "github.com/kubemq-io/kubemq-go/v2"
	pb "github.com/kubemq-io/kubemq-keda/pkg/externalscaler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

const brokerAddress = "localhost:50000"

func integrationLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func uniqueQueue(prefix string) string {
	return fmt.Sprintf("keda-integ-%s-%d", prefix, time.Now().UnixNano())
}

func sendMessages(t *testing.T, ctx context.Context, queue string, count int) {
	t.Helper()
	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("integ-sender"),
		kubemq.WithCheckConnection(true),
	)
	require.NoError(t, err)
	defer client.Close()

	for i := 0; i < count; i++ {
		msg := kubemq.NewQueueMessage().
			SetChannel(queue).
			SetBody([]byte(fmt.Sprintf("msg-%d", i)))
		_, err := client.SendQueueMessage(ctx, msg)
		require.NoError(t, err, "failed to send message %d", i)
	}
}

func consumeMessages(t *testing.T, ctx context.Context, queue string, count int) {
	t.Helper()
	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("integ-consumer"),
		kubemq.WithCheckConnection(true),
	)
	require.NoError(t, err)
	defer client.Close()

	resp, err := client.PollQueue(ctx, &kubemq.PollRequest{
		Channel:            queue,
		MaxItems:           int32(count),
		WaitTimeoutSeconds: 5,
		AutoAck:            true,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
}

func newIntegScaler(t *testing.T) (*ClientPool, *ExternalScaler) {
	t.Helper()
	logger := integrationLogger()
	pool := NewClientPool(logger)
	t.Cleanup(func() { pool.CloseAll() })
	s := NewExternalScaler(pool, logger)
	return pool, s
}

func makeRef(queue string) *pb.ScaledObjectRef {
	return &pb.ScaledObjectRef{
		ScalerMetadata: map[string]string{
			"kubemqAddress": brokerAddress,
			"queueName":     queue,
		},
	}
}

func waitForWaiting(t *testing.T, s *ExternalScaler, queue string, minWaiting float64, timeout time.Duration) float64 {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastVal float64
	for time.Now().Before(deadline) {
		resp, err := s.GetMetrics(context.Background(), &pb.GetMetricsRequest{
			ScaledObjectRef: makeRef(queue),
			MetricName:      metricName,
		})
		if err == nil && len(resp.MetricValues) > 0 {
			lastVal = resp.MetricValues[0].MetricValueFloat
			if lastVal >= minWaiting {
				return lastVal
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for queue %q to have >= %.0f waiting (last seen: %.0f)", queue, minWaiting, lastVal)
	return 0
}

func waitForDrain(t *testing.T, s *ExternalScaler, queue string, maxWaiting float64, timeout time.Duration) float64 {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastVal float64
	for time.Now().Before(deadline) {
		resp, err := s.GetMetrics(context.Background(), &pb.GetMetricsRequest{
			ScaledObjectRef: makeRef(queue),
			MetricName:      metricName,
		})
		if err == nil && len(resp.MetricValues) > 0 {
			lastVal = resp.MetricValues[0].MetricValueFloat
			if lastVal <= maxWaiting {
				return lastVal
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for queue %q to drain to <= %.0f (last seen: %.0f)", queue, maxWaiting, lastVal)
	return 0
}

// --- NewKubeMQClient (real SDK wrapper) ---

func TestIntegration_NewKubeMQClient_Success(t *testing.T) {
	meta := &ScalerMetadata{
		KubeMQAddress: brokerAddress,
		KubeMQHost:    "localhost",
		KubeMQPort:    50000,
		QueueName:     "test",
	}

	client, err := NewKubeMQClient(context.Background(), meta)
	require.NoError(t, err)
	require.NotNil(t, client)
	defer client.Close()

	channels, err := client.ListQueuesChannels(context.Background(), "")
	require.NoError(t, err)
	assert.NotNil(t, channels)
}

func TestIntegration_NewKubeMQClient_BadAddress(t *testing.T) {
	meta := &ScalerMetadata{
		KubeMQAddress: "nonexistent-host:50000",
		KubeMQHost:    "nonexistent-host",
		KubeMQPort:    50000,
		QueueName:     "test",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := NewKubeMQClient(ctx, meta)
	assert.Error(t, err)
}

// --- IsActive ---

func TestIntegration_IsActive_EmptyQueue(t *testing.T) {
	_, s := newIntegScaler(t)
	queue := uniqueQueue("isactive-empty")

	resp, err := s.IsActive(context.Background(), makeRef(queue))
	require.NoError(t, err)
	assert.False(t, resp.Result)
}

func TestIntegration_IsActive_WithMessages(t *testing.T) {
	ctx := context.Background()
	queue := uniqueQueue("isactive-msgs")

	sendMessages(t, ctx, queue, 5)

	_, s := newIntegScaler(t)
	waitForWaiting(t, s, queue, 1, 10*time.Second)

	resp, err := s.IsActive(ctx, makeRef(queue))
	require.NoError(t, err)
	assert.True(t, resp.Result)
}

func TestIntegration_IsActive_WithActivationThreshold(t *testing.T) {
	ctx := context.Background()
	queue := uniqueQueue("isactive-threshold")

	sendMessages(t, ctx, queue, 3)

	_, s := newIntegScaler(t)
	waitForWaiting(t, s, queue, 3, 10*time.Second)

	ref := &pb.ScaledObjectRef{
		ScalerMetadata: map[string]string{
			"kubemqAddress":           brokerAddress,
			"queueName":               queue,
			"activationTargetWaiting": "5",
		},
	}
	resp, err := s.IsActive(ctx, ref)
	require.NoError(t, err)
	assert.False(t, resp.Result, "3 messages should be below activation threshold of 5")

	sendMessages(t, ctx, queue, 5)
	waitForWaiting(t, s, queue, 6, 10*time.Second)

	resp, err = s.IsActive(ctx, ref)
	require.NoError(t, err)
	assert.True(t, resp.Result, "8 messages should be above activation threshold of 5")
}

// --- GetMetrics ---

func TestIntegration_GetMetrics_EmptyQueue(t *testing.T) {
	_, s := newIntegScaler(t)
	queue := uniqueQueue("metrics-empty")

	resp, err := s.GetMetrics(context.Background(), &pb.GetMetricsRequest{
		ScaledObjectRef: makeRef(queue),
		MetricName:      metricName,
	})
	require.NoError(t, err)
	require.Len(t, resp.MetricValues, 1)
	assert.Equal(t, float64(0), resp.MetricValues[0].MetricValueFloat)
}

func TestIntegration_GetMetrics_WithMessages(t *testing.T) {
	ctx := context.Background()
	queue := uniqueQueue("metrics-msgs")
	const msgCount = 7

	sendMessages(t, ctx, queue, msgCount)

	_, s := newIntegScaler(t)
	val := waitForWaiting(t, s, queue, float64(msgCount), 10*time.Second)
	assert.GreaterOrEqual(t, val, float64(msgCount))
}

func TestIntegration_GetMetrics_AfterConsume(t *testing.T) {
	ctx := context.Background()
	queue := uniqueQueue("metrics-consume")
	const sendCount = 10
	const consumeCount = 6

	sendMessages(t, ctx, queue, sendCount)

	_, s := newIntegScaler(t)
	before := waitForWaiting(t, s, queue, float64(sendCount), 10*time.Second)
	assert.GreaterOrEqual(t, before, float64(sendCount))

	consumeMessages(t, ctx, queue, consumeCount)
	after := waitForDrain(t, s, queue, before-1, 10*time.Second)
	assert.Less(t, after, before, "waiting count should decrease after consuming")
}

func TestIntegration_GetMetrics_MetricName(t *testing.T) {
	_, s := newIntegScaler(t)
	queue := uniqueQueue("metrics-name")

	resp, err := s.GetMetrics(context.Background(), &pb.GetMetricsRequest{
		ScaledObjectRef: makeRef(queue),
		MetricName:      metricName,
	})
	require.NoError(t, err)
	require.Len(t, resp.MetricValues, 1)
	assert.Equal(t, "kubemq-queue-waiting", resp.MetricValues[0].MetricName)
}

// --- GetMetricSpec ---

func TestIntegration_GetMetricSpec_Default(t *testing.T) {
	_, s := newIntegScaler(t)
	queue := uniqueQueue("spec-default")

	resp, err := s.GetMetricSpec(context.Background(), makeRef(queue))
	require.NoError(t, err)
	require.Len(t, resp.MetricSpecs, 1)
	assert.Equal(t, "kubemq-queue-waiting", resp.MetricSpecs[0].MetricName)
	assert.Equal(t, 10.0, resp.MetricSpecs[0].TargetSizeFloat)
}

func TestIntegration_GetMetricSpec_Custom(t *testing.T) {
	_, s := newIntegScaler(t)
	queue := uniqueQueue("spec-custom")

	ref := &pb.ScaledObjectRef{
		ScalerMetadata: map[string]string{
			"kubemqAddress": brokerAddress,
			"queueName":     queue,
			"targetWaiting": "25",
		},
	}
	resp, err := s.GetMetricSpec(context.Background(), ref)
	require.NoError(t, err)
	require.Len(t, resp.MetricSpecs, 1)
	assert.Equal(t, 25.0, resp.MetricSpecs[0].TargetSizeFloat)
}

// --- StreamIsActive ---

type integMockStream struct {
	ctx  context.Context
	mu   sync.Mutex
	sent []*pb.IsActiveResponse
}

func (m *integMockStream) Send(resp *pb.IsActiveResponse) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sent = append(m.sent, resp)
	return nil
}

func (m *integMockStream) getSent() []*pb.IsActiveResponse {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]*pb.IsActiveResponse, len(m.sent))
	copy(cp, m.sent)
	return cp
}

func (m *integMockStream) SetHeader(metadata.MD) error  { return nil }
func (m *integMockStream) SendHeader(metadata.MD) error { return nil }
func (m *integMockStream) SetTrailer(metadata.MD)       {}
func (m *integMockStream) Context() context.Context     { return m.ctx }
func (m *integMockStream) SendMsg(any) error            { return nil }
func (m *integMockStream) RecvMsg(any) error            { return nil }

func TestIntegration_StreamIsActive_EmptyQueue(t *testing.T) {
	_, s := newIntegScaler(t)
	s.streamInterval = 100 * time.Millisecond
	queue := uniqueQueue("stream-empty")

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	ms := &integMockStream{ctx: ctx}
	err := s.StreamIsActive(makeRef(queue), ms)
	assert.NoError(t, err)

	sent := ms.getSent()
	require.NotEmpty(t, sent)
	assert.False(t, sent[0].Result)
}

func TestIntegration_StreamIsActive_WithMessages(t *testing.T) {
	ctx := context.Background()
	queue := uniqueQueue("stream-msgs")
	sendMessages(t, ctx, queue, 5)

	_, s := newIntegScaler(t)
	waitForWaiting(t, s, queue, 5, 10*time.Second)

	s.streamInterval = 200 * time.Millisecond

	streamCtx, cancel := context.WithTimeout(ctx, 700*time.Millisecond)
	defer cancel()

	ms := &integMockStream{ctx: streamCtx}
	err := s.StreamIsActive(makeRef(queue), ms)
	assert.NoError(t, err)

	sent := ms.getSent()
	require.NotEmpty(t, sent)
	hasActive := false
	for _, r := range sent {
		if r.Result {
			hasActive = true
			break
		}
	}
	assert.True(t, hasActive, "at least one poll should detect waiting messages")
}

// --- Connection Pooling ---

func TestIntegration_ConnectionPooling_Reuse(t *testing.T) {
	pool, _ := newIntegScaler(t)

	meta := &ScalerMetadata{
		KubeMQAddress: brokerAddress,
		KubeMQHost:    "localhost",
		KubeMQPort:    50000,
		QueueName:     "pool-test",
	}

	c1, err := pool.GetOrCreate(context.Background(), meta)
	require.NoError(t, err)
	c2, err := pool.GetOrCreate(context.Background(), meta)
	require.NoError(t, err)
	assert.Same(t, c1, c2, "same address should return same client")
}

func TestIntegration_ConnectionPooling_ConcurrentAccess(t *testing.T) {
	pool, s := newIntegScaler(t)
	_ = pool
	queue := uniqueQueue("pool-concurrent")

	sendMessages(t, context.Background(), queue, 3)
	waitForWaiting(t, s, queue, 3, 10*time.Second)

	const goroutines = 20
	var wg sync.WaitGroup
	errs := make([]error, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, errs[idx] = s.IsActive(context.Background(), makeRef(queue))
		}(i)
	}
	wg.Wait()

	for i := 0; i < goroutines; i++ {
		assert.NoError(t, errs[i], "goroutine %d failed", i)
	}
}

// --- End-to-End Scaling Scenario ---

func TestIntegration_E2E_ScaleUpAndDown(t *testing.T) {
	ctx := context.Background()
	queue := uniqueQueue("e2e-scaling")
	_, s := newIntegScaler(t)

	resp, err := s.IsActive(ctx, makeRef(queue))
	require.NoError(t, err)
	assert.False(t, resp.Result, "should be inactive with no messages")

	sendMessages(t, ctx, queue, 20)
	waiting := waitForWaiting(t, s, queue, 20, 10*time.Second)

	resp, err = s.IsActive(ctx, makeRef(queue))
	require.NoError(t, err)
	assert.True(t, resp.Result, "should be active after sending messages")

	spec, err := s.GetMetricSpec(ctx, makeRef(queue))
	require.NoError(t, err)
	target := spec.MetricSpecs[0].TargetSizeFloat
	expectedReplicas := int(waiting/target) + 1
	assert.GreaterOrEqual(t, expectedReplicas, 2, "with 20 messages and target 10, should want 2+ replicas")

	consumeMessages(t, ctx, queue, 20)
	waitForDrain(t, s, queue, 1, 10*time.Second)

	resp, err = s.IsActive(ctx, makeRef(queue))
	require.NoError(t, err)
	assert.False(t, resp.Result, "should be inactive after consuming all messages")
}

// --- Real Client Wrapper ---

func TestIntegration_RealClient_ListQueuesChannels(t *testing.T) {
	ctx := context.Background()
	queue := uniqueQueue("realclient-list")
	sendMessages(t, ctx, queue, 3)

	meta := &ScalerMetadata{
		KubeMQAddress: brokerAddress,
		KubeMQHost:    "localhost",
		KubeMQPort:    50000,
		QueueName:     queue,
	}

	client, err := NewKubeMQClient(ctx, meta)
	require.NoError(t, err)
	defer client.Close()

	require.Eventually(t, func() bool {
		channels, err := client.ListQueuesChannels(ctx, queue)
		if err != nil {
			return false
		}
		for _, ch := range channels {
			if ch.Name == queue && ch.Outgoing != nil && ch.Outgoing.Waiting >= 3 {
				return true
			}
		}
		return false
	}, 15*time.Second, 500*time.Millisecond, "queue %q should appear with >= 3 waiting", queue)
}

func TestIntegration_RealClient_Close(t *testing.T) {
	meta := &ScalerMetadata{
		KubeMQAddress: brokerAddress,
		KubeMQHost:    "localhost",
		KubeMQPort:    50000,
		QueueName:     "close-test",
	}

	client, err := NewKubeMQClient(context.Background(), meta)
	require.NoError(t, err)

	err = client.Close()
	assert.NoError(t, err)
}

// --- Multiple Queues ---

func TestIntegration_MultipleQueues_ExactMatch(t *testing.T) {
	ctx := context.Background()
	queue1 := uniqueQueue("multi-q1")
	queue2 := uniqueQueue("multi-q2")

	sendMessages(t, ctx, queue1, 3)
	sendMessages(t, ctx, queue2, 7)

	_, s := newIntegScaler(t)

	val1 := waitForWaiting(t, s, queue1, 3, 10*time.Second)
	val2 := waitForWaiting(t, s, queue2, 7, 10*time.Second)

	assert.GreaterOrEqual(t, val1, float64(3))
	assert.GreaterOrEqual(t, val2, float64(7))
	assert.NotEqual(t, val1, val2, "different queues should have different waiting counts")
}
