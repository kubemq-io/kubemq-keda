package scaler

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	kubemq "github.com/kubemq-io/kubemq-go/v2"
	pb "github.com/kubemq-io/kubemq-keda/pkg/externalscaler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newTestScaler(mock KubeMQClient) (*ClientPool, *ExternalScaler) {
	logger := testLogger()
	pool := &ClientPool{
		logger: logger,
		newClient: func(_ context.Context, _ *ScalerMetadata) (KubeMQClient, error) {
			return mock, nil
		},
	}
	s := NewExternalScaler(pool, logger)
	s.streamInterval = 20 * time.Millisecond
	return pool, s
}

func validRef(overrides ...map[string]string) *pb.ScaledObjectRef {
	m := map[string]string{
		"kubemqAddress": "localhost:50000",
		"queueName":     "test-queue",
	}
	for _, o := range overrides {
		for k, v := range o {
			m[k] = v
		}
	}
	return &pb.ScaledObjectRef{ScalerMetadata: m}
}

// --- mock stream ---

type mockStream struct {
	ctx     context.Context
	sent    []*pb.IsActiveResponse
	sendErr error
}

func (m *mockStream) Send(resp *pb.IsActiveResponse) error {
	if m.sendErr != nil {
		return m.sendErr
	}
	m.sent = append(m.sent, resp)
	return nil
}

func (m *mockStream) SetHeader(metadata.MD) error  { return nil }
func (m *mockStream) SendHeader(metadata.MD) error { return nil }
func (m *mockStream) SetTrailer(metadata.MD)       {}
func (m *mockStream) Context() context.Context     { return m.ctx }
func (m *mockStream) SendMsg(any) error            { return nil }
func (m *mockStream) RecvMsg(any) error            { return nil }

// --- IsActive ---

func TestIsActive_Active(t *testing.T) {
	mock := &mockKubeMQClient{channels: []*kubemq.ChannelInfo{
		{Name: "test-queue", Outgoing: &kubemq.ChannelStats{Waiting: 15}},
	}}
	_, s := newTestScaler(mock)
	resp, err := s.IsActive(context.Background(), validRef())
	require.NoError(t, err)
	assert.True(t, resp.Result)
}

func TestIsActive_Inactive(t *testing.T) {
	mock := &mockKubeMQClient{channels: []*kubemq.ChannelInfo{
		{Name: "test-queue", Outgoing: &kubemq.ChannelStats{Waiting: 0}},
	}}
	_, s := newTestScaler(mock)
	resp, err := s.IsActive(context.Background(), validRef())
	require.NoError(t, err)
	assert.False(t, resp.Result)
}

func TestIsActive_BelowActivation(t *testing.T) {
	mock := &mockKubeMQClient{channels: []*kubemq.ChannelInfo{
		{Name: "test-queue", Outgoing: &kubemq.ChannelStats{Waiting: 3}},
	}}
	_, s := newTestScaler(mock)
	resp, err := s.IsActive(context.Background(), validRef(map[string]string{"activationTargetWaiting": "5"}))
	require.NoError(t, err)
	assert.False(t, resp.Result)
}

func TestIsActive_AboveActivation(t *testing.T) {
	mock := &mockKubeMQClient{channels: []*kubemq.ChannelInfo{
		{Name: "test-queue", Outgoing: &kubemq.ChannelStats{Waiting: 6}},
	}}
	_, s := newTestScaler(mock)
	resp, err := s.IsActive(context.Background(), validRef(map[string]string{"activationTargetWaiting": "5"}))
	require.NoError(t, err)
	assert.True(t, resp.Result)
}

func TestIsActive_QueueNotFound(t *testing.T) {
	mock := &mockKubeMQClient{channels: []*kubemq.ChannelInfo{}}
	_, s := newTestScaler(mock)
	resp, err := s.IsActive(context.Background(), validRef())
	require.NoError(t, err)
	assert.False(t, resp.Result)
}

func TestIsActive_Error(t *testing.T) {
	mock := &mockKubeMQClient{err: fmt.Errorf("connection refused")}
	_, s := newTestScaler(mock)
	_, err := s.IsActive(context.Background(), validRef())
	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err))
}

func TestIsActive_NilOutgoing(t *testing.T) {
	mock := &mockKubeMQClient{channels: []*kubemq.ChannelInfo{
		{Name: "test-queue", Outgoing: nil},
	}}
	_, s := newTestScaler(mock)
	resp, err := s.IsActive(context.Background(), validRef())
	require.NoError(t, err)
	assert.False(t, resp.Result)
}

func TestIsActive_MissingMetadata(t *testing.T) {
	_, s := newTestScaler(&mockKubeMQClient{})
	_, err := s.IsActive(context.Background(), &pb.ScaledObjectRef{ScalerMetadata: map[string]string{}})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

// --- GetMetrics ---

func TestGetMetrics_Success(t *testing.T) {
	mock := &mockKubeMQClient{channels: []*kubemq.ChannelInfo{
		{Name: "test-queue", Outgoing: &kubemq.ChannelStats{Waiting: 42}},
	}}
	_, s := newTestScaler(mock)
	resp, err := s.GetMetrics(context.Background(), &pb.GetMetricsRequest{
		ScaledObjectRef: validRef(),
		MetricName:      metricName,
	})
	require.NoError(t, err)
	require.Len(t, resp.MetricValues, 1)
	assert.Equal(t, float64(42), resp.MetricValues[0].MetricValueFloat)
}

func TestGetMetrics_NilScaledObjectRef(t *testing.T) {
	_, s := newTestScaler(&mockKubeMQClient{})
	_, err := s.GetMetrics(context.Background(), &pb.GetMetricsRequest{ScaledObjectRef: nil})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestGetMetrics_NilReq(t *testing.T) {
	_, s := newTestScaler(&mockKubeMQClient{})
	_, err := s.GetMetrics(context.Background(), nil)
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestGetMetrics_WrongMetricName(t *testing.T) {
	_, s := newTestScaler(&mockKubeMQClient{})
	_, err := s.GetMetrics(context.Background(), &pb.GetMetricsRequest{
		ScaledObjectRef: validRef(),
		MetricName:      "wrong-metric",
	})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestGetMetrics_EmptyMetricNameAccepted(t *testing.T) {
	mock := &mockKubeMQClient{channels: []*kubemq.ChannelInfo{
		{Name: "test-queue", Outgoing: &kubemq.ChannelStats{Waiting: 5}},
	}}
	_, s := newTestScaler(mock)
	resp, err := s.GetMetrics(context.Background(), &pb.GetMetricsRequest{
		ScaledObjectRef: validRef(),
		MetricName:      "",
	})
	require.NoError(t, err)
	assert.Len(t, resp.MetricValues, 1)
}

// --- GetMetricSpec ---

func TestGetMetricSpec_Default(t *testing.T) {
	_, s := newTestScaler(&mockKubeMQClient{})
	resp, err := s.GetMetricSpec(context.Background(), validRef())
	require.NoError(t, err)
	require.Len(t, resp.MetricSpecs, 1)
	assert.Equal(t, 10.0, resp.MetricSpecs[0].TargetSizeFloat)
	assert.Equal(t, "kubemq-queue-waiting", resp.MetricSpecs[0].MetricName)
}

func TestGetMetricSpec_Custom(t *testing.T) {
	_, s := newTestScaler(&mockKubeMQClient{})
	resp, err := s.GetMetricSpec(context.Background(), validRef(map[string]string{"targetWaiting": "25"}))
	require.NoError(t, err)
	assert.Equal(t, 25.0, resp.MetricSpecs[0].TargetSizeFloat)
}

// --- StreamIsActive ---

func TestStreamIsActive_SendsActive(t *testing.T) {
	mock := &mockKubeMQClient{channels: []*kubemq.ChannelInfo{
		{Name: "test-queue", Outgoing: &kubemq.ChannelStats{Waiting: 15}},
	}}
	_, s := newTestScaler(mock)

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()

	ms := &mockStream{ctx: ctx}
	err := s.StreamIsActive(validRef(), ms)
	assert.NoError(t, err)
	require.NotEmpty(t, ms.sent)
	assert.True(t, ms.sent[0].Result)
}

func TestStreamIsActive_StopsOnCancel(t *testing.T) {
	mock := &mockKubeMQClient{channels: []*kubemq.ChannelInfo{
		{Name: "test-queue", Outgoing: &kubemq.ChannelStats{Waiting: 5}},
	}}
	_, s := newTestScaler(mock)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()

	ms := &mockStream{ctx: ctx}
	err := s.StreamIsActive(validRef(), ms)
	assert.NoError(t, err)
}

func TestStreamIsActive_InitialError_Returns(t *testing.T) {
	mock := &mockKubeMQClient{err: fmt.Errorf("connection refused")}
	_, s := newTestScaler(mock)

	ctx := context.Background()
	ms := &mockStream{ctx: ctx}
	err := s.StreamIsActive(validRef(), ms)
	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err))
	assert.Empty(t, ms.sent)
}

func TestStreamIsActive_InvalidMetadata(t *testing.T) {
	_, s := newTestScaler(&mockKubeMQClient{})
	ms := &mockStream{ctx: context.Background()}
	err := s.StreamIsActive(&pb.ScaledObjectRef{ScalerMetadata: map[string]string{}}, ms)
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestStreamIsActive_SendError(t *testing.T) {
	mock := &mockKubeMQClient{channels: []*kubemq.ChannelInfo{
		{Name: "test-queue", Outgoing: &kubemq.ChannelStats{Waiting: 5}},
	}}
	_, s := newTestScaler(mock)
	ms := &mockStream{ctx: context.Background(), sendErr: fmt.Errorf("stream broken")}
	err := s.StreamIsActive(validRef(), ms)
	require.Error(t, err)
}

func TestStreamIsActive_TickerSendsMultiple(t *testing.T) {
	mock := &mockKubeMQClient{channels: []*kubemq.ChannelInfo{
		{Name: "test-queue", Outgoing: &kubemq.ChannelStats{Waiting: 10}},
	}}
	_, s := newTestScaler(mock)

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()

	ms := &mockStream{ctx: ctx}
	err := s.StreamIsActive(validRef(), ms)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, len(ms.sent), 2)
}

// --- Error Mapping ---

func TestMapKubeMQError_Nil(t *testing.T) {
	assert.Nil(t, mapKubeMQError(nil))
}

func TestMapKubeMQError_Authentication(t *testing.T) {
	err := mapKubeMQError(&kubemq.KubeMQError{Code: kubemq.ErrCodeAuthentication, Message: "bad token"})
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
}

func TestMapKubeMQError_Authorization(t *testing.T) {
	err := mapKubeMQError(&kubemq.KubeMQError{Code: kubemq.ErrCodeAuthorization, Message: "denied"})
	assert.Equal(t, codes.PermissionDenied, status.Code(err))
}

func TestMapKubeMQError_Timeout(t *testing.T) {
	err := mapKubeMQError(&kubemq.KubeMQError{Code: kubemq.ErrCodeTimeout})
	assert.Equal(t, codes.DeadlineExceeded, status.Code(err))
}

func TestMapKubeMQError_Cancellation(t *testing.T) {
	err := mapKubeMQError(&kubemq.KubeMQError{Code: kubemq.ErrCodeCancellation})
	assert.Equal(t, codes.Canceled, status.Code(err))
}

func TestMapKubeMQError_Validation(t *testing.T) {
	err := mapKubeMQError(&kubemq.KubeMQError{Code: kubemq.ErrCodeValidation})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestMapKubeMQError_Transient(t *testing.T) {
	err := mapKubeMQError(&kubemq.KubeMQError{Code: kubemq.ErrCodeTransient})
	assert.Equal(t, codes.Unavailable, status.Code(err))
}

func TestMapKubeMQError_Throttling(t *testing.T) {
	err := mapKubeMQError(&kubemq.KubeMQError{Code: kubemq.ErrCodeThrottling})
	assert.Equal(t, codes.ResourceExhausted, status.Code(err))
}

func TestMapKubeMQError_NotFound(t *testing.T) {
	err := mapKubeMQError(&kubemq.KubeMQError{Code: kubemq.ErrCodeNotFound})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestMapKubeMQError_Fatal(t *testing.T) {
	err := mapKubeMQError(&kubemq.KubeMQError{Code: kubemq.ErrCodeFatal})
	assert.Equal(t, codes.Internal, status.Code(err))
}

func TestMapKubeMQError_Backpressure(t *testing.T) {
	err := mapKubeMQError(&kubemq.KubeMQError{Code: kubemq.ErrCodeBackpressure})
	assert.Equal(t, codes.ResourceExhausted, status.Code(err))
}

func TestMapKubeMQError_ContextCanceled(t *testing.T) {
	err := mapKubeMQError(context.Canceled)
	assert.Equal(t, codes.Canceled, status.Code(err))
}

func TestMapKubeMQError_ContextDeadline(t *testing.T) {
	err := mapKubeMQError(context.DeadlineExceeded)
	assert.Equal(t, codes.DeadlineExceeded, status.Code(err))
}

func TestMapKubeMQError_ExistingGRPCStatus(t *testing.T) {
	original := status.Error(codes.ResourceExhausted, "rate limited")
	result := mapKubeMQError(original)
	assert.Equal(t, codes.ResourceExhausted, status.Code(result))
}

func TestMapKubeMQError_DefaultUnavailable(t *testing.T) {
	err := mapKubeMQError(fmt.Errorf("unknown error"))
	assert.Equal(t, codes.Unavailable, status.Code(err))
}

func TestMapKubeMQError_SanitizedMessage(t *testing.T) {
	err := mapKubeMQError(&kubemq.KubeMQError{
		Code:    kubemq.ErrCodeAuthentication,
		Message: "secret internal details about /path/to/file",
	})
	st := status.Convert(err)
	assert.Equal(t, "kubemq: authentication failed", st.Message())
	assert.NotContains(t, st.Message(), "/path/to/file")
}

// --- getWaiting ---

func TestGetWaiting_PoolError(t *testing.T) {
	logger := testLogger()
	pool := &ClientPool{
		logger: logger,
		newClient: func(_ context.Context, _ *ScalerMetadata) (KubeMQClient, error) {
			return nil, fmt.Errorf("connection refused")
		},
	}
	s := NewExternalScaler(pool, logger)
	_, err := s.IsActive(context.Background(), validRef())
	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err))
}

func TestGetWaiting_MultipleQueues_ExactMatch(t *testing.T) {
	mock := &mockKubeMQClient{channels: []*kubemq.ChannelInfo{
		{Name: "other-queue", Outgoing: &kubemq.ChannelStats{Waiting: 100}},
		{Name: "test-queue", Outgoing: &kubemq.ChannelStats{Waiting: 7}},
		{Name: "test-queue-2", Outgoing: &kubemq.ChannelStats{Waiting: 200}},
	}}
	_, s := newTestScaler(mock)
	resp, err := s.GetMetrics(context.Background(), &pb.GetMetricsRequest{
		ScaledObjectRef: validRef(),
		MetricName:      metricName,
	})
	require.NoError(t, err)
	assert.Equal(t, float64(7), resp.MetricValues[0].MetricValueFloat)
}

func TestGetWaiting_NilChannelEntry(t *testing.T) {
	mock := &mockKubeMQClient{channels: []*kubemq.ChannelInfo{
		nil,
		{Name: "test-queue", Outgoing: &kubemq.ChannelStats{Waiting: 5}},
	}}
	_, s := newTestScaler(mock)
	resp, err := s.GetMetrics(context.Background(), &pb.GetMetricsRequest{
		ScaledObjectRef: validRef(),
		MetricName:      metricName,
	})
	require.NoError(t, err)
	assert.Equal(t, float64(5), resp.MetricValues[0].MetricValueFloat)
}

// --- dynamic mock for per-call control ---

type dynamicMockClient struct {
	fn     func(ctx context.Context, search string) ([]*kubemq.ChannelInfo, error)
	closed bool
}

func (d *dynamicMockClient) ListQueuesChannels(ctx context.Context, search string) ([]*kubemq.ChannelInfo, error) {
	return d.fn(ctx, search)
}

func (d *dynamicMockClient) Close() error {
	d.closed = true
	return nil
}

func TestGetWaiting_RetryOnTransient(t *testing.T) {
	callCount := 0
	mock := &dynamicMockClient{
		fn: func(_ context.Context, _ string) ([]*kubemq.ChannelInfo, error) {
			callCount++
			if callCount == 1 {
				return nil, fmt.Errorf("transient error")
			}
			return []*kubemq.ChannelInfo{
				{Name: "test-queue", Outgoing: &kubemq.ChannelStats{Waiting: 10}},
			}, nil
		},
	}
	logger := testLogger()
	pool := &ClientPool{
		logger: logger,
		newClient: func(_ context.Context, _ *ScalerMetadata) (KubeMQClient, error) {
			return mock, nil
		},
	}
	s := NewExternalScaler(pool, logger)
	resp, err := s.GetMetrics(context.Background(), &pb.GetMetricsRequest{
		ScaledObjectRef: validRef(),
		MetricName:      metricName,
	})
	require.NoError(t, err)
	assert.Equal(t, float64(10), resp.MetricValues[0].MetricValueFloat)
	assert.Equal(t, 2, callCount)
}
