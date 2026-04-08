package scaler

import (
	"context"
	"errors"
	"log/slog"
	"time"

	kubemq "github.com/kubemq-io/kubemq-go/v2"
	pb "github.com/kubemq-io/kubemq-keda/pkg/externalscaler"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const metricName = "kubemq-queue-waiting"
const streamIsActiveInterval = 5 * time.Second
const getWaitingMaxRetries = 2
const getWaitingRetryDelay = 200 * time.Millisecond

type ExternalScaler struct {
	pb.UnimplementedExternalScalerServer
	pool           *ClientPool
	logger         *slog.Logger
	streamInterval time.Duration
}

func NewExternalScaler(pool *ClientPool, logger *slog.Logger) *ExternalScaler {
	return &ExternalScaler{
		pool:           pool,
		logger:         logger,
		streamInterval: streamIsActiveInterval,
	}
}

func (s *ExternalScaler) IsActive(ctx context.Context, ref *pb.ScaledObjectRef) (*pb.IsActiveResponse, error) {
	meta, err := ParseScalerMetadata(ref.ScalerMetadata)
	if err != nil {
		return nil, err
	}

	waiting, err := s.getWaiting(ctx, meta)
	if err != nil {
		return nil, err
	}

	return &pb.IsActiveResponse{
		Result: float64(waiting) > meta.ActivationTargetWaiting,
	}, nil
}

func (s *ExternalScaler) StreamIsActive(ref *pb.ScaledObjectRef, stream grpc.ServerStreamingServer[pb.IsActiveResponse]) error {
	meta, err := ParseScalerMetadata(ref.ScalerMetadata)
	if err != nil {
		return err
	}

	waiting, err := s.getWaiting(stream.Context(), meta)
	if err != nil {
		return mapKubeMQError(err)
	}
	if sendErr := stream.Send(&pb.IsActiveResponse{
		Result: float64(waiting) > meta.ActivationTargetWaiting,
	}); sendErr != nil {
		return sendErr
	}

	ticker := time.NewTicker(s.streamInterval)
	defer ticker.Stop()

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case <-ticker.C:
			waiting, err := s.getWaiting(stream.Context(), meta)
			if err != nil {
				s.logger.Warn("StreamIsActive poll failed",
					"kubemq_address", meta.KubeMQAddress,
					"queue_name", meta.QueueName,
					"error", err,
				)
				continue
			}
			if sendErr := stream.Send(&pb.IsActiveResponse{
				Result: float64(waiting) > meta.ActivationTargetWaiting,
			}); sendErr != nil {
				return sendErr
			}
		}
	}
}

func (s *ExternalScaler) GetMetricSpec(ctx context.Context, ref *pb.ScaledObjectRef) (*pb.GetMetricSpecResponse, error) {
	meta, err := ParseScalerMetadata(ref.ScalerMetadata)
	if err != nil {
		return nil, err
	}

	return &pb.GetMetricSpecResponse{
		MetricSpecs: []*pb.MetricSpec{
			{
				MetricName:      metricName,
				TargetSizeFloat: meta.TargetWaiting,
			},
		},
	}, nil
}

func (s *ExternalScaler) GetMetrics(ctx context.Context, req *pb.GetMetricsRequest) (*pb.GetMetricsResponse, error) {
	if req == nil || req.ScaledObjectRef == nil {
		return nil, status.Error(codes.InvalidArgument, "scaledObjectRef is required")
	}

	if req.MetricName != "" && req.MetricName != metricName {
		return nil, status.Errorf(codes.InvalidArgument, "unknown metric %q; only %q is supported", req.MetricName, metricName)
	}

	meta, err := ParseScalerMetadata(req.ScaledObjectRef.ScalerMetadata)
	if err != nil {
		return nil, err
	}

	waiting, err := s.getWaiting(ctx, meta)
	if err != nil {
		return nil, err
	}

	return &pb.GetMetricsResponse{
		MetricValues: []*pb.MetricValue{
			{
				MetricName:       metricName,
				MetricValueFloat: float64(waiting),
			},
		},
	}, nil
}

func (s *ExternalScaler) getWaiting(ctx context.Context, meta *ScalerMetadata) (int64, error) {
	var lastErr error

	for attempt := 0; attempt < getWaitingMaxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return 0, mapKubeMQError(ctx.Err())
			case <-time.After(getWaitingRetryDelay):
			}
		}

		start := time.Now()
		client, err := s.pool.GetOrCreate(ctx, meta)
		if err != nil {
			s.logger.Warn("failed to get KubeMQ client",
				"kubemq_address", meta.KubeMQAddress,
				"queue_name", meta.QueueName,
				"error", err,
				"duration_ms", float64(time.Since(start).Microseconds())/1000,
			)
			return 0, mapKubeMQError(err)
		}

		channels, err := client.ListQueuesChannels(ctx, meta.QueueName)
		if err != nil {
			lastErr = err
			mapped := mapKubeMQError(err)
			s.logger.Warn("ListChannels failed",
				"kubemq_address", meta.KubeMQAddress,
				"queue_name", meta.QueueName,
				"error", err,
				"attempt", attempt+1,
				"duration_ms", float64(time.Since(start).Microseconds())/1000,
			)

			code := status.Code(mapped)
			if code == codes.Unavailable || code == codes.DeadlineExceeded {
				if attempt == getWaitingMaxRetries-1 {
					s.pool.Evict(meta)
				}
				continue
			}
			return 0, mapped
		}

		var waiting int64
		found := false
		for _, ch := range channels {
			if ch == nil {
				continue
			}
			if ch.Name == meta.QueueName {
				found = true
				if ch.Outgoing != nil {
					waiting = ch.Outgoing.Waiting
				}
				break
			}
		}

		if !found && len(channels) > 0 {
			s.logger.Debug("queue not found in channel list",
				"kubemq_address", meta.KubeMQAddress,
				"queue_name", meta.QueueName,
				"channels_returned", len(channels),
			)
		}

		duration := float64(time.Since(start).Microseconds()) / 1000
		s.logger.Debug("poll result",
			"kubemq_address", meta.KubeMQAddress,
			"queue_name", meta.QueueName,
			"waiting_count", waiting,
			"is_active", float64(waiting) > meta.ActivationTargetWaiting,
			"duration_ms", duration,
		)

		return waiting, nil
	}

	return 0, mapKubeMQError(lastErr)
}

func mapKubeMQError(err error) error {
	if err == nil {
		return nil
	}

	var kubemqErr *kubemq.KubeMQError
	if errors.As(err, &kubemqErr) {
		switch kubemqErr.Code {
		case kubemq.ErrCodeAuthentication:
			return status.Error(codes.Unauthenticated, "kubemq: authentication failed")
		case kubemq.ErrCodeAuthorization:
			return status.Error(codes.PermissionDenied, "kubemq: permission denied")
		case kubemq.ErrCodeTimeout:
			return status.Error(codes.DeadlineExceeded, "kubemq: operation timed out")
		case kubemq.ErrCodeCancellation:
			return status.Error(codes.Canceled, "kubemq: operation canceled")
		case kubemq.ErrCodeValidation:
			return status.Error(codes.InvalidArgument, "kubemq: validation error")
		case kubemq.ErrCodeTransient:
			return status.Error(codes.Unavailable, "kubemq: transient error")
		case kubemq.ErrCodeThrottling:
			return status.Error(codes.ResourceExhausted, "kubemq: throttled")
		case kubemq.ErrCodeNotFound:
			return status.Error(codes.NotFound, "kubemq: not found")
		case kubemq.ErrCodeFatal:
			return status.Error(codes.Internal, "kubemq: internal error")
		case kubemq.ErrCodeBackpressure:
			return status.Error(codes.ResourceExhausted, "kubemq: backpressure")
		}
	}

	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, "kubemq: operation canceled")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, "kubemq: deadline exceeded")
	}

	if st, ok := status.FromError(err); ok {
		return st.Err()
	}

	return status.Error(codes.Unavailable, "kubemq: unavailable")
}
