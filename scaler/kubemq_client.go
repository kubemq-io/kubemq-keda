package scaler

import (
	"context"
	"fmt"

	kubemq "github.com/kubemq-io/kubemq-go/v2"
)

type KubeMQClient interface {
	ListQueuesChannels(ctx context.Context, search string) ([]*kubemq.ChannelInfo, error)
	Close() error
}

type realKubeMQClient struct {
	client *kubemq.Client
}

func (r *realKubeMQClient) ListQueuesChannels(ctx context.Context, search string) ([]*kubemq.ChannelInfo, error) {
	return r.client.ListQueuesChannels(ctx, search)
}

func (r *realKubeMQClient) Close() error {
	return r.client.Close()
}

func NewKubeMQClient(ctx context.Context, meta *ScalerMetadata) (KubeMQClient, error) {
	opts := []kubemq.Option{
		kubemq.WithAddress(meta.KubeMQHost, meta.KubeMQPort),
		kubemq.WithClientId("kubemq-keda-scaler"),
		kubemq.WithCheckConnection(true),
	}

	if meta.AuthToken != "" {
		opts = append(opts, kubemq.WithAuthToken(meta.AuthToken))
	}

	if meta.TLS {
		opts = append(opts, kubemq.WithTLS(meta.CertFile))
		if meta.ServerOverrideDomain != "" {
			opts = append(opts, kubemq.WithServerNameOverride(meta.ServerOverrideDomain))
		}
	}

	client, err := kubemq.NewClient(ctx, opts...)
	if err != nil {
		return nil, err
	}

	if _, err := client.Ping(ctx); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("kubemq: ping failed after connect: %w", err)
	}

	return &realKubeMQClient{client: client}, nil
}
