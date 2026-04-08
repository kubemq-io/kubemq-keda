package scaler

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestParseMetadata_Valid(t *testing.T) {
	meta := map[string]string{
		"kubemqAddress": "localhost:50000",
		"queueName":     "test-queue",
		"targetWaiting": "5",
	}
	result, err := ParseScalerMetadata(meta)
	require.NoError(t, err)
	assert.Equal(t, "localhost", result.KubeMQHost)
	assert.Equal(t, 50000, result.KubeMQPort)
	assert.Equal(t, "test-queue", result.QueueName)
	assert.Equal(t, 5.0, result.TargetWaiting)
	assert.Equal(t, 0.0, result.ActivationTargetWaiting)
}

func TestParseMetadata_Defaults(t *testing.T) {
	meta := map[string]string{
		"kubemqAddress": "localhost:50000",
		"queueName":     "test-queue",
	}
	result, err := ParseScalerMetadata(meta)
	require.NoError(t, err)
	assert.Equal(t, 10.0, result.TargetWaiting)
	assert.Equal(t, 0.0, result.ActivationTargetWaiting)
	assert.False(t, result.TLS)
	assert.Empty(t, result.AuthToken)
}

func TestParseMetadata_MissingAddress(t *testing.T) {
	_, err := ParseScalerMetadata(map[string]string{"queueName": "q"})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestParseMetadata_MissingQueueName(t *testing.T) {
	_, err := ParseScalerMetadata(map[string]string{"kubemqAddress": "localhost:50000"})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestParseMetadata_InvalidTargetWaiting(t *testing.T) {
	meta := map[string]string{
		"kubemqAddress": "localhost:50000",
		"queueName":     "q",
		"targetWaiting": "invalid",
	}
	_, err := ParseScalerMetadata(meta)
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestParseMetadata_NegativeTargetWaiting(t *testing.T) {
	meta := map[string]string{
		"kubemqAddress": "localhost:50000",
		"queueName":     "q",
		"targetWaiting": "-5",
	}
	_, err := ParseScalerMetadata(meta)
	require.Error(t, err)
}

func TestParseMetadata_ZeroTargetWaiting(t *testing.T) {
	meta := map[string]string{
		"kubemqAddress": "localhost:50000",
		"queueName":     "q",
		"targetWaiting": "0",
	}
	_, err := ParseScalerMetadata(meta)
	require.Error(t, err)
}

func TestParseMetadata_NaNTargetWaiting(t *testing.T) {
	meta := map[string]string{
		"kubemqAddress": "localhost:50000",
		"queueName":     "q",
		"targetWaiting": "NaN",
	}
	_, err := ParseScalerMetadata(meta)
	require.Error(t, err)
	assert.Contains(t, status.Convert(err).Message(), "finite")
}

func TestParseMetadata_InfTargetWaiting(t *testing.T) {
	meta := map[string]string{
		"kubemqAddress": "localhost:50000",
		"queueName":     "q",
		"targetWaiting": "+Inf",
	}
	_, err := ParseScalerMetadata(meta)
	require.Error(t, err)
}

func TestParseMetadata_NaNActivation(t *testing.T) {
	meta := map[string]string{
		"kubemqAddress":           "localhost:50000",
		"queueName":               "q",
		"activationTargetWaiting": "NaN",
	}
	_, err := ParseScalerMetadata(meta)
	require.Error(t, err)
}

func TestParseMetadata_NegativeActivation(t *testing.T) {
	meta := map[string]string{
		"kubemqAddress":           "localhost:50000",
		"queueName":               "q",
		"activationTargetWaiting": "-1",
	}
	_, err := ParseScalerMetadata(meta)
	require.Error(t, err)
}

func TestParseMetadata_BooleanTLS(t *testing.T) {
	trueCases := []string{"true", "True", "TRUE", "1", "yes", "Yes"}
	falseCases := []string{"false", "0", "no", "anything"}

	for _, tc := range trueCases {
		t.Run(tc, func(t *testing.T) {
			meta := map[string]string{
				"kubemqAddress": "localhost:50000",
				"queueName":     "q",
				"tls":           tc,
			}
			result, err := ParseScalerMetadata(meta)
			require.NoError(t, err)
			assert.True(t, result.TLS)
		})
	}
	for _, tc := range falseCases {
		t.Run(tc, func(t *testing.T) {
			meta := map[string]string{
				"kubemqAddress": "localhost:50000",
				"queueName":     "q",
				"tls":           tc,
			}
			result, err := ParseScalerMetadata(meta)
			require.NoError(t, err)
			assert.False(t, result.TLS)
		})
	}
}

func TestParseMetadata_IPv6(t *testing.T) {
	meta := map[string]string{
		"kubemqAddress": "[::1]:50000",
		"queueName":     "q",
	}
	result, err := ParseScalerMetadata(meta)
	require.NoError(t, err)
	assert.Equal(t, "::1", result.KubeMQHost)
	assert.Equal(t, 50000, result.KubeMQPort)
}

func TestParseMetadata_MalformedAddress(t *testing.T) {
	_, err := ParseScalerMetadata(map[string]string{
		"kubemqAddress": "no-port",
		"queueName":     "q",
	})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestParseMetadata_NonNumericPort(t *testing.T) {
	_, err := ParseScalerMetadata(map[string]string{
		"kubemqAddress": "localhost:abc",
		"queueName":     "q",
	})
	require.Error(t, err)
	assert.Contains(t, status.Convert(err).Message(), "port")
}

func TestParseMetadata_ZeroPort(t *testing.T) {
	_, err := ParseScalerMetadata(map[string]string{
		"kubemqAddress": "localhost:0",
		"queueName":     "q",
	})
	require.Error(t, err)
}

func TestParseMetadata_PortAbove65535(t *testing.T) {
	_, err := ParseScalerMetadata(map[string]string{
		"kubemqAddress": "localhost:99999",
		"queueName":     "q",
	})
	require.Error(t, err)
	assert.Contains(t, status.Convert(err).Message(), "65535")
}

func TestParseMetadata_CertFileAllowed(t *testing.T) {
	for _, path := range []string{"/certs/ca.pem", "/etc/ssl/ca.pem", "/etc/pki/tls/ca.pem"} {
		meta := map[string]string{
			"kubemqAddress": "localhost:50000",
			"queueName":     "q",
			"certFile":      path,
		}
		result, err := ParseScalerMetadata(meta)
		require.NoError(t, err, "path %s should be allowed", path)
		assert.Equal(t, path, result.CertFile)
	}
}

func TestParseMetadata_CertFileTraversal(t *testing.T) {
	_, err := ParseScalerMetadata(map[string]string{
		"kubemqAddress": "localhost:50000",
		"queueName":     "q",
		"certFile":      "/certs/../etc/passwd",
	})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestParseMetadata_CertFileRelativeTraversal(t *testing.T) {
	_, err := ParseScalerMetadata(map[string]string{
		"kubemqAddress": "localhost:50000",
		"queueName":     "q",
		"certFile":      "../../etc/passwd",
	})
	require.Error(t, err)
	assert.Contains(t, status.Convert(err).Message(), "traversal")
}

func TestParseMetadata_CertFileDisallowedPath(t *testing.T) {
	_, err := ParseScalerMetadata(map[string]string{
		"kubemqAddress": "localhost:50000",
		"queueName":     "q",
		"certFile":      "/tmp/ca.pem",
	})
	require.Error(t, err)
	assert.Contains(t, status.Convert(err).Message(), "must be under")
}

func TestParseMetadata_AddressTooLong(t *testing.T) {
	long := make([]byte, 300)
	for i := range long {
		long[i] = 'a'
	}
	_, err := ParseScalerMetadata(map[string]string{
		"kubemqAddress": string(long),
		"queueName":     "q",
	})
	require.Error(t, err)
	assert.Contains(t, status.Convert(err).Message(), "maximum length")
}

func TestParseMetadata_QueueNameTooLong(t *testing.T) {
	long := make([]byte, 300)
	for i := range long {
		long[i] = 'q'
	}
	_, err := ParseScalerMetadata(map[string]string{
		"kubemqAddress": "localhost:50000",
		"queueName":     string(long),
	})
	require.Error(t, err)
}

func TestParseMetadata_AuthTokenTooLong(t *testing.T) {
	long := make([]byte, 5000)
	for i := range long {
		long[i] = 't'
	}
	_, err := ParseScalerMetadata(map[string]string{
		"kubemqAddress": "localhost:50000",
		"queueName":     "q",
		"authToken":     string(long),
	})
	require.Error(t, err)
}

func TestParseMetadata_WithAuthAndTLS(t *testing.T) {
	meta := map[string]string{
		"kubemqAddress":        "host:50000",
		"queueName":            "q",
		"authToken":            "my-token",
		"tls":                  "true",
		"certFile":             "/certs/ca.pem",
		"serverOverrideDomain": "kubemq.local",
	}
	result, err := ParseScalerMetadata(meta)
	require.NoError(t, err)
	assert.Equal(t, "my-token", result.AuthToken)
	assert.True(t, result.TLS)
	assert.Equal(t, "/certs/ca.pem", result.CertFile)
	assert.Equal(t, "kubemq.local", result.ServerOverrideDomain)
}

func TestPoolKey_DifferentAuth(t *testing.T) {
	m1 := &ScalerMetadata{KubeMQAddress: "host:50000", AuthToken: "token1"}
	m2 := &ScalerMetadata{KubeMQAddress: "host:50000", AuthToken: "token2"}
	assert.NotEqual(t, m1.PoolKey(), m2.PoolKey())
}

func TestPoolKey_DifferentTLS(t *testing.T) {
	m1 := &ScalerMetadata{KubeMQAddress: "host:50000", TLS: false}
	m2 := &ScalerMetadata{KubeMQAddress: "host:50000", TLS: true}
	assert.NotEqual(t, m1.PoolKey(), m2.PoolKey())
}

func TestPoolKey_SameConfig(t *testing.T) {
	m1 := &ScalerMetadata{KubeMQAddress: "host:50000", AuthToken: "tok", TLS: true, CertFile: "/certs/ca.pem"}
	m2 := &ScalerMetadata{KubeMQAddress: "host:50000", AuthToken: "tok", TLS: true, CertFile: "/certs/ca.pem"}
	assert.Equal(t, m1.PoolKey(), m2.PoolKey())
}

func TestLoadConfig_Defaults(t *testing.T) {
	cfg, err := LoadConfig()
	require.NoError(t, err)
	assert.Equal(t, 9090, cfg.GRPCPort)
	assert.Equal(t, "info", cfg.LogLevel)
}

func TestLoadConfig_Custom(t *testing.T) {
	t.Setenv("GRPC_PORT", "8080")
	t.Setenv("LOG_LEVEL", "debug")
	cfg, err := LoadConfig()
	require.NoError(t, err)
	assert.Equal(t, 8080, cfg.GRPCPort)
	assert.Equal(t, "debug", cfg.LogLevel)
}

func TestLoadConfig_InvalidPort(t *testing.T) {
	t.Setenv("GRPC_PORT", "invalid")
	_, err := LoadConfig()
	require.Error(t, err)
}

func TestLoadConfig_PortOutOfRange(t *testing.T) {
	t.Setenv("GRPC_PORT", "99999")
	_, err := LoadConfig()
	require.Error(t, err)
}

func TestLoadConfig_InvalidLogLevel(t *testing.T) {
	t.Setenv("GRPC_PORT", "")
	t.Setenv("LOG_LEVEL", "verbose")
	_, err := LoadConfig()
	require.Error(t, err)
}
