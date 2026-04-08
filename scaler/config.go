package scaler

import (
	"crypto/sha256"
	"fmt"
	"math"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	maxAddressLen   = 253
	maxQueueNameLen = 256
	maxAuthTokenLen = 4096
	maxCertFileLen  = 512
)

type Config struct {
	GRPCPort int
	LogLevel string
}

func LoadConfig() (*Config, error) {
	cfg := &Config{
		GRPCPort: 9090,
		LogLevel: "info",
	}
	if v := os.Getenv("GRPC_PORT"); v != "" {
		port, err := strconv.Atoi(v)
		if err != nil || port <= 0 || port > 65535 {
			return nil, fmt.Errorf("invalid GRPC_PORT %q: must be 1-65535", v)
		}
		cfg.GRPCPort = port
	}
	if v := os.Getenv("LOG_LEVEL"); v != "" {
		v = strings.ToLower(v)
		switch v {
		case "debug", "info", "warn", "error":
			cfg.LogLevel = v
		default:
			return nil, fmt.Errorf("invalid LOG_LEVEL %q: must be debug, info, warn, or error", v)
		}
	}
	return cfg, nil
}

type ScalerMetadata struct {
	KubeMQAddress           string
	KubeMQHost              string
	KubeMQPort              int
	QueueName               string
	TargetWaiting           float64
	ActivationTargetWaiting float64
	AuthToken               string
	TLS                     bool
	CertFile                string
	ServerOverrideDomain    string
}

func ParseScalerMetadata(metadata map[string]string) (*ScalerMetadata, error) {
	kubemqAddress := metadata["kubemqAddress"]
	if kubemqAddress == "" {
		return nil, status.Error(codes.InvalidArgument, "kubemqAddress is required")
	}
	if len(kubemqAddress) > maxAddressLen {
		return nil, status.Error(codes.InvalidArgument, "kubemqAddress exceeds maximum length")
	}

	host, portStr, err := net.SplitHostPort(kubemqAddress)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid kubemqAddress: %v", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port <= 0 || port > 65535 {
		return nil, status.Error(codes.InvalidArgument, "invalid port in kubemqAddress: must be 1-65535")
	}

	queueName := metadata["queueName"]
	if queueName == "" {
		return nil, status.Error(codes.InvalidArgument, "queueName is required")
	}
	if len(queueName) > maxQueueNameLen {
		return nil, status.Error(codes.InvalidArgument, "queueName exceeds maximum length")
	}

	targetWaiting := 10.0
	if v := metadata["targetWaiting"]; v != "" {
		parsed, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid targetWaiting: must be a number")
		}
		if parsed <= 0 || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
			return nil, status.Error(codes.InvalidArgument, "targetWaiting must be a finite positive number")
		}
		targetWaiting = parsed
	}

	activationTargetWaiting := 0.0
	if v := metadata["activationTargetWaiting"]; v != "" {
		parsed, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid activationTargetWaiting: must be a number")
		}
		if parsed < 0 || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
			return nil, status.Error(codes.InvalidArgument, "activationTargetWaiting must be a finite non-negative number")
		}
		activationTargetWaiting = parsed
	}

	authToken := metadata["authToken"]
	if len(authToken) > maxAuthTokenLen {
		return nil, status.Error(codes.InvalidArgument, "authToken exceeds maximum length")
	}

	certFile := metadata["certFile"]
	if certFile != "" {
		if len(certFile) > maxCertFileLen {
			return nil, status.Error(codes.InvalidArgument, "certFile exceeds maximum length")
		}
		if err := validateCertPath(certFile); err != nil {
			return nil, err
		}
	}

	return &ScalerMetadata{
		KubeMQAddress:           kubemqAddress,
		KubeMQHost:              host,
		KubeMQPort:              port,
		QueueName:               queueName,
		TargetWaiting:           targetWaiting,
		ActivationTargetWaiting: activationTargetWaiting,
		AuthToken:               authToken,
		TLS:                     parseBool(metadata["tls"]),
		CertFile:                certFile,
		ServerOverrideDomain:    metadata["serverOverrideDomain"],
	}, nil
}

var allowedCertPrefixes = []string{"/certs/", "/etc/ssl/", "/etc/pki/"}

func validateCertPath(path string) error {
	cleaned := filepath.Clean(path)
	if strings.Contains(cleaned, "..") {
		return status.Error(codes.InvalidArgument, "certFile must not contain path traversal")
	}
	for _, prefix := range allowedCertPrefixes {
		if strings.HasPrefix(cleaned, prefix) {
			return nil
		}
	}
	return status.Errorf(codes.InvalidArgument, "certFile must be under one of: %v", allowedCertPrefixes)
}

func parseBool(s string) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "true", "1", "yes":
		return true
	default:
		return false
	}
}

// PoolKey returns a composite key for connection pooling that includes
// all transport- and auth-affecting fields. The auth token is hashed
// to avoid leaking credentials in logs or map keys.
func (m *ScalerMetadata) PoolKey() string {
	tokenHash := ""
	if m.AuthToken != "" {
		h := sha256.Sum256([]byte(m.AuthToken))
		tokenHash = fmt.Sprintf("%x", h[:8])
	}
	return fmt.Sprintf("%s|tls=%t|cert=%s|sni=%s|tok=%s",
		m.KubeMQAddress, m.TLS, m.CertFile, m.ServerOverrideDomain, tokenHash)
}
