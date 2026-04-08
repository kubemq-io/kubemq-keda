package main

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	pb "github.com/kubemq-io/kubemq-keda/pkg/externalscaler"
	"github.com/kubemq-io/kubemq-keda/scaler"
)

// Version is injected at build time via -ldflags="-X main.Version=..."
var Version = "dev"

// The gRPC server runs plaintext by design (v1). It is intended to be
// deployed as a ClusterIP Service, accessible only within the cluster.
// For production, apply a NetworkPolicy restricting ingress to the KEDA
// operator namespace.
func main() {
	cfg, err := scaler.LoadConfig()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	logger := setupLogger(cfg.LogLevel)

	grpcServer := grpc.NewServer()
	pool := scaler.NewClientPool(logger)
	scalerService := scaler.NewExternalScaler(pool, logger)
	pb.RegisterExternalScalerServer(grpcServer, scalerService)

	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", cfg.GRPCPort))
	if err != nil {
		logger.Error("failed to listen", "port", cfg.GRPCPort, "error", err)
		os.Exit(1)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			logger.Error("gRPC server failed", "error", err)
			os.Exit(1)
		}
	}()

	logger.Info("kubemq-keda-scaler started", "version", Version, "port", cfg.GRPCPort)

	<-sigCh
	logger.Info("shutdown signal received")

	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	pool.Shutdown()

	shutdownTimer := time.NewTimer(5 * time.Second)
	defer shutdownTimer.Stop()

	done := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
		logger.Info("graceful shutdown complete")
	case <-shutdownTimer.C:
		logger.Warn("graceful shutdown timed out, forcing stop")
		grpcServer.Stop()
	}

	pool.CloseAll()
	logger.Info("shutdown complete")
}

func setupLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}
