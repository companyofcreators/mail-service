package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/companyofcreators/mail-service/internal/app"
	"github.com/companyofcreators/mail-service/internal/config"
	"github.com/companyofcreators/mail-service/internal/pkg"
)

func main() {
	// Load configuration from environment and .env file
	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", slog.String("error", err.Error()))
		os.Exit(1)
	}

	// Initialize structured logger
	logger := pkg.NewLogger(cfg.LogLevel)
	logger.Info("starting mail-service",
		slog.String("smtp_host", cfg.SMTPHost),
		slog.Int("smtp_port", cfg.SMTPPort),
		slog.Bool("smtp_tls", cfg.SMTPUseTLS),
		slog.String("from_address", cfg.FromAddress),
		slog.Int("max_retries", cfg.MaxRetries),
		slog.String("log_level", cfg.LogLevel),
	)

	// Build dependency container (validates templates on startup)
	container, err := app.NewContainer(cfg, logger)
	if err != nil {
		logger.Error("failed to build container", slog.String("error", err.Error()))
		os.Exit(1)
	}

	// Start Kafka consumers in goroutines
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := container.KafkaConsumer.Start(ctx); err != nil {
		logger.Error("failed to start kafka consumers", slog.String("error", err.Error()))
		os.Exit(1)
	}

	logger.Info("mail-service is running, waiting for email delivery commands")

	// Wait for shutdown signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit

	logger.Info("received shutdown signal", slog.String("signal", sig.String()))

	// Graceful shutdown
	logger.Info("shutting down kafka consumers...")
	container.KafkaConsumer.Shutdown()

	// Cancel context to stop any in-flight operations
	cancel()

	logger.Info("mail-service stopped gracefully")
}
