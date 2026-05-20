package kafka

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	appmail "github.com/companyofcreators/mail-service/internal/application/mail"
	"github.com/segmentio/kafka-go"
)

// Consumer manages Kafka consumers for the mail service.
type Consumer struct {
	emailConsumer        *kafka.Reader
	verificationConsumer *kafka.Reader
	processCommand       *appmail.ProcessCommandUseCase
	logger               *slog.Logger
	wg                   sync.WaitGroup
	shutdown             chan struct{}
}

// NewConsumer creates a new Kafka Consumer for mail topics.
func NewConsumer(
	brokers []string,
	consumerGroup string,
	processCommand *appmail.ProcessCommandUseCase,
	logger *slog.Logger,
) *Consumer {
	emailReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		GroupID:     consumerGroup,
		Topic:       "notification.email",
		MinBytes:    10e3, // 10KB
		MaxBytes:    10e6, // 10MB
		MaxWait:     500 * time.Millisecond,
		StartOffset: kafka.LastOffset,
	})

	verificationReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		GroupID:     consumerGroup,
		Topic:       "user.verification.created",
		MinBytes:    10e3,
		MaxBytes:    10e6,
		MaxWait:     500 * time.Millisecond,
		StartOffset: kafka.LastOffset,
	})

	return &Consumer{
		emailConsumer:        emailReader,
		verificationConsumer: verificationReader,
		processCommand:       processCommand,
		logger:               logger,
		shutdown:             make(chan struct{}),
	}
}

// Start begins consuming messages from all configured Kafka topics.
func (c *Consumer) Start(ctx context.Context) error {
	c.logger.Info("starting kafka consumers",
		slog.String("group", c.emailConsumer.Config().GroupID),
	)

	// Consumer 1: notification.email
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.consumeEmailCommands(ctx)
	}()

	// Consumer 2: user.verification.created
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.consumeVerificationEvents(ctx)
	}()

	c.logger.Info("kafka consumers started")
	return nil
}

// Shutdown gracefully stops all consumers.
func (c *Consumer) Shutdown() {
	c.logger.Info("shutting down kafka consumers")
	close(c.shutdown)

	// Close readers first to unblock any pending FetchMessage calls
	if err := c.emailConsumer.Close(); err != nil {
		c.logger.Warn("error closing email consumer", slog.String("error", err.Error()))
	}
	if err := c.verificationConsumer.Close(); err != nil {
		c.logger.Warn("error closing verification consumer", slog.String("error", err.Error()))
	}

	// Wait for goroutines to finish
	c.wg.Wait()
	c.logger.Info("kafka consumers shut down")
}

// consumeEmailCommands reads from notification.email topic.
func (c *Consumer) consumeEmailCommands(ctx context.Context) {
	c.logger.Info("consuming from notification.email topic")

	for {
		select {
		case <-c.shutdown:
			c.logger.Info("email command consumer stopped")
			return
		default:
		}

		// Use a per-message context with timeout to avoid blocking shutdown
		msgCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		msg, err := c.emailConsumer.FetchMessage(msgCtx)
		cancel()

		if err != nil {
			if isConnClosed(err) {
				c.logger.Info("email consumer connection closed")
				return
			}
			c.logger.Warn("failed to fetch email message",
				slog.String("error", err.Error()),
			)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		c.logger.Info("received email command message",
			slog.Int64("offset", msg.Offset),
			slog.Int("partition", msg.Partition),
		)

		processCtx, processCancel := context.WithTimeout(ctx, 60*time.Second)
		err = c.processCommand.HandleEmailCommand(processCtx, msg.Value)
		processCancel()

		if err != nil {
			c.logger.Error("failed to process email command",
				slog.Int64("offset", msg.Offset),
				slog.Int("partition", msg.Partition),
				slog.String("error", err.Error()),
			)
			// Even on failure, commit the offset to avoid reprocessing loops
			// Failed messages are logged for manual inspection
		}

		// Commit offset
		commitCtx, commitCancel := context.WithTimeout(ctx, 10*time.Second)
		err = c.emailConsumer.CommitMessages(commitCtx, msg)
		commitCancel()

		if err != nil {
			if isConnClosed(err) {
				c.logger.Info("email consumer connection closed during commit")
				return
			}
			c.logger.Warn("failed to commit email message offset",
				slog.Int64("offset", msg.Offset),
				slog.String("error", err.Error()),
			)
		}
	}
}

// consumeVerificationEvents reads from user.verification.created topic.
func (c *Consumer) consumeVerificationEvents(ctx context.Context) {
	c.logger.Info("consuming from user.verification.created topic")

	for {
		select {
		case <-c.shutdown:
			c.logger.Info("verification event consumer stopped")
			return
		default:
		}

		msgCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		msg, err := c.verificationConsumer.FetchMessage(msgCtx)
		cancel()

		if err != nil {
			if isConnClosed(err) {
				c.logger.Info("verification consumer connection closed")
				return
			}
			c.logger.Warn("failed to fetch verification event",
				slog.String("error", err.Error()),
			)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		c.logger.Info("received verification event",
			slog.Int64("offset", msg.Offset),
			slog.Int("partition", msg.Partition),
		)

		processCtx, processCancel := context.WithTimeout(ctx, 60*time.Second)
		err = c.processCommand.HandleVerificationEvent(processCtx, msg.Value)
		processCancel()

		if err != nil {
			c.logger.Error("failed to process verification event",
				slog.Int64("offset", msg.Offset),
				slog.Int("partition", msg.Partition),
				slog.String("error", err.Error()),
			)
		}

		commitCtx, commitCancel := context.WithTimeout(ctx, 10*time.Second)
		err = c.verificationConsumer.CommitMessages(commitCtx, msg)
		commitCancel()

		if err != nil {
			if isConnClosed(err) {
				c.logger.Info("verification consumer connection closed during commit")
				return
			}
			c.logger.Warn("failed to commit verification event offset",
				slog.Int64("offset", msg.Offset),
				slog.String("error", err.Error()),
			)
		}
	}
}

// isConnClosed checks if the error indicates a closed connection.
func isConnClosed(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	patterns := []string{
		"closed pipe",
		"use of closed network connection",
		"kafka.Reader is closed",
		"context canceled",
		"EOF",
	}
	for _, p := range patterns {
		if strings.Contains(s, p) {
			return true
		}
	}
	return false
}
