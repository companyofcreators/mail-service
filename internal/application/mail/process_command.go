package mail

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	domain "github.com/companyofcreators/mail-service/internal/domain/mail"
)

// ProcessCommandUseCase routes incoming notification.email commands to the appropriate handler.
type ProcessCommandUseCase struct {
	sendVerification  *SendVerificationUseCase
	sendTransactional *SendTransactionalUseCase
	mailer            domain.Mailer
	maxRetries        int
	logger            *slog.Logger
}

// NewProcessCommandUseCase creates a new ProcessCommandUseCase.
func NewProcessCommandUseCase(
	sendVerification *SendVerificationUseCase,
	sendTransactional *SendTransactionalUseCase,
	mailer domain.Mailer,
	maxRetries int,
	logger *slog.Logger,
) *ProcessCommandUseCase {
	return &ProcessCommandUseCase{
		sendVerification:  sendVerification,
		sendTransactional: sendTransactional,
		mailer:            mailer,
		maxRetries:        maxRetries,
		logger:            logger,
	}
}

// HandleEmailCommand parses and routes a Kafka message from notification.email topic.
func (uc *ProcessCommandUseCase) HandleEmailCommand(ctx context.Context, message []byte) error {
	startTime := time.Now()

	var cmd domain.EmailCommand
	if err := json.Unmarshal(message, &cmd); err != nil {
		uc.logger.Error("failed to unmarshal email command",
			slog.String("error", err.Error()),
		)
		return fmt.Errorf("unmarshal email command: %w", err)
	}

	uc.logger.Info("processing email command",
		slog.String("template", cmd.Template),
		slog.String("to_email", cmd.ToEmail),
		slog.String("subject", cmd.Subject),
		slog.String("priority", cmd.Priority),
	)

	switch cmd.Template {
	case "verification":
		return uc.handleVerification(ctx, cmd, startTime)
	case "welcome", "notification", "password_reset":
		return uc.sendWithRetry(ctx, cmd, startTime)
	default:
		uc.logger.Warn("unknown template type, treating as notification",
			slog.String("template", cmd.Template),
		)
		return uc.sendWithRetry(ctx, cmd, startTime)
	}
}

// HandleVerificationEvent processes a verification event from user.verification.created topic.
func (uc *ProcessCommandUseCase) HandleVerificationEvent(ctx context.Context, message []byte) error {
	startTime := time.Now()

	var event domain.VerifiedUserEvent
	if err := json.Unmarshal(message, &event); err != nil {
		uc.logger.Error("failed to unmarshal verification event",
			slog.String("error", err.Error()),
		)
		return fmt.Errorf("unmarshal verification event: %w", err)
	}

	uc.logger.Info("processing verification event",
		slog.String("user_id", event.UserID),
		slog.String("email", event.Email),
	)

	expiryTime := "24 hours"
	if event.VerificationURL == "" {
		uc.logger.Warn("empty verification URL, skipping",
			slog.String("user_id", event.UserID),
		)
		return nil
	}

	// Use retry logic for verification emails as well
	return uc.sendVerificationWithRetry(ctx, event, expiryTime, startTime)
}

// handleVerification processes a verification email command.
func (uc *ProcessCommandUseCase) handleVerification(ctx context.Context, cmd domain.EmailCommand, startTime time.Time) error {
	name := "User"
	if n, ok := cmd.Data["Name"]; ok {
		if s, ok := n.(string); ok {
			name = s
		}
	}

	verificationURL := ""
	if u, ok := cmd.Data["VerificationURL"]; ok {
		if s, ok := u.(string); ok {
			verificationURL = s
		}
	}

	expiryTime := "24 hours"
	if e, ok := cmd.Data["ExpiryTime"]; ok {
		if s, ok := e.(string); ok {
			expiryTime = s
		}
	}

	if verificationURL == "" {
		uc.logger.Warn("empty verification URL in command, skipping",
			slog.String("to_email", cmd.ToEmail),
		)
		return nil
	}

	return uc.retrySendVerification(ctx, cmd.ToEmail, name, verificationURL, expiryTime, startTime)
}

// retrySendVerification sends a verification email with retry logic.
func (uc *ProcessCommandUseCase) retrySendVerification(ctx context.Context, toEmail, name, verificationURL, expiryTime string, startTime time.Time) error {
	var lastErr error
	backoff := 1 * time.Second

	for attempt := 0; attempt <= uc.maxRetries; attempt++ {
		if attempt > 0 {
			uc.logger.Warn("retrying verification email send",
				slog.Int("attempt", attempt),
				slog.String("to", toEmail),
			)
			select {
			case <-time.After(backoff):
				backoff *= 2
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		if err := uc.sendVerification.Execute(ctx, toEmail, name, verificationURL, expiryTime); err != nil {
			lastErr = err
			continue
		}

		uc.logger.Info("verification email delivered",
			slog.String("to", toEmail),
			slog.Int("attempts", attempt+1),
			slog.Duration("duration", time.Since(startTime)),
		)
		return nil
	}

	uc.logger.Error("verification email failed after max retries",
		slog.String("to", toEmail),
		slog.Int("max_retries", uc.maxRetries),
		slog.String("error", lastErr.Error()),
	)

	return fmt.Errorf("verification email failed after %d retries: %w", uc.maxRetries, lastErr)
}

// sendWithRetry sends a transactional email with retry logic.
func (uc *ProcessCommandUseCase) sendWithRetry(ctx context.Context, cmd domain.EmailCommand, startTime time.Time) error {
	var lastErr error
	backoff := 1 * time.Second

	for attempt := 0; attempt <= uc.maxRetries; attempt++ {
		if attempt > 0 {
			uc.logger.Warn("retrying transactional email send",
				slog.Int("attempt", attempt),
				slog.String("to", cmd.ToEmail),
				slog.String("template", cmd.Template),
			)
			select {
			case <-time.After(backoff):
				backoff *= 2
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		if err := uc.sendTransactional.Execute(ctx, cmd); err != nil {
			lastErr = err
			continue
		}

		uc.logger.Info("transactional email delivered",
			slog.String("to", cmd.ToEmail),
			slog.String("template", cmd.Template),
			slog.Int("attempts", attempt+1),
			slog.Duration("duration", time.Since(startTime)),
		)
		return nil
	}

	uc.logger.Error("transactional email failed after max retries",
		slog.String("to", cmd.ToEmail),
		slog.String("template", cmd.Template),
		slog.Int("max_retries", uc.maxRetries),
		slog.String("error", lastErr.Error()),
	)

	return fmt.Errorf("transactional email failed after %d retries: %w", uc.maxRetries, lastErr)
}

// sendVerificationWithRetry sends a verification event email with retry logic.
func (uc *ProcessCommandUseCase) sendVerificationWithRetry(ctx context.Context, event domain.VerifiedUserEvent, expiryTime string, startTime time.Time) error {
	var lastErr error
	backoff := 1 * time.Second

	for attempt := 0; attempt <= uc.maxRetries; attempt++ {
		if attempt > 0 {
			uc.logger.Warn("retrying verification event send",
				slog.Int("attempt", attempt),
				slog.String("to", event.Email),
			)
			select {
			case <-time.After(backoff):
				backoff *= 2
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		if err := uc.sendVerification.Execute(ctx, event.Email, event.Name, event.VerificationURL, expiryTime); err != nil {
			lastErr = err
			continue
		}

		uc.logger.Info("verification event delivered",
			slog.String("to", event.Email),
			slog.Int("attempts", attempt+1),
			slog.Duration("duration", time.Since(startTime)),
		)
		return nil
	}

	uc.logger.Error("verification event failed after max retries",
		slog.String("to", event.Email),
		slog.Int("max_retries", uc.maxRetries),
		slog.String("error", lastErr.Error()),
	)

	return fmt.Errorf("verification event failed after %d retries: %w", uc.maxRetries, lastErr)
}
