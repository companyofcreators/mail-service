package mail

import (
	"context"
	"log/slog"

	domain "github.com/companyofcreators/mail-service/internal/domain/mail"
)

// SendVerificationUseCase handles sending verification emails to new users.
type SendVerificationUseCase struct {
	mailer domain.Mailer
	from   string
	name   string
	logger *slog.Logger
}

// NewSendVerificationUseCase creates a new SendVerificationUseCase.
func NewSendVerificationUseCase(mailer domain.Mailer, from, fromName string, logger *slog.Logger) *SendVerificationUseCase {
	return &SendVerificationUseCase{
		mailer: mailer,
		from:   from,
		name:   fromName,
		logger: logger,
	}
}

// Execute sends a verification email to the specified recipient.
func (uc *SendVerificationUseCase) Execute(ctx context.Context, toEmail, toName, verificationURL, expiryTime string) error {
	email := &domain.Email{
		To:       []string{toEmail},
		From:     uc.from,
		FromName: uc.name,
		Subject:  "Verify your email address",
		Template: domain.TemplateVerification,
		Data: map[string]interface{}{
			"Name":            toName,
			"VerificationURL": verificationURL,
			"ExpiryTime":      expiryTime,
		},
	}

	uc.logger.Info("sending verification email",
		slog.String("to", toEmail),
		slog.String("name", toName),
	)

	if err := uc.mailer.Send(ctx, email); err != nil {
		uc.logger.Error("failed to send verification email",
			slog.String("to", toEmail),
			slog.String("error", err.Error()),
		)
		return err
	}

	uc.logger.Info("verification email sent successfully",
		slog.String("to", toEmail),
	)

	return nil
}
