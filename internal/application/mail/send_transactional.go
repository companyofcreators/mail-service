package mail

import (
	"context"
	"log/slog"

	domain "github.com/companyofcreators/mail-service/internal/domain/mail"
)

// SendTransactionalUseCase handles sending generic transactional/notification emails.
type SendTransactionalUseCase struct {
	mailer domain.Mailer
	from   string
	name   string
	logger *slog.Logger
}

// NewSendTransactionalUseCase creates a new SendTransactionalUseCase.
func NewSendTransactionalUseCase(mailer domain.Mailer, from, fromName string, logger *slog.Logger) *SendTransactionalUseCase {
	return &SendTransactionalUseCase{
		mailer: mailer,
		from:   from,
		name:   fromName,
		logger: logger,
	}
}

// Execute sends a transactional email using the specified template.
func (uc *SendTransactionalUseCase) Execute(ctx context.Context, cmd domain.EmailCommand) error {
	template := domain.EmailTemplate(cmd.Template)
	if err := template.Validate(); err != nil {
		uc.logger.Error("invalid template type",
			slog.String("template", cmd.Template),
			slog.String("error", err.Error()),
		)
		return err
	}

	recipients := cmd.ToEmailList
	if len(recipients) == 0 && cmd.ToEmail != "" {
		recipients = []string{cmd.ToEmail}
	}

	if len(recipients) == 0 {
		uc.logger.Error("no recipients specified for transactional email",
			slog.String("template", cmd.Template),
		)
		return nil
	}

	email := &domain.Email{
		To:       recipients,
		From:     uc.from,
		FromName: uc.name,
		Subject:  cmd.Subject,
		Template: template,
		Data:     cmd.Data,
	}

	uc.logger.Info("sending transactional email",
		slog.String("template", string(template)),
		slog.String("subject", cmd.Subject),
		slog.Any("recipients", recipients),
	)

	if err := uc.mailer.Send(ctx, email); err != nil {
		uc.logger.Error("failed to send transactional email",
			slog.String("template", string(template)),
			slog.String("error", err.Error()),
		)
		return err
	}

	uc.logger.Info("transactional email sent successfully",
		slog.String("template", string(template)),
		slog.Any("recipients", recipients),
	)

	return nil
}
