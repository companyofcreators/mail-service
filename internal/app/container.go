package app

import (
	"log/slog"

	appmail "github.com/companyofcreators/mail-service/internal/application/mail"
	"github.com/companyofcreators/mail-service/internal/config"
	domain "github.com/companyofcreators/mail-service/internal/domain/mail"
	kafkaconsumer "github.com/companyofcreators/mail-service/internal/infrastructure/kafka"
	"github.com/companyofcreators/mail-service/internal/infrastructure/smtp"
)

// Container holds all wired dependencies for the mail service.
type Container struct {
	Config   *config.Config
	Logger   *slog.Logger
	Mailer   domain.Mailer

	// Use cases
	SendVerificationUseCase  *appmail.SendVerificationUseCase
	SendTransactionalUseCase *appmail.SendTransactionalUseCase
	ProcessCommandUseCase    *appmail.ProcessCommandUseCase

	// Infrastructure
	KafkaConsumer *kafkaconsumer.Consumer
}

// NewContainer creates and wires all dependencies for the mail service.
func NewContainer(cfg *config.Config, logger *slog.Logger) (*Container, error) {
	c := &Container{
		Config: cfg,
		Logger: logger,
	}

	// Create SMTP mailer (validates templates at startup)
	mailer, err := smtp.NewSMTPMailer(
		cfg.SMTPHost,
		cfg.SMTPPort,
		cfg.SMTPUsername,
		cfg.SMTPPassword,
		cfg.FromAddress,
		cfg.FromName,
		cfg.SMTPUseTLS,
		logger,
	)
	if err != nil {
		return nil, err
	}
	c.Mailer = mailer

	// Use cases
	c.SendVerificationUseCase = appmail.NewSendVerificationUseCase(
		mailer,
		cfg.FromAddress,
		cfg.FromName,
		logger,
	)

	c.SendTransactionalUseCase = appmail.NewSendTransactionalUseCase(
		mailer,
		cfg.FromAddress,
		cfg.FromName,
		logger,
	)

	c.ProcessCommandUseCase = appmail.NewProcessCommandUseCase(
		c.SendVerificationUseCase,
		c.SendTransactionalUseCase,
		mailer,
		cfg.MaxRetries,
		logger,
	)

	// Kafka consumer
	c.KafkaConsumer = kafkaconsumer.NewConsumer(
		cfg.KafkaBrokersList(),
		cfg.ConsumerGroup,
		c.ProcessCommandUseCase,
		logger,
	)

	return c, nil
}
