package mail

import (
	"fmt"
	"time"
)

// EmailTemplate represents the type of email template to use.
type EmailTemplate string

const (
	TemplateVerification  EmailTemplate = "verification"
	TemplateWelcome       EmailTemplate = "welcome"
	TemplatePasswordReset EmailTemplate = "password_reset"
	TemplateNotification  EmailTemplate = "notification"
)

// Validate checks that the template type is a known value.
func (t EmailTemplate) Validate() error {
	switch t {
	case TemplateVerification, TemplateWelcome, TemplatePasswordReset, TemplateNotification:
		return nil
	default:
		return fmt.Errorf("unknown email template: %s", t)
	}
}

// TemplateFile returns the HTML template filename for this template type.
func (t EmailTemplate) TemplateFile() string {
	return fmt.Sprintf("%s.html", string(t))
}

// Email represents an email message to be sent.
type Email struct {
	To          []string
	From        string
	FromName    string
	Subject     string
	Template    EmailTemplate
	Data        map[string]interface{}
	HTML        string
	Attachments []Attachment
}

// Attachment represents an email attachment.
type Attachment struct {
	Filename    string
	Content     []byte
	ContentType string
}

// EmailCommand is the Kafka message format received from notification-service.
type EmailCommand struct {
	ToEmail      string                 `json:"to_email"`
	ToEmailList  []string               `json:"to_emails,omitempty"`
	Template     string                 `json:"template_type"`
	Subject      string                 `json:"subject"`
	Data         map[string]interface{} `json:"data"`
	Priority     string                 `json:"priority"`
}

// VerifiedUserEvent is the Kafka message format from user.verification.created topic.
type VerifiedUserEvent struct {
	UserID          string    `json:"user_id"`
	Email           string    `json:"email"`
	Name            string    `json:"name"`
	Role            string    `json:"role"`
	VerificationURL string    `json:"verification_url"`
	CreatedAt       time.Time `json:"created_at"`
}

// DeliveryResult holds the result of an email delivery attempt.
type DeliveryResult struct {
	Email     string        `json:"email"`
	Subject   string        `json:"subject"`
	Success   bool          `json:"success"`
	Error     string        `json:"error,omitempty"`
	Attempts  int           `json:"attempts"`
	Duration  time.Duration `json:"duration_ms"`
	Timestamp time.Time     `json:"timestamp"`
}
