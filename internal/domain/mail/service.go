package mail

import "context"

// Mailer defines the interface for sending emails.
type Mailer interface {
	Send(ctx context.Context, email *Email) error
}
