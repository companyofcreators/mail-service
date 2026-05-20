# Mail Service

Standalone Kafka consumer responsible for email delivery via SMTP.
Does NOT expose HTTP endpoints. Listens to Kafka topics and sends emails.

## Architecture

- **Consumers**: `notification.email` and `user.verification.created` Kafka topics
- **Delivery**: SMTP via `net/smtp` standard library
- **Retry**: Exponential backoff (1s, 2s, 4s, 8s) with configurable max retries
- **Templates**: Go `html/template` with `embed` for HTML email templates
- **Dev SMTP**: [Mailpit](https://mailpit.axllent.org/) on `localhost:1025`

## Configuration

Copy `.env.example` to `.env` and adjust values:

| Variable               | Default                    | Description                       |
|------------------------|----------------------------|-----------------------------------|
| KAFKA_BROKERS          | localhost:9092             | Comma-separated Kafka broker list |
| KAFKA_CONSUMER_GROUP   | mail-service               | Consumer group ID                 |
| SMTP_HOST              | localhost                  | SMTP server host                  |
| SMTP_PORT              | 1025                       | SMTP server port                  |
| SMTP_USERNAME          |                            | SMTP auth username (optional)     |
| SMTP_PASSWORD          |                            | SMTP auth password (optional)     |
| SMTP_USE_TLS           | false                      | Use TLS for SMTP connection       |
| FROM_ADDRESS           | noreply@diploma.local      | Sender email address              |
| FROM_NAME              | Diploma Marketplace        | Sender display name               |
| MAX_RETRIES            | 3                          | Max delivery retry attempts       |
| LOG_LEVEL              | info                       | Logging level (debug/info/warn/error) |

## Kafka Topics

### Consumer
- `notification.email` - Email delivery commands from notification-service
- `user.verification.created` - Verification email events

### Message Format

**notification.email**:
```json
{
  "to_email": "user@example.com",
  "to_emails": ["user1@example.com", "user2@example.com"],
  "template_type": "verification|welcome|notification|password_reset",
  "subject": "Email Subject",
  "data": {
    "Name": "John",
    "VerificationURL": "https://..."
  },
  "priority": "normal|high"
}
```

**user.verification.created**:
```json
{
  "user_id": "uuid",
  "email": "user@example.com",
  "name": "John Doe",
  "role": "student",
  "verification_url": "https://api.example.com/verify?token=...",
  "created_at": "2025-01-01T00:00:00Z"
}
```

## Running

```bash
# With Docker Compose (starts all infrastructure)
docker compose up -d

# Run locally
cd mail-service
cp .env.example .env
go run ./cmd/main.go
```

## Email Templates

Templates are embedded at compile time using Go's `embed` package:

- `verification.html` - Email verification with branded CTA button
- `welcome.html` - Welcome message for new verified users
- `password_reset.html` - Password reset with security notice
- `notification.html` - Generic notification with optional action button

## Development

For local development, use Mailpit:
- SMTP: `localhost:1025`
- Web UI: `http://localhost:8025`

All emails are captured by Mailpit and can be viewed in the web interface.
