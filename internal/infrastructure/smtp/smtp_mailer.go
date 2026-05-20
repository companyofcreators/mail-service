package smtp

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"html/template"
	"log/slog"
	"net"
	"net/smtp"
	"strings"
	"sync"
	"time"

	domain "github.com/companyofcreators/mail-service/internal/domain/mail"
	"github.com/companyofcreators/mail-service/internal/infrastructure/templates"
)

// SMTPMailer implements the domain.Mailer interface using net/smtp.
type SMTPMailer struct {
	host        string
	port        int
	username    string
	password    string
	from        string
	fromName    string
	useTLS      bool
	templates   *template.Template
	mu          sync.RWMutex
	logger      *slog.Logger
	dialTimeout time.Duration
}

// NewSMTPMailer creates a new SMTPMailer, loading and validating all templates at startup.
func NewSMTPMailer(
	host string,
	port int,
	username string,
	password string,
	from string,
	fromName string,
	useTLS bool,
	logger *slog.Logger,
) (*SMTPMailer, error) {
	// Load and parse all templates at startup
	tmpl, err := templates.Load()
	if err != nil {
		return nil, fmt.Errorf("load templates: %w", err)
	}

	logger.Info("smtp mailer initialized",
		slog.String("host", host),
		slog.Int("port", port),
		slog.Bool("tls", useTLS),
		slog.String("from", from),
	)

	return &SMTPMailer{
		host:        host,
		port:        port,
		username:    username,
		password:    password,
		from:        from,
		fromName:    fromName,
		useTLS:      useTLS,
		templates:   tmpl,
		logger:      logger,
		dialTimeout: 10 * time.Second,
	}, nil
}

// Send renders the template and sends the email via SMTP.
func (m *SMTPMailer) Send(ctx context.Context, email *domain.Email) error {
	if err := m.validateEmail(email); err != nil {
		return fmt.Errorf("validate email: %w", err)
	}

	// Render HTML from template
	html, err := m.renderTemplate(email.Template, email.Data)
	if err != nil {
		return fmt.Errorf("render template %s: %w", email.Template, err)
	}
	email.HTML = html

	// Build MIME message
	msg, err := m.buildMIMEMessage(email)
	if err != nil {
		return fmt.Errorf("build MIME message: %w", err)
	}

	// Establish SMTP connection and send
	if err := m.deliver(ctx, email, msg); err != nil {
		return fmt.Errorf("deliver email: %w", err)
	}

	m.logger.Info("email sent successfully",
		slog.String("to", strings.Join(email.To, ",")),
		slog.String("subject", email.Subject),
		slog.String("template", string(email.Template)),
	)

	return nil
}

// validateEmail performs basic validation on the email fields.
func (m *SMTPMailer) validateEmail(email *domain.Email) error {
	if len(email.To) == 0 {
		return fmt.Errorf("no recipients specified")
	}

	for i, addr := range email.To {
		if addr == "" {
			return fmt.Errorf("recipient at index %d is empty", i)
		}
	}

	if email.Subject == "" {
		return fmt.Errorf("email subject is empty")
	}

	if email.Template == "" {
		return fmt.Errorf("email template is not specified")
	}

	return nil
}

// renderTemplate executes the HTML template with the provided data.
func (m *SMTPMailer) renderTemplate(templateType domain.EmailTemplate, data map[string]interface{}) (string, error) {
	tmplFile := templateType.TemplateFile()

	m.mu.RLock()
	t := m.templates.Lookup(tmplFile)
	m.mu.RUnlock()

	if t == nil {
		return "", fmt.Errorf("template %s not found", tmplFile)
	}

	var buf bytes.Buffer
	if err := t.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("execute template %s: %w", tmplFile, err)
	}

	result := buf.String()
	if result == "" {
		return "", fmt.Errorf("rendered template %s produced empty output", tmplFile)
	}

	return result, nil
}

// buildMIMEMessage constructs a multipart/alternative MIME message with plain text and HTML.
func (m *SMTPMailer) buildMIMEMessage(email *domain.Email) ([]byte, error) {
	var buf bytes.Buffer

	// Boundary for multipart message
	boundary := fmt.Sprintf("boundary-%d", time.Now().UnixNano())

	// Headers
	buf.WriteString(fmt.Sprintf("From: %s <%s>\r\n", m.fromName, m.from))
	buf.WriteString(fmt.Sprintf("To: %s\r\n", strings.Join(email.To, ", ")))

	subject := email.Subject
	// Encode subject line to handle non-ASCII characters
	buf.WriteString(fmt.Sprintf("Subject: %s\r\n", mimeEncodeHeader(subject)))

	buf.WriteString("MIME-Version: 1.0\r\n")
	buf.WriteString(fmt.Sprintf("Content-Type: multipart/alternative; boundary=\"%s\"\r\n", boundary))
	buf.WriteString("Date: " + time.Now().Format(time.RFC1123Z) + "\r\n")
	buf.WriteString("\r\n")

	// Plain text part
	buf.WriteString(fmt.Sprintf("--%s\r\n", boundary))
	buf.WriteString("Content-Type: text/plain; charset=\"UTF-8\"\r\n")
	buf.WriteString("Content-Transfer-Encoding: 7bit\r\n")
	buf.WriteString("\r\n")
	buf.WriteString(stripHTML(email.HTML))
	buf.WriteString("\r\n")

	// HTML part
	buf.WriteString(fmt.Sprintf("--%s\r\n", boundary))
	buf.WriteString("Content-Type: text/html; charset=\"UTF-8\"\r\n")
	buf.WriteString("Content-Transfer-Encoding: 7bit\r\n")
	buf.WriteString("\r\n")
	buf.WriteString(email.HTML)
	buf.WriteString("\r\n")

	// Closing boundary
	buf.WriteString(fmt.Sprintf("--%s--\r\n", boundary))

	return buf.Bytes(), nil
}

// deliver sends the email via SMTP.
func (m *SMTPMailer) deliver(ctx context.Context, email *domain.Email, msg []byte) error {
	addr := fmt.Sprintf("%s:%d", m.host, m.port)

	var client *smtp.Client
	var conn net.Conn
	var err error

	dialer := &net.Dialer{
		Timeout: m.dialTimeout,
	}

	if m.useTLS {
		tlsConfig := &tls.Config{
			ServerName: m.host,
			MinVersion: tls.VersionTLS12,
		}
		conn, err = tls.DialWithDialer(dialer, "tcp", addr, tlsConfig)
	} else {
		conn, err = dialer.DialContext(ctx, "tcp", addr)
	}

	if err != nil {
		return fmt.Errorf("connect to SMTP server %s: %w", addr, err)
	}
	defer conn.Close()

	client, err = smtp.NewClient(conn, m.host)
	if err != nil {
		return fmt.Errorf("create SMTP client: %w", err)
	}
	defer func() {
		if err := client.Quit(); err != nil {
			m.logger.Warn("SMTP quit failed", slog.String("error", err.Error()))
		}
	}()

	// Authenticate if credentials are provided
	if m.username != "" && m.password != "" {
		auth := smtp.PlainAuth("", m.username, m.password, m.host)
		if err := client.Auth(auth); err != nil {
			return fmt.Errorf("SMTP auth: %w", err)
		}
	}

	// Set sender
	if err := client.Mail(m.from); err != nil {
		return fmt.Errorf("SMTP MAIL FROM: %w", err)
	}

	// Set recipients
	for _, to := range email.To {
		if err := client.Rcpt(to); err != nil {
			return fmt.Errorf("SMTP RCPT TO %s: %w", to, err)
		}
	}

	// Write message body
	w, err := client.Data()
	if err != nil {
		return fmt.Errorf("SMTP DATA: %w", err)
	}

	_, err = w.Write(msg)
	if err != nil {
		_ = w.Close()
		return fmt.Errorf("write message: %w", err)
	}

	if err := w.Close(); err != nil {
		return fmt.Errorf("close message writer: %w", err)
	}

	return nil
}

// mimeEncodeHeader encodes a header value using RFC 2047 MIME encoding.
func mimeEncodeHeader(s string) string {
	// For ASCII-only headers, return as-is
	if isASCII(s) {
		return s
	}
	// For non-ASCII, use UTF-8 B encoding
	return fmt.Sprintf("=?UTF-8?B?%s?=", base64Encode(s))
}

// isASCII checks if a string contains only ASCII printable characters.
func isASCII(s string) bool {
	for _, r := range s {
		if r > 127 {
			return false
		}
	}
	return true
}

// base64Encode encodes a string to base64 (RFC 2047 B encoding).
func base64Encode(s string) string {
	table := "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	var result strings.Builder
	b := []byte(s)
	for i := 0; i < len(b); i += 3 {
		chunk := make([]byte, 0, 3)
		chunk = append(chunk, b[i])
		if i+1 < len(b) {
			chunk = append(chunk, b[i+1])
		} else {
			chunk = append(chunk, 0)
		}
		if i+2 < len(b) {
			chunk = append(chunk, b[i+2])
		} else {
			chunk = append(chunk, 0)
		}

		result.WriteByte(table[chunk[0]>>2])
		result.WriteByte(table[((chunk[0]&0x03)<<4)|(chunk[1]>>4)])
		if i+1 < len(b) {
			result.WriteByte(table[((chunk[1]&0x0f)<<2)|(chunk[2]>>6)])
		} else {
			result.WriteByte('=')
		}
		if i+2 < len(b) {
			result.WriteByte(table[chunk[2]&0x3f])
		} else {
			result.WriteByte('=')
		}
	}
	return result.String()
}

// stripHTML removes HTML tags to produce a plain text version.
func stripHTML(html string) string {
	var buf strings.Builder
	inTag := false

	for _, r := range html {
		switch {
		case r == '<':
			inTag = true
		case r == '>':
			inTag = false
			buf.WriteByte(' ')
		case !inTag:
			buf.WriteRune(r)
		}
	}

	// Clean up multiple spaces and trim
	result := buf.String()
	// Collapse multiple spaces
	for strings.Contains(result, "  ") {
		result = strings.ReplaceAll(result, "  ", " ")
	}
	result = strings.TrimSpace(result)

	return result
}
