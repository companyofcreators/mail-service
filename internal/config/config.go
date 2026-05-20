package config

import (
	"fmt"

	"github.com/ilyakaznacheev/cleanenv"
	"github.com/joho/godotenv"
)

// Config holds all configuration for the mail service.
type Config struct {
	KafkaBrokers  string `env:"KAFKA_BROKERS" env-default:"localhost:9092"`
	ConsumerGroup string `env:"KAFKA_CONSUMER_GROUP" env-default:"mail-service"`
	SMTPHost      string `env:"SMTP_HOST" env-default:"localhost"`
	SMTPPort      int    `env:"SMTP_PORT" env-default:"1025"`
	SMTPUsername  string `env:"SMTP_USERNAME"`
	SMTPPassword  string `env:"SMTP_PASSWORD"`
	SMTPUseTLS    bool   `env:"SMTP_USE_TLS" env-default:"false"`
	FromAddress   string `env:"FROM_ADDRESS" env-default:"noreply@diploma.local"`
	FromName      string `env:"FROM_NAME" env-default:"Diploma Marketplace"`
	MaxRetries    int    `env:"MAX_RETRIES" env-default:"3"`
	LogLevel      string `env:"LOG_LEVEL" env-default:"info"`
}

// Load reads configuration from environment variables and .env file.
func Load() (*Config, error) {
	_ = godotenv.Load(".env")

	var cfg Config
	if err := cleanenv.ReadConfig(".env", &cfg); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}

	return &cfg, nil
}

// KafkaBrokersList returns a slice of Kafka broker addresses.
func (c *Config) KafkaBrokersList() []string {
	if c.KafkaBrokers == "" {
		return []string{"localhost:9092"}
	}
	return splitAndTrim(c.KafkaBrokers)
}

func splitAndTrim(s string) []string {
	var result []string
	current := ""
	for _, ch := range s {
		if ch == ',' {
			trimmed := trimSpace(current)
			if trimmed != "" {
				result = append(result, trimmed)
			}
			current = ""
		} else {
			current += string(ch)
		}
	}
	trimmed := trimSpace(current)
	if trimmed != "" {
		result = append(result, trimmed)
	}
	return result
}

func trimSpace(s string) string {
	start := 0
	end := len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t') {
		end--
	}
	return s[start:end]
}
