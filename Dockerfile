FROM golang:1.22-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build the mail-service binary (Kafka consumer only, no HTTP server)
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/bin/mail-service ./cmd/main.go

FROM alpine:3.19

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=builder /app/bin/mail-service .

# No ports exposed - mail-service is a pure Kafka consumer
# It only sends emails via SMTP, no inbound HTTP

ENTRYPOINT ["./mail-service"]
