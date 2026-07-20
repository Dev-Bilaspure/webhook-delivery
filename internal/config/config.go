// Package config loads configuration from the environment, with defaults.
package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	KafkaBrokers []string

	APIAddr      string
	ReceiverAddr string
	MetricsAddr  string

	EventsTopic   string
	RetriesTopic  string
	DLQTopic      string
	DeliveryGroup string
	RetryGroup    string

	BatchCapacity         int
	BatchFillTimeout      time.Duration
	MaxConcurrency        int
	MaxConcurrencyPerHost int

	RetryCountLimit int
	BaseBackoff     time.Duration

	BreakerFailureThreshold int
	BreakerCooldown         time.Duration

	DeliveryTimeout time.Duration

	LogLevel string
}

func Load() Config {
	return Config{
		KafkaBrokers: envSlice("KAFKA_BROKERS", []string{"localhost:9092"}),

		APIAddr:      env("API_ADDR", ":8000"),
		ReceiverAddr: env("RECEIVER_ADDR", ":8080"),
		MetricsAddr:  env("METRICS_ADDR", ":9100"),

		EventsTopic:   env("EVENTS_TOPIC", "events"),
		RetriesTopic:  env("RETRIES_TOPIC", "retries"),
		DLQTopic:      env("DLQ_TOPIC", "dead-letter"),
		DeliveryGroup: env("DELIVERY_GROUP", "delivery-worker"),
		RetryGroup:    env("RETRY_GROUP", "retry-worker"),

		BatchCapacity:         envInt("BATCH_CAPACITY", 50),
		BatchFillTimeout:      envDuration("BATCH_FILL_TIMEOUT", 200*time.Millisecond),
		MaxConcurrency:        envInt("MAX_CONCURRENCY", 10),
		MaxConcurrencyPerHost: envInt("MAX_CONCURRENCY_PER_HOST", 5),

		RetryCountLimit: envInt("RETRY_COUNT_LIMIT", 5),
		BaseBackoff:     envDuration("BASE_BACKOFF", 1*time.Second),

		BreakerFailureThreshold: envInt("BREAKER_FAILURE_THRESHOLD", 5),
		BreakerCooldown:         envDuration("BREAKER_COOLDOWN", 45*time.Second),

		DeliveryTimeout: envDuration("DELIVERY_TIMEOUT", 10*time.Second),

		LogLevel: env("LOG_LEVEL", "info"),
	}
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

func envSlice(key string, def []string) []string {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	parts := strings.Split(v, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

func envInt(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(key); ok {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
