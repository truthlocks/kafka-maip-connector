// Copyright 2026 Truthlocks Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package config provides configuration loading for the Kafka MAIP connector.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all configuration for the Kafka MAIP connector.
type Config struct {
	// MAIPApiURL is the base URL for the MAIP receipt API.
	MAIPApiURL string
	// MAIPApiKey is the API key for authenticating with the MAIP API.
	MAIPApiKey string
	// MAIPTenantID is deprecated and ignored; the credential determines the tenant.
	MAIPTenantID string
	// MAIPAgentID identifies this connector instance as a MAIP agent.
	MAIPAgentID string

	// KafkaBrokers is the list of Kafka broker addresses.
	KafkaBrokers []string
	// KafkaTopic is the target Kafka topic.
	KafkaTopic string
	// ConsumerGroup is the Kafka consumer group identifier.
	ConsumerGroup string

	// AutoReceipt enables automatic MAIP receipt generation for every message.
	AutoReceipt bool
	// ReceiptType is data.attestation; data_flow_receipt is accepted as a legacy alias.
	ReceiptType string
	// BatchSize is the number of receipts to batch before flushing.
	BatchSize int
	// FlushInterval is the maximum time between receipt batch flushes.
	FlushInterval time.Duration
}

// LoadFromEnv reads configuration from environment variables with sensible defaults.
func LoadFromEnv() (*Config, error) {
	cfg := &Config{
		MAIPApiURL:    envOrDefault("MAIP_API_URL", "https://sandbox-api.truthlocks.com/v1"),
		MAIPApiKey:    os.Getenv("MAIP_API_KEY"),
		MAIPTenantID:  os.Getenv("MAIP_TENANT_ID"),
		MAIPAgentID:   os.Getenv("MAIP_AGENT_ID"),
		KafkaTopic:    os.Getenv("KAFKA_TOPIC"),
		ConsumerGroup: envOrDefault("KAFKA_CONSUMER_GROUP", "maip-connector"),
		ReceiptType:   envOrDefault("MAIP_RECEIPT_TYPE", "data.attestation"),
	}

	// Parse broker list.
	brokers := envOrDefault("KAFKA_BROKERS", "localhost:9092")
	cfg.KafkaBrokers = strings.Split(brokers, ",")

	// Parse boolean.
	autoReceipt := envOrDefault("MAIP_AUTO_RECEIPT", "true")
	ar, err := strconv.ParseBool(autoReceipt)
	if err != nil {
		return nil, fmt.Errorf("invalid MAIP_AUTO_RECEIPT value %q: %w", autoReceipt, err)
	}
	cfg.AutoReceipt = ar

	// Parse batch size.
	batchStr := envOrDefault("MAIP_BATCH_SIZE", "100")
	bs, err := strconv.Atoi(batchStr)
	if err != nil {
		return nil, fmt.Errorf("invalid MAIP_BATCH_SIZE value %q: %w", batchStr, err)
	}
	cfg.BatchSize = bs

	// Parse flush interval.
	flushStr := envOrDefault("MAIP_FLUSH_INTERVAL", "5s")
	fi, err := time.ParseDuration(flushStr)
	if err != nil {
		return nil, fmt.Errorf("invalid MAIP_FLUSH_INTERVAL value %q: %w", flushStr, err)
	}
	cfg.FlushInterval = fi

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// Validate checks that required configuration fields are present.
func (c *Config) Validate() error {
	if c.MAIPApiKey == "" {
		return fmt.Errorf("MAIP_API_KEY is required")
	}
	if c.ReceiptType != "" && c.ReceiptType != "data.attestation" && c.ReceiptType != "data_flow_receipt" {
		return fmt.Errorf("MAIP_RECEIPT_TYPE must be data.attestation")
	}
	if c.MAIPAgentID == "" {
		return fmt.Errorf("MAIP_AGENT_ID is required")
	}
	if len(c.KafkaBrokers) == 0 {
		return fmt.Errorf("KAFKA_BROKERS is required")
	}
	if c.BatchSize < 1 {
		return fmt.Errorf("MAIP_BATCH_SIZE must be >= 1, got %d", c.BatchSize)
	}
	if c.FlushInterval < time.Millisecond {
		return fmt.Errorf("MAIP_FLUSH_INTERVAL must be >= 1ms, got %s", c.FlushInterval)
	}
	return nil
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
