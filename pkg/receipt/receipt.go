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

// Package receipt provides MAIP data-flow receipt generation for Kafka messages.
package receipt

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

// DataFlowReceipt represents a MAIP receipt for a Kafka message transit event.
type DataFlowReceipt struct {
	// MessageID is a unique identifier for the message this receipt covers.
	MessageID string `json:"message_id"`
	// Topic is the Kafka topic the message was produced to or consumed from.
	Topic string `json:"topic"`
	// Partition is the Kafka partition number.
	Partition int `json:"partition"`
	// Offset is the Kafka message offset within the partition.
	Offset int64 `json:"offset"`
	// Key is the message key (hex-encoded if binary, or UTF-8 string).
	Key string `json:"key,omitempty"`
	// PayloadHash is the SHA-256 hex digest of the message value.
	PayloadHash string `json:"payload_hash"`
	// Timestamp is when the receipt was generated.
	Timestamp time.Time `json:"timestamp"`
	// ProducerAgent identifies the producing MAIP agent (empty for consumer receipts).
	ProducerAgent string `json:"producer_agent,omitempty"`
	// ConsumerAgent identifies the consuming MAIP agent (empty for producer receipts).
	ConsumerAgent string `json:"consumer_agent,omitempty"`
}

// GenerateReceipt creates a DataFlowReceipt for a given Kafka message.
// The agentID is recorded as the ProducerAgent; use SetConsumerAgent to
// re-attribute it to a consumer after creation.
func GenerateReceipt(msg kafka.Message, agentID string) DataFlowReceipt {
	key := ""
	if len(msg.Key) > 0 {
		key = string(msg.Key)
	}

	messageID := buildMessageID(msg)

	return DataFlowReceipt{
		MessageID:     messageID,
		Topic:         msg.Topic,
		Partition:     msg.Partition,
		Offset:        msg.Offset,
		Key:           key,
		PayloadHash:   HashPayload(msg.Value),
		Timestamp:     time.Now().UTC(),
		ProducerAgent: agentID,
	}
}

// GenerateConsumerReceipt creates a DataFlowReceipt attributed to a consumer agent.
func GenerateConsumerReceipt(msg kafka.Message, agentID string) DataFlowReceipt {
	r := GenerateReceipt(msg, "")
	r.ProducerAgent = ""
	r.ConsumerAgent = agentID
	return r
}

// HashPayload returns the hex-encoded SHA-256 digest of data.
func HashPayload(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

// buildMessageID creates a deterministic message identifier from Kafka coordinates.
func buildMessageID(msg kafka.Message) string {
	return fmt.Sprintf("%s-%d-%d", msg.Topic, msg.Partition, msg.Offset)
}
