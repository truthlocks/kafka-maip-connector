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

package receipt

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHashPayload(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected string
	}{
		{
			name:  "simple string",
			input: []byte("hello world"),
			expected: func() string {
				h := sha256.Sum256([]byte("hello world"))
				return hex.EncodeToString(h[:])
			}(),
		},
		{
			name:  "empty payload",
			input: []byte{},
			expected: func() string {
				h := sha256.Sum256([]byte{})
				return hex.EncodeToString(h[:])
			}(),
		},
		{
			name:  "binary payload",
			input: []byte{0x00, 0xFF, 0xAB, 0xCD},
			expected: func() string {
				h := sha256.Sum256([]byte{0x00, 0xFF, 0xAB, 0xCD})
				return hex.EncodeToString(h[:])
			}(),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := HashPayload(tc.input)
			assert.Equal(t, tc.expected, result)
			assert.Len(t, result, 64, "SHA-256 hex digest should be 64 characters")
		})
	}
}

func TestHashPayload_Deterministic(t *testing.T) {
	data := []byte("deterministic test data")
	hash1 := HashPayload(data)
	hash2 := HashPayload(data)
	assert.Equal(t, hash1, hash2, "same input must produce same hash")
}

func TestHashPayload_Different(t *testing.T) {
	hash1 := HashPayload([]byte("data-a"))
	hash2 := HashPayload([]byte("data-b"))
	assert.NotEqual(t, hash1, hash2, "different input must produce different hash")
}

func TestGenerateReceipt(t *testing.T) {
	msg := kafka.Message{
		Topic:     "orders",
		Partition: 3,
		Offset:    42,
		Key:       []byte("order-123"),
		Value:     []byte(`{"amount":99.99}`),
		Time:      time.Now(),
	}

	r := GenerateReceipt(msg, "agent-producer-1")

	require.NotEmpty(t, r.MessageID)
	assert.Equal(t, "orders-3-42", r.MessageID)
	assert.Equal(t, "orders", r.Topic)
	assert.Equal(t, 3, r.Partition)
	assert.Equal(t, int64(42), r.Offset)
	assert.Equal(t, "order-123", r.Key)
	assert.Equal(t, HashPayload(msg.Value), r.PayloadHash)
	assert.Equal(t, "agent-producer-1", r.ProducerAgent)
	assert.Empty(t, r.ConsumerAgent)
	assert.False(t, r.Timestamp.IsZero())
}

func TestGenerateReceipt_EmptyKey(t *testing.T) {
	msg := kafka.Message{
		Topic:     "events",
		Partition: 0,
		Offset:    1,
		Value:     []byte("event-data"),
	}

	r := GenerateReceipt(msg, "agent-1")
	assert.Empty(t, r.Key, "empty key should result in empty string")
}

func TestGenerateConsumerReceipt(t *testing.T) {
	msg := kafka.Message{
		Topic:     "payments",
		Partition: 1,
		Offset:    100,
		Key:       []byte("pay-456"),
		Value:     []byte(`{"status":"completed"}`),
	}

	r := GenerateConsumerReceipt(msg, "agent-consumer-1")

	assert.Equal(t, "payments-1-100", r.MessageID)
	assert.Equal(t, "payments", r.Topic)
	assert.Equal(t, 1, r.Partition)
	assert.Equal(t, int64(100), r.Offset)
	assert.Equal(t, "pay-456", r.Key)
	assert.Equal(t, HashPayload(msg.Value), r.PayloadHash)
	assert.Empty(t, r.ProducerAgent, "consumer receipt should have empty ProducerAgent")
	assert.Equal(t, "agent-consumer-1", r.ConsumerAgent)
	assert.False(t, r.Timestamp.IsZero())
}

func TestBuildMessageID(t *testing.T) {
	tests := []struct {
		topic     string
		partition int
		offset    int64
		expected  string
	}{
		{"orders", 0, 0, "orders-0-0"},
		{"events", 5, 12345, "events-5-12345"},
		{"topic.with.dots", 1, 99, "topic.with.dots-1-99"},
	}

	for _, tc := range tests {
		msg := kafka.Message{
			Topic:     tc.topic,
			Partition: tc.partition,
			Offset:    tc.offset,
		}
		result := buildMessageID(msg)
		assert.Equal(t, tc.expected, result)
	}
}
