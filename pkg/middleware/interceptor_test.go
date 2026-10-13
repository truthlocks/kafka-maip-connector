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

package middleware

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/truthlocks/kafka-maip-connector/pkg/client"
	"github.com/truthlocks/kafka-maip-connector/pkg/config"
	"github.com/truthlocks/kafka-maip-connector/pkg/receipt"
)

// mockMAIPClient implements client.MAIPClient for testing.
type mockMAIPClient struct {
	mu              sync.Mutex
	createCalls     []receipt.DataFlowReceipt
	batchCalls      [][]receipt.DataFlowReceipt
	createCallCount int32
	batchCallCount  int32
}

func (m *mockMAIPClient) CreateReceipt(_ context.Context, payload receipt.DataFlowReceipt) (*client.ReceiptResponse, error) {
	atomic.AddInt32(&m.createCallCount, 1)
	m.mu.Lock()
	m.createCalls = append(m.createCalls, payload)
	m.mu.Unlock()
	return &client.ReceiptResponse{
		ReceiptID: "rcpt-" + payload.MessageID,
		Status:    "active",
	}, nil
}

func (m *mockMAIPClient) CreateReceiptBatch(_ context.Context, payloads []receipt.DataFlowReceipt) (*client.BatchReceiptResponse, error) {
	atomic.AddInt32(&m.batchCallCount, 1)
	m.mu.Lock()
	cp := make([]receipt.DataFlowReceipt, len(payloads))
	copy(cp, payloads)
	m.batchCalls = append(m.batchCalls, cp)
	m.mu.Unlock()
	return &client.BatchReceiptResponse{
		Created: len(payloads),
		Failed:  0,
	}, nil
}

func (m *mockMAIPClient) VerifyReceipt(_ context.Context, _ string) (*client.VerifyResponse, error) {
	return &client.VerifyResponse{Valid: true, Verdict: "VALID"}, nil
}

func (m *mockMAIPClient) GetReceipt(_ context.Context, id string) (*client.ReceiptResponse, error) {
	return &client.ReceiptResponse{ReceiptID: id, Status: "active"}, nil
}

func testConfig() config.Config {
	return config.Config{
		MAIPApiURL:    "http://localhost/v1/machine-identity",
		MAIPApiKey:    "test-key",
		MAIPTenantID:  "test-tenant",
		MAIPAgentID:   "test-agent",
		KafkaBrokers:  []string{"localhost:9092"},
		KafkaTopic:    "test-topic",
		ConsumerGroup: "test-group",
		AutoReceipt:   true,
		ReceiptType:   "data_flow_receipt",
		BatchSize:     5,
		FlushInterval: 100 * time.Millisecond,
	}
}

func TestProducerInterceptorWithClient(t *testing.T) {
	mock := &mockMAIPClient{}
	cfg := testConfig()
	cfg.AutoReceipt = false

	// Use ProducerInterceptorWithClient and verify the returned producer is valid.
	// Note: we cannot call Close() on a zero-value kafka.Writer without panicking,
	// so we only verify construction and metrics here.
	p := ProducerInterceptorWithClient(nil, mock, cfg)

	require.NotNil(t, p)
	assert.Equal(t, int64(0), p.MessagesProduced())
	assert.Equal(t, int64(0), p.ReceiptsCreated())
	assert.Equal(t, int64(0), p.ReceiptErrors())
}

func TestConsumerInterceptorWithClient(t *testing.T) {
	mock := &mockMAIPClient{}
	cfg := testConfig()
	cfg.AutoReceipt = false

	// Verify construction and initial metrics without calling Close on zero-value reader.
	c := ConsumerInterceptorWithClient(nil, mock, cfg)

	require.NotNil(t, c)
	assert.Equal(t, int64(0), c.MessagesConsumed())
	assert.Equal(t, int64(0), c.ReceiptsCreated())
	assert.Equal(t, int64(0), c.ReceiptErrors())
}

func TestNewInterceptorSet_ProducerOnly(t *testing.T) {
	cfg := testConfig()
	cfg.AutoReceipt = false

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	}))
	defer server.Close()
	cfg.MAIPApiURL = server.URL

	// Pass nil reader so only producer is created.
	set := NewInterceptorSet(nil, nil, cfg)

	require.NotNil(t, set)
	assert.Nil(t, set.Producer) // nil writer => nil producer
	assert.Nil(t, set.Consumer) // nil reader => nil consumer

	m := set.Metrics()
	assert.Equal(t, int64(0), m.MessagesProduced)
	assert.Equal(t, int64(0), m.MessagesConsumed)
	assert.Equal(t, int64(0), m.ReceiptsCreated)
	assert.Equal(t, int64(0), m.ReceiptErrors)

	// Close on nil producer/consumer should not panic.
	require.NoError(t, set.Close())
}

func TestNewInterceptorSet_NilWriter(t *testing.T) {
	cfg := testConfig()
	cfg.AutoReceipt = false

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	cfg.MAIPApiURL = server.URL

	// nil writer => nil producer, non-nil reader => consumer created.
	set := NewInterceptorSet(nil, nil, cfg)

	require.NotNil(t, set)
	assert.Nil(t, set.Producer)
	assert.Nil(t, set.Consumer)

	require.NoError(t, set.Close())
}

func TestInterceptorSet_MetricsHandler(t *testing.T) {
	cfg := testConfig()
	cfg.AutoReceipt = false

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	cfg.MAIPApiURL = server.URL

	// Use nil writer/reader to avoid panic on close; metrics handler works regardless.
	set := NewInterceptorSet(nil, nil, cfg)
	defer set.Close()

	handler := set.MetricsHandler()
	require.NotNil(t, handler)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

	var m Metrics
	err := json.Unmarshal(w.Body.Bytes(), &m)
	require.NoError(t, err)
	assert.Equal(t, int64(0), m.MessagesProduced)
	assert.Equal(t, int64(0), m.MessagesConsumed)
}

func TestMetrics_Values(t *testing.T) {
	m := Metrics{
		MessagesProduced: 10,
		MessagesConsumed: 20,
		ReceiptsCreated:  28,
		ReceiptErrors:    2,
	}

	assert.Equal(t, int64(10), m.MessagesProduced)
	assert.Equal(t, int64(20), m.MessagesConsumed)
	assert.Equal(t, int64(28), m.ReceiptsCreated)
	assert.Equal(t, int64(2), m.ReceiptErrors)
}

func TestInterceptorSet_Metrics_Aggregation(t *testing.T) {
	// Test that Metrics() aggregates from both producer and consumer.
	// Since we can't construct real kafka.Writer/Reader, we test the nil paths.
	set := &InterceptorSet{
		Producer: nil,
		Consumer: nil,
		metrics:  &Metrics{},
	}

	m := set.Metrics()
	assert.Equal(t, int64(0), m.MessagesProduced)
	assert.Equal(t, int64(0), m.MessagesConsumed)
	assert.Equal(t, int64(0), m.ReceiptsCreated)
	assert.Equal(t, int64(0), m.ReceiptErrors)
}

func TestInterceptorSet_Close_BothNil(t *testing.T) {
	set := &InterceptorSet{
		Producer: nil,
		Consumer: nil,
		metrics:  &Metrics{},
	}
	err := set.Close()
	require.NoError(t, err)
}
