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

package producer

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
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
	createErr       error
	batchErr        error
	partialResponse *client.BatchReceiptResponse
	createCallCount int32
	batchCallCount  int32
}

func (m *mockMAIPClient) CreateReceipt(_ context.Context, payload receipt.DataFlowReceipt) (*client.ReceiptResponse, error) {
	atomic.AddInt32(&m.createCallCount, 1)
	m.mu.Lock()
	m.createCalls = append(m.createCalls, payload)
	m.mu.Unlock()
	if m.createErr != nil {
		return nil, m.createErr
	}
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
	if m.batchErr != nil {
		return m.partialResponse, m.batchErr
	}
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

func TestMAIPProducer_WriteMessage(t *testing.T) {
	mock := &mockMAIPClient{}
	cfg := testConfig()
	cfg.AutoReceipt = false // Disable auto-receipt to test WriteMessage directly

	// We cannot use a real kafka.Writer in unit tests, so we test the receipt
	// generation logic via WriteBatch with auto-receipt off, then verify metrics.
	// For the full integration, the kafka.Writer would be tested against a broker.

	// Test that producer initializes without error.
	// Pass nil writer since zero-value kafka.Writer panics on Close.
	p := NewMAIPProducer(nil, mock, cfg)
	require.NotNil(t, p)

	assert.Equal(t, int64(0), p.MessagesProduced())
	assert.Equal(t, int64(0), p.ReceiptsCreated())
	assert.Equal(t, int64(0), p.ReceiptErrors())

	require.NoError(t, p.Close())
}

func TestMAIPProducer_ReceiptGeneration(t *testing.T) {
	mock := &mockMAIPClient{}
	cfg := testConfig()
	cfg.AutoReceipt = true
	cfg.BatchSize = 2
	cfg.FlushInterval = 50 * time.Millisecond

	p := &MAIPProducer{
		writer:     &kafka.Writer{},
		maipClient: mock,
		config:     cfg,
		receiptCh:  make(chan receipt.DataFlowReceipt, cfg.BatchSize*2),
		stopCh:     make(chan struct{}),
	}

	p.wg.Add(1)
	go p.receiptFlusher()

	// Simulate receipt enqueue (as if WriteMessage had successfully written to Kafka).
	msg1 := kafka.Message{Topic: "test", Partition: 0, Offset: 1, Value: []byte("v1")}
	msg2 := kafka.Message{Topic: "test", Partition: 0, Offset: 2, Value: []byte("v2")}
	msg3 := kafka.Message{Topic: "test", Partition: 0, Offset: 3, Value: []byte("v3")}

	p.receiptCh <- receipt.GenerateReceipt(msg1, cfg.MAIPAgentID)
	p.receiptCh <- receipt.GenerateReceipt(msg2, cfg.MAIPAgentID)
	p.receiptCh <- receipt.GenerateReceipt(msg3, cfg.MAIPAgentID)

	// Wait for batching to complete.
	time.Sleep(200 * time.Millisecond)
	require.NoError(t, p.Close())

	// Verify batch calls were made.
	mock.mu.Lock()
	defer mock.mu.Unlock()

	totalReceipts := 0
	for _, batch := range mock.batchCalls {
		totalReceipts += len(batch)
	}
	assert.Equal(t, 3, totalReceipts, "all 3 receipts should have been flushed")
	assert.Equal(t, int64(3), p.ReceiptsCreated())
}

func TestMAIPProducer_FlushOnClose(t *testing.T) {
	mock := &mockMAIPClient{}
	cfg := testConfig()
	cfg.AutoReceipt = true
	cfg.BatchSize = 100 // Large batch to ensure no mid-flight flush
	cfg.FlushInterval = 10 * time.Second

	p := &MAIPProducer{
		writer:     &kafka.Writer{},
		maipClient: mock,
		config:     cfg,
		receiptCh:  make(chan receipt.DataFlowReceipt, cfg.BatchSize*2),
		stopCh:     make(chan struct{}),
	}

	p.wg.Add(1)
	go p.receiptFlusher()

	// Enqueue a receipt.
	msg := kafka.Message{Topic: "test", Partition: 0, Offset: 1, Value: []byte("v1")}
	p.receiptCh <- receipt.GenerateReceipt(msg, cfg.MAIPAgentID)

	// Close should drain the channel.
	require.NoError(t, p.Close())

	mock.mu.Lock()
	defer mock.mu.Unlock()

	totalReceipts := 0
	for _, batch := range mock.batchCalls {
		totalReceipts += len(batch)
	}
	assert.Equal(t, 1, totalReceipts, "receipt should be flushed on close")
}

func TestMAIPProducer_Metrics(t *testing.T) {
	p := &MAIPProducer{}

	atomic.StoreInt64(&p.messagesProduced, 42)
	atomic.StoreInt64(&p.receiptsCreated, 40)
	atomic.StoreInt64(&p.receiptErrors, 2)

	assert.Equal(t, int64(42), p.MessagesProduced())
	assert.Equal(t, int64(40), p.ReceiptsCreated())
	assert.Equal(t, int64(2), p.ReceiptErrors())
}

func TestPartialBatchMetricsPreserveConfirmedCreations(t *testing.T) {
	mock := &mockMAIPClient{batchErr: fmt.Errorf("second request failed"), partialResponse: &client.BatchReceiptResponse{Created: 1, Failed: 1, Unattempted: 1}}
	obj := &MAIPProducer{maipClient: mock}
	obj.flushBatch(context.Background(), make([]receipt.DataFlowReceipt, 3))
	if obj.ReceiptsCreated() != 1 || obj.ReceiptErrors() != 2 {
		t.Fatalf("wrong partial metrics: created=%d errors=%d", obj.ReceiptsCreated(), obj.ReceiptErrors())
	}
}
