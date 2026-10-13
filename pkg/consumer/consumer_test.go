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

package consumer

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

func TestMAIPConsumer_RegisterHandler(t *testing.T) {
	mock := &mockMAIPClient{}
	cfg := testConfig()
	cfg.AutoReceipt = false

	c := NewMAIPConsumer(&kafka.Reader{}, mock, cfg)
	require.NotNil(t, c)

	called := false
	c.RegisterHandler(func(msg kafka.Message) error {
		called = true
		return nil
	})

	c.mu.RLock()
	handler := c.handler
	c.mu.RUnlock()

	require.NotNil(t, handler)

	// Invoke handler directly to verify registration.
	err := handler(kafka.Message{})
	require.NoError(t, err)
	assert.True(t, called)
}

func TestMAIPConsumer_Run_NoHandler(t *testing.T) {
	mock := &mockMAIPClient{}
	cfg := testConfig()
	cfg.AutoReceipt = false

	c := NewMAIPConsumer(&kafka.Reader{}, mock, cfg)

	err := c.Run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no message handler registered")
}

func TestMAIPConsumer_ReceiptFlusher(t *testing.T) {
	mock := &mockMAIPClient{}
	cfg := testConfig()
	cfg.AutoReceipt = true
	cfg.BatchSize = 2
	cfg.FlushInterval = 50 * time.Millisecond

	c := &MAIPConsumer{
		reader:     &kafka.Reader{},
		maipClient: mock,
		config:     cfg,
		receiptCh:  make(chan receipt.DataFlowReceipt, cfg.BatchSize*2),
		stopCh:     make(chan struct{}),
	}

	c.wg.Add(1)
	go c.receiptFlusher()

	// Enqueue 3 receipts: first 2 form a batch, third is flushed on interval or close.
	for i := 0; i < 3; i++ {
		msg := kafka.Message{
			Topic:     "test",
			Partition: 0,
			Offset:    int64(i),
			Value:     []byte(fmt.Sprintf("value-%d", i)),
		}
		c.receiptCh <- receipt.GenerateConsumerReceipt(msg, cfg.MAIPAgentID)
	}

	// Wait for batching.
	time.Sleep(200 * time.Millisecond)
	close(c.stopCh)
	c.wg.Wait()

	mock.mu.Lock()
	defer mock.mu.Unlock()

	totalReceipts := 0
	for _, batch := range mock.batchCalls {
		totalReceipts += len(batch)
	}
	assert.Equal(t, 3, totalReceipts, "all 3 consumer receipts should be flushed")
}

func TestMAIPConsumer_ReceiptFlusher_Error(t *testing.T) {
	mock := &mockMAIPClient{
		batchErr: fmt.Errorf("api unavailable"),
	}
	cfg := testConfig()
	cfg.AutoReceipt = true
	cfg.BatchSize = 1
	cfg.FlushInterval = 50 * time.Millisecond

	c := &MAIPConsumer{
		reader:     &kafka.Reader{},
		maipClient: mock,
		config:     cfg,
		receiptCh:  make(chan receipt.DataFlowReceipt, cfg.BatchSize*2),
		stopCh:     make(chan struct{}),
	}

	c.wg.Add(1)
	go c.receiptFlusher()

	msg := kafka.Message{Topic: "test", Partition: 0, Offset: 1, Value: []byte("v")}
	c.receiptCh <- receipt.GenerateConsumerReceipt(msg, cfg.MAIPAgentID)

	time.Sleep(200 * time.Millisecond)
	close(c.stopCh)
	c.wg.Wait()

	assert.Equal(t, int64(0), c.ReceiptsCreated(), "no receipts should be created on error")
	assert.Equal(t, int64(1), c.ReceiptErrors(), "1 receipt error should be recorded")
}

func TestMAIPConsumer_Metrics(t *testing.T) {
	c := &MAIPConsumer{}

	atomic.StoreInt64(&c.messagesConsumed, 100)
	atomic.StoreInt64(&c.receiptsCreated, 98)
	atomic.StoreInt64(&c.receiptErrors, 2)

	assert.Equal(t, int64(100), c.MessagesConsumed())
	assert.Equal(t, int64(98), c.ReceiptsCreated())
	assert.Equal(t, int64(2), c.ReceiptErrors())
}

func TestMAIPConsumer_FlushOnClose(t *testing.T) {
	mock := &mockMAIPClient{}
	cfg := testConfig()
	cfg.AutoReceipt = true
	cfg.BatchSize = 100
	cfg.FlushInterval = 10 * time.Second

	c := &MAIPConsumer{
		reader:     &kafka.Reader{},
		maipClient: mock,
		config:     cfg,
		receiptCh:  make(chan receipt.DataFlowReceipt, cfg.BatchSize*2),
		stopCh:     make(chan struct{}),
	}

	c.wg.Add(1)
	go c.receiptFlusher()

	msg := kafka.Message{Topic: "t", Partition: 0, Offset: 0, Value: []byte("x")}
	c.receiptCh <- receipt.GenerateConsumerReceipt(msg, cfg.MAIPAgentID)

	// Close triggers drain.
	close(c.stopCh)
	c.wg.Wait()

	mock.mu.Lock()
	defer mock.mu.Unlock()

	totalReceipts := 0
	for _, batch := range mock.batchCalls {
		totalReceipts += len(batch)
	}
	assert.Equal(t, 1, totalReceipts, "pending receipt must be flushed on close")
}

func TestPartialBatchMetricsPreserveConfirmedCreations(t *testing.T) {
	mock := &mockMAIPClient{batchErr: fmt.Errorf("second request failed"), partialResponse: &client.BatchReceiptResponse{Created: 1, Failed: 1, Unattempted: 1}}
	obj := &MAIPConsumer{maipClient: mock}
	obj.flushBatch(context.Background(), make([]receipt.DataFlowReceipt, 3))
	if obj.ReceiptsCreated() != 1 || obj.ReceiptErrors() != 2 {
		t.Fatalf("wrong partial metrics: created=%d errors=%d", obj.ReceiptsCreated(), obj.ReceiptErrors())
	}
}
