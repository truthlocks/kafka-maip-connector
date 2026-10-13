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

// Package consumer provides a MAIP-aware Kafka consumer that automatically
// generates data-flow receipts for every message consumed from Kafka.
package consumer

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/truthlocks/kafka-maip-connector/pkg/client"
	"github.com/truthlocks/kafka-maip-connector/pkg/config"
	"github.com/truthlocks/kafka-maip-connector/pkg/receipt"
)

// MessageHandler processes a Kafka message. Return nil to indicate success.
type MessageHandler func(msg kafka.Message) error

// MAIPConsumer wraps a kafka.Reader and generates MAIP receipts for consumed messages.
type MAIPConsumer struct {
	reader     *kafka.Reader
	maipClient client.MAIPClient
	config     config.Config

	handler   MessageHandler
	mu        sync.RWMutex
	receiptCh chan receipt.DataFlowReceipt
	stopCh    chan struct{}
	wg        sync.WaitGroup

	// Metrics
	messagesConsumed int64
	receiptsCreated  int64
	receiptErrors    int64
}

// NewMAIPConsumer creates a MAIP-aware consumer wrapping the given kafka.Reader.
func NewMAIPConsumer(reader *kafka.Reader, maipClient client.MAIPClient, cfg config.Config) *MAIPConsumer {
	c := &MAIPConsumer{
		reader:     reader,
		maipClient: maipClient,
		config:     cfg,
		receiptCh:  make(chan receipt.DataFlowReceipt, cfg.BatchSize*2),
		stopCh:     make(chan struct{}),
	}

	if cfg.AutoReceipt {
		c.wg.Add(1)
		go c.receiptFlusher()
	}

	return c
}

// RegisterHandler sets the message handler for the Run loop.
func (c *MAIPConsumer) RegisterHandler(handler MessageHandler) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.handler = handler
}

// ReadMessage reads a single message from Kafka, generates a receipt, and returns
// the message. The message is auto-committed by the underlying kafka.Reader.
func (c *MAIPConsumer) ReadMessage(ctx context.Context) (kafka.Message, error) {
	msg, err := c.reader.ReadMessage(ctx)
	if err != nil {
		return msg, err
	}

	atomic.AddInt64(&c.messagesConsumed, 1)

	if c.config.AutoReceipt {
		r := receipt.GenerateConsumerReceipt(msg, c.config.MAIPAgentID)
		select {
		case c.receiptCh <- r:
		default:
			c.flushSingle(ctx, r)
		}
	}

	return msg, nil
}

// FetchMessage reads a message without committing it.
func (c *MAIPConsumer) FetchMessage(ctx context.Context) (kafka.Message, error) {
	msg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return msg, err
	}

	atomic.AddInt64(&c.messagesConsumed, 1)
	return msg, nil
}

// CommitMessages commits the given messages and generates receipts for them.
func (c *MAIPConsumer) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	if err := c.reader.CommitMessages(ctx, msgs...); err != nil {
		return fmt.Errorf("commit messages: %w", err)
	}

	if c.config.AutoReceipt {
		for _, msg := range msgs {
			r := receipt.GenerateConsumerReceipt(msg, c.config.MAIPAgentID)
			select {
			case c.receiptCh <- r:
			default:
				c.flushSingle(ctx, r)
			}
		}
	}

	return nil
}

// Run starts the consumer loop, reading messages and passing them to the registered
// handler. Receipts are generated for every successfully processed message.
// Run blocks until the context is cancelled or an unrecoverable error occurs.
func (c *MAIPConsumer) Run(ctx context.Context) error {
	c.mu.RLock()
	handler := c.handler
	c.mu.RUnlock()

	if handler == nil {
		return fmt.Errorf("no message handler registered; call RegisterHandler first")
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Printf("[maip-consumer] fetch error: %v", err)
			continue
		}

		atomic.AddInt64(&c.messagesConsumed, 1)

		if err := handler(msg); err != nil {
			log.Printf("[maip-consumer] handler error for %s-%d-%d: %v",
				msg.Topic, msg.Partition, msg.Offset, err)
			continue
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("[maip-consumer] commit error for %s-%d-%d: %v",
				msg.Topic, msg.Partition, msg.Offset, err)
			continue
		}

		if c.config.AutoReceipt {
			r := receipt.GenerateConsumerReceipt(msg, c.config.MAIPAgentID)
			select {
			case c.receiptCh <- r:
			default:
				c.flushSingle(ctx, r)
			}
		}
	}
}

// Close flushes pending receipts and closes the underlying Kafka reader.
func (c *MAIPConsumer) Close() error {
	close(c.stopCh)
	c.wg.Wait()
	if c.reader != nil {
		return c.reader.Close()
	}
	return nil
}

// MessagesConsumed returns the total number of messages consumed.
func (c *MAIPConsumer) MessagesConsumed() int64 {
	return atomic.LoadInt64(&c.messagesConsumed)
}

// ReceiptsCreated returns the total number of receipts successfully created.
func (c *MAIPConsumer) ReceiptsCreated() int64 {
	return atomic.LoadInt64(&c.receiptsCreated)
}

// ReceiptErrors returns the total number of receipt creation failures.
func (c *MAIPConsumer) ReceiptErrors() int64 {
	return atomic.LoadInt64(&c.receiptErrors)
}

// receiptFlusher runs in a goroutine, batching and flushing receipts.
func (c *MAIPConsumer) receiptFlusher() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.config.FlushInterval)
	defer ticker.Stop()

	batch := make([]receipt.DataFlowReceipt, 0, c.config.BatchSize)

	for {
		select {
		case r := <-c.receiptCh:
			batch = append(batch, r)
			if len(batch) >= c.config.BatchSize {
				c.flushBatch(context.Background(), batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				c.flushBatch(context.Background(), batch)
				batch = batch[:0]
			}
		case <-c.stopCh:
			for {
				select {
				case r := <-c.receiptCh:
					batch = append(batch, r)
				default:
					if len(batch) > 0 {
						c.flushBatch(context.Background(), batch)
					}
					return
				}
			}
		}
	}
}

// flushBatch submits a batch of receipts to the MAIP API.
func (c *MAIPConsumer) flushBatch(ctx context.Context, receipts []receipt.DataFlowReceipt) {
	resp, err := c.maipClient.CreateReceiptBatch(ctx, receipts)
	if resp != nil {
		atomic.AddInt64(&c.receiptsCreated, int64(resp.Created))
		atomic.AddInt64(&c.receiptErrors, int64(resp.Failed+resp.Unattempted))
	}
	if err != nil {
		if resp == nil {
			atomic.AddInt64(&c.receiptErrors, int64(len(receipts)))
		}
		log.Printf("[maip-consumer] batch receipt creation failed (%d receipts): %v", len(receipts), err)
		return
	}
}

// flushSingle submits a single receipt to the MAIP API.
func (c *MAIPConsumer) flushSingle(ctx context.Context, r receipt.DataFlowReceipt) {
	_, err := c.maipClient.CreateReceipt(ctx, r)
	if err != nil {
		atomic.AddInt64(&c.receiptErrors, 1)
		log.Printf("[maip-consumer] receipt creation failed for %s: %v", r.MessageID, err)
		return
	}
	atomic.AddInt64(&c.receiptsCreated, 1)
}
