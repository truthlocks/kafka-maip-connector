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

// Package producer provides a MAIP-aware Kafka producer that automatically
// generates data-flow receipts for every message written to Kafka.
package producer

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/truthlocks/kafka-maip-connector/pkg/client"
	"github.com/truthlocks/kafka-maip-connector/pkg/config"
	"github.com/truthlocks/kafka-maip-connector/pkg/receipt"
)

// MAIPProducer wraps a kafka.Writer and generates MAIP receipts for produced messages.
type MAIPProducer struct {
	writer     *kafka.Writer
	maipClient client.MAIPClient
	config     config.Config

	receiptCh chan receipt.DataFlowReceipt
	stopCh    chan struct{}
	wg        sync.WaitGroup

	// Metrics
	messagesProduced int64
	receiptsCreated  int64
	receiptErrors    int64
}

// NewMAIPProducer creates a MAIP-aware producer wrapping the given kafka.Writer.
func NewMAIPProducer(writer *kafka.Writer, maipClient client.MAIPClient, cfg config.Config) *MAIPProducer {
	p := &MAIPProducer{
		writer:     writer,
		maipClient: maipClient,
		config:     cfg,
		receiptCh:  make(chan receipt.DataFlowReceipt, cfg.BatchSize*2),
		stopCh:     make(chan struct{}),
	}

	if cfg.AutoReceipt {
		p.wg.Add(1)
		go p.receiptFlusher()
	}

	return p
}

// WriteMessage writes a single message to Kafka and generates a MAIP receipt.
func (p *MAIPProducer) WriteMessage(ctx context.Context, msg kafka.Message) error {
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return err
	}

	atomic.AddInt64(&p.messagesProduced, 1)

	if p.config.AutoReceipt {
		r := receipt.GenerateReceipt(msg, p.config.MAIPAgentID)
		select {
		case p.receiptCh <- r:
		default:
			// Channel full: flush synchronously to avoid dropping receipts.
			p.flushSingle(ctx, r)
		}
	}

	return nil
}

// WriteBatch writes a batch of messages to Kafka and generates receipts for each.
func (p *MAIPProducer) WriteBatch(ctx context.Context, msgs []kafka.Message) error {
	if err := p.writer.WriteMessages(ctx, msgs...); err != nil {
		return err
	}

	atomic.AddInt64(&p.messagesProduced, int64(len(msgs)))

	if p.config.AutoReceipt {
		receipts := make([]receipt.DataFlowReceipt, 0, len(msgs))
		for _, msg := range msgs {
			receipts = append(receipts, receipt.GenerateReceipt(msg, p.config.MAIPAgentID))
		}
		p.flushBatch(ctx, receipts)
	}

	return nil
}

// Close flushes any pending receipts and closes the underlying Kafka writer.
func (p *MAIPProducer) Close() error {
	close(p.stopCh)
	p.wg.Wait()
	if p.writer != nil {
		return p.writer.Close()
	}
	return nil
}

// MessagesProduced returns the total number of messages produced.
func (p *MAIPProducer) MessagesProduced() int64 {
	return atomic.LoadInt64(&p.messagesProduced)
}

// ReceiptsCreated returns the total number of receipts successfully created.
func (p *MAIPProducer) ReceiptsCreated() int64 {
	return atomic.LoadInt64(&p.receiptsCreated)
}

// ReceiptErrors returns the total number of receipt creation failures.
func (p *MAIPProducer) ReceiptErrors() int64 {
	return atomic.LoadInt64(&p.receiptErrors)
}

// receiptFlusher runs in a goroutine, batching and flushing receipts.
func (p *MAIPProducer) receiptFlusher() {
	defer p.wg.Done()

	ticker := time.NewTicker(p.config.FlushInterval)
	defer ticker.Stop()

	batch := make([]receipt.DataFlowReceipt, 0, p.config.BatchSize)

	for {
		select {
		case r := <-p.receiptCh:
			batch = append(batch, r)
			if len(batch) >= p.config.BatchSize {
				p.flushBatch(context.Background(), batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				p.flushBatch(context.Background(), batch)
				batch = batch[:0]
			}
		case <-p.stopCh:
			// Drain remaining receipts from channel.
			for {
				select {
				case r := <-p.receiptCh:
					batch = append(batch, r)
				default:
					if len(batch) > 0 {
						p.flushBatch(context.Background(), batch)
					}
					return
				}
			}
		}
	}
}

// flushBatch submits a batch of receipts to the MAIP API.
func (p *MAIPProducer) flushBatch(ctx context.Context, receipts []receipt.DataFlowReceipt) {
	resp, err := p.maipClient.CreateReceiptBatch(ctx, receipts)
	if resp != nil {
		atomic.AddInt64(&p.receiptsCreated, int64(resp.Created))
		atomic.AddInt64(&p.receiptErrors, int64(resp.Failed+resp.Unattempted))
	}
	if err != nil {
		if resp == nil {
			atomic.AddInt64(&p.receiptErrors, int64(len(receipts)))
		}
		log.Printf("[maip-producer] batch receipt creation failed (%d receipts): %v", len(receipts), err)
		return
	}
}

// flushSingle submits a single receipt to the MAIP API.
func (p *MAIPProducer) flushSingle(ctx context.Context, r receipt.DataFlowReceipt) {
	_, err := p.maipClient.CreateReceipt(ctx, r)
	if err != nil {
		atomic.AddInt64(&p.receiptErrors, 1)
		log.Printf("[maip-producer] receipt creation failed for %s: %v", r.MessageID, err)
		return
	}
	atomic.AddInt64(&p.receiptsCreated, 1)
}
