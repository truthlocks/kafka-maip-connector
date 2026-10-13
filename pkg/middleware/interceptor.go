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

// Package middleware provides generic Kafka interceptors that add MAIP receipt
// generation to any kafka.Writer or kafka.Reader instance.
package middleware

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/segmentio/kafka-go"

	"github.com/truthlocks/kafka-maip-connector/pkg/client"
	"github.com/truthlocks/kafka-maip-connector/pkg/config"
	"github.com/truthlocks/kafka-maip-connector/pkg/consumer"
	"github.com/truthlocks/kafka-maip-connector/pkg/producer"
)

// Metrics holds a point-in-time snapshot of operational counters.
type Metrics struct {
	MessagesProduced int64 `json:"messages_produced"`
	MessagesConsumed int64 `json:"messages_consumed"`
	ReceiptsCreated  int64 `json:"receipts_created"`
	ReceiptErrors    int64 `json:"receipt_errors"`
}

// InterceptorSet holds references to a producer and consumer interceptor pair.
type InterceptorSet struct {
	Producer *producer.MAIPProducer
	Consumer *consumer.MAIPConsumer
	metrics  *Metrics
}

// ProducerInterceptor wraps a kafka.Writer with MAIP receipt generation.
func ProducerInterceptor(writer *kafka.Writer, cfg config.Config) *producer.MAIPProducer {
	maipClient := client.NewMAIPClient(cfg.MAIPApiURL, cfg.MAIPApiKey, cfg.MAIPTenantID, nil)
	return producer.NewMAIPProducer(writer, maipClient, cfg)
}

// ConsumerInterceptor wraps a kafka.Reader with MAIP receipt generation.
func ConsumerInterceptor(reader *kafka.Reader, cfg config.Config) *consumer.MAIPConsumer {
	maipClient := client.NewMAIPClient(cfg.MAIPApiURL, cfg.MAIPApiKey, cfg.MAIPTenantID, nil)
	return consumer.NewMAIPConsumer(reader, maipClient, cfg)
}

// ProducerInterceptorWithClient wraps a kafka.Writer using an existing MAIPClient.
func ProducerInterceptorWithClient(writer *kafka.Writer, maipClient client.MAIPClient, cfg config.Config) *producer.MAIPProducer {
	return producer.NewMAIPProducer(writer, maipClient, cfg)
}

// ConsumerInterceptorWithClient wraps a kafka.Reader using an existing MAIPClient.
func ConsumerInterceptorWithClient(reader *kafka.Reader, maipClient client.MAIPClient, cfg config.Config) *consumer.MAIPConsumer {
	return consumer.NewMAIPConsumer(reader, maipClient, cfg)
}

// NewInterceptorSet creates both a producer and consumer interceptor sharing
// a MAIP client, along with a unified metrics view.
func NewInterceptorSet(writer *kafka.Writer, reader *kafka.Reader, cfg config.Config) *InterceptorSet {
	maipClient := client.NewMAIPClient(cfg.MAIPApiURL, cfg.MAIPApiKey, cfg.MAIPTenantID, nil)

	set := &InterceptorSet{
		metrics: &Metrics{},
	}

	if writer != nil {
		set.Producer = producer.NewMAIPProducer(writer, maipClient, cfg)
	}
	if reader != nil {
		set.Consumer = consumer.NewMAIPConsumer(reader, maipClient, cfg)
	}

	return set
}

// Metrics returns an aggregated metrics snapshot from producer and consumer.
func (s *InterceptorSet) Metrics() Metrics {
	m := Metrics{}
	if s.Producer != nil {
		m.MessagesProduced = s.Producer.MessagesProduced()
		m.ReceiptsCreated += s.Producer.ReceiptsCreated()
		m.ReceiptErrors += s.Producer.ReceiptErrors()
	}
	if s.Consumer != nil {
		m.MessagesConsumed = s.Consumer.MessagesConsumed()
		m.ReceiptsCreated += s.Consumer.ReceiptsCreated()
		m.ReceiptErrors += s.Consumer.ReceiptErrors()
	}
	return m
}

// Close shuts down both producer and consumer interceptors.
func (s *InterceptorSet) Close() error {
	var errs []error
	if s.Producer != nil {
		if err := s.Producer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close producer: %w", err))
		}
	}
	if s.Consumer != nil {
		if err := s.Consumer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close consumer: %w", err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("interceptor close errors: %v", errs)
	}
	return nil
}

// MetricsHandler returns an http.HandlerFunc that serves JSON metrics.
func (s *InterceptorSet) MetricsHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		m := s.Metrics()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(m) //nolint:errcheck
	}
}
