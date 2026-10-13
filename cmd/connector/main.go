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

// Package main provides the standalone Kafka MAIP connector binary.
// It reads configuration from environment variables and runs a MAIP-aware
// Kafka consumer that generates data-flow receipts for every message.
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/truthlocks/kafka-maip-connector/pkg/client"
	"github.com/truthlocks/kafka-maip-connector/pkg/config"
	"github.com/truthlocks/kafka-maip-connector/pkg/consumer"
)

var healthy int32

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Println("[connector] starting kafka-maip-connector")

	cfg, err := config.LoadFromEnv()
	if err != nil {
		log.Fatalf("[connector] configuration error: %v", err)
	}

	log.Printf("[connector] brokers=%v topic=%s group=%s agent=%s",
		cfg.KafkaBrokers, cfg.KafkaTopic, cfg.ConsumerGroup, cfg.MAIPAgentID)

	// Build Kafka reader.
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.KafkaBrokers,
		Topic:          cfg.KafkaTopic,
		GroupID:        cfg.ConsumerGroup,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
	})

	// Build MAIP client.
	maipClient := client.NewMAIPClient(cfg.MAIPApiURL, cfg.MAIPApiKey, cfg.MAIPTenantID, nil)

	// Build MAIP consumer.
	maipConsumer := consumer.NewMAIPConsumer(reader, maipClient, *cfg)

	// Register a pass-through handler that logs and acknowledges every message.
	maipConsumer.RegisterHandler(func(msg kafka.Message) error {
		log.Printf("[connector] processed %s-%d-%d (%d bytes)",
			msg.Topic, msg.Partition, msg.Offset, len(msg.Value))
		return nil
	})

	// Start health/metrics HTTP server.
	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/metrics", metricsHandler(maipConsumer))

	srv := &http.Server{
		Addr:         ":8090",
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	go func() {
		log.Println("[connector] health/metrics server listening on :8090")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[connector] http server error: %v", err)
		}
	}()

	// Graceful shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	atomic.StoreInt32(&healthy, 1)

	// Run consumer in a goroutine.
	errCh := make(chan error, 1)
	go func() {
		errCh <- maipConsumer.Run(ctx)
	}()

	// Wait for signal or error.
	select {
	case sig := <-sigCh:
		log.Printf("[connector] received signal %s, shutting down", sig)
	case err := <-errCh:
		if err != nil && err != context.Canceled {
			log.Printf("[connector] consumer error: %v", err)
		}
	}

	atomic.StoreInt32(&healthy, 0)
	cancel()

	// Shutdown HTTP server.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("[connector] http shutdown error: %v", err)
	}

	// Close MAIP consumer (flushes receipts).
	if err := maipConsumer.Close(); err != nil {
		log.Printf("[connector] consumer close error: %v", err)
	}

	log.Println("[connector] shutdown complete")
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	if atomic.LoadInt32(&healthy) == 1 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "healthy"}) //nolint:errcheck
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusServiceUnavailable)
	json.NewEncoder(w).Encode(map[string]string{"status": "unhealthy"}) //nolint:errcheck
}

func metricsHandler(c *consumer.MAIPConsumer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		m := map[string]int64{
			"messages_consumed": c.MessagesConsumed(),
			"receipts_created":  c.ReceiptsCreated(),
			"receipt_errors":    c.ReceiptErrors(),
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(m) //nolint:errcheck
	}
}
