package integration

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/truthlocks/kafka-maip-connector/pkg/client"
	"github.com/truthlocks/kafka-maip-connector/pkg/config"
	"github.com/truthlocks/kafka-maip-connector/pkg/consumer"
	"github.com/truthlocks/kafka-maip-connector/pkg/producer"
	"github.com/truthlocks/kafka-maip-connector/pkg/receipt"
	"net"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
)

type recordingClient struct {
	client.MAIPClient
	mu  sync.Mutex
	ids []string
}

func (c *recordingClient) CreateReceiptBatch(ctx context.Context, p []receipt.DataFlowReceipt) (*client.BatchReceiptResponse, error) {
	r, e := c.MAIPClient.CreateReceiptBatch(ctx, p)
	if r != nil {
		c.mu.Lock()
		for _, v := range r.Receipts {
			c.ids = append(c.ids, v.ReceiptID)
		}
		c.mu.Unlock()
	}
	return r, e
}
func TestKafkaBrokerToHostedReceipts(t *testing.T) {
	broker, token := os.Getenv("TRUTHLOCKS_QA_BROKER"), os.Getenv("TRUTHLOCKS_QA_TOKEN")
	if broker == "" || token == "" {
		t.Skip("explicit isolated broker and QA session required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	var conn *kafka.Conn
	var err error
	for i := 0; i < 45; i++ {
		conn, err = kafka.DialContext(ctx, "tcp", broker)
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("broker readiness timeout")
		case <-time.After(time.Second):
		}
	}
	if err != nil {
		t.Fatal("isolated broker unavailable")
	}
	defer conn.Close()
	controller, err := conn.Controller()
	if err != nil {
		t.Fatal(err)
	}
	admin, err := kafka.DialContext(ctx, "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	topic := fmt.Sprintf("truthlocks-qa-%d", time.Now().UnixNano())
	if err = admin.CreateTopics(kafka.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
	api := client.NewBearerClient("https://api.truthlocks.com/v1", token, nil)
	prior, err := api.GetReceipt(ctx, "maip-receipt:01m4nx75rxvg56e890anryj6wn")
	if err != nil || prior.AgentID == "" {
		t.Fatal("QA identity unavailable")
	}
	tracked := &recordingClient{MAIPClient: api}
	cfg := config.Config{MAIPAgentID: prior.AgentID, AutoReceipt: true, BatchSize: 1, FlushInterval: 50 * time.Millisecond}
	writer := &kafka.Writer{Addr: kafka.TCP(broker), RequiredAcks: kafka.RequireAll, BatchTimeout: 10 * time.Millisecond}
	prod := producer.NewMAIPProducer(writer, tracked, cfg)
	defer prod.Close()
	data := []byte("Truthlocks isolated Kafka broker QA " + topic)
	if err = prod.WriteMessage(ctx, kafka.Message{Topic: topic, Key: []byte("synthetic"), Value: data}); err != nil {
		t.Fatal(err)
	}
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{broker}, Topic: topic, GroupID: topic, MinBytes: 1, MaxBytes: 10000, StartOffset: kafka.FirstOffset})
	cons := consumer.NewMAIPConsumer(reader, tracked, cfg)
	defer cons.Close()
	msg, err := cons.FetchMessage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(msg.Value) != string(data) {
		t.Fatal("broker returned different bytes")
	}
	if err = cons.CommitMessages(ctx, msg); err != nil {
		t.Fatal(err)
	}
	var ids []string
	for i := 0; i < 100; i++ {
		tracked.mu.Lock()
		ids = append([]string(nil), tracked.ids...)
		tracked.mu.Unlock()
		if len(ids) == 2 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("receipt flush timeout")
		case <-time.After(100 * time.Millisecond):
		}
	}
	if len(ids) != 2 || prod.ReceiptsCreated() != 1 || cons.ReceiptsCreated() != 1 || prod.ReceiptErrors()+cons.ReceiptErrors() != 0 {
		t.Fatalf("producer/consumer receipt delivery incomplete: %d", len(ids))
	}
	for _, id := range ids {
		r, e := api.GetReceipt(ctx, id)
		if e != nil || r.OutputsHash != receipt.HashPayload(data) {
			t.Fatal("hosted receipt hash mismatch")
		}
		v, e := api.VerifyReceipt(ctx, id)
		if e != nil || !v.Valid || !v.SignatureVerified {
			t.Fatal("broker receipt signature verification failed")
		}
	}
	t.Logf("Broker QA passed: producerReceipt=%s consumerReceipt=%s originalBytesMatched=true signaturesVerified=true", ids[0], ids[1])
}
