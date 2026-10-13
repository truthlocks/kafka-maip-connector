package client

import (
	"context"
	"github.com/truthlocks/kafka-maip-connector/pkg/receipt"
	"os"
	"testing"
	"time"
)

// Opt-in: only the existing Truthlocks QA workspace. Creates one labeled synthetic
// receipt. No Kafka broker is contacted by this HTTP client acceptance test.
func TestHostedQAReceiptLifecycle(t *testing.T) {
	token := os.Getenv("TRUTHLOCKS_QA_TOKEN")
	if token == "" {
		t.Skip("explicit QA session not supplied")
	}
	c := NewBearerClient("https://api.truthlocks.com/v1", token, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	existing, e := c.GetReceipt(ctx, "maip-receipt:01m4nx75rxvg56e890anryj6wn")
	if e != nil || existing.AgentID == "" {
		t.Fatal("existing QA receipt lookup failed")
	}
	label := "Truthlocks Kafka connector QA " + time.Now().UTC().Format(time.RFC3339Nano)
	p := receipt.DataFlowReceipt{ConsumerAgent: existing.AgentID, MessageID: label, Topic: "truthlocks-synthetic-qa", PayloadHash: receipt.HashPayload([]byte(label)), Timestamp: time.Now().UTC()}
	created, e := c.CreateReceipt(ctx, p)
	if e != nil {
		t.Fatalf("synthetic QA mint failed: %v", e)
	}
	loaded, e := c.GetReceipt(ctx, created.ReceiptID)
	if e != nil || loaded.OutputsHash != p.PayloadHash {
		t.Fatal("stored payload fingerprint does not match")
	}
	verified, e := c.VerifyReceipt(ctx, created.ReceiptID)
	if e != nil || !verified.Valid || !verified.SignatureVerified || verified.Verdict != "VALID" {
		t.Fatal("synthetic receipt signature verification failed")
	}
	t.Logf("Hosted QA passed: receipt=%s signature_verified=true; brokerAcceptance=false", created.ReceiptID)
}
