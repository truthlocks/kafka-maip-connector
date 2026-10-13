package client

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/truthlocks/kafka-maip-connector/pkg/receipt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func payload() receipt.DataFlowReceipt {
	return receipt.DataFlowReceipt{ProducerAgent: "qa-agent", Topic: "qa", MessageID: "qa-0-1", PayloadHash: receipt.HashPayload([]byte("sample")), Key: "private key", Timestamp: time.Now()}
}
func TestCurrentCreateContract(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/v1/agent-receipts" || r.Header.Get("X-API-Key") != "key" || r.Header.Get("X-Tenant-ID") != "" {
			t.Errorf("wrong request contract")
		}
		var b map[string]interface{}
		json.NewDecoder(r.Body).Decode(&b)
		if b["agent_id"] != "qa-agent" || b["receipt_type"] != "data.attestation" || b["outputs_hash"] != payload().PayloadHash {
			t.Errorf("wrong body: %v", b)
		}
		m := b["metadata"].(map[string]interface{})
		if m["key"] != nil || m["key_sha256"] != receipt.HashPayload([]byte("private key")) {
			t.Error("raw key leaked or hash missing")
		}
		fmt.Fprint(w, `{"receipt_id":"maip-receipt:qa","created_at":"now","outputs_hash":"hash"}`)
	}))
	defer s.Close()
	for _, suffix := range []string{"", "/v1", "/v1/", "/v1/machine-identity", "/maip/v1"} {
		r, e := NewMAIPClient(s.URL+suffix, "key", "not-trusted", nil).CreateReceipt(context.Background(), payload())
		if e != nil || r.IssuedAt != "now" || r.PayloadHash != "hash" {
			t.Fatalf("%s: %v", suffix, e)
		}
	}
}
func TestNoMutationRetryAndPartialBatch(t *testing.T) {
	var calls atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			fmt.Fprint(w, `{"receipt_id":"first"}`)
		} else {
			w.WriteHeader(503)
		}
	}))
	defer s.Close()
	r, e := NewMAIPClient(s.URL, "key", "", nil).CreateReceiptBatch(context.Background(), []receipt.DataFlowReceipt{payload(), payload(), payload()})
	if e == nil || calls.Load() != 2 || r.Created != 1 || r.Failed != 1 || r.Unattempted != 1 || r.Receipts[0].ReceiptID != "first" {
		t.Fatalf("partial result lost: %+v, %v, calls=%d", r, e, calls.Load())
	}
}
func TestVerificationRequiresSignature(t *testing.T) {
	for _, signature := range []bool{false, true} {
		t.Run(fmt.Sprint(signature), func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" || r.URL.EscapedPath() != "/v1/agent-receipts/verify/maip-receipt:qa%2Fpart" {
					t.Errorf("bad verify route %s %s", r.Method, r.URL.EscapedPath())
				}
				fmt.Fprintf(w, `{"valid":true,"verdict":"VALID","signature_verified":%t}`, signature)
			}))
			defer s.Close()
			r, e := NewMAIPClient(s.URL, "key", "", nil).VerifyReceipt(context.Background(), "maip-receipt:qa/part")
			if e != nil || r.Valid != signature {
				t.Fatalf("bad verification: %+v %v", r, e)
			}
			if !signature && r.Verdict != "INCOMPLETE" {
				t.Error("unsigned VALID claim preserved")
			}
		})
	}
}
func TestGetBearerAndRedirectProtection(t *testing.T) {
	var targetCalls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { targetCalls.Add(1) }))
	defer target.Close()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer token" || r.Header.Get("X-API-Key") != "" {
			t.Error("invalid auth headers")
		}
		http.Redirect(w, r, target.URL, 307)
	}))
	defer s.Close()
	original := &http.Client{}
	_, e := NewBearerClient(s.URL, "token", original).GetReceipt(context.Background(), "qa")
	if e == nil || targetCalls.Load() != 0 || original.CheckRedirect != nil {
		t.Fatal("redirect followed or caller client mutated")
	}
}
func TestMalformedResponsesFail(t *testing.T) {
	for _, body := range []string{"", `{}`, `<html>`, strings.Repeat("x", maxResponseBytes+1)} {
		t.Run(fmt.Sprint(len(body)), func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body) }))
			defer s.Close()
			if _, e := NewMAIPClient(s.URL, "key", "", nil).GetReceipt(context.Background(), "qa"); e == nil {
				t.Fatal("invalid response accepted")
			}
		})
	}
}
func TestValidationBeforeNetwork(t *testing.T) {
	for _, base := range []string{"http://example.com/v1", "https://user:pass@example.com/v1", "https://example.com/v1?token=abc", "https://example.com/other"} {
		if _, e := NewMAIPClient(base, "key", "", nil).GetReceipt(context.Background(), "qa"); e == nil {
			t.Fatalf("unsafe URL accepted: %s", base)
		}
	}
	var calls atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
	defer s.Close()
	c := NewMAIPClient(s.URL, "key", "", nil)
	for _, p := range []receipt.DataFlowReceipt{{}, {ProducerAgent: "agent", PayloadHash: "wrong"}, {ProducerAgent: "a", ConsumerAgent: "b", PayloadHash: payload().PayloadHash}} {
		if _, e := c.CreateReceipt(context.Background(), p); e == nil {
			t.Error("invalid request accepted")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e := c.CreateReceipt(ctx, payload()); e == nil {
		t.Error("cancellation ignored")
	}
	if calls.Load() != 0 {
		t.Error("invalid request reached network")
	}
}
