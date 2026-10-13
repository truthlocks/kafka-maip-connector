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

// Package client provides an HTTP client for the Truthlocks MAIP receipt API.
package client

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/truthlocks/kafka-maip-connector/pkg/receipt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const maxResponseBytes = 2 << 20

type MAIPClient interface {
	CreateReceipt(context.Context, receipt.DataFlowReceipt) (*ReceiptResponse, error)
	// CreateReceiptBatch submits sequential requests and stops at the first failure.
	// The response is non-nil on partial failure and retains successful receipt IDs.
	CreateReceiptBatch(context.Context, []receipt.DataFlowReceipt) (*BatchReceiptResponse, error)
	VerifyReceipt(context.Context, string) (*VerifyResponse, error)
	GetReceipt(context.Context, string) (*ReceiptResponse, error)
}

type ReceiptResponse struct {
	AgentID     string `json:"agent_id"`
	ReceiptID   string `json:"receipt_id"`
	ReceiptType string `json:"receipt_type"`
	Status      string `json:"status"`
	CreatedAt   string `json:"created_at"`
	OutputsHash string `json:"outputs_hash"`
	Signature   string `json:"signature"`
	// Compatibility aliases populated from the current response fields.
	IssuedAt    string `json:"issued_at,omitempty"`
	PayloadHash string `json:"payload_hash,omitempty"`
}
type BatchReceiptResponse struct {
	Receipts    []ReceiptResponse `json:"receipts"`
	Created     int               `json:"created"`
	Failed      int               `json:"failed"`
	Unattempted int               `json:"unattempted"`
}
type VerifyResponse struct {
	Valid             bool   `json:"valid"`
	SignatureVerified bool   `json:"signature_verified"`
	Verdict           string `json:"verdict"`
	ReceiptID         string `json:"receipt_id"`
	Reason            string `json:"reason,omitempty"`
}
type APIError struct {
	StatusCode int    `json:"status_code"`
	Message    string `json:"message"`
	Code       string `json:"code,omitempty"`
}

func (e *APIError) Error() string {
	return fmt.Sprintf("maip api error %d: %s (code=%s)", e.StatusCode, e.Message, e.Code)
}

type httpMAIPClient struct {
	baseURL     string
	apiKey      string
	bearerToken string
	configErr   error
	httpClient  *http.Client
}

// NewMAIPClient authenticates with an API key. tenantID is retained for source
// compatibility only: authorization determines the tenant, never a caller header.
func NewMAIPClient(baseURL, apiKey, tenantID string, httpClient *http.Client) MAIPClient {
	return newClient(baseURL, apiKey, "", httpClient)
}

// NewBearerClient authenticates with an explicitly supplied authorized bearer token.
func NewBearerClient(baseURL, token string, httpClient *http.Client) MAIPClient {
	return newClient(baseURL, "", token, httpClient)
}
func newClient(baseURL, key, token string, h *http.Client) *httpMAIPClient {
	if baseURL == "" {
		baseURL = "https://sandbox-api.truthlocks.com/v1"
	}
	baseURL = strings.TrimRight(baseURL, "/")
	for _, suffix := range []string{"/v1/machine-identity", "/maip/v1"} {
		if strings.HasSuffix(baseURL, suffix) {
			baseURL = strings.TrimSuffix(baseURL, suffix) + "/v1"
		}
	}
	u, err := url.Parse(baseURL)
	if err == nil {
		if u.Path == "" {
			u.Path = "/v1"
		}
		loopback := u.Hostname() == "localhost"
		if ip := net.ParseIP(u.Hostname()); ip != nil {
			loopback = ip.IsLoopback()
		}
		if u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "/v1" || (u.Scheme != "https" && !(u.Scheme == "http" && loopback)) {
			err = fmt.Errorf("MAIP URL must be an HTTPS /v1 origin (HTTP is allowed only on loopback)")
		}
		baseURL = u.String()
	}
	if strings.TrimSpace(key+token) == "" || strings.ContainsAny(key+token, "\r\n") {
		err = fmt.Errorf("an API key or bearer token without line breaks is required")
	}
	copied := http.Client{Timeout: 30 * time.Second}
	if h != nil {
		copied = *h
		if copied.Timeout <= 0 {
			copied.Timeout = 30 * time.Second
		}
	}
	copied.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &httpMAIPClient{baseURL: baseURL, apiKey: key, bearerToken: token, configErr: err, httpClient: &copied}
}

func (c *httpMAIPClient) CreateReceipt(ctx context.Context, p receipt.DataFlowReceipt) (*ReceiptResponse, error) {
	agent, action := p.ProducerAgent, "kafka.produce"
	if p.ConsumerAgent != "" {
		if agent != "" {
			return nil, fmt.Errorf("choose exactly one producer or consumer agent")
		}
		agent = p.ConsumerAgent
		action = "kafka.consume"
	}
	hash, err := hex.DecodeString(p.PayloadHash)
	if strings.TrimSpace(agent) == "" || err != nil || len(hash) != 32 {
		return nil, fmt.Errorf("agent ID and SHA-256 payload hash are required")
	}
	metadata := map[string]string{"source": "kafka", "message_id": p.MessageID, "topic": p.Topic, "partition": strconv.Itoa(p.Partition), "offset": strconv.FormatInt(p.Offset, 10), "observed_at": p.Timestamp.UTC().Format(time.RFC3339Nano)}
	// Keys may contain personal data; only their fingerprint leaves the process.
	if p.Key != "" {
		metadata["key_sha256"] = receipt.HashPayload([]byte(p.Key))
	}
	if action == "kafka.produce" {
		metadata["coordinates_source"] = "caller_supplied"
	} else {
		metadata["coordinates_source"] = "consumer_message"
	}
	body, err := json.Marshal(map[string]interface{}{"agent_id": agent, "receipt_type": "data.attestation", "action": action, "resource": p.Topic, "outputs_hash": strings.ToLower(p.PayloadHash), "metadata": metadata})
	if err != nil {
		return nil, err
	}
	var r ReceiptResponse
	if err = c.do(ctx, http.MethodPost, "/agent-receipts", body, &r); err != nil {
		return nil, err
	}
	if err = normalizeReceipt(&r); err != nil {
		return nil, err
	}
	return &r, nil
}
func normalizeReceipt(r *ReceiptResponse) error {
	if r.ReceiptID == "" {
		return fmt.Errorf("receipt response is missing receipt_id; inspect the remote outcome before retrying")
	}
	if r.IssuedAt == "" {
		r.IssuedAt = r.CreatedAt
	}
	if r.PayloadHash == "" {
		r.PayloadHash = r.OutputsHash
	}
	return nil
}
func (c *httpMAIPClient) CreateReceiptBatch(ctx context.Context, ps []receipt.DataFlowReceipt) (*BatchReceiptResponse, error) {
	out := &BatchReceiptResponse{Receipts: []ReceiptResponse{}}
	for i, p := range ps {
		r, err := c.CreateReceipt(ctx, p)
		if err != nil {
			out.Failed = 1
			out.Unattempted = len(ps) - i - 1
			return out, fmt.Errorf("receipt %d failed after %d confirmed creations; inspect outcome before retry: %w", i, out.Created, err)
		}
		out.Receipts = append(out.Receipts, *r)
		out.Created++
	}
	return out, nil
}
func receiptPath(id string) (string, error) {
	if strings.TrimSpace(id) == "" || id == "." || id == ".." {
		return "", fmt.Errorf("receipt ID is required")
	}
	return url.PathEscape(id), nil
}
func (c *httpMAIPClient) VerifyReceipt(ctx context.Context, id string) (*VerifyResponse, error) {
	escaped, err := receiptPath(id)
	if err != nil {
		return nil, err
	}
	var r VerifyResponse
	if err = c.do(ctx, http.MethodGet, "/agent-receipts/verify/"+escaped, nil, &r); err != nil {
		return nil, err
	}
	r.Valid = r.Valid && r.SignatureVerified && r.Verdict == "VALID"
	if !r.Valid && r.Verdict == "VALID" {
		r.Verdict = "INCOMPLETE"
		r.Reason = "The response did not confirm a verified signature"
	}
	return &r, nil
}
func (c *httpMAIPClient) GetReceipt(ctx context.Context, id string) (*ReceiptResponse, error) {
	escaped, err := receiptPath(id)
	if err != nil {
		return nil, err
	}
	var r ReceiptResponse
	if err = c.do(ctx, http.MethodGet, "/agent-receipts/"+escaped, nil, &r); err != nil {
		return nil, err
	}
	if err = normalizeReceipt(&r); err != nil {
		return nil, err
	}
	return &r, nil
}

// Requests are attempted once. A network failure can occur after a remote write.
func (c *httpMAIPClient) do(ctx context.Context, method, path string, body []byte, result interface{}) error {
	if c.configErr != nil {
		return c.configErr
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	if c.bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.bearerToken)
	} else {
		req.Header.Set("X-API-Key", c.apiKey)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("MAIP request failed: %w", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return err
	}
	if len(data) > maxResponseBytes {
		return fmt.Errorf("MAIP response exceeds 2 MiB")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &APIError{StatusCode: resp.StatusCode, Message: http.StatusText(resp.StatusCode)}
	}
	if len(data) == 0 {
		return fmt.Errorf("MAIP returned an empty receipt response")
	}
	if err = json.Unmarshal(data, result); err != nil {
		return fmt.Errorf("invalid MAIP JSON response: %w", err)
	}
	return nil
}
