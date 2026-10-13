# Truthlocks Kafka receipt connector

Record Kafka message fingerprints as signed MAIP data attestations. The Go HTTP
client submits hashes and event metadata to Truthlocks; it does not upload message
values or raw message keys. Producer and consumer wrappers integrate with kafka-go.

## Release status

Install the Go library from its dedicated public module:

```sh
go get github.com/truthlocks/kafka-maip-connector@v0.1.0
```

[Release v0.1.0](https://github.com/truthlocks/kafka-maip-connector/releases/tag/v0.1.0)
contains the repaired HTTP client and producer/consumer wrappers. For local changes,
use `go mod edit -replace` to point the module path at your reviewed source checkout.

Use Go 1.26.9 or later. In the module directory, run `go test -race ./...`.
Go race tests and hosted create/get/signature verification passed. An isolated Apache
Kafka 3.9.1 producer/consumer test also passed against the hosted Truthlocks QA
workspace: original bytes matched and both resulting receipt signatures verified.
These checks do not establish durable recovery after a process crash.

## Submit and verify a message fingerprint

Use an existing active MAIP agent from the same workspace and environment as your
API key. Set `MAIP_API_KEY` and `MAIP_AGENT_ID` in your process environment; never
commit credentials. This example creates one remote receipt for labeled sample data.
It does not contact a Kafka broker or prove that Kafka accepted a message.

```go
package main

import (
    "context"
    "fmt"
    "log"
    "os"
    "time"

    "github.com/segmentio/kafka-go"
    "github.com/truthlocks/kafka-maip-connector/pkg/client"
    "github.com/truthlocks/kafka-maip-connector/pkg/receipt"
)

func main() {
    ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
    defer cancel()
    api := client.NewMAIPClient("https://sandbox-api.truthlocks.com/v1",
        os.Getenv("MAIP_API_KEY"), "", nil)
    message := kafka.Message{Topic: "qa-events", Partition: 0, Offset: 42,
        Value: []byte("Truthlocks synthetic Kafka QA")}
    evidence := receipt.GenerateConsumerReceipt(message, os.Getenv("MAIP_AGENT_ID"))
    created, err := api.CreateReceipt(ctx, evidence)
    if err != nil { log.Fatal(err) }
    fmt.Println("Retain receipt:", created.ReceiptID)
    verified, err := api.VerifyReceipt(ctx, created.ReceiptID)
    if err != nil { log.Fatal(err) }
    if !verified.Valid || !verified.SignatureVerified {
        log.Fatalf("Verification failed: %s", verified.Verdict)
    }
    fmt.Println("Verified signature:", verified.ReceiptID)
}
```

For approved production credentials, explicitly use `https://api.truthlocks.com/v1`.
API keys determine tenant and environment; `MAIP_TENANT_ID` is deprecated and is
never sent as an authorization override. `NewBearerClient` is available for an
explicitly authorized bearer token. It sends only bearer authentication.

## HTTP contract

| Operation | Request | Behavior |
| --- | --- | --- |
| Create | `POST /v1/agent-receipts` | `data.attestation`, agent ID, `kafka.produce` or `kafka.consume`, payload hash and string metadata. |
| Get | `GET /v1/agent-receipts/{id}` | Retrieve the stored receipt; this alone does not verify it. |
| Verify | `GET /v1/agent-receipts/verify/{id}` | Requires `valid`, `signature_verified`, and `verdict: VALID` together. |
| Batch | Sequential create requests | Stops at the first failure; no nonexistent batch endpoint or atomicity claim. |

Receipt IDs are URL-escaped. Calls have a bounded timeout and response size, reject
redirects, and make one attempt. A failed network request may already have created
a receipt: inspect the outcome before retrying. `CreateReceiptBatch` returns both
a partial result and an error on failure. Retain `Receipts` and inspect `Created`,
`Failed`, and `Unattempted`; do not blindly replay successful items.

The current response uses `created_at` and `outputs_hash`. Compatibility fields
`IssuedAt` and `PayloadHash` are populated from them. An unsigned `VALID` response
is downgraded to `INCOMPLETE` and `Valid=false`.

## Connector settings

| Variable | Purpose / default |
| --- | --- |
| `MAIP_API_URL` | Sandbox `/v1` origin by default. |
| `MAIP_API_KEY` | Required scoped API key. |
| `MAIP_AGENT_ID` | Required existing workspace agent. |
| `KAFKA_BROKERS` | Comma-separated addresses; `localhost:9092`. |
| `KAFKA_TOPIC` | Topic for the standalone connector. |
| `KAFKA_CONSUMER_GROUP` | `maip-connector`. |
| `MAIP_AUTO_RECEIPT` | `true`; wrappers submit receipts when enabled. |
| `MAIP_RECEIPT_TYPE` | `data.attestation`; legacy `data_flow_receipt` maps to this type. |
| `MAIP_BATCH_SIZE` | Local flush size, `100`; not a server batch API. |
| `MAIP_FLUSH_INTERVAL` | Local flush interval, `5s`. |

Configure broker TLS, SASL, topic permissions and consumer ownership separately in
the kafka-go reader/writer supplied to the wrappers.

## Delivery guarantees and limitations

The wrappers buffer receipts in memory and expose `ReceiptsCreated` and
`ReceiptErrors`. They log asynchronous failures; they do not provide a durable
outbox, crash recovery, or an atomic transaction between Kafka and Truthlocks.
`ReadMessage` auto-commits Kafka consumption, and a successful producer write or
consumer commit does not guarantee that the later receipt submission succeeded.
For required audit delivery, retain a durable application outbox and use the HTTP
client with explicit outcome handling. Do not treat these wrappers as a durable
audit transport until that workflow has passed your broker acceptance tests.

Producer coordinates are caller supplied: the wrapper does not obtain authoritative
broker offsets through the writer's completion callback. Consumer coordinates come
from the message supplied to the receipt builder. Topic/partition/offset identifiers
are not globally unique across clusters, and caller metadata is not proof of broker
acceptance. Hashes confirm byte equality, not the truth of event descriptions.

## Troubleshooting

- **Module not found:** confirm the exact module path and `v0.1.0` tag, or use a local replacement.
- **401/403:** confirm API key scope, environment, active agent and account access.
- **404:** use the `/v1` API origin; legacy receipt and batch URLs are unsupported.
- **Partial batch:** preserve confirmed IDs and inspect the failed operation before retrying.
- **INCOMPLETE:** the server did not confirm a valid signature; fail closed.
- **Missing receipts after restart:** the wrapper queue is volatile; use a durable outbox.

[MAIP documentation](https://docs.truthlocks.com) ·
[Source repository](https://github.com/truthlocks/truthlock/tree/main/packages/kafka-maip-connector)

Apache License 2.0.
