package config

import "testing"

func TestSandboxDefaultsDoNotTrustTenantOverride(t *testing.T) {
	for _, key := range []string{"MAIP_API_URL", "MAIP_TENANT_ID", "MAIP_RECEIPT_TYPE", "MAIP_AUTO_RECEIPT", "MAIP_BATCH_SIZE", "MAIP_FLUSH_INTERVAL", "KAFKA_BROKERS"} {
		t.Setenv(key, "")
	}
	t.Setenv("MAIP_API_KEY", "test-key")
	t.Setenv("MAIP_AGENT_ID", "qa-agent")
	c, e := LoadFromEnv()
	if e != nil {
		t.Fatal(e)
	}
	if c.MAIPApiURL != "https://sandbox-api.truthlocks.com/v1" || c.ReceiptType != "data.attestation" {
		t.Fatal("incorrect defaults")
	}
	t.Setenv("MAIP_RECEIPT_TYPE", "made-up")
	if _, e = LoadFromEnv(); e == nil {
		t.Fatal("unsupported receipt type accepted")
	}
}
