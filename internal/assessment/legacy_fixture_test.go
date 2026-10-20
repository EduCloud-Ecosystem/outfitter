package assessment

import (
	"encoding/json"
	"testing"

	"github.com/EduCloud-Ecosystem/outfitter/internal/provider"
)

// The external integration tests register their actual Broker-backed fixture
// during package init, before any internal codec test runs. No Broker import or
// positive fixture factory is compiled into production assessment code.
var legacyReceiptFixture func(*testing.T) provider.Receipt

func RegisterLegacyReceiptFixtureForTest(f func(*testing.T) provider.Receipt) {
	if f == nil || legacyReceiptFixture != nil {
		panic("legacy test receipt fixture requires one nonnil registration")
	}
	legacyReceiptFixture = f
}
func successfulReceipt(t *testing.T) provider.Receipt {
	t.Helper()
	if legacyReceiptFixture == nil {
		t.Fatal("external legacy Broker receipt fixture was not registered")
	}
	return legacyReceiptFixture(t)
}
func encoded(t *testing.T, value any) []byte {
	t.Helper()
	raw, e := json.Marshal(value)
	if e != nil {
		t.Fatal(e)
	}
	return raw
}
