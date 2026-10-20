package assessment

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"
)

type countedOwnedInput struct {
	source   *bytes.Reader
	consumed int
}

func (r *countedOwnedInput) Read(p []byte) (int, error) {
	n, e := r.source.Read(p)
	r.consumed += n
	return n, e
}

func ownedHeader(t *testing.T) []byte {
	return encoded(t, map[string]any{"schema": "outfitter.owned-data-control/v1", "operation": "ingest", "scope_id": strings.Repeat("4", 32), "waypoint_authority_id": strings.Repeat("6", 32), "expected_broker_authority_id": strings.Repeat("5", 32), "expected_generation": 4, "writer_epoch": strings.Repeat("8", 32), "admitting_owner_epoch": strings.Repeat("7", 32), "content_length": 3})
}

func TestOwnedHeaderNeverPrefetchesPayload(t *testing.T) {
	header := ownedHeader(t)
	for _, raw := range [][]byte{header, bytes.ReplaceAll(header, []byte(`"content_length":3`), []byte(`"content_length":true`)), bytes.ReplaceAll(header, []byte(`"scope_id":"`+strings.Repeat("4", 32)+`"`), []byte(`"scope_id":"unknown"`))} {
		line := append(append([]byte{}, raw...), '\n')
		input := &countedOwnedInput{source: bytes.NewReader(append(append([]byte{}, line...), []byte("RAW_PAYLOAD_MARKER")...))}
		_, remaining, e := ReadOwnedHeader("ingest", input)
		if input.consumed != len(line) {
			t.Errorf("header parser consumed %d bytes; header ends at %d", input.consumed, len(line))
		}
		if bytes.Equal(raw, header) {
			if e != nil {
				t.Fatal(e)
			}
			payload, e := io.ReadAll(remaining)
			if e != nil || string(payload) != "RAW_PAYLOAD_MARKER" {
				t.Fatal("payload discarded", e)
			}
		} else if e == nil {
			t.Fatal("malformed header accepted")
		}
	}
}

func TestOwnedPreparePreservesManifestAndStrictEnvelope(t *testing.T) {
	raw, e := os.ReadFile("../owneddata/testdata/prepare-request.json")
	if e != nil {
		t.Fatal(e)
	}
	c, e := DecodeOwnedControl("prepare", raw)
	if e != nil {
		t.Fatal(e)
	}
	if c.ManifestSHA256 != "506a3587d73e70c1d545631ea2dd455056db92570e394230d5f56d81a7738420" || c.AssessmentID != strings.Repeat("1", 32) || c.ScopeID == c.AssessmentID || c.ExpiresAt != "2026-10-19T13:00:00Z" || c.ManifestExpiresAt != "2026-10-20T00:00:00Z" {
		t.Fatal("manifest identity/digest or earlier control expiry rewritten")
	}
	for _, change := range []string{"missing", "bool", "zero", "negative", "fraction", "too-large", "late-expiry", "fractional-expiry", "project", "unknown", "operation"} {
		t.Run(change, func(t *testing.T) {
			var d map[string]any
			_ = json.Unmarshal(raw, &d)
			switch change {
			case "missing":
				delete(d, "declared_upload_ceiling_bytes")
			case "bool":
				d["declared_upload_ceiling_bytes"] = true
			case "zero":
				d["declared_upload_ceiling_bytes"] = 0
			case "negative":
				d["declared_upload_ceiling_bytes"] = -1
			case "fraction":
				d["declared_upload_ceiling_bytes"] = 1.5
			case "too-large":
				d["declared_upload_ceiling_bytes"] = c.ManifestScratchBytes + 1
			case "late-expiry":
				d["expires_at"] = "2026-10-21T00:00:00Z"
			case "fractional-expiry":
				d["expires_at"] = "2026-10-19T13:00:00.000000Z"
			case "project":
				d["project_id"] = strings.Repeat("3", 32)
			case "unknown":
				d["verified"] = true
			case "operation":
				d["operation"] = "status"
			}
			bad, _ := json.Marshal(d)
			if _, e := DecodeOwnedControl("prepare", bad); e == nil {
				t.Fatal("invalid envelope accepted")
			}
		})
	}
}
