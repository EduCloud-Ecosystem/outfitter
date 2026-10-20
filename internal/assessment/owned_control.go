package assessment

import (
	"encoding/json"
	"io"

	"github.com/EduCloud-Ecosystem/outfitter/internal/owneddata"
)

type ownedControlWire struct {
	Schema                     string          `json:"schema"`
	Operation                  string          `json:"operation"`
	ScopeID                    string          `json:"scope_id"`
	WaypointAuthorityID        string          `json:"waypoint_authority_id"`
	ExpectedBrokerAuthorityID  string          `json:"expected_broker_authority_id"`
	ProjectID                  string          `json:"project_id"`
	Manifest                   json.RawMessage `json:"manifest"`
	DeclaredUploadCeilingBytes int64           `json:"declared_upload_ceiling_bytes"`
	AdmittingOwnerEpoch        string          `json:"admitting_owner_epoch"`
	ExpiresAt                  string          `json:"expires_at"`
	ExpectedGeneration         int64           `json:"expected_generation"`
	WriterEpoch                string          `json:"writer_epoch"`
	ContentLength              int64           `json:"content_length"`
	Reason                     string          `json:"reason"`
}

// DecodeOwnedControl verifies a closed operation-specific schema. The operation
// was selected by fixed argv before reading any bytes; it cannot be overridden.
func DecodeOwnedControl(operation string, raw []byte) (owneddata.Control, error) {
	invalid := owneddata.ErrInvalid
	var wire ownedControlWire
	if strictJSON(raw, &wire, owneddata.MaxControlBytes) != nil || wire.Schema != owneddata.ControlSchema || wire.Operation != operation || !owneddata.ID(wire.ScopeID) || !owneddata.ID(wire.WaypointAuthorityID) || !owneddata.ID(wire.ExpectedBrokerAuthorityID) {
		return owneddata.Control{}, invalid
	}
	keys := []string{"schema", "operation", "scope_id", "waypoint_authority_id", "expected_broker_authority_id"}
	switch operation {
	case "prepare":
		keys = append(keys, "project_id", "manifest", "declared_upload_ceiling_bytes", "admitting_owner_epoch", "expires_at")
	case "ingest":
		keys = append(keys, "expected_generation", "writer_epoch", "admitting_owner_epoch", "content_length")
	case "seal":
		keys = append(keys, "expected_generation", "writer_epoch", "admitting_owner_epoch")
	case "cancel":
		keys = append(keys, "reason")
	case "status", "reconcile":
	default:
		return owneddata.Control{}, invalid
	}
	fields, e := objectFields(raw, keys, nil)
	if e != nil || len(fields) != len(keys) {
		return owneddata.Control{}, invalid
	}
	c := owneddata.Control{Operation: operation, ScopeID: wire.ScopeID, WaypointAuthorityID: wire.WaypointAuthorityID, ExpectedBrokerAuthorityID: wire.ExpectedBrokerAuthorityID, ProjectID: wire.ProjectID, DeclaredUploadCeilingBytes: wire.DeclaredUploadCeilingBytes, AdmittingOwnerEpoch: wire.AdmittingOwnerEpoch, ExpiresAt: wire.ExpiresAt, ExpectedGeneration: wire.ExpectedGeneration, WriterEpoch: wire.WriterEpoch, ContentLength: wire.ContentLength, Reason: wire.Reason}
	switch operation {
	case "prepare":
		m, e := DecodeManifest(wire.Manifest)
		expires, te := owneddata.Time(c.ExpiresAt, false)
		manifestExpiry, me := owneddata.Time(m.ExpiresAt, false)
		if e != nil || te != nil || me != nil || expires.After(manifestExpiry) || !owneddata.ID(c.ProjectID) || c.ProjectID != m.ProjectID || !owneddata.ID(c.AdmittingOwnerEpoch) || !owneddata.Uint(c.DeclaredUploadCeilingBytes) || c.DeclaredUploadCeilingBytes == 0 || c.DeclaredUploadCeilingBytes > m.Ceilings.ScratchBytes {
			return owneddata.Control{}, invalid
		}
		c.AssessmentID = m.AssessmentID
		c.ManifestExpiresAt = m.ExpiresAt
		c.ManifestScratchBytes = m.Ceilings.ScratchBytes
		c.ManifestSHA256, e = hashCanonical(m)
		if e != nil {
			return owneddata.Control{}, invalid
		}
	case "ingest", "seal":
		if !owneddata.Uint(c.ExpectedGeneration) || c.ExpectedGeneration == 0 || !owneddata.ID(c.WriterEpoch) || !owneddata.ID(c.AdmittingOwnerEpoch) || operation == "ingest" && (!owneddata.Uint(c.ContentLength) || c.ContentLength == 0) {
			return owneddata.Control{}, invalid
		}
	case "cancel":
		if !owneddata.OneOf(c.Reason, "cancelled", "access_ended", "expired", "restart", "clock_rollback", "incomplete_upload", "ipc_uncertain") {
			return owneddata.Control{}, invalid
		}
	}
	return c, nil
}

// ReadOwnedHeader consumes one LF-terminated, bounded JSON header. An ingest
// body begins immediately after LF. Each underlying read requests one byte:
// parsing may not prefetch content before the broker checks ownership/ceilings,
// current lifelines and its irreversible writer claim. Return the untouched
// underlying reader for body consumption only after those checks succeed.
func ReadOwnedHeader(operation string, in io.Reader) (owneddata.Control, io.Reader, error) {
	header := make([]byte, 0, 1024)
	var one [1]byte
	for len(header) <= owneddata.MaxControlBytes {
		if _, e := io.ReadFull(in, one[:]); e != nil {
			return owneddata.Control{}, in, owneddata.ErrInvalid
		}
		if one[0] == '\n' {
			c, e := DecodeOwnedControl(operation, header)
			return c, in, e
		}
		header = append(header, one[0])
	}
	return owneddata.Control{}, in, owneddata.ErrInvalid
}

// ReadOwnedControl reads a complete control-only request. Ingest alone uses
// ReadOwnedHeader because its bounded body shares stdin after the JSON line.
func ReadOwnedControl(operation string, in io.Reader) (owneddata.Control, error) {
	raw, e := io.ReadAll(io.LimitReader(in, owneddata.MaxControlBytes+1))
	if e != nil {
		return owneddata.Control{}, owneddata.ErrInvalid
	}
	return DecodeOwnedControl(operation, raw)
}
