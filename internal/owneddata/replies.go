package owneddata

import (
	"bytes"
	"encoding/json"
)

// CanonicalReply emits sorted closed metadata, with exactly one trailing LF.
// It deliberately accepts only the three reply unions, never arbitrary data.
func CanonicalReply(value any) ([]byte, error) {
	var e error
	switch v := value.(type) {
	case Status:
		e = v.Validate()
	case Ready:
		e = v.Validate()
	case ErrorReply:
		if v.Schema != ErrorSchema || !OneOf(v.Code, "ASSESSMENT_INVALID", "ASSESSMENT_MISSING", "ASSESSMENT_UPLOAD_DISABLED", "ASSESSMENT_UNRESOLVED", "ASSESSMENT_DENIED", "ASSESSMENT_COLLISION", "ASSESSMENT_STALE", "ASSESSMENT_WRITER_ACTIVE") {
			e = ErrInvalid
		}
	default:
		e = ErrInvalid
	}
	if e != nil {
		return nil, ErrInvalid
	}
	raw, e := json.Marshal(value)
	if e != nil {
		return nil, ErrInvalid
	}
	var sorted any
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if d.Decode(&sorted) != nil {
		return nil, ErrInvalid
	}
	raw, e = json.Marshal(sorted)
	if e != nil || len(raw)+1 > MaxReplyBytes {
		return nil, ErrInvalid
	}
	return append(raw, '\n'), nil
}

func (r Ready) Validate() error {
	for _, id := range []string{r.ScopeID, r.AuthorityID, r.WaypointAuthorityID, r.ProjectID, r.AdmittingOwnerEpoch, r.WriterEpoch, r.WatchEpoch, r.ClockEpoch} {
		if !ID(id) {
			return ErrInvalid
		}
	}
	_, e := Time(r.ExpiresAt, false)
	if r.Schema != ReadySchema || !Hex(r.ManifestSHA256, 32) || !Uint(r.DeclaredUploadCeilingBytes) || r.DeclaredUploadCeilingBytes == 0 || !Uint(r.Generation) || r.Generation == 0 || !Uint(r.DeadlineTickMS) || r.DeadlineTickMS == 0 || e != nil {
		return ErrInvalid
	}
	return nil
}

func (s Status) Validate() error {
	for _, id := range []string{s.ScopeID, s.AuthorityID, s.WaypointAuthorityID, s.ProjectID, s.AdmittingOwnerEpoch, s.WriterEpoch} {
		if !ID(id) {
			return ErrInvalid
		}
	}
	_, e := Time(s.ExpiresAt, false)
	if s.Schema != StatusSchema || !Hex(s.ManifestSHA256, 32) || !Uint(s.DeclaredUploadCeilingBytes) || s.DeclaredUploadCeilingBytes == 0 || !Uint(s.Generation) || s.Generation == 0 || e != nil || s.WatchEpoch != nil && !ID(*s.WatchEpoch) || s.UploadAuthorized || s.ExecutionAuthorized || s.CleanupTrusted || s.ActualResourceAccepted || !OneOf(s.State, "preparing", "accepting", "sealed", "cleaning", "cleanup_unresolved") || s.Observations == nil || len(s.Observations) > MaxObservations {
		return ErrInvalid
	}
	denied := OneOf(s.State, "cleaning", "cleanup_unresolved")
	code := "OK"
	if denied {
		code = "DENIED"
	}
	if s.State == "cleanup_unresolved" {
		code = "UNRESOLVED"
	}
	if denied != s.DenyLatched || s.Code != code || !OneOf(s.TestPermission, "none", "synthetic_ingest_only") || !OneOf(s.Liveness, "not_observed", "observed_current") || s.TestPermission == "synthetic_ingest_only" && (s.Liveness != "observed_current" || s.State != "accepting" || s.WatchEpoch == nil) {
		return ErrInvalid
	}
	seen := map[string]bool{}
	for _, o := range s.Observations {
		if o.Validate() != nil || o.ScopeID != s.ScopeID || o.ScopeGeneration > s.Generation || seen[o.ObservationID] || !denied {
			return ErrInvalid
		}
		seen[o.ObservationID] = true
	}
	return nil
}
