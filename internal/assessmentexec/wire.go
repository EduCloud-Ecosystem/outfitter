package assessmentexec

import "github.com/EduCloud-Ecosystem/outfitter/internal/assessment"

// Public projection types deliberately omit private filesystem/resource
// identity, argv, raw content and diagnostics. No public field grants authority.
type PhasePublic struct {
	Phase                   string  `json:"phase"`
	CreateClaimed           bool    `json:"create_claimed"`
	CreateReturned          bool    `json:"create_returned"`
	CreateAmbiguous         bool    `json:"create_ambiguous"`
	StartClaimed            bool    `json:"start_claimed"`
	StartReturned           bool    `json:"start_returned"`
	StartedObserved         bool    `json:"started_observed"`
	StoppedObserved         bool    `json:"stopped_observed"`
	RemovalObserved         bool    `json:"removal_observed"`
	ExitCode                *int64  `json:"exit_code"`
	StartedAt               *string `json:"started_at"`
	FinishedAt              *string `json:"finished_at"`
	TransportStarted        bool    `json:"transport_started"`
	TransportCloseConfirmed bool    `json:"transport_close_confirmed"`
	TransportCloseUncertain bool    `json:"transport_close_uncertain"`
}

type PhasesPublic struct {
	Workload *PhasePublic `json:"workload"`
	Checker  *PhasePublic `json:"checker"`
}

type AttemptPublic struct {
	Slot                   Slot                        `json:"slot"`
	BindingDigest          string                      `json:"binding_digest"`
	HandoffNonce           string                      `json:"handoff_nonce"`
	ClaimEpoch             string                      `json:"claim_epoch"`
	ClaimedAt              string                      `json:"claimed_at"`
	Deadline               string                      `json:"deadline"`
	Generation             int64                       `json:"generation"`
	State                  string                      `json:"state"`
	Outcome                string                      `json:"outcome"`
	DenyLatched            bool                        `json:"deny_latched"`
	FailureCode            string                      `json:"failure_code"`
	Compute                Compute                     `json:"compute"`
	AccountingUnresolved   bool                        `json:"accounting_unresolved"`
	ComputeCleanupObserved bool                        `json:"compute_cleanup_observed"`
	Phases                 PhasesPublic                `json:"phases"`
	Metrics                Measurements                `json:"metrics"`
	Quality                Quality                     `json:"quality"`
	CleanupObservations    []AttemptCleanupObservation `json:"cleanup_observations"`
	ObservationsSaturated  bool                        `json:"observations_saturated"`
	MeasurementDigest      *string                     `json:"measurement_digest"`
}

type Trust struct {
	Assurance              string `json:"assurance"`
	UploadAuthorized       bool   `json:"upload_authorized"`
	ExecutionAuthorized    bool   `json:"execution_authorized"`
	CleanupTrusted         bool   `json:"cleanup_trusted"`
	ActualResourceAccepted bool   `json:"actual_resource_accepted"`
}

type Control struct {
	Schema                     string                 `json:"schema"`
	Operation                  string                 `json:"operation"`
	Binding                    ScopeBinding           `json:"binding"`
	ExpectedGeneration         *int64                 `json:"expected_generation"`
	AdmittingOwnerEpoch        *string                `json:"admitting_owner_epoch"`
	WriterEpoch                *string                `json:"writer_epoch"`
	Manifest                   *assessment.ManifestV2 `json:"manifest"`
	DeclaredUploadCeilingBytes *int64                 `json:"declared_upload_ceiling_bytes"`
	ExpiresAt                  *string                `json:"expires_at"`
	ContentLength              *int64                 `json:"content_length"`
	Slot                       *Slot                  `json:"slot"`
	HandoffNonce               *string                `json:"handoff_nonce"`
	Reason                     *string                `json:"reason"`
}

type Status struct {
	Schema                     string             `json:"schema"`
	Operation                  string             `json:"operation"`
	Code                       string             `json:"code"`
	Binding                    ScopeBinding       `json:"binding"`
	Generation                 int64              `json:"generation"`
	State                      string             `json:"state"`
	DenyLatched                bool               `json:"deny_latched"`
	Reason                     string             `json:"reason"`
	ExpiresAt                  string             `json:"expires_at"`
	DeclaredUploadCeilingBytes int64              `json:"declared_upload_ceiling_bytes"`
	SealedLengthBytes          *int64             `json:"sealed_length_bytes"`
	WriterEpoch                string             `json:"writer_epoch"`
	WatchEpoch                 *string            `json:"watch_epoch"`
	AdmittingOwnerEpoch        string             `json:"admitting_owner_epoch"`
	ClockEpoch                 string             `json:"clock_epoch"`
	DeadlineTickMS             int64              `json:"deadline_tick_ms"`
	WriterStarted              bool               `json:"writer_started"`
	UploadComplete             bool               `json:"upload_complete"`
	ContentCloseConfirmed      bool               `json:"content_close_confirmed"`
	ContentCloseUncertain      bool               `json:"content_close_uncertain"`
	ExecutionDenyLatched       bool               `json:"execution_deny_latched"`
	Liveness                   string             `json:"liveness"`
	Observations               []ScopeObservation `json:"observations"`
	ObservationsSaturated      bool               `json:"observations_saturated"`
	Attempt                    *AttemptPublic     `json:"attempt"`
	Trust                      Trust              `json:"trust"`
}

type Ready struct {
	Schema                     string       `json:"schema"`
	Binding                    ScopeBinding `json:"binding"`
	Generation                 int64        `json:"generation"`
	ExpiresAt                  string       `json:"expires_at"`
	DeclaredUploadCeilingBytes int64        `json:"declared_upload_ceiling_bytes"`
	AdmittingOwnerEpoch        string       `json:"admitting_owner_epoch"`
	WriterEpoch                string       `json:"writer_epoch"`
	WatchEpoch                 string       `json:"watch_epoch"`
	ClockEpoch                 string       `json:"clock_epoch"`
	DeadlineTickMS             int64        `json:"deadline_tick_ms"`
	Assurance                  string       `json:"assurance"`
}

type Result struct {
	Schema                   string                   `json:"schema"`
	Operation                string                   `json:"operation"`
	Code                     string                   `json:"code"`
	Disposition              string                   `json:"disposition"`
	Binding                  ScopeBinding             `json:"binding"`
	Slot                     Slot                     `json:"slot"`
	HandoffNonce             string                   `json:"handoff_nonce"`
	BindingDigest            string                   `json:"binding_digest"`
	ScopeGeneration          int64                    `json:"scope_generation"`
	Attempt                  *AttemptPublic           `json:"attempt"`
	Compute                  Compute                  `json:"compute"`
	AccountingUnresolved     bool                     `json:"accounting_unresolved"`
	NormalizedReceipts       *assessment.ReceiptSetV2 `json:"normalized_receipts"`
	NormalizedReceiptsSHA256 *string                  `json:"normalized_receipts_sha256"`
	Trust                    Trust                    `json:"trust"`
}

type Error struct {
	Schema string `json:"schema"`
	Code   string `json:"code"`
}
