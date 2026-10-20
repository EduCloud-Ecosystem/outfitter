package assessment

const (
	ManifestSchemaV2    = "educloud.benchmark-manifest/v2"
	MeasurementSchemaV2 = "educloud.measurement-receipt/v2"
	CleanupSchemaV2     = "educloud.cleanup-receipt/v2"
	SetSchemaV2         = "educloud.receipt-set/v2"
	ResultSchemaV2      = "outfitter.assessment-receipts-result/v2"
	MaxManifestV2Bytes  = 64 << 10
	// ProfilePIDsLimit is fixed by cpu-python-r-bounded/version 1. It is not
	// a selectable wire field or a claim of enforcement by this pure codec.
	ProfilePIDsLimit = 64
)

type ExecutionProfile struct {
	ID      string `json:"id"`
	Version int64  `json:"version"`
}

// ManifestV2 is intentionally separate from Manifest. Old control and legacy
// projection callers must never acquire a larger execution profile implicitly.
type ManifestV2 struct {
	Schema             string               `json:"schema"`
	ExecutionProfile   ExecutionProfile     `json:"execution_profile"`
	AssessmentID       string               `json:"assessment_id"`
	ProjectID          string               `json:"project_id"`
	Pathway            string               `json:"pathway"`
	DataClassification string               `json:"data_classification"`
	Workload           Workload             `json:"workload"`
	Hardware           Hardware             `json:"hardware"`
	Measurement        Method               `json:"measurement"`
	Design             Design               `json:"design"`
	Quality            QualityPlan          `json:"quality"`
	Ceilings           CeilingsV2           `json:"ceilings"`
	ExpiresAt          string               `json:"expires_at"`
	Reservations       []ReservationBinding `json:"reservations"`
	Instruction        *Instruction         `json:"instruction"`
}

type CeilingsV2 struct {
	MaxAttempts              int64 `json:"max_attempts"`
	CPUSeconds               int64 `json:"cpu_seconds"`
	CPUCountPerAttempt       int64 `json:"cpu_count_per_attempt"`
	MemoryBytes              int64 `json:"memory_bytes"`
	InputBytes               int64 `json:"input_bytes"`
	InputRows                int64 `json:"input_rows"`
	ScratchBytes             int64 `json:"scratch_bytes"`
	OutputBytes              int64 `json:"output_bytes"`
	TimeoutSecondsPerAttempt int64 `json:"timeout_seconds_per_attempt"`
}

// Binding is embedded only to share its field declarations, not its validator.
// The canonical v2 digest binds the complete v2 manifest, including the recipe.
type BindingV2 struct {
	Binding
	ManifestSchema   string           `json:"manifest_schema"`
	ExecutionProfile ExecutionProfile `json:"execution_profile"`
}

type MeasurementReceiptV2 struct {
	Schema                 string             `json:"schema"`
	Binding                BindingV2          `json:"binding"`
	Observation            Observation        `json:"observation"`
	Execution              Execution          `json:"execution"`
	MeasuredConsumption    map[string]Metric  `json:"measured_consumption"`
	ReservationCeiling     ReservationCeiling `json:"reservation_ceiling"`
	ConservativeAccounting Accounting         `json:"conservative_accounting"`
	ProviderReportedCharge ProviderCharge     `json:"provider_reported_charge"`
	Quality                Quality            `json:"quality"`
	CleanupReceiptID       string             `json:"cleanup_receipt_id"`
	CleanupReceiptSHA256   string             `json:"cleanup_receipt_sha256"`
}

type CleanupReceiptV2 struct {
	Schema               string          `json:"schema"`
	ReceiptID            string          `json:"receipt_id"`
	Binding              BindingV2       `json:"binding"`
	Observation          Observation     `json:"observation"`
	ObservedAt           *string         `json:"observed_at"`
	Method               CleanupMethod   `json:"method"`
	Termination          string          `json:"termination"`
	AccessRevocation     string          `json:"access_revocation"`
	ObservedScopes       []ObservedScope `json:"observed_scopes"`
	VerifierID           *string         `json:"verifier_id"`
	Outcome              string          `json:"outcome"`
	RemainingLiabilities []string        `json:"remaining_liabilities"`
	Quarantine           bool            `json:"quarantine"`
	ReuseDenied          bool            `json:"reuse_denied"`
	PriorReceiptSHA256   *string         `json:"prior_receipt_sha256"`
}

type ReceiptSetV2 struct {
	Schema      string               `json:"schema"`
	Measurement MeasurementReceiptV2 `json:"measurement"`
	Cleanup     CleanupReceiptV2     `json:"cleanup"`
}

type ResultV2 struct {
	Schema                 string       `json:"schema"`
	Validation             string       `json:"validation"`
	ManifestSHA256         string       `json:"manifest_sha256"`
	MeasurementSHA256      string       `json:"measurement_sha256"`
	CleanupSHA256          string       `json:"cleanup_sha256"`
	ExecutionAuthorized    bool         `json:"execution_authorized"`
	CleanupTrusted         bool         `json:"cleanup_trusted"`
	ActualResourceAccepted bool         `json:"actual_resource_accepted"`
	Receipts               ReceiptSetV2 `json:"receipts"`
}
