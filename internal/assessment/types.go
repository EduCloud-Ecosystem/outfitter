package assessment

const (
	MeasurementSchema = "educloud.measurement-receipt/v1"
	CleanupSchema     = "educloud.cleanup-receipt/v1"
	SetSchema         = "educloud.receipt-set/v1"
	ResultSchema      = "outfitter.assessment-receipts-result/v1"
	MaxRequestBytes   = 8 << 20
	MaxReceiptBytes   = 128 << 10
	MaxOutputBytes    = 256 << 10
)

// Manifest is the sanitized Waypoint-owned manifest shape. Outfitter checks its
// structure and receipt bindings, not Waypoint catalog/membership authorization.
type Manifest struct {
	Schema             string               `json:"schema"`
	AssessmentID       string               `json:"assessment_id"`
	ProjectID          string               `json:"project_id"`
	Pathway            string               `json:"pathway"`
	DataClassification string               `json:"data_classification"`
	Workload           Workload             `json:"workload"`
	Hardware           Hardware             `json:"hardware"`
	Measurement        Method               `json:"measurement"`
	Design             Design               `json:"design"`
	Quality            QualityPlan          `json:"quality"`
	Ceilings           Ceilings             `json:"ceilings"`
	ExpiresAt          string               `json:"expires_at"`
	Reservations       []ReservationBinding `json:"reservations"`
	Instruction        *Instruction         `json:"instruction"`
}
type ReservationBinding struct {
	AttemptID     string `json:"attempt_id"`
	StageID       string `json:"stage_id"`
	SampleRole    string `json:"sample_role"`
	SampleSize    int64  `json:"sample_size"`
	Repetition    int64  `json:"repetition"`
	ReservationID string `json:"reservation_id"`
}
type Workload struct {
	RecipeID      string   `json:"recipe_id"`
	RecipeSHA256  string   `json:"recipe_sha256"`
	EnvironmentID string   `json:"environment_id"`
	Image         string   `json:"image"`
	Stages        []string `json:"stages"`
	Dependencies  []string `json:"dependencies"`
}
type Hardware struct {
	Kind         string `json:"kind"`
	Architecture string `json:"architecture"`
	CPUCount     int64  `json:"cpu_count"`
	MemoryBytes  int64  `json:"memory_bytes"`
	GPUCount     int64  `json:"gpu_count"`
}
type Method struct {
	Method  string `json:"method"`
	Version int64  `json:"version"`
}
type Design struct {
	SizeUnit      string         `json:"size_unit"`
	FittingSizes  []int64        `json:"fitting_sizes"`
	HeldOutSize   int64          `json:"held_out_size"`
	Repetitions   int64          `json:"repetitions"`
	Seeds         []int64        `json:"seeds"`
	Sampling      string         `json:"sampling"`
	Exclusions    string         `json:"exclusions"`
	Skew          string         `json:"skew"`
	SmallerDesign *SmallerDesign `json:"smaller_design"`
}
type SmallerDesign struct {
	ApprovedByUser bool   `json:"approved_by_user"`
	Reason         string `json:"reason"`
}
type QualityPlan struct {
	Check                             string `json:"check"`
	RelativeTolerancePartsPerTrillion int64  `json:"relative_tolerance_parts_per_trillion"`
	AbsoluteToleranceNanounits        int64  `json:"absolute_tolerance_nanounits"`
	MaximumFailurePartsPerMillion     int64  `json:"maximum_failure_parts_per_million"`
}
type Ceilings struct {
	MaxAttempts              int64 `json:"max_attempts"`
	CPUSeconds               int64 `json:"cpu_seconds"`
	MemoryBytes              int64 `json:"memory_bytes"`
	ScratchBytes             int64 `json:"scratch_bytes"`
	OutputBytes              int64 `json:"output_bytes"`
	TimeoutSecondsPerAttempt int64 `json:"timeout_seconds_per_attempt"`
}
type Instruction struct {
	Enrollment             int64 `json:"enrollment"`
	SimultaneousSessions   int64 `json:"simultaneous_sessions"`
	AssignmentCount        int64 `json:"assignment_count"`
	DeadlinePeakSessions   int64 `json:"deadline_peak_sessions"`
	PerStudentStorageBytes int64 `json:"per_student_storage_bytes"`
}
type Binding struct {
	ManifestSHA256    string   `json:"manifest_sha256"`
	AssessmentID      string   `json:"assessment_id"`
	ProjectID         string   `json:"project_id"`
	AttemptID         string   `json:"attempt_id"`
	StageID           string   `json:"stage_id"`
	ReservationID     string   `json:"reservation_id"`
	EnvironmentID     string   `json:"environment_id"`
	Image             string   `json:"image"`
	Hardware          Hardware `json:"hardware"`
	MeasurementMethod Method   `json:"measurement_method"`
	SampleRole        string   `json:"sample_role"`
	SampleSize        int64    `json:"sample_size"`
	Repetition        int64    `json:"repetition"`
}
type Observation struct {
	SourceKind            string `json:"source_kind"`
	Authenticated         bool   `json:"authenticated"`
	IndependentlyVerified bool   `json:"independently_verified"`
}
type Metric struct {
	Availability string `json:"availability"`
	Value        *int64 `json:"value"`
	Unit         string `json:"unit"`
	Scope        string `json:"scope"`
	Source       string `json:"source"`
}
type Execution struct {
	Status               string `json:"status"`
	Admitted             bool   `json:"admitted"`
	ExecutionStarted     bool   `json:"execution_started"`
	CancelRequested      bool   `json:"cancel_requested"`
	ExitCode             *int   `json:"exit_code"`
	FailureCategory      string `json:"failure_category"`
	DiagnosticsAvailable bool   `json:"diagnostics_available"`
	Reused               *bool  `json:"reused"`
}
type ReservationCeiling struct {
	CPUSeconds int64  `json:"cpu_seconds"`
	Source     string `json:"source"`
}
type Accounting struct {
	ChargedCPUSeconds   int64  `json:"charged_cpu_seconds"`
	Provenance          string `json:"provenance"`
	UnresolvedLiability bool   `json:"unresolved_liability"`
}
type ProviderCharge struct {
	Availability string `json:"availability"`
	Value        *int64 `json:"value"`
	Unit         string `json:"unit"`
	Source       string `json:"source"`
}
type Quality struct {
	Status string `json:"status"`
	Source string `json:"source"`
}
type MeasurementReceipt struct {
	Schema                 string             `json:"schema"`
	Binding                Binding            `json:"binding"`
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
type CleanupMethod struct {
	Name    string `json:"name"`
	Version int64  `json:"version"`
}
type ObservedScope struct {
	Kind     string `json:"kind"`
	ScopeID  string `json:"scope_id"`
	Deletion string `json:"deletion"`
}
type CleanupReceipt struct {
	Schema               string          `json:"schema"`
	ReceiptID            string          `json:"receipt_id"`
	Binding              Binding         `json:"binding"`
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
type ReceiptSet struct {
	Schema      string             `json:"schema"`
	Measurement MeasurementReceipt `json:"measurement"`
	Cleanup     CleanupReceipt     `json:"cleanup"`
}
type Result struct {
	Schema                 string     `json:"schema"`
	Validation             string     `json:"validation"`
	ManifestSHA256         string     `json:"manifest_sha256"`
	MeasurementSHA256      string     `json:"measurement_sha256"`
	CleanupSHA256          string     `json:"cleanup_sha256"`
	ExecutionAuthorized    bool       `json:"execution_authorized"`
	CleanupTrusted         bool       `json:"cleanup_trusted"`
	ActualResourceAccepted bool       `json:"actual_resource_accepted"`
	Receipts               ReceiptSet `json:"receipts"`
}
