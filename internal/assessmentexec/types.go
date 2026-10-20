// Package assessmentexec defines closed, content-free metadata for the existing
// broker's disabled assessment execution path. It constructs no authority,
// provider, storage, collector, process, grant, or production capability.
package assessmentexec

import (
	"github.com/EduCloud-Ecosystem/outfitter/internal/assessment"
	"github.com/EduCloud-Ecosystem/outfitter/internal/owneddata"
)

const (
	AuthoritySchema          = "outfitter.assessment-execution-authority/v1"
	ScopeSchema              = "outfitter.assessment-execution-scope/v1"
	AttemptSchema            = "outfitter.assessment-execution-attempt/v1"
	CapabilitySchema         = "outfitter.assessment-execution-capability/v1"
	ScopeObservationSchema   = "outfitter.assessment-execution-scope-observation/v1"
	AttemptObservationSchema = "outfitter.assessment-execution-attempt-observation/v1"
	ControlSchema            = "outfitter.assessment-execution-control/v1"
	StatusSchema             = "outfitter.assessment-execution-status/v1"
	ReadySchema              = "outfitter.assessment-execution-ready/v1"
	ResultSchema             = "outfitter.assessment-execution-result/v1"
	ErrorSchema              = "outfitter.assessment-execution-error/v1"
	MaxControlBytes          = 128 << 10
	MaxReplyBytes            = 64 << 10
	MaxUnitBytes             = 96 << 10
	MaxLedgerBytes           = 64 << 20
	MaxUnits                 = 512
	MaxSlots                 = 12
	MaxObservations          = 8
	ScopeNormalCapacity      = 48
	ScopeSafetyCapacity      = 32
	ScopeGenerationMax       = 81
	AttemptNormalCapacity    = 48
	AttemptSafetyCapacity    = 48
	AttemptGenerationMax     = 97
)

type ProfileRef = assessment.ExecutionProfile
type Slot = assessment.ReservationBinding
type FilesystemIdentity = owneddata.FilesystemIdentity

type CapabilityRef struct {
	ID      string `json:"id"`
	Version int64  `json:"version"`
	SHA256  string `json:"sha256"`
}

type CodeBinding struct {
	RecipeID                  string `json:"recipe_id"`
	CodeSHA256                string `json:"code_sha256"`
	EnvironmentID             string `json:"environment_id"`
	Image                     string `json:"image"`
	InterpreterID             string `json:"interpreter_id"`
	InterpreterVersion        string `json:"interpreter_version"`
	InterpreterIdentitySHA256 string `json:"interpreter_identity_sha256"`
}

type ImplementationBinding struct {
	ID           string `json:"id"`
	Version      int64  `json:"version"`
	SourceSHA256 string `json:"source_sha256"`
}

// CapabilityMapping is private installed-code identity. Receiving a value of
// this type does not construct the trusted capability or authorize dispatch.
type CapabilityMapping struct {
	Schema              string                `json:"schema"`
	ID                  string                `json:"id"`
	Version             int64                 `json:"version"`
	ExecutionProfile    ProfileRef            `json:"execution_profile"`
	Architecture        string                `json:"architecture"`
	Workload            CodeBinding           `json:"workload"`
	Checker             CodeBinding           `json:"checker"`
	WorkloadArgv        []string              `json:"workload_argv"`
	CheckerArgv         []string              `json:"checker_argv"`
	Provider            ImplementationBinding `json:"provider"`
	Storage             ImplementationBinding `json:"storage"`
	Collector           ImplementationBinding `json:"collector"`
	Quality             ImplementationBinding `json:"quality"`
	Limits              Limits                `json:"limits"`
	PhaseOrder          []string              `json:"phase_order"`
	MaxRunningResources int64                 `json:"max_running_resources"`
	DataClassifications []string              `json:"data_classifications"`
}

type Limits struct {
	CPUCountPerAttempt       int64 `json:"cpu_count_per_attempt"`
	TimeoutSecondsPerAttempt int64 `json:"timeout_seconds_per_attempt"`
	MemoryBytes              int64 `json:"memory_bytes"`
	PIDsLimit                int64 `json:"pids_limit"`
	InputBudgetBytes         int64 `json:"input_budget_bytes"`
	InputRows                int64 `json:"input_rows"`
	OutputBudgetBytes        int64 `json:"output_budget_bytes"`
	WorkBudgetBytes          int64 `json:"work_budget_bytes"`
	ScratchBudgetBytes       int64 `json:"scratch_budget_bytes"`
	MaxAttempts              int64 `json:"max_attempts"`
}

type ScopeBinding struct {
	BrokerAuthorityID   string        `json:"broker_authority_id"`
	WaypointAuthorityID string        `json:"waypoint_authority_id"`
	ScopeID             string        `json:"scope_id"`
	AssessmentID        string        `json:"assessment_id"`
	ProjectID           string        `json:"project_id"`
	ManifestSchema      string        `json:"manifest_schema"`
	ManifestSHA256      string        `json:"manifest_sha256"`
	ExecutionProfile    ProfileRef    `json:"execution_profile"`
	Capability          CapabilityRef `json:"capability"`
	Workload            CodeBinding   `json:"workload"`
	Checker             CodeBinding   `json:"checker"`
}

type AuthorityIdentity struct {
	WaypointAuthorityID string             `json:"waypoint_authority_id"`
	Directory           FilesystemIdentity `json:"directory"`
	JournalLease        FilesystemIdentity `json:"journal_lease"`
	ExecutionLease      FilesystemIdentity `json:"execution_lease"`
}

// Authority is one field inside ledger.Data, never a separate file or store.
type Authority struct {
	Schema   string                       `json:"schema"`
	Identity AuthorityIdentity            `json:"identity"`
	Scopes   map[string]ExecutionScope    `json:"scopes"`
	Attempts map[string]AssessmentAttempt `json:"attempts"`
}

type ExecutionFilesystemBinding struct {
	Root           FilesystemIdentity  `json:"root"`
	Scope          *FilesystemIdentity `json:"scope"`
	WriterLease    *FilesystemIdentity `json:"writer_lease"`
	WatchLease     *FilesystemIdentity `json:"watch_lease"`
	Content        *FilesystemIdentity `json:"content"`
	OwnerRead      FilesystemIdentity  `json:"owner_read"`
	WatchRead      FilesystemIdentity  `json:"watch_read"`
	WatchWrite     FilesystemIdentity  `json:"watch_write"`
	ExecutionLease *FilesystemIdentity `json:"execution_lease"`
	WorkRoot       *FilesystemIdentity `json:"work_root"`
	OutputRoot     *FilesystemIdentity `json:"output_root"`
}

type ScopeObservation struct {
	Schema                 string `json:"schema"`
	ObservationID          string `json:"observation_id"`
	ScopeID                string `json:"scope_id"`
	ScopeGeneration        int64  `json:"scope_generation"`
	ObservedAt             string `json:"observed_at"`
	Method                 string `json:"method"`
	Assurance              string `json:"assurance"`
	NewAdmission           string `json:"new_admission"`
	WritersDrained         string `json:"writers_drained"`
	ReadersDrained         string `json:"readers_drained"`
	CollectorDrained       string `json:"collector_drained"`
	ScopeAccessRevocation  string `json:"scope_access_revocation"`
	OwnedRemoval           string `json:"owned_removal"`
	Reason                 string `json:"reason"`
	CleanupTrusted         bool   `json:"cleanup_trusted"`
	ActualResourceAccepted bool   `json:"actual_resource_accepted"`
}

type ExecutionScope struct {
	Schema                         string                     `json:"schema"`
	ScopeID                        string                     `json:"scope_id"`
	AssessmentID                   string                     `json:"assessment_id"`
	ProjectID                      string                     `json:"project_id"`
	WaypointAuthorityID            string                     `json:"waypoint_authority_id"`
	BrokerAuthorityID              string                     `json:"broker_authority_id"`
	ManifestSHA256                 string                     `json:"manifest_sha256"`
	ManifestExpiresAt              string                     `json:"manifest_expires_at"`
	BoundaryID                     string                     `json:"boundary_id"`
	CreatedAt                      string                     `json:"created_at"`
	ExpiresAt                      string                     `json:"expires_at"`
	WallHighWater                  string                     `json:"wall_high_water"`
	Generation                     int64                      `json:"generation"`
	State                          string                     `json:"state"`
	DenyLatched                    bool                       `json:"deny_latched"`
	Reason                         string                     `json:"reason"`
	WriterStarted                  bool                       `json:"writer_started"`
	WriterEpoch                    string                     `json:"writer_epoch"`
	WatchEpoch                     *string                    `json:"watch_epoch"`
	WatchStarted                   bool                       `json:"watch_started"`
	UploadComplete                 bool                       `json:"upload_complete"`
	ContentCloseConfirmed          bool                       `json:"content_close_confirmed"`
	ContentCloseUncertain          bool                       `json:"content_close_uncertain"`
	Assurance                      string                     `json:"assurance"`
	Observations                   []ScopeObservation         `json:"observations"`
	ObservationsSaturated          bool                       `json:"observations_saturated"`
	DeclaredUploadCeilingBytes     int64                      `json:"declared_upload_ceiling_bytes"`
	AdmittingOwnerEpoch            string                     `json:"admitting_owner_epoch"`
	ClockEpoch                     string                     `json:"clock_epoch"`
	DeadlineTickMS                 int64                      `json:"deadline_tick_ms"`
	TickHighWaterMS                int64                      `json:"tick_high_water_ms"`
	FilesystemIdentity             ExecutionFilesystemBinding `json:"filesystem_identity"`
	Manifest                       assessment.ManifestV2      `json:"manifest"`
	ManifestSchema                 string                     `json:"manifest_schema"`
	ExecutionProfile               ProfileRef                 `json:"execution_profile"`
	Capability                     CapabilityMapping          `json:"capability"`
	CapabilitySHA256               string                     `json:"capability_sha256"`
	Limits                         Limits                     `json:"limits"`
	Slots                          []Slot                     `json:"slots"`
	SealedLengthBytes              *int64                     `json:"sealed_length_bytes"`
	ExecutionDenyLatched           bool                       `json:"execution_deny_latched"`
	NormalTransitionsUsed          int64                      `json:"normal_transitions_used"`
	SafetyTransitionsUsed          int64                      `json:"safety_transitions_used"`
	PrepareOwnershipCloseConfirmed bool                       `json:"prepare_ownership_close_confirmed"`
	WriterOwnershipCloseConfirmed  bool                       `json:"writer_ownership_close_confirmed"`
	SealStarted                    bool                       `json:"seal_started"`
	SealOwnershipCloseConfirmed    bool                       `json:"seal_ownership_close_confirmed"`
	WatchOwnershipCloseConfirmed   bool                       `json:"watch_ownership_close_confirmed"`
	CleanupOwnerEpoch              *string                    `json:"cleanup_owner_epoch"`
	CleanupOwnershipCloseConfirmed bool                       `json:"cleanup_ownership_close_confirmed"`
	LeaseCloseUncertain            bool                       `json:"lease_close_uncertain"`
}

type Compute struct {
	ReservedCPUSeconds int64  `json:"reserved_cpu_seconds"`
	ChargedCPUSeconds  int64  `json:"charged_cpu_seconds"`
	Provenance         string `json:"provenance"`
}

type DockerResourceIdentity struct {
	ContainerID      string `json:"container_id"`
	BrokerOwner      string `json:"broker_owner"`
	AttemptID        string `json:"attempt_id"`
	Phase            string `json:"phase"`
	ScopeID          string `json:"scope_id"`
	CapabilitySHA256 string `json:"capability_sha256"`
}

type PhaseRecord struct {
	Phase                   string                  `json:"phase"`
	OwnerName               string                  `json:"owner_name"`
	CreateClaimed           bool                    `json:"create_claimed"`
	CreateReturned          bool                    `json:"create_returned"`
	CreateAmbiguous         bool                    `json:"create_ambiguous"`
	StartClaimed            bool                    `json:"start_claimed"`
	StartReturned           bool                    `json:"start_returned"`
	StartedObserved         bool                    `json:"started_observed"`
	StoppedObserved         bool                    `json:"stopped_observed"`
	RemovalObserved         bool                    `json:"removal_observed"`
	ExitCode                *int64                  `json:"exit_code"`
	StartedAt               *string                 `json:"started_at"`
	FinishedAt              *string                 `json:"finished_at"`
	ResourceIdentity        *DockerResourceIdentity `json:"resource_identity"`
	TransportStarted        bool                    `json:"transport_started"`
	TransportCloseConfirmed bool                    `json:"transport_close_confirmed"`
	TransportCloseUncertain bool                    `json:"transport_close_uncertain"`
}

type PhaseRecords struct {
	Workload *PhaseRecord `json:"workload"`
	Checker  *PhaseRecord `json:"checker"`
}

type Metric = assessment.Metric

type MetricSet struct {
	CPUConsumed             Metric `json:"cpu_consumed"`
	GPUAllocatedDuration    Metric `json:"gpu_allocated_duration"`
	SetupDuration           Metric `json:"setup_duration"`
	LoadingDuration         Metric `json:"loading_duration"`
	TransferDuration        Metric `json:"transfer_duration"`
	QueueDuration           Metric `json:"queue_duration"`
	ExecutionDuration       Metric `json:"execution_duration"`
	CleanupDuration         Metric `json:"cleanup_duration"`
	SandboxOverhead         Metric `json:"sandbox_overhead"`
	InstrumentationOverhead Metric `json:"instrumentation_overhead"`
	PeakMemory              Metric `json:"peak_memory"`
	ScratchPeak             Metric `json:"scratch_peak"`
	OutputPeak              Metric `json:"output_peak"`
	IORead                  Metric `json:"io_read"`
	IOWrite                 Metric `json:"io_write"`
	GPUPeakMemory           Metric `json:"gpu_peak_memory"`
	GPUUtilization          Metric `json:"gpu_utilization"`
	CompletedUnits          Metric `json:"completed_units"`
}

type Measurements struct {
	Workload MetricSet `json:"workload"`
	Checker  MetricSet `json:"checker"`
	Attempt  MetricSet `json:"attempt"`
}

type Quality struct {
	Status  string      `json:"status"`
	Source  string      `json:"source"`
	Checker CodeBinding `json:"checker"`
}

type AttemptCleanupObservation struct {
	Schema                 string `json:"schema"`
	ObservationID          string `json:"observation_id"`
	AttemptID              string `json:"attempt_id"`
	ScopeID                string `json:"scope_id"`
	AttemptGeneration      int64  `json:"attempt_generation"`
	ObservedAt             string `json:"observed_at"`
	WorkloadRemoval        string `json:"workload_removal"`
	CheckerRemoval         string `json:"checker_removal"`
	ReadersDrained         string `json:"readers_drained"`
	CollectorDrained       string `json:"collector_drained"`
	AccountingUnresolved   bool   `json:"accounting_unresolved"`
	Reason                 string `json:"reason"`
	Assurance              string `json:"assurance"`
	CleanupTrusted         bool   `json:"cleanup_trusted"`
	ActualResourceAccepted bool   `json:"actual_resource_accepted"`
}

type AssessmentAttempt struct {
	Schema                  string                      `json:"schema"`
	Binding                 ScopeBinding                `json:"binding"`
	Slot                    Slot                        `json:"slot"`
	BindingDigest           string                      `json:"binding_digest"`
	HandoffNonce            string                      `json:"handoff_nonce"`
	ClaimEpoch              string                      `json:"claim_epoch"`
	ClaimedAt               string                      `json:"claimed_at"`
	Deadline                string                      `json:"deadline"`
	ClockEpoch              string                      `json:"clock_epoch"`
	DeadlineTickMS          int64                       `json:"deadline_tick_ms"`
	Generation              int64                       `json:"generation"`
	State                   string                      `json:"state"`
	Outcome                 string                      `json:"outcome"`
	DenyLatched             bool                        `json:"deny_latched"`
	FailureCode             string                      `json:"failure_code"`
	Compute                 Compute                     `json:"compute"`
	AccountingUnresolved    bool                        `json:"accounting_unresolved"`
	ComputeCleanupObserved  bool                        `json:"compute_cleanup_observed"`
	ReaderStarted           bool                        `json:"reader_started"`
	ReaderCloseConfirmed    bool                        `json:"reader_close_confirmed"`
	ReaderCloseUncertain    bool                        `json:"reader_close_uncertain"`
	CollectorStarted        bool                        `json:"collector_started"`
	CollectorCloseConfirmed bool                        `json:"collector_close_confirmed"`
	CollectorCloseUncertain bool                        `json:"collector_close_uncertain"`
	PhaseRecords            PhaseRecords                `json:"phase_records"`
	Metrics                 Measurements                `json:"metrics"`
	Quality                 Quality                     `json:"quality"`
	CleanupObservations     []AttemptCleanupObservation `json:"cleanup_observations"`
	ObservationsSaturated   bool                        `json:"observations_saturated"`
	MeasurementDigest       *string                     `json:"measurement_digest"`
	NormalTransitionsUsed   int64                       `json:"normal_transitions_used"`
	SafetyTransitionsUsed   int64                       `json:"safety_transitions_used"`
	WallHighWater           string                      `json:"wall_high_water"`
	TickHighWaterMS         int64                       `json:"tick_high_water_ms"`
	OwnershipCloseConfirmed bool                        `json:"ownership_close_confirmed"`
	OwnershipCloseUncertain bool                        `json:"ownership_close_uncertain"`
}
