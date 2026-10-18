// Package provider defines the narrow, provider-independent local execution contract.
package provider

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

type Request struct {
	Consumer              *ConsumerBinding `json:"consumer_binding,omitempty"`
	SchemaVersion         int              `json:"schema_version"`
	RequestID             string           `json:"request_id"`
	ProjectID             string           `json:"project_id"`
	Image                 string           `json:"image"`
	Command               []string         `json:"command"`
	InputDir              string           `json:"input_dir"`
	RecipeDir             string           `json:"recipe_dir"`
	CPUs                  float64          `json:"cpus"`
	MemoryMB              int              `json:"memory_mb"`
	PIDsLimit             int              `json:"pids_limit"`
	TimeoutSeconds        int              `json:"timeout_seconds"`
	MaxOutputBytes        int              `json:"max_output_bytes"`
	Isolation             string           `json:"isolation"`
	AllowSupervisedReaper bool             `json:"allow_supervised_reaper"`
}

var identifier = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,79}$`)
var imageID = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
var imageDigest = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._:/-]*@sha256:[a-f0-9]{64}$`)

func ValidID(s string) bool { return identifier.MatchString(s) }
func (r Request) Validate() error {
	if r.SchemaVersion != 1 || !ValidID(r.RequestID) || !ValidID(r.ProjectID) {
		return errors.New("invalid schema or opaque identifier")
	}
	if len(r.Image) > 512 || (!imageID.MatchString(r.Image) && !imageDigest.MatchString(r.Image)) {
		return errors.New("image must be an immutable local sha256 ID or digest")
	}
	if r.Isolation != "reviewed-local" {
		return errors.New("unsupported isolation: only reviewed-local is declared")
	}
	if !r.AllowSupervisedReaper {
		return errors.New("Docker lacks self-expiry: explicit supervised-reaper exception required")
	}
	if math.IsNaN(r.CPUs) || math.IsInf(r.CPUs, 0) || r.CPUs < .25 || r.CPUs > 2 || r.MemoryMB < 32 || r.MemoryMB > 1024 || r.PIDsLimit < 1 || r.PIDsLimit > 128 || r.TimeoutSeconds < 1 || r.TimeoutSeconds > 300 || r.MaxOutputBytes < 1 || r.MaxOutputBytes > 1048576 {
		return errors.New("resource limits outside local admission bounds")
	}
	if len(r.Command) < 1 || len(r.Command) > 32 || strings.HasPrefix(r.Command[0], "-") {
		return errors.New("invalid command array")
	}
	total := 0
	for _, s := range r.Command {
		total += len(s)
		if strings.ContainsRune(s, 0) {
			return errors.New("NUL in command")
		}
	}
	if r.Command[0] == "" || total > 8192 {
		return errors.New("command empty or too large")
	}
	for _, p := range []string{r.InputDir, r.RecipeDir} {
		if len(p) > 4096 || !filepath.IsAbs(p) || filepath.Clean(p) != p || strings.ContainsAny(p, ",\n\r") {
			return errors.New("mount directories must be clean absolute paths without comma or newline")
		}
	}
	if e := r.validateConsumer(); e != nil {
		return e
	}
	encoded, _ := json.Marshal(r)
	if len(encoded) > 32768 {
		return errors.New("encoded request exceeds metadata capacity")
	}
	return nil
}
func (r Request) Hash() string {
	// Consumer identity binds logical bytes/configuration, never temporary host paths.
	if r.Consumer != nil {
		r.InputDir = ""
		r.RecipeDir = ""
	}
	b, _ := json.Marshal(r)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func (r Request) ReservationAmount() int64 {
	return int64(math.Ceil(r.CPUs * float64(r.TimeoutSeconds)))
}

type Compute struct {
	Reserved   int64  `json:"reserved_cpu_seconds"`
	Charged    int64  `json:"charged_cpu_seconds"`
	Provenance string `json:"provenance"`
}
type Receipt struct {
	OutputError         string     `json:"output_error,omitempty"`
	Admitted            bool       `json:"admitted"`
	ExecutionStarted    bool       `json:"execution_started"`
	SchemaVersion       int        `json:"schema_version"`
	RequestID           string     `json:"request_id"`
	ProjectID           string     `json:"project_id"`
	Status              string     `json:"status"`
	ExitCode            *int       `json:"exit_code"`
	Error               string     `json:"error"`
	Stdout              string     `json:"stdout"`
	Stderr              string     `json:"stderr"`
	CleanupVerified     bool       `json:"cleanup_verified"`
	UnresolvedLiability bool       `json:"unresolved_liability"`
	Image               string     `json:"image"`
	CreatedAt           time.Time  `json:"created_at"`
	StartedAt           *time.Time `json:"started_at"`
	FinishedAt          *time.Time `json:"finished_at"`
	Deadline            time.Time  `json:"deadline"`
	CancelRequested     bool       `json:"cancel_requested"`
	Compute             Compute    `json:"compute"`
	TeardownRisk        string     `json:"teardown_risk"`
}
type Record struct {
	OutputSHA256 string `json:"output_sha256,omitempty"`
	OutputBytes  int64  `json:"output_bytes,omitempty"`
	Receipt
	Request         Request `json:"request"`
	PayloadHash     string  `json:"payload_hash"`
	Name            string  `json:"container_name"`
	Owner           string  `json:"owner"`
	PID             int     `json:"broker_pid"`
	CreateAttempted bool    `json:"create_attempted"`
	CreateFinished  bool    `json:"create_finished"`
	CreateAmbiguous bool    `json:"create_ambiguous"`
}

func (r Record) Active() bool { return !r.CleanupVerified || r.UnresolvedLiability }

type Reservation struct {
	ID       string
	Amount   int64
	Deadline time.Time
	Owner    string
	Name     string
}

func (r Reservation) Validate(w Request) error {
	if r.ID != w.RequestID || r.Amount < w.ReservationAmount() || !time.Now().Before(r.Deadline) || r.Owner == "" || r.Name == "" {
		return errors.New("missing, insufficient or expired reservation")
	}
	return nil
}

type State struct {
	Exists    bool
	Running   bool
	ExitCode  int
	Owner     string
	RequestID string
}
type Capabilities struct {
	Isolation     []string `json:"isolation"`
	SelfExpiry    bool     `json:"self_expiry"`
	UsageSampling bool     `json:"usage_sampling"`
}
type Describer interface {
	Name() string
	Capabilities() Capabilities
	Estimate(Request) (int64, error)
}
type Provisioner interface {
	Describer
	Provision(context.Context, Request, Reservation) error
	Run(context.Context, Record, io.Writer, io.Writer) (int, error)
	Status(context.Context, Record) (State, error)
	Teardown(context.Context, Record) error
}

func CheckOwnership(r Record, s State) error {
	if s.Exists && (s.Owner != r.Owner || s.RequestID != r.RequestID) {
		return fmt.Errorf("refusing resource with mismatched ownership")
	}
	return nil
}
