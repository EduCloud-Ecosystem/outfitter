package assessment

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/EduCloud-Ecosystem/outfitter/internal/broker"
	"github.com/EduCloud-Ecosystem/outfitter/internal/consumer"
	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
	"github.com/EduCloud-Ecosystem/outfitter/internal/provider"
)

type receiptFake struct {
	createError, runError, cleanupError bool
	exitCode                            int
	creates                             int
	onProvision, onRun                  func()
}

func (*receiptFake) Name() string { return "docker" }
func (*receiptFake) Capabilities() provider.Capabilities {
	return provider.Capabilities{Isolation: []string{"reviewed-local"}}
}
func (*receiptFake) Estimate(w provider.Request) (int64, error) {
	return w.ReservationAmount(), w.Validate()
}
func (p *receiptFake) Provision(context.Context, provider.Request, provider.Reservation) error {
	p.creates++
	if p.onProvision != nil {
		p.onProvision()
	}
	if p.createError {
		return errors.New("synthetic ambiguous create")
	}
	return nil
}
func (p *receiptFake) Run(_ context.Context, _ provider.Record, out, errout io.Writer) (int, error) {
	if p.onRun != nil {
		p.onRun()
	}
	_, _ = io.WriteString(out, "SOURCE_DATA_MARKER")
	_, _ = io.WriteString(errout, "SOURCE_DIAGNOSTIC_MARKER")
	if p.runError {
		return -1, errors.New("synthetic missing observation")
	}
	return p.exitCode, nil
}
func (*receiptFake) Status(context.Context, provider.Record) (provider.State, error) {
	return provider.State{}, nil
}
func (p *receiptFake) Teardown(context.Context, provider.Record) error {
	if p.cleanupError {
		return errors.New("synthetic cleanup unavailable")
	}
	return nil
}

func fixture(t *testing.T, budget int64) (*broker.Broker, provider.Request, *receiptFake) {
	t.Helper()
	root := t.TempDir()
	state := filepath.Join(root, "state")
	s, e := ledger.Open(state, true)
	if e != nil {
		t.Fatal(e)
	}
	if _, e := s.InitProject("project-1", budget); e != nil {
		t.Fatal(e)
	}
	in, recipe := filepath.Join(root, "input"), filepath.Join(root, "recipe")
	for _, path := range []string{in, recipe} {
		if e := os.Mkdir(path, 0700); e != nil {
			t.Fatal(e)
		}
	}
	w := provider.Request{SchemaVersion: 1, RequestID: "attempt-1", ProjectID: "project-1", Image: "python@sha256:" + strings.Repeat("a", 64), Command: []string{"python", "/recipe/check.py"}, InputDir: in, RecipeDir: recipe, CPUs: 1, MemoryMB: 256, PIDsLimit: 64, TimeoutSeconds: 30, MaxOutputBytes: 65536, Isolation: "reviewed-local", AllowSupervisedReaper: true}
	p := &receiptFake{}
	b := broker.New(s, p)
	b.StartWatchdog = func(string) error { return nil }
	return b, w, p
}

func encoded(t *testing.T, value any) []byte {
	t.Helper()
	raw, e := json.Marshal(value)
	if e != nil {
		t.Fatal(e)
	}
	return raw
}

func successfulReceipt(t *testing.T) provider.Receipt {
	t.Helper()
	b, w, _ := fixture(t, 30)
	r, e := b.Run(context.Background(), w)
	if e != nil || r.Status != "succeeded" {
		t.Fatal(r, e)
	}
	return r
}

func TestExistingBrokerLifecycleReceiptsAcceptedWithoutAccountingChanges(t *testing.T) {
	for _, scenario := range []string{"succeeded", "failed", "cancelled", "reserved", "create-ambiguous", "run-unknown", "cleanup-unknown", "not-admitted", "missing-diagnostics"} {
		t.Run(scenario, func(t *testing.T) {
			budget := int64(30)
			if scenario == "not-admitted" {
				budget = 1
			}
			b, w, p := fixture(t, budget)
			var r provider.Receipt
			var e error
			switch scenario {
			case "cancelled", "reserved":
				var record provider.Record
				record, _, e = b.Reserve(w)
				r = record.Receipt
				if e == nil && scenario == "cancelled" {
					r, e = b.Cancel(w.RequestID, "synthetic cancellation")
				}
			default:
				p.createError = scenario == "create-ambiguous"
				p.runError = scenario == "run-unknown"
				p.cleanupError = scenario == "cleanup-unknown"
				if scenario == "failed" {
					p.exitCode = 1
				}
				r, e = b.Run(context.Background(), w)
				if e == nil && scenario == "missing-diagnostics" {
					if e := os.RemoveAll(filepath.Join(b.Store.Dir, "outputs")); e != nil {
						t.Fatal(e)
					}
					r, e = b.Store.Receipt(w.RequestID)
				}
			}
			if e != nil {
				t.Fatal(e)
			}
			before, e := os.ReadFile(filepath.Join(b.Store.Dir, "ledger.json"))
			if e != nil {
				t.Fatal(e)
			}
			got, e := DecodeLegacy(encoded(t, r))
			if e != nil || got.Compute != r.Compute || got.UnresolvedLiability != r.UnresolvedLiability || got.CancelRequested != r.CancelRequested || got.Status != r.Status {
				t.Fatal("legacy semantics changed", got, e)
			}
			result, e := invoke(t, projectRequest(t, r))
			if e != nil {
				t.Fatal(e)
			}
			m, c := result.Receipts.Measurement, result.Receipts.Cleanup
			if m.Execution.Status != r.Status || m.Execution.CancelRequested != r.CancelRequested || m.ConservativeAccounting.UnresolvedLiability != r.UnresolvedLiability || m.ConservativeAccounting.ChargedCPUSeconds != r.Compute.Charged || m.ReservationCeiling.CPUSeconds != r.Compute.Reserved || m.Execution.Reused != nil || m.Execution.DiagnosticsAvailable != (r.OutputError == "") {
				t.Fatal("projection changed legacy facts")
			}
			for _, metric := range m.MeasuredConsumption {
				if metric.Availability != "unsupported" || metric.Value != nil {
					t.Fatal("invented measurement")
				}
			}
			if c.Outcome != "unresolved" || c.Termination != "unknown" || c.AccessRevocation != "unknown" || !c.Quarantine || !c.ReuseDenied || result.ExecutionAuthorized || result.CleanupTrusted || result.ActualResourceAccepted {
				t.Fatal("legacy claim promoted")
			}
			output := string(encoded(t, result))
			for _, forbidden := range []string{"SOURCE_DATA_MARKER", "SOURCE_DIAGNOSTIC_MARKER", "synthetic missing observation", b.Store.Dir, "stdout", "stderr", "output_error", "teardown_risk"} {
				if strings.Contains(output, forbidden) {
					t.Fatalf("raw diagnostics retained: %s", forbidden)
				}
			}
			after, e := os.ReadFile(filepath.Join(b.Store.Dir, "ledger.json"))
			if e != nil || string(before) != string(after) {
				t.Fatal("validation changed accounting", e)
			}
		})
	}
}

func TestExistingConsumerReceiptAndDuplicateRemainOneAttempt(t *testing.T) {
	b, w, p := fixture(t, 30)
	w.RequestID, w.ProjectID = "", ""
	req := consumer.Request{SchemaVersion: 1, ProjectID: "project-1", ConsumerBinding: provider.ConsumerBinding{Consumer: "cairn-synthetic-v1", JobID: "job-1", Attempt: 1, SubmissionCommit: strings.Repeat("a", 40), PolicyCommit: strings.Repeat("b", 40), PolicyManifestSHA256: strings.Repeat("c", 64), InputFiles: []provider.ContentFile{}, RecipeFiles: []provider.ContentFile{}}, Workload: w}
	for i := 0; i < 2; i++ {
		r, e := consumer.Run(context.Background(), b, req)
		if e != nil {
			t.Fatal(e)
		}
		got, e := DecodeLegacy(encoded(t, r))
		if e != nil || got.Consumer != req.Consumer || got.Attempt != 1 || got.GradePublished || got.Reused != (i == 1) || p.creates != 1 || got.Compute.Charged != 30 {
			t.Fatal(got, e, p.creates)
		}
		result, e := invoke(t, projectRequest(t, r))
		if e != nil || result.Receipts.Measurement.Execution.Reused == nil || *result.Receipts.Measurement.Execution.Reused != (i == 1) || p.creates != 1 {
			t.Fatal("consumer replay semantics changed", e)
		}
	}
}

func TestRealCreatingRunningAndContextCancellationRemainAccepted(t *testing.T) {
	b, w, p := fixture(t, 30)
	observed := []string{}
	check := func(want string) func() {
		return func() {
			r, e := b.Store.Receipt(w.RequestID)
			if e != nil || r.Status != want {
				t.Fatalf("active receipt: %v %s", e, r.Status)
			}
			if _, e := invoke(t, projectRequest(t, r)); e != nil {
				t.Fatalf("legitimate %s refused: %v", want, e)
			}
			observed = append(observed, want)
		}
	}
	p.onProvision, p.onRun = check("creating"), check("running")
	if _, e := b.Run(context.Background(), w); e != nil || len(observed) != 2 {
		t.Fatal("active observations missing", e, observed)
	}
	b, w, _ = fixture(t, 30)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r, e := b.Run(ctx, w)
	if e != nil || r.Status != "cancelled" || r.CancelRequested || r.ExecutionStarted {
		t.Fatal("context cancellation fixture changed", r, e)
	}
	result, e := invoke(t, projectRequest(t, r))
	if e != nil || result.Receipts.Measurement.Execution.CancelRequested || result.Receipts.Measurement.Execution.Status != "cancelled" {
		t.Fatal("cancellation mislabeled as user request", e)
	}
}

func TestLegacyTypedJSONAttacksFailWithoutEchoingInput(t *testing.T) {
	raw := encoded(t, successfulReceipt(t))
	attacks := map[string][]byte{
		"duplicate":  append([]byte(`{"status":"SECRET_ATTACK",`), raw[1:]...),
		"unknown":    append([]byte(`{"secret_field":"SECRET_ATTACK",`), raw[1:]...),
		"case alias": bytes.ReplaceAll(raw, []byte(`"admitted":true`), []byte(`"Admitted":true`)),
		"trailing":   append(append([]byte{}, raw...), []byte(` {"secret":"SECRET_ATTACK"}`)...),
		"oversized":  []byte(strings.Repeat(" ", MaxLegacyBytes+1)),
	}
	for _, value := range []string{"null", "true", "1.0", "1e0", "9007199254740992", "-1", `"SECRET_ATTACK"`} {
		attacks["numeric "+value] = bytes.ReplaceAll(raw, []byte(`"schema_version":1`), []byte(`"schema_version":`+value))
	}
	for _, field := range []string{"admitted", "cleanup_verified", "unresolved_liability", "compute", "image", "created_at"} {
		var fields map[string]json.RawMessage
		if e := json.Unmarshal(raw, &fields); e != nil {
			t.Fatal(e)
		}
		delete(fields, field)
		attacks["missing "+field] = encoded(t, fields)
		fields[field] = json.RawMessage("null")
		attacks["null "+field] = encoded(t, fields)
	}
	for name, payload := range attacks {
		t.Run(name, func(t *testing.T) {
			_, e := DecodeLegacy(payload)
			if e == nil || e.Error() != "INVALID_LEGACY_RECEIPT" {
				t.Fatal("attack accepted or error echoed input", e)
			}
		})
	}
}
