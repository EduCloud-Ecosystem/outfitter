package ledger

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/EduCloud-Ecosystem/outfitter/internal/provider"
)

func serializationFixture(t *testing.T, count int) *Store {
	t.Helper()
	s, e := Open(filepath.Join(t.TempDir(), "state"), true)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.InitProject("project", int64(count*30)); e != nil {
		t.Fatal(e)
	}
	e = s.Update(func(d *Data) error {
		for i := 0; i < count; i++ {
			id := fmt.Sprintf("run%d", i)
			w := provider.Request{SchemaVersion: 1, RequestID: id, ProjectID: "project", Image: "sha256:" + strings.Repeat("a", 64), Command: []string{"true"}, InputDir: "/fixture/input", RecipeDir: "/fixture/recipe", CPUs: 1, MemoryMB: 32, PIDsLimit: 1, TimeoutSeconds: 30, MaxOutputBytes: 1048576, Isolation: "reviewed-local", AllowSupervisedReaper: true}
			now := time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC)
			h := sha256.Sum256([]byte(d.Owner + ":" + id))
			d.Runs[id] = provider.Record{Receipt: provider.Receipt{Admitted: true, SchemaVersion: 1, RequestID: id, ProjectID: w.ProjectID, Status: "reserved", Image: w.Image, CreatedAt: now, Deadline: now.Add(30 * time.Second), UnresolvedLiability: true, Compute: provider.Compute{Reserved: 30, Provenance: "reserved"}, TeardownRisk: "supervised external watchdog; no provider self-expiry"}, Request: w, PayloadHash: w.Hash(), Name: "outfitter-" + hex.EncodeToString(h[:16]), Owner: d.Owner, PID: os.Getpid()}
		}
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
	return s
}
func serializationRead(t *testing.T, path string) []byte {
	t.Helper()
	b, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	return b
}
func serializationFiles(t *testing.T, root string) map[string]string {
	t.Helper()
	out := map[string]string{}
	e := filepath.WalkDir(root, func(p string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		rel, e := filepath.Rel(root, p)
		if e != nil {
			return e
		}
		if d.IsDir() {
			out[rel] = "directory"
		} else {
			b := serializationRead(t, p)
			h := sha256.Sum256(b)
			out[rel] = hex.EncodeToString(h[:])
		}
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
	return out
}
func TestSerializationRefusalHasNoSidecarEffects(t *testing.T) {
	for _, mode := range []string{"record-serializer", "aggregate-capacity"} {
		t.Run(mode, func(t *testing.T) {
			s := serializationFixture(t, 1)
			unknown := filepath.Join(s.Dir, "unknown.bin")
			if e := os.WriteFile(unknown, []byte("unknown owner must survive"), 0600); e != nil {
				t.Fatal(e)
			}
			before := serializationFiles(t, s.Dir)
			e := s.Update(func(d *Data) error {
				r := d.Runs["run0"]
				r.Stdout = "preflight-output"
				if mode == "record-serializer" {
					bad := time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
					r.StartedAt = &bad
				} else {
					d.Version = 3
					d.NativeAccounts = map[string]NativeAccount{}
					d.NativeRuns = map[string]NativeRecord{}
					expanded := json.RawMessage(`"` + strings.Repeat("<", 16000) + `"`)
					for i := 0; i < MaxRecords-1; i++ {
						account, id := fmt.Sprintf("account%d", i), fmt.Sprintf("native%d", i)
						d.NativeAccounts[account] = NativeAccount{ID: account, Provider: "offline", Unit: "su", Scope: "fixture", Budget: 1, Policy: expanded}
						d.NativeRuns[id] = NativeRecord{RequestID: id, AccountID: account, ReservationID: strings.Repeat("a", 32), Owner: d.Owner, PayloadHash: strings.Repeat("b", 64), Reserved: 1, CreatedAt: r.CreatedAt, Deadline: r.CreatedAt.Add(time.Minute), Revision: 1, State: expanded}
					}
				}
				d.Runs["run0"] = r
				// Both fixtures pass logical validation; the refusal is actual serialization.
				if e := validate(*d); e != nil {
					t.Fatal("fixture failed preliminary validation", e)
				}
				return nil
			})
			if e == nil {
				t.Fatal("serialization refusal not reached")
			}
			if mode == "aggregate-capacity" && !strings.Contains(e.Error(), "store exceeds") {
				t.Fatal("wrong capacity refusal", e)
			}
			after := serializationFiles(t, s.Dir)
			if fmt.Sprint(before) != fmt.Sprint(after) {
				t.Fatalf("serialization refusal changed filesystem: before=%v after=%v error=%v", before, after, e)
			}
			u, e := s.Usage("project")
			if e != nil || u.Unknown != 30 || u.Reserved != 30 || u.Charged != 0 {
				t.Fatal("old accounting changed", u, e)
			}
		})
	}
}

func TestSerializationMixedDiagnosticFailuresPreserveAccounting(t *testing.T) {
	raw := []string{"alpha", "\x00<>&", "omega"}
	encoded := [][]byte{[]byte(`{"stdout":"alpha","stderr":""}`), []byte(`{"stdout":"\u0000\u003c\u003e\u0026","stderr":""}`), []byte(`{"stdout":"omega","stderr":""}`)}
	digests := make([]string, len(encoded))
	for i, b := range encoded {
		h := sha256.Sum256(b)
		digests[i] = hex.EncodeToString(h[:])
	}
	for mask := 0; mask < 8; mask++ {
		t.Run(fmt.Sprintf("failed-mask-%03b", mask), func(t *testing.T) {
			s := serializationFixture(t, 3)
			for i, digest := range digests {
				if mask&(1<<i) != 0 {
					path := filepath.Join(s.Dir, "outputs", digest+".json")
					if e := os.MkdirAll(path, 0700); e != nil {
						t.Fatal(e)
					}
					if e := os.WriteFile(filepath.Join(path, "owned-fixture"), []byte("preserve obstruction"), 0600); e != nil {
						t.Fatal(e)
					}
				}
			}
			e := s.Update(func(d *Data) error {
				for i, text := range raw {
					id := fmt.Sprintf("run%d", i)
					r := d.Runs[id]
					r.Stdout = text
					if i != 1 {
						r.Status = "succeeded"
						r.CleanupVerified = true
						r.UnresolvedLiability = false
						r.Compute.Charged = r.Compute.Reserved
						r.Compute.Provenance = "estimated_upper_bound"
					}
					d.Runs[id] = r
				}
				return nil
			})
			if e != nil {
				t.Fatal(e)
			}
			for i, digest := range digests {
				r, e := s.Record(fmt.Sprintf("run%d", i))
				if e != nil {
					t.Fatal(e)
				}
				if r.Stdout != "" || r.Stderr != "" || r.UnresolvedLiability != (i == 1) || r.Compute.Charged != map[bool]int64{true: 0, false: 30}[i == 1] {
					t.Fatal("accounting or compact fields changed", r)
				}
				path := filepath.Join(s.Dir, "outputs", digest+".json")
				if mask&(1<<i) != 0 {
					if r.OutputSHA256 != "" || r.OutputBytes != 0 || r.OutputError != "execution diagnostics could not be persisted; authority accounting remains intact" {
						t.Fatal("wrong diagnostic failure", r)
					}
					if string(serializationRead(t, filepath.Join(path, "owned-fixture"))) != "preserve obstruction" {
						t.Fatal("obstruction changed")
					}
				} else {
					if r.OutputSHA256 != digest || r.OutputBytes != int64(len(encoded[i])) || r.OutputError != "" || !bytes.Equal(serializationRead(t, path), encoded[i]) {
						t.Fatal("wrong planned output", r)
					}
				}
			}
			u, e := s.Usage("project")
			if e != nil || u.Charged != 60 || u.Reserved != 30 || u.Unknown != 30 || u.Remaining != 0 {
				t.Fatal(u, e)
			}
			entries, e := os.ReadDir(filepath.Join(s.Dir, "outputs"))
			if e != nil {
				t.Fatal(e)
			}
			if len(entries) != 3 {
				t.Fatal("unexpected sidecar or leftover temp", len(entries))
			}
		})
	}
}

func TestSerializationMaximumMixedFormsAndPrivateLimits(t *testing.T) {
	s := serializationFixture(t, 3)
	var input Data
	if e := s.View(func(d Data) error { input = d; return nil }); e != nil {
		t.Fatal(e)
	}
	expectedOutputs := [][]byte{[]byte(`{"stdout":"x","stderr":""}`), []byte(`{"stdout":"\u0000","stderr":""}`), []byte(`{"stdout":"longer","stderr":""}`)}
	for i, text := range []string{"x", "\x00", "longer"} {
		id := fmt.Sprintf("run%d", i)
		r := input.Runs[id]
		r.Stdout = text
		input.Runs[id] = r
	}
	before, _ := json.Marshal(input)
	fsBefore := serializationFiles(t, s.Dir)
	// Independently build every compact outcome before preparing the production plan.
	maximum := 0
	largestRecord := 0
	var cases []Data
	for mask := 0; mask < 8; mask++ {
		d := input
		d.Runs = map[string]provider.Record{}
		for i, out := range expectedOutputs {
			id := fmt.Sprintf("run%d", i)
			r := input.Runs[id]
			r.Stdout = ""
			r.Stderr = ""
			if mask&(1<<i) == 0 {
				h := sha256.Sum256(out)
				r.OutputSHA256 = hex.EncodeToString(h[:])
				r.OutputBytes = int64(len(out))
				r.OutputError = ""
			} else {
				r.OutputSHA256 = ""
				r.OutputBytes = 0
				r.OutputError = "execution diagnostics could not be persisted; authority accounting remains intact"
			}
			d.Runs[id] = r
			b, e := json.Marshal(r)
			if e != nil {
				t.Fatal(e)
			}
			if len(b) > largestRecord {
				largestRecord = len(b)
			}
		}
		b, e := json.Marshal(d)
		if e != nil {
			t.Fatal(e)
		}
		if len(b) > maximum {
			maximum = len(b)
		}
		cases = append(cases, d)
	}
	plan, e := prepareLedgerWrite(input, s.Dir, largestRecord, maximum)
	if e != nil {
		t.Fatal(e)
	}
	if plan.maximumBytes != maximum || len(plan.outputs) != 3 {
		t.Fatal("maximum differs from exhaustive independent oracle", plan.maximumBytes, maximum)
	}
	for _, d := range cases {
		b, e := marshalCompact(d, largestRecord, maximum)
		if e != nil || len(b) > plan.maximumBytes {
			t.Fatal("mixed form not admitted by preflight bound", len(b), e)
		}
	}
	// These limits are intentionally lowered private seams, not new runtime limits.
	if _, e = prepareLedgerWrite(input, s.Dir, largestRecord-1, maximum); e == nil {
		t.Fatal("one-byte record excess accepted")
	}
	if _, e = prepareLedgerWrite(input, s.Dir, largestRecord, maximum-1); e == nil {
		t.Fatal("one-byte ledger excess accepted")
	}
	after, _ := json.Marshal(input)
	if !bytes.Equal(before, after) || fmt.Sprint(fsBefore) != fmt.Sprint(serializationFiles(t, s.Dir)) {
		t.Fatal("pure preparation mutated caller or filesystem")
	}
}

func TestSerializationPlannedOutputMismatchBeforeEffects(t *testing.T) {
	for _, mode := range []string{"digest", "length", "content"} {
		t.Run(mode, func(t *testing.T) {
			s := serializationFixture(t, 1)
			out, e := planOutput("planned", "")
			if e != nil {
				t.Fatal(e)
			}
			switch mode {
			case "digest":
				out.digest = strings.Repeat("0", 64)
			case "length":
				out.bytes++
			case "content":
				out.stdout = "changed"
			}
			before := serializationFiles(t, s.Dir)
			if e = s.persistPlannedOutput(out); e == nil {
				t.Fatal("mismatched plan accepted")
			}
			if fmt.Sprint(before) != fmt.Sprint(serializationFiles(t, s.Dir)) {
				t.Fatal("mismatched plan caused sidecar effects")
			}
		})
	}
}

func TestSerializationNativeAndConsumerMetadataPreserved(t *testing.T) {
	s := nativeExperimentFixture(t)
	if _, e := s.InitProject("project", 30); e != nil {
		t.Fatal(e)
	}
	// Obtain a valid local record from the fixture, then bind a fixed consumer job.
	local := serializationFixture(t, 1)
	r, e := local.Record("run0")
	if e != nil {
		t.Fatal(e)
	}
	id, e := provider.ConsumerRequestID("cairn-synthetic-v1", "project", "fixture")
	if e != nil {
		t.Fatal(e)
	}
	binding := &provider.ConsumerBinding{Consumer: "cairn-synthetic-v1", JobID: "fixture", Attempt: 1, SubmissionCommit: strings.Repeat("a", 40), PolicyCommit: strings.Repeat("b", 40), PolicyManifestSHA256: strings.Repeat("c", 64), InputFiles: []provider.ContentFile{}, RecipeFiles: []provider.ContentFile{}}
	r.RequestID = id
	r.Request.RequestID = id
	r.Request.Consumer = binding
	r.Request.MaxOutputBytes = 65536
	r.PayloadHash = r.Request.Hash()
	if e = s.Update(func(d *Data) error {
		d.Version = 4
		r.Owner = d.Owner
		h := sha256.Sum256([]byte(d.Owner + ":" + id))
		r.Name = "outfitter-" + hex.EncodeToString(h[:16])
		d.Runs[id] = r
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	var before Data
	if e = s.View(func(d Data) error { before = d; return nil }); e != nil {
		t.Fatal(e)
	}
	accountBefore, _ := json.Marshal(before.NativeAccounts)
	nativeBefore, _ := json.Marshal(before.NativeRuns)
	requestBefore, _ := json.Marshal(before.Runs[id].Request)
	for _, fail := range []bool{false, true} {
		text := "consumer-success"
		if fail {
			text = "consumer-failure"
			if e := os.WriteFile(filepath.Join(s.Dir, "outputs-disabled"), []byte("fixture"), 0600); e != nil {
				t.Fatal(e)
			}
			if e := os.Rename(filepath.Join(s.Dir, "outputs"), filepath.Join(s.Dir, "retained-outputs")); e != nil {
				t.Fatal(e)
			}
			if e := os.Rename(filepath.Join(s.Dir, "outputs-disabled"), filepath.Join(s.Dir, "outputs")); e != nil {
				t.Fatal(e)
			}
		}
		if e = s.Update(func(d *Data) error { x := d.Runs[id]; x.Stdout = text; d.Runs[id] = x; return nil }); e != nil {
			t.Fatal(e)
		}
		if e = s.View(func(d Data) error {
			a, _ := json.Marshal(d.NativeAccounts)
			n, _ := json.Marshal(d.NativeRuns)
			q, _ := json.Marshal(d.Runs[id].Request)
			if d.Version != 4 || !bytes.Equal(a, accountBefore) || !bytes.Equal(n, nativeBefore) || !bytes.Equal(q, requestBefore) || d.Runs[id].PayloadHash != before.Runs[id].PayloadHash || !d.Runs[id].UnresolvedLiability || d.Runs[id].Compute.Charged != 0 {
				t.Fatal("native, consumer or accounting metadata changed")
			}
			if (d.Runs[id].OutputError != "") != fail {
				t.Fatal("diagnostic outcome mismatch")
			}
			return nil
		}); e != nil {
			t.Fatal(e)
		}
	}
	// The existing immutable native experiment guard still runs before any write.
	beforeFiles := serializationFiles(t, s.Dir)
	if e = s.Update(func(d *Data) error {
		d.NativeAccounts["account"].Experiment.Consumed = false
		delete(d.NativeRuns, "one")
		x := d.Runs[id]
		x.Stdout = "must not spill"
		d.Runs[id] = x
		return nil
	}); e == nil {
		t.Fatal("native experiment replenished")
	}
	if fmt.Sprint(beforeFiles) != fmt.Sprint(serializationFiles(t, s.Dir)) {
		t.Fatal("rejected native mutation had effects")
	}
}
