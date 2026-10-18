package ledger

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/EduCloud-Ecosystem/outfitter/internal/provider"
	"github.com/EduCloud-Ecosystem/outfitter/internal/qualification"
)

func swCheck(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func swRead(t *testing.T, path string) []byte {
	t.Helper()
	b, e := os.ReadFile(path)
	swCheck(t, e)
	return b
}
func swEmit(t *testing.T, id, property string, values map[string]any) {
	t.Helper()
	b, e := json.Marshal(map[string]any{"id": id, "production_property": property, "observations": values})
	swCheck(t, e)
	t.Log("WITNESS " + string(b))
}
func swFixture(t *testing.T) *Store {
	t.Helper()
	s, e := Open(filepath.Join(t.TempDir(), "state"), true)
	swCheck(t, e)
	_, e = s.InitProject("project", 30)
	swCheck(t, e)
	swCheck(t, s.Update(func(d *Data) error {
		w := provider.Request{SchemaVersion: 1, RequestID: "run", ProjectID: "project", Image: "sha256:" + strings.Repeat("a", 64), Command: []string{"true"}, InputDir: "/fixture-input", RecipeDir: "/fixture-recipe", CPUs: 1, MemoryMB: 32, PIDsLimit: 1, TimeoutSeconds: 30, MaxOutputBytes: 1048576, Isolation: "reviewed-local", AllowSupervisedReaper: true}
		now := time.Now().UTC()
		h := sha256.Sum256([]byte(d.Owner + ":run"))
		d.Runs["run"] = provider.Record{Receipt: provider.Receipt{Admitted: true, SchemaVersion: 1, RequestID: "run", ProjectID: "project", Image: w.Image, Status: "reserved", CreatedAt: now, Deadline: now.Add(30 * time.Second), UnresolvedLiability: true, Compute: provider.Compute{Reserved: 30, Provenance: "reserved"}, TeardownRisk: "supervised external watchdog; no provider self-expiry"}, Request: w, PayloadHash: w.Hash(), Name: "outfitter-" + hex.EncodeToString(h[:16]), Owner: d.Owner, PID: os.Getpid()}
		return nil
	}))
	t.Cleanup(func() { qualification.Hook = nil; qualification.LowerRecordLimit = 0 })
	return s
}
func swOutput(t *testing.T, s *Store, out string, settle bool) error {
	t.Helper()
	return s.Update(func(d *Data) error {
		r := d.Runs["run"]
		r.Stdout = out
		if settle {
			r.Status = "succeeded"
			r.CleanupVerified = true
			r.UnresolvedLiability = false
			r.Compute.Charged = 30
			r.Compute.Provenance = "estimated_upper_bound"
		}
		d.Runs["run"] = r
		return nil
	})
}
func swFiles(t *testing.T, root string) map[string]string {
	t.Helper()
	files := map[string]string{}
	swCheck(t, filepath.WalkDir(root, func(p string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.IsDir() {
			return nil
		}
		b := swRead(t, p)
		h := sha256.Sum256(b)
		rel, e := filepath.Rel(root, p)
		if e != nil {
			return e
		}
		files[filepath.ToSlash(rel)] = hex.EncodeToString(h[:])
		return nil
	}))
	return files
}
func TestStorageWriterSpillBeforeRefusal(t *testing.T) {
	s := swFixture(t)
	old := swRead(t, filepath.Join(s.Dir, "ledger.json"))
	expected := map[string]string{}
	for i := 0; i < 3; i++ {
		raw := fmt.Sprintf("orphan-%d", i)
		encoded := []byte(fmt.Sprintf(`{"stdout":"orphan-%d","stderr":""}`, i))
		h := sha256.Sum256(encoded)
		expected[hex.EncodeToString(h[:])+".json"] = hex.EncodeToString(h[:])
		qualification.Hook = func(stage, path string) error {
			if stage == "sync" && strings.HasPrefix(filepath.Base(path), ".ledger-") {
				return errors.New("injected ledger file sync failure")
			}
			return nil
		}
		e := swOutput(t, s, raw, false)
		qualification.Hook = nil
		if e == nil || !strings.Contains(e.Error(), "injected") {
			t.Fatal("required fault not reached", e)
		}
		if !bytes.Equal(old, swRead(t, filepath.Join(s.Dir, "ledger.json"))) {
			t.Fatal("prior authority changed")
		}
	}
	actual := swFiles(t, filepath.Join(s.Dir, "outputs"))
	if fmt.Sprint(actual) != fmt.Sprint(expected) {
		t.Fatalf("orphan set mismatch: %v %v", actual, expected)
	}
	u, e := s.Usage("project")
	swCheck(t, e)
	if u.Reserved != 30 || u.Unknown != 30 || u.Charged != 0 {
		t.Fatal(u)
	}
	swEmit(t, "repeated-spill-before-ledger-failure", "FAIL", map[string]any{"attempts": 3, "retained_distinct_orphans": len(actual), "prior_authority_unchanged": true, "unknown_cpu_seconds": u.Unknown, "fault": "injected pre-file-sync error; no physical exhaustion"})
}
func TestStorageWriterLoweredCompactLimit(t *testing.T) {
	s := swFixture(t)
	old := swRead(t, filepath.Join(s.Dir, "ledger.json"))
	encoded := []byte(`{"stdout":"threshold","stderr":""}`)
	h := sha256.Sum256(encoded)
	qualification.LowerRecordLimit = 1
	e := swOutput(t, s, "threshold", false)
	qualification.LowerRecordLimit = 0
	if e == nil || e.Error() != "authority record exceeds reserved metadata capacity" {
		t.Fatal(e)
	}
	if !bytes.Equal(old, swRead(t, filepath.Join(s.Dir, "ledger.json"))) {
		t.Fatal("authority changed")
	}
	if !bytes.Equal(encoded, swRead(t, filepath.Join(s.Dir, "outputs", hex.EncodeToString(h[:])+".json"))) {
		t.Fatal("spill mismatch")
	}
	swEmit(t, "spill-before-lowered-compact-limit", "NOT_DEMONSTRATED_AT_NORMAL_LIMIT", map[string]any{"test_limit": 1, "production_limit": maxRecordBytes, "ordered_refusal_reproduced": true, "ordinary_cli_reachability": "NOT_DEMONSTRATED"})
}
func TestStorageWriterEncodedOutputOverlap(t *testing.T) {
	s := swFixture(t)
	raw := strings.Repeat("\x00", 1<<20)
	encoded := []byte(`{"stdout":"` + strings.Repeat(`\u0000`, 1<<20) + `","stderr":""}`)
	h := sha256.Sum256(encoded)
	name := hex.EncodeToString(h[:]) + ".json"
	swCheck(t, swOutput(t, s, raw, false))
	if !bytes.Equal(encoded, swRead(t, filepath.Join(s.Dir, "outputs", name))) {
		t.Fatal("independent encoding expectation differs")
	}
	overlap := false
	qualification.Hook = func(stage, path string) error {
		if stage == "rename" && strings.HasPrefix(filepath.Base(path), ".output-") {
			files, e := os.ReadDir(filepath.Dir(path))
			if e != nil {
				return e
			}
			if len(files) != 2 {
				return fmt.Errorf("expected old digest and new temp, got %d", len(files))
			}
			if !bytes.Equal(encoded, swRead(t, path)) || !bytes.Equal(encoded, swRead(t, filepath.Join(filepath.Dir(path), name))) {
				return errors.New("overlap content differs")
			}
			overlap = true
		}
		return nil
	}
	swCheck(t, swOutput(t, s, raw, false))
	qualification.Hook = nil
	if !overlap {
		t.Fatal("no production rename observed")
	}
	swEmit(t, "encoded-output-and-same-digest-overlap", "FAIL", map[string]any{"raw_bytes": len(raw), "encoded_bytes": len(encoded), "two_simultaneous_encoded_files": true, "observed_output_peak_bytes": 2 * len(encoded), "encoding": "Go JSON NUL escaping"})
}
func TestStorageWriterPublicationFaults(t *testing.T) {
	for _, family := range []string{"ledger", "output"} {
		for _, phase := range []string{"file-sync", "rename", "directory-sync"} {
			t.Run(family+"-"+phase, func(t *testing.T) {
				s := swFixture(t)
				old := swRead(t, filepath.Join(s.Dir, "ledger.json"))
				hit := false
				qualification.Hook = func(stage, path string) error {
					tempPrefix := ".ledger-"
					dir := s.Dir
					if family == "output" {
						tempPrefix = ".output-"
						dir = filepath.Join(s.Dir, "outputs")
					}
					match := (phase == "file-sync" && stage == "sync" && strings.HasPrefix(filepath.Base(path), tempPrefix)) || (phase == "rename" && stage == "rename" && strings.HasPrefix(filepath.Base(path), tempPrefix)) || (phase == "directory-sync" && stage == "sync" && path == dir)
					if match {
						hit = true
						return errors.New("injected publication failure")
					}
					return nil
				}
				expectedOutput := []byte(`{"stdout":"fault","stderr":""}`)
				expectedHash := sha256.Sum256(expectedOutput)
				e := swOutput(t, s, "fault", true)
				qualification.Hook = nil
				if !hit {
					t.Fatal("fault not reached")
				}
				same := bytes.Equal(old, swRead(t, filepath.Join(s.Dir, "ledger.json")))
				r, re := s.Record("run")
				swCheck(t, re)
				if family == "ledger" {
					if e == nil {
						t.Fatal("ledger error lost")
					}
					if same != (phase != "directory-sync") {
						t.Fatal("wrong publication boundary")
					}
					if phase != "directory-sync" && (!r.UnresolvedLiability || r.Compute.Charged != 0) {
						t.Fatal("prior liability changed")
					}
				} else {
					if e != nil || r.OutputError == "" || r.OutputSHA256 != "" || r.UnresolvedLiability || r.Compute.Charged != 30 {
						t.Fatalf("diagnostic failure blocked conservative accounting: %+v %v", r, e)
					}
				}
				outputs := swFiles(t, filepath.Join(s.Dir, "outputs"))
				wantOutput := family == "ledger" || phase == "directory-sync"
				if wantOutput {
					name := hex.EncodeToString(expectedHash[:]) + ".json"
					if len(outputs) != 1 || outputs[name] != hex.EncodeToString(expectedHash[:]) {
						t.Fatal("wrong retained output", outputs)
					}
				} else if len(outputs) != 0 {
					t.Fatal("unexpected retained output", outputs)
				}
				entries, scanErr := os.ReadDir(s.Dir)
				swCheck(t, scanErr)
				for _, entry := range entries {
					if strings.HasPrefix(entry.Name(), ".ledger-") {
						t.Fatal("unexpected fixture ledger temp")
					}
				}
				swEmit(t, "publication-"+family+"-"+phase, "BOUNDED_OBSERVATION", map[string]any{"retained_output_files": len(outputs), "injected": true, "prior_ledger_bytes_unchanged": same, "new_generation_visible": !same, "durability": "NOT_PROVED for post-rename directory-sync fault", "charged_cpu_seconds": r.Compute.Charged, "unknown_liability": r.UnresolvedLiability})
			})
		}
	}
}
func TestStorageWriterLegacyOpenSpills(t *testing.T) {
	s := swFixture(t)
	d, e := s.read()
	swCheck(t, e)
	d.Version = 1
	d.Authority = nil
	r := d.Runs["run"]
	r.Stdout = "legacy"
	d.Runs["run"] = r
	b, e := json.Marshal(d)
	swCheck(t, e)
	swCheck(t, os.WriteFile(filepath.Join(s.Dir, "ledger.json"), b, 0600))
	expected := []byte(`{"stdout":"legacy","stderr":""}`)
	h := sha256.Sum256(expected)
	reopened, e := Open(s.Dir, false)
	swCheck(t, e)
	migrated, e := reopened.read()
	swCheck(t, e)
	if migrated.Version != 2 || migrated.Runs["run"].OutputSHA256 != hex.EncodeToString(h[:]) || !bytes.Equal(expected, swRead(t, filepath.Join(s.Dir, "outputs", hex.EncodeToString(h[:])+".json"))) {
		t.Fatal("migration did not spill expected output")
	}
	swEmit(t, "legacy-open-is-writer", "FAIL", map[string]any{"before_version": 1, "after_version": 2, "new_output_files": 1, "unknown_cpu_seconds": Calculate(migrated, "project").Unknown})
}
func TestStorageWriterCheckpointReferencesOnly(t *testing.T) {
	s := swFixture(t)
	encoded := []byte(`{"stdout":"referenced","stderr":""}`)
	h := sha256.Sum256(encoded)
	name := "outputs/" + hex.EncodeToString(h[:]) + ".json"
	swCheck(t, swOutput(t, s, "referenced", false))
	unknown := map[string][]byte{"unknown.bin": []byte("unknown owner"), "outputs/orphan.json": []byte("orphan"), "snapshots/partial/input": []byte("partial"), "watch-run.ready": []byte("stale\n")}
	for p, b := range unknown {
		swCheck(t, os.MkdirAll(filepath.Dir(filepath.Join(s.Dir, p)), 0700))
		swCheck(t, os.WriteFile(filepath.Join(s.Dir, p), b, 0600))
	}
	before := swFiles(t, s.Dir)
	release, e := s.executionLease(true)
	swCheck(t, e)
	dest := filepath.Join(t.TempDir(), "checkpoint")
	report, e := s.Checkpoint(dest)
	release()
	swCheck(t, e)
	files := swFiles(t, dest)
	if len(files) != 3 || files[name] != hex.EncodeToString(h[:]) {
		t.Fatal("checkpoint reference set differs", files)
	}
	for _, p := range []string{"ledger.json", "manifest.json"} {
		if _, ok := files[p]; !ok {
			t.Fatal("missing checkpoint file", p)
		}
	}
	for p, b := range unknown {
		if !bytes.Equal(b, swRead(t, filepath.Join(s.Dir, p))) {
			t.Fatal("unknown source changed")
		}
		if _, ok := files[p]; ok {
			t.Fatal("unknown copied")
		}
	}
	for p, h := range before {
		if swFiles(t, s.Dir)[p] != h {
			t.Fatal("source changed", p)
		}
	}
	if report.CheckpointSHA256 == "" || len(report.Unresolved) != 1 {
		t.Fatal(report)
	}
	swEmit(t, "checkpoint-reference-only-under-exclusive-execution-lease", "FAIL", map[string]any{"checkpoint_regular_files": 3, "unknown_source_files_preserved_and_excluded": len(unknown), "execution_exclusive_did_not_fence_checkpoint": true, "whole_root_capacity": "UNKNOWN"})
	// Corrupt a referenced file with same-length bytes: integrity, not length, must reject it.
	swCheck(t, os.WriteFile(filepath.Join(s.Dir, name), bytes.Repeat([]byte("x"), len(encoded)), 0600))
	broken := filepath.Join(t.TempDir(), "checkpoint")
	_, e = s.Checkpoint(broken)
	if e == nil {
		t.Fatal("corrupt reference accepted")
	}
	if st, se := os.Stat(broken); se != nil || !st.IsDir() {
		t.Fatal("partial destination not retained")
	}
	if _, e = os.Stat(filepath.Join(broken, "manifest.json")); !os.IsNotExist(e) {
		t.Fatal("failed checkpoint sealed")
	}
	if _, e = s.Checkpoint(broken); e == nil {
		t.Fatal("partial destination reused")
	}
	swEmit(t, "checkpoint-corrupt-reference-retains-partial", "PASS", map[string]any{"same_length_corruption_refused": true, "partial_destination_retained": true, "retry_same_path_refused": true, "capacity_credit": 0})
}

func TestStorageWriterActivationPostPublicationUncertainty(t *testing.T) {
	s := swFixture(t)
	swCheck(t, swOutput(t, s, "settled", true))
	cp := filepath.Join(t.TempDir(), "checkpoint")
	c, e := s.Checkpoint(cp)
	swCheck(t, e)
	dest := filepath.Join(t.TempDir(), "restored")
	_, e = Restore(dest, cp, c.CheckpointSHA256)
	swCheck(t, e)
	dst, e := Open(dest, false)
	swCheck(t, e)
	hit := false
	qualification.Hook = func(stage, path string) error {
		if stage == "sync" && path == dst.Dir {
			hit = true
			return errors.New("injected destination post-rename directory sync failure")
		}
		return nil
	}
	_, e = dst.Activate(s.Dir, cp, c.CheckpointSHA256)
	qualification.Hook = nil
	if !hit || e == nil || !strings.Contains(e.Error(), "injected") {
		t.Fatal("destination publication fault missing", e)
	}
	source, se := s.RecoveryStatus()
	swCheck(t, se)
	destination, de := dst.RecoveryStatus()
	swCheck(t, de)
	if source.Admission != "fenced" || destination.Admission != "active" {
		t.Fatal("incorrect publication outcome", source, destination)
	}
	swCheck(t, dst.View(func(d Data) error { return RequireAdmission(d) }))
	if e = s.View(func(d Data) error { return RequireAdmission(d) }); !errors.Is(e, ErrAdmission) {
		t.Fatal("source unexpectedly admits", e)
	}
	_, e = dst.Activate(s.Dir, cp, c.CheckpointSHA256)
	swCheck(t, e)
	swEmit(t, "activation-error-can-leave-destination-active", "FAIL", map[string]any{"injected": "destination directory sync after rename", "source_admission": "fenced", "destination_admission_visible": "active", "error_does_not_prove_both_nonadmitting": true, "durability": "UNKNOWN", "both_roots_preserved": true, "retry_idempotent": true})
}
