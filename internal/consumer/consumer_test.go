package consumer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/EduCloud-Ecosystem/outfitter/internal/broker"
	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
	"github.com/EduCloud-Ecosystem/outfitter/internal/provider"
)

type mockDocker struct {
	mu        sync.Mutex
	creates   int
	ambiguous bool
	started   chan struct{}
	release   chan struct{}
}

func (*mockDocker) Name() string { return "docker" }
func (*mockDocker) Capabilities() provider.Capabilities {
	return provider.Capabilities{Isolation: []string{"reviewed-local"}}
}
func (*mockDocker) Estimate(w provider.Request) (int64, error) {
	return w.ReservationAmount(), w.Validate()
}
func (m *mockDocker) Provision(_ context.Context, w provider.Request, r provider.Reservation) error {
	if e := r.Validate(w); e != nil {
		return e
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.creates++
	if m.ambiguous {
		return errors.New("lost response")
	}
	return nil
}
func (m *mockDocker) Run(_ context.Context, _ provider.Record, out, _ io.Writer) (int, error) {
	if m.started != nil {
		close(m.started)
		<-m.release
	}
	_, e := io.WriteString(out, "{\"answer\":42}\n")
	return 0, e
}
func (*mockDocker) Status(context.Context, provider.Record) (provider.State, error) {
	return provider.State{}, nil
}
func (*mockDocker) Teardown(context.Context, provider.Record) error { return nil }
func digest(s string) string                                        { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }
func setup(t *testing.T, budget int64) (*broker.Broker, Request, *mockDocker) {
	t.Helper()
	root := t.TempDir()
	s, e := ledger.Open(filepath.Join(root, "state"), true)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.InitProject("project", budget); e != nil {
		t.Fatal(e)
	}
	in, recipe := filepath.Join(root, "input"), filepath.Join(root, "recipe")
	for _, p := range []string{in, recipe} {
		if e = os.Mkdir(p, 0700); e != nil {
			t.Fatal(e)
		}
	}
	for p, content := range map[string]string{filepath.Join(in, "data.txt"): "42\n", filepath.Join(recipe, "check.py"): "print(42)\n"} {
		if e = os.WriteFile(p, []byte(content), 0600); e != nil {
			t.Fatal(e)
		}
	}
	req := Request{SchemaVersion: 1, ProjectID: "project", ConsumerBinding: provider.ConsumerBinding{Consumer: "cairn-synthetic-v1", JobID: "job", Attempt: 1, SubmissionCommit: strings.Repeat("a", 40), PolicyCommit: strings.Repeat("b", 40), PolicyManifestSHA256: digest("policy"), InputFiles: []provider.ContentFile{{Path: "data.txt", Size: 3, SHA256: digest("42\n")}}, RecipeFiles: []provider.ContentFile{{Path: "check.py", Size: 10, SHA256: digest("print(42)\n")}}}, Workload: provider.Request{SchemaVersion: 1, Image: "sha256:" + strings.Repeat("c", 64), Command: []string{"python", "/recipe/check.py"}, InputDir: in, RecipeDir: recipe, CPUs: 1, MemoryMB: 256, PIDsLimit: 64, TimeoutSeconds: 30, MaxOutputBytes: 4096, Isolation: "reviewed-local", AllowSupervisedReaper: true}}
	m := &mockDocker{}
	b := broker.New(s, m)
	b.StartWatchdog = func(string) error { return nil }
	return b, req, m
}
func clone(t *testing.T, r Request) Request {
	t.Helper()
	raw, _ := json.Marshal(r)
	var c Request
	if e := json.Unmarshal(raw, &c); e != nil {
		t.Fatal(e)
	}
	return c
}

func TestPurePreflightAndStrictProfile(t *testing.T) {
	b, r, m := setup(t, 60)
	before, _ := os.ReadFile(filepath.Join(b.Store.Dir, "ledger.json"))
	os.RemoveAll(r.Workload.InputDir)
	os.RemoveAll(r.Workload.RecipeDir)
	p, e := Check(r)
	if e != nil || p.ReservedCPUSeconds != 30 || p.GradePublished || m.creates != 0 {
		t.Fatal(p, e)
	}
	after, _ := os.ReadFile(filepath.Join(b.Store.Dir, "ledger.json"))
	if string(before) != string(after) {
		t.Fatal("preflight mutated authority")
	}
	cases := map[string]func(*Request){
		"consumer": func(r *Request) { r.Consumer = "production" }, "attempt": func(r *Request) { r.Attempt = 2 },
		"identity": func(r *Request) { r.Workload.RequestID = "override" }, "commit": func(r *Request) { r.SubmissionCommit = "main" },
		"policy": func(r *Request) { r.PolicyManifestSHA256 = "" }, "mutable-image": func(r *Request) { r.Workload.Image = "python:latest" },
		"native": func(r *Request) { r.Workload.Isolation = "native" }, "ack": func(r *Request) { r.Workload.AllowSupervisedReaper = false },
		"time": func(r *Request) { r.Workload.TimeoutSeconds = 31 }, "cpu": func(r *Request) { r.Workload.CPUs = 2 },
		"memory": func(r *Request) { r.Workload.MemoryMB = 512 }, "pids": func(r *Request) { r.Workload.PIDsLimit = 128 },
		"missing-manifest": func(r *Request) { r.InputFiles = nil }, "traversal": func(r *Request) { r.InputFiles[0].Path = "../x" },
		"duplicate": func(r *Request) { r.InputFiles = append(r.InputFiles, r.InputFiles[0]) }, "size": func(r *Request) { r.InputFiles[0].Size = 17 << 20 },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			c := clone(t, r)
			change(&c)
			if _, e := Check(c); e == nil {
				t.Fatal("unsupported request accepted")
			}
		})
	}
	raw, _ := json.Marshal(r)
	for _, bad := range []string{strings.Replace(string(raw), `"workload":{`, `"workload":{"network":true,`, 1), string(raw) + "{}", strings.Repeat(" ", 65537)} {
		if _, e := Decode(strings.NewReader(bad)); e == nil {
			t.Fatal("invalid JSON accepted")
		}
	}
}
func TestStableBindingReplayAndConflicts(t *testing.T) {
	b, r, m := setup(t, 90)
	first, e := Run(context.Background(), b, r)
	if e != nil || first.Reused || first.GradePublished || first.ReconciliationRequired || first.Status != "succeeded" {
		t.Fatal(first, e)
	}
	relocated := clone(t, r)
	relocated.Workload.InputDir = "/missing/input"
	relocated.Workload.RecipeDir = "/missing/recipe"
	replay, e := Run(context.Background(), b, relocated)
	if e != nil || !replay.Reused || replay.BindingSHA256 != first.BindingSHA256 || replay.Compute != first.Compute || m.creates != 1 {
		t.Fatal(replay, e)
	}
	for name, change := range map[string]func(*Request){
		"policy-commit": func(r *Request) { r.PolicyCommit = strings.Repeat("d", 40) }, "policy-manifest": func(r *Request) { r.PolicyManifestSHA256 = digest("changed") },
		"submission": func(r *Request) { r.SubmissionCommit = strings.Repeat("e", 40) }, "same-path-content": func(r *Request) { r.InputFiles[0].SHA256 = digest("43\n") },
		"recipe": func(r *Request) { r.RecipeFiles[0].SHA256 = digest("print(43)\n") }, "limits": func(r *Request) { r.Workload.MemoryMB = 128 },
		"command": func(r *Request) { r.Workload.Command = []string{"python", "-c", "print(43)"} }, "image": func(r *Request) { r.Workload.Image = "sha256:" + strings.Repeat("f", 64) },
	} {
		t.Run(name, func(t *testing.T) {
			c := clone(t, r)
			change(&c)
			if _, e := Run(context.Background(), b, c); e == nil {
				t.Fatal("changed binding replayed")
			}
			if m.creates != 1 {
				t.Fatal("redispatched")
			}
		})
	}
	usage, _ := b.Store.Usage("project")
	if usage.Charged != 30 || usage.Remaining != 60 {
		t.Fatal(usage)
	}
}
func TestConcurrentReplayReturnsActiveReceiptWithoutDispatch(t *testing.T) {
	b, r, m := setup(t, 90)
	m.started = make(chan struct{})
	m.release = make(chan struct{})
	done := make(chan error, 1)
	go func() { _, e := Run(context.Background(), b, r); done <- e }()
	<-m.started
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			receipt, e := Run(context.Background(), b, r)
			if e != nil || !receipt.Reused || !receipt.ReconciliationRequired || receipt.Status != "running" {
				t.Error(receipt, e)
			}
		}()
	}
	wg.Wait()
	another := clone(t, r)
	another.JobID = "another"
	if _, e := Run(context.Background(), b, another); e == nil {
		t.Fatal("consumer concurrency ceiling exceeded")
	}
	close(m.release)
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	if m.creates != 1 {
		t.Fatal(m.creates)
	}
}
func TestBudgetRefusalBeforeLaunch(t *testing.T) {
	b, r, m := setup(t, 1)
	out, e := Run(context.Background(), b, r)
	if e != nil || out.Admitted || out.ExecutionStarted || out.Compute.Reserved != 0 || out.ReconciliationRequired || m.creates != 0 {
		t.Fatal(out, e)
	}
	if _, e = b.Store.Record(out.RequestID); e == nil {
		t.Fatal("refusal created a durable job")
	}
}
func TestSnapshotMustMatchBoundBytes(t *testing.T) {
	for _, mode := range []string{"replace-input", "replace-recipe", "extra", "missing", "symlink"} {
		t.Run(mode, func(t *testing.T) {
			b, r, m := setup(t, 60)
			if _, e := Check(r); e != nil {
				t.Fatal(e)
			}
			switch mode {
			case "replace-input":
				os.WriteFile(filepath.Join(r.Workload.InputDir, "data.txt"), []byte("43\n"), 0600)
			case "replace-recipe":
				os.WriteFile(filepath.Join(r.Workload.RecipeDir, "check.py"), []byte("print(43)\n"), 0600)
			case "extra":
				os.WriteFile(filepath.Join(r.Workload.InputDir, "extra"), []byte("x"), 0600)
			case "missing":
				os.Remove(filepath.Join(r.Workload.InputDir, "data.txt"))
			case "symlink":
				os.Symlink(filepath.Join(r.Workload.InputDir, "data.txt"), filepath.Join(r.Workload.InputDir, "link"))
			}
			out, e := Run(context.Background(), b, r)
			if e != nil || out.Status != "failed" || out.ExecutionStarted || m.creates != 0 {
				t.Fatal(out, e)
			}
			replay, e := Run(context.Background(), b, r)
			if e != nil || !replay.Reused || m.creates != 0 {
				t.Fatal(replay, e)
			}
		})
	}
}
func TestAmbiguousProviderAndHeldRecoveryNeverRedispatch(t *testing.T) {
	b, r, m := setup(t, 60)
	m.ambiguous = true
	original, e := Run(context.Background(), b, r)
	if e != nil || !original.ReconciliationRequired || !original.UnresolvedLiability {
		t.Fatal(original, e)
	}
	replay, e := Run(context.Background(), b, r)
	if e != nil || !replay.Reused || m.creates != 1 {
		t.Fatal(replay, e)
	}
	cp := filepath.Join(t.TempDir(), "checkpoint")
	saved, e := b.Store.Checkpoint(cp)
	if e != nil {
		t.Fatal(e)
	}
	destPath := filepath.Join(t.TempDir(), "restored")
	if _, e = ledger.Restore(destPath, cp, saved.CheckpointSHA256); e != nil {
		t.Fatal(e)
	}
	dest, e := ledger.Open(destPath, false)
	if e != nil {
		t.Fatal(e)
	}
	os.RemoveAll(r.Workload.InputDir)
	os.RemoveAll(r.Workload.RecipeDir)
	before, _ := os.ReadFile(filepath.Join(dest.Dir, "ledger.json"))
	status, e := Status(dest, r.Consumer, r.ProjectID, r.JobID)
	if e != nil || status.BindingSHA256 != original.BindingSHA256 || !status.UnresolvedLiability || !status.ReconciliationRequired {
		t.Fatal(status, e)
	}
	after, _ := os.ReadFile(filepath.Join(dest.Dir, "ledger.json"))
	if string(before) != string(after) {
		t.Fatal("status modified held authority")
	}
	held, e := Run(context.Background(), broker.New(dest, m), r)
	if !errors.Is(e, ledger.ErrAdmission) || held.Admitted || m.creates != 1 {
		t.Fatal(held, e)
	}
	if _, e = dest.Activate(b.Store.Dir, cp, saved.CheckpointSHA256); e == nil {
		t.Fatal("unknown liability allowed activation")
	}
}
func TestCompletedRecoveryFencedStatusAndMissingDiagnostics(t *testing.T) {
	b, r, m := setup(t, 90)
	original, e := Run(context.Background(), b, r)
	if e != nil {
		t.Fatal(e)
	}
	cp := filepath.Join(t.TempDir(), "checkpoint")
	saved, e := b.Store.Checkpoint(cp)
	if e != nil {
		t.Fatal(e)
	}
	destPath := filepath.Join(t.TempDir(), "restored")
	if _, e = ledger.Restore(destPath, cp, saved.CheckpointSHA256); e != nil {
		t.Fatal(e)
	}
	dest, e := ledger.Open(destPath, false)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = dest.Activate(b.Store.Dir, cp, saved.CheckpointSHA256); e != nil {
		t.Fatal(e)
	}
	for _, s := range []*ledger.Store{b.Store, dest} {
		before, _ := os.ReadFile(filepath.Join(s.Dir, "ledger.json"))
		out, e := Status(s, r.Consumer, r.ProjectID, r.JobID)
		if e != nil || out.BindingSHA256 != original.BindingSHA256 || out.Compute != original.Compute || out.GradePublished {
			t.Fatal(out, e)
		}
		after, _ := os.ReadFile(filepath.Join(s.Dir, "ledger.json"))
		if string(before) != string(after) {
			t.Fatal("status mutated authority")
		}
	}
	record, _ := dest.Record(original.RequestID)
	os.Remove(filepath.Join(dest.Dir, "outputs", record.OutputSHA256+".json"))
	status, e := Status(dest, r.Consumer, r.ProjectID, r.JobID)
	if e != nil || status.OutputError == "" || !status.ReconciliationRequired || status.Stdout != "" {
		t.Fatal(status, e)
	}
	replay, e := Run(context.Background(), broker.New(dest, m), r)
	if e != nil || !replay.Reused || replay.OutputError == "" || m.creates != 1 {
		t.Fatal(replay, e)
	}
	fenced, e := Run(context.Background(), b, r)
	if !errors.Is(e, ledger.ErrAdmission) || fenced.Admitted || m.creates != 1 {
		t.Fatal(fenced, e)
	}
}

func TestVersionFourNativeCoexistenceAndBindingIntegrity(t *testing.T) {
	b, r, _ := setup(t, 90)
	before, e := b.Store.RecoveryStatus()
	if e != nil {
		t.Fatal(e)
	}
	var version int
	b.Store.View(func(d ledger.Data) error { version = d.Version; return nil })
	if version != 2 {
		t.Fatal("fresh ordinary authority unnecessarily upgraded", version)
	}
	out, e := Run(context.Background(), b, r)
	if e != nil {
		t.Fatal(e)
	}
	native := ledger.NativeAccount{ID: "synthetic-native-account", Provider: "mock", Unit: "milli-su", Scope: "synthetic-project", Budget: 100, Policy: json.RawMessage(`{"synthetic":true}`)}
	if e = b.Store.InitNativeAccount(native); e != nil {
		t.Fatal(e)
	}
	if e = b.Store.View(func(d ledger.Data) error {
		if d.Version != 4 || d.Authority.ID != before.AuthorityID || ledger.Calculate(d, "project").Charged != 30 || ledger.NativeCalculate(d, native.ID).Remaining != 100 {
			t.Fatal("version/budget changed", d.Version)
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	if e = b.Store.Update(func(d *ledger.Data) error { d.Version = 3; return nil }); e == nil {
		t.Fatal("consumer accepted with old schema")
	}
	if e = b.Store.Update(func(d *ledger.Data) error {
		x := d.Runs[out.RequestID]
		x.Request.Consumer.PolicyCommit = strings.Repeat("d", 40)
		d.Runs[out.RequestID] = x
		return nil
	}); e == nil {
		t.Fatal("binding mutation accepted without original hash")
	}
	again, e := Status(b.Store, r.Consumer, r.ProjectID, r.JobID)
	if e != nil || again.BindingSHA256 != out.BindingSHA256 {
		t.Fatal(again, e)
	}
}
