package broker

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
	"github.com/EduCloud-Ecosystem/outfitter/internal/provider"
)

type fake struct {
	mu           sync.Mutex
	created      int
	exists       bool
	createError  bool
	cleanupError bool
	output       string
	wait         bool
}

func (f *fake) Name() string { return "fake" }
func (f *fake) Capabilities() provider.Capabilities {
	return provider.Capabilities{Isolation: []string{"reviewed-local"}}
}
func (f *fake) Estimate(r provider.Request) (int64, error) {
	return r.ReservationAmount(), r.Validate()
}
func (f *fake) Provision(_ context.Context, r provider.Request, v provider.Reservation) error {
	if e := v.Validate(r); e != nil {
		return e
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.created++
	f.exists = true
	if f.createError {
		return errors.New("lost response")
	}
	return nil
}
func (f *fake) Run(ctx context.Context, _ provider.Record, out, _ io.Writer) (int, error) {
	if f.wait {
		<-ctx.Done()
		return -1, ctx.Err()
	}
	if _, e := io.WriteString(out, f.output); e != nil {
		return -1, e
	}
	return 0, nil
}
func (f *fake) Status(_ context.Context, r provider.Record) (provider.State, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return provider.State{Exists: f.exists, Owner: r.Owner, RequestID: r.RequestID}, nil
}
func (f *fake) Teardown(_ context.Context, _ provider.Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.cleanupError {
		return errors.New("daemon unavailable")
	}
	f.exists = false
	return nil
}
func setup(t *testing.T, limit int64) (*Broker, provider.Request, *fake) {
	t.Helper()
	root := t.TempDir()
	s, e := ledger.Open(filepath.Join(root, "state"), true)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.InitProject("project", limit); e != nil {
		t.Fatal(e)
	}
	in, recipe := filepath.Join(root, "input"), filepath.Join(root, "recipe")
	os.Mkdir(in, 0700)
	os.Mkdir(recipe, 0700)
	os.WriteFile(filepath.Join(in, "data.csv"), []byte("group,value\na,1\n"), 0600)
	os.WriteFile(filepath.Join(recipe, "recipe.py"), []byte("print(1)\n"), 0600)
	r := provider.Request{SchemaVersion: 1, RequestID: "run", ProjectID: "project", Image: "sha256:" + strings.Repeat("a", 64), Command: []string{"python", "/recipe/recipe.py"}, InputDir: in, RecipeDir: recipe, CPUs: 1, MemoryMB: 256, PIDsLimit: 64, TimeoutSeconds: 30, MaxOutputBytes: 65536, Isolation: "reviewed-local", AllowSupervisedReaper: true}
	f := &fake{output: `{"n":1}`}
	b := New(s, f)
	b.StartWatchdog = func(string) error { return nil }
	return b, r, f
}
func TestAtomicConcurrentReservation(t *testing.T) {
	b, r, _ := setup(t, 30)
	var wg sync.WaitGroup
	var mu sync.Mutex
	accepted := 0
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			w := r
			w.RequestID = string(rune('a' + i))
			_, fresh, e := b.Reserve(w)
			if e == nil && fresh {
				mu.Lock()
				accepted++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	if accepted != 1 {
		t.Fatalf("accepted %d reservations against one allowance", accepted)
	}
	u, e := b.Store.Usage("project")
	if e != nil || u.Reserved != 30 || u.Remaining != 0 {
		t.Fatal(u, e)
	}
}
func TestDuplicateNeverExecutesTwice(t *testing.T) {
	b, r, f := setup(t, 100)
	first, e := b.Run(context.Background(), r)
	if e != nil || first.Status != "succeeded" || !first.CleanupVerified {
		t.Fatal(first, e)
	}
	second, e := b.Run(context.Background(), r)
	if e != nil || second.Status != first.Status || f.created != 1 {
		t.Fatal(second, e, f.created)
	}
	r.Command = append(r.Command, "changed")
	if _, e = b.Run(context.Background(), r); e == nil {
		t.Fatal("payload collision accepted")
	}
}
func TestUnknownCreateRetainsLiabilityAcrossReopen(t *testing.T) {
	b, r, f := setup(t, 30)
	f.createError = true
	out, e := b.Run(context.Background(), r)
	if e != nil || !out.UnresolvedLiability || out.CleanupVerified || out.Compute.Charged != 0 {
		t.Fatal(out, e)
	}
	s, e := ledger.Open(b.Store.Dir, false)
	if e != nil {
		t.Fatal(e)
	}
	u, e := s.Usage("project")
	if e != nil || u.Remaining != 0 || u.Unknown != 30 {
		t.Fatal(u, e)
	}
}
func TestCleanupFailureNeverClaimsSuccess(t *testing.T) {
	b, r, f := setup(t, 30)
	f.cleanupError = true
	out, e := b.Run(context.Background(), r)
	if e != nil || out.Status != "unknown" || out.CleanupVerified || !out.UnresolvedLiability {
		t.Fatal(out, e)
	}
}
func TestBoundedOutputAndTimeout(t *testing.T) {
	for _, mode := range []string{"output", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			b, r, f := setup(t, 30)
			if mode == "output" {
				r.MaxOutputBytes = 5
				f.output = strings.Repeat("x", 100)
			} else {
				r.TimeoutSeconds = 1
				f.wait = true
			}
			out, e := b.Run(context.Background(), r)
			if e != nil || out.Status != "failed" || !out.CleanupVerified || out.UnresolvedLiability {
				t.Fatal(out, e)
			}
			if len(out.Stdout)+len(out.Stderr) > r.MaxOutputBytes {
				t.Fatal("unbounded output")
			}
		})
	}
}
func TestNoWatchdogNoProviderEffect(t *testing.T) {
	b, r, f := setup(t, 30)
	b.StartWatchdog = nil
	out, e := b.Run(context.Background(), r)
	if e != nil || out.Status != "failed" || f.created != 0 {
		t.Fatal(out, e, f.created)
	}
}
func TestSymlinkInputRefused(t *testing.T) {
	b, r, f := setup(t, 30)
	os.Symlink("/etc/passwd", filepath.Join(r.InputDir, "secret"))
	out, e := b.Run(context.Background(), r)
	if e != nil || out.Status != "failed" || f.created != 0 {
		t.Fatal(out, e)
	}
}
func TestCancelUnfinishedCreationRemainsUncertain(t *testing.T) {
	b, r, _ := setup(t, 30)
	rec, _, e := b.Reserve(r)
	if e != nil {
		t.Fatal(e)
	}
	_, e = b.change(rec.RequestID, func(x *provider.Record) { x.CreateAttempted = true; x.Status = "creating" })
	if e != nil {
		t.Fatal(e)
	}
	out, e := b.Cancel(rec.RequestID, "cancel")
	if e != nil || out.CleanupVerified || !out.UnresolvedLiability {
		t.Fatal(out, e)
	}
	out, e = b.Reconcile(rec.RequestID)
	if e != nil || out.CleanupVerified || !out.UnresolvedLiability {
		t.Fatal(out, e)
	}
}
func TestReconcileChargesFullCeilingNeverRefunds(t *testing.T) {
	b, r, f := setup(t, 30)
	f.cleanupError = true
	_, e := b.Run(context.Background(), r)
	if e != nil {
		t.Fatal(e)
	}
	f.cleanupError = false
	_, e = b.change(r.RequestID, func(x *provider.Record) { x.CancelRequested = true })
	if e != nil {
		t.Fatal(e)
	}
	out, e := b.Reconcile(r.RequestID)
	if e != nil || !out.CleanupVerified || out.UnresolvedLiability || out.Compute.Charged != 30 {
		t.Fatal(out, e)
	}
	u, _ := b.Store.Usage(r.ProjectID)
	if u.Remaining != 0 {
		t.Fatal("reconciliation refunded uncertain work")
	}
}
