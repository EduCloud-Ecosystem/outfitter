package kubernetes

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/EduCloud-Ecosystem/outfitter/internal/broker"
	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
	"github.com/EduCloud-Ecosystem/outfitter/internal/provider"
	"github.com/EduCloud-Ecosystem/outfitter/internal/provider/docker"
)

// Fake only: no network, process, kubeconfig, engine or real workload is used.
type fakeAPI struct {
	createCause            error
	identityArrived        chan struct{}
	identityRelease        <-chan struct{}
	mu                     sync.Mutex
	target                 Target
	pod                    *Pod
	intent                 Intent
	calls                  []string
	creates, deletes, logs int
	deletedUID             string
	stdout, stderr         string

	createError, noCreateObject      bool
	deleteError, keepObject          bool
	stopUnknown, waitError, logError bool
	waitUntilDeadline                bool
	identityHook                     func()
	createHook                       func(Intent)
	waitHook                         func()
	deleteHook                       func()
}

func (f *fakeAPI) Identity(ctx context.Context) (Target, error) {
	if f.identityArrived != nil {
		f.identityArrived <- struct{}{}
		select {
		case <-f.identityRelease:
		case <-ctx.Done():
			return Target{}, ctx.Err()
		}
	}
	f.mu.Lock()
	f.calls = append(f.calls, "identity")
	hook := f.identityHook
	f.identityHook = nil
	f.mu.Unlock()
	if hook != nil {
		hook()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.target, ctx.Err()
}
func (f *fakeAPI) targetOK(ctx context.Context, target Target, operation string) error {
	f.calls = append(f.calls, operation)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if target != f.target {
		return errors.New("fake target changed")
	}
	return nil
}
func (f *fakeAPI) podFor(intent Intent, uid string) Pod {
	code := 0
	return Pod{Target: f.target, Name: intent.Name, UID: uid, Owner: intent.Owner, RequestID: intent.RequestID, AttemptID: intent.AttemptID, PayloadHash: intent.PayloadHash, Containers: 1, ExitCode: &code}
}
func (f *fakeAPI) Create(ctx context.Context, target Target, intent Intent) (Pod, error) {
	if f.createHook != nil {
		f.createHook(intent)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if e := f.targetOK(ctx, target, "create"); e != nil {
		return Pod{}, e
	}
	f.creates++
	f.intent = intent
	if f.pod != nil {
		return Pod{}, errors.New("already exists")
	}
	pod := f.podFor(intent, "pod-uid-1")
	if !f.noCreateObject {
		f.pod = &pod
	}
	if f.createError {
		if f.createCause != nil {
			return Pod{}, f.createCause
		}
		return Pod{}, errors.New("ambiguous create response")
	}
	return pod, nil
}
func (f *fakeAPI) Get(ctx context.Context, target Target, name string) (Pod, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if e := f.targetOK(ctx, target, "get"); e != nil {
		return Pod{}, e
	}
	if f.pod == nil || f.pod.Name != name {
		return Pod{}, ErrNotFound
	}
	return *f.pod, nil
}
func (f *fakeAPI) Wait(ctx context.Context, target Target, name, uid string) (Pod, error) {
	if f.waitUntilDeadline {
		<-ctx.Done()
		return Pod{}, ctx.Err()
	}
	if f.waitHook != nil {
		f.waitHook()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if e := f.targetOK(ctx, target, "wait"); e != nil {
		return Pod{}, e
	}
	if f.waitError {
		return Pod{}, errors.New("node/API observation lost")
	}
	if f.pod == nil || f.pod.Name != name {
		return Pod{}, ErrNotFound
	}
	// Return even a changed UID to test the adapter's independent verification.
	return *f.pod, nil
}
func (f *fakeAPI) Logs(ctx context.Context, target Target, name, uid string, out, errout io.Writer) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if e := f.targetOK(ctx, target, "logs"); e != nil {
		return e
	}
	if f.pod == nil || f.pod.Name != name || f.pod.UID != uid {
		return errors.New("foreign log UID")
	}
	f.logs++
	if f.logError {
		return errors.New("output observation lost")
	}
	if _, e := io.WriteString(out, f.stdout); e != nil {
		return e
	}
	_, e := io.WriteString(errout, f.stderr)
	return e
}
func (f *fakeAPI) Delete(ctx context.Context, target Target, name, uid string) error {
	if f.deleteHook != nil {
		f.deleteHook()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if e := f.targetOK(ctx, target, "delete"); e != nil {
		return e
	}
	if f.deleteError {
		return errors.New("API unavailable during cleanup")
	}
	if f.pod == nil {
		return nil
	}
	if f.pod.Name != name || f.pod.UID != uid {
		return errors.New("UID precondition conflict")
	}
	f.deletes++
	f.deletedUID = uid
	if !f.keepObject {
		f.pod = nil
	}
	return nil
}
func (f *fakeAPI) ConfirmStopped(ctx context.Context, target Target, uid string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if e := f.targetOK(ctx, target, "stopped"); e != nil {
		return false, e
	}
	return !f.stopUnknown && f.deletedUID == uid, nil
}
func (f *fakeAPI) count() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.calls) }

func setup(t *testing.T, budget int64) (*broker.Broker, *Adapter, provider.Request, *fakeAPI) {
	t.Helper()
	root := t.TempDir()
	store, e := ledger.Open(filepath.Join(root, "state"), true)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = store.InitProject("project", budget); e != nil {
		t.Fatal(e)
	}
	in, recipe := filepath.Join(root, "input"), filepath.Join(root, "recipe")
	for _, dir := range []string{in, recipe} {
		if e = os.Mkdir(dir, 0700); e != nil {
			t.Fatal(e)
		}
	}
	if e = os.WriteFile(filepath.Join(in, "data.csv"), []byte("group,value\na,1\n"), 0600); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(recipe, "recipe.py"), []byte("print(1)\n"), 0600); e != nil {
		t.Fatal(e)
	}
	target := Target{strings.Repeat("a", 64), "offline-project", "namespace-uid-1"}
	fake := &fakeAPI{target: target, stdout: "1\n"}
	a, e := NewOffline(store, target, fake)
	if e != nil {
		t.Fatal(e)
	}
	w := provider.Request{Placement: &provider.Placement{Backend: provider.OfflineKubernetes, ClusterID: target.ClusterID, Namespace: target.Namespace, NamespaceUID: target.NamespaceUID, AttemptID: "attempt-1"}, SchemaVersion: 1, RequestID: "request-1", ProjectID: "project", Image: "python@sha256:" + strings.Repeat("b", 64), Command: []string{"python", "/recipe/recipe.py"}, InputDir: in, RecipeDir: recipe, CPUs: 1, MemoryMB: 256, PIDsLimit: 64, TimeoutSeconds: 30, MaxOutputBytes: 65536, Isolation: "reviewed-local", AllowSupervisedReaper: true}
	b := broker.New(store, a)
	b.StartWatchdog = func(string) error { return nil } // Synthetic readiness, never a process.
	return b, a, w, fake
}
func cloneRequest(w provider.Request) provider.Request {
	b, _ := json.Marshal(w)
	var copied provider.Request
	_ = json.Unmarshal(b, &copied)
	return copied
}
func record(t *testing.T, b *broker.Broker, id string) provider.Record {
	t.Helper()
	r, e := b.Store.Record(id)
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func requireHeld(t *testing.T, b *broker.Broker, r provider.Receipt) {
	t.Helper()
	u, e := b.Store.Usage("project")
	if e != nil || !r.UnresolvedLiability || r.Compute.Charged != 0 || r.Compute.Reserved != 30 || u.Remaining != 0 || u.Unknown != 30 {
		t.Fatalf("liability lost: receipt=%+v usage=%+v err=%v", r, u, e)
	}
}

func TestOfflineConformanceReserveBeforeEffectAndFullCharge(t *testing.T) {
	b, a, w, f := setup(t, 30)
	f.createHook = func(intent Intent) {
		r := record(t, b, w.RequestID)
		u, e := b.Store.Usage(w.ProjectID)
		if e != nil || u.Remaining != 0 || u.Reserved != 30 || !r.CreateAttempted || !r.PlacementState.DispatchClaimed || r.Request.Placement == nil || r.Request.Placement.Backend != provider.OfflineKubernetes {
			t.Fatal("effect preceded durable reservation/identity", r, u, e)
		}
		if intent.RestartPolicy != "Never" || intent.Containers != 1 || intent.InitContainers != 0 || intent.Controller || intent.AttemptID != w.Placement.AttemptID || intent.PayloadHash != w.Hash() || intent.Request.InputDir == w.InputDir || !intent.Deadline.Equal(r.Deadline) {
			t.Fatal("wrong one-Pod bounded intent", intent)
		}
	}
	r, e := b.Run(context.Background(), w)
	if e != nil || r.Status != "succeeded" || !r.CleanupVerified || r.UnresolvedLiability || r.Compute.Charged != 30 || r.Compute.Provenance != "estimated_upper_bound" || r.Stdout != "1\n" || f.creates != 1 || f.deletes != 1 {
		t.Fatal(r, e, f.creates, f.deletes)
	}
	if a.Capabilities().SelfExpiry || a.Capabilities().UsageSampling {
		t.Fatal("live capability overstated")
	}
	saved := record(t, b, w.RequestID)
	if saved.PlacementState.ResourceUID != "pod-uid-1" || !saved.PlacementState.Stopped {
		t.Fatal("UID/stop evidence not durable")
	}
	if e = a.Teardown(context.Background(), saved); e != nil {
		t.Fatal(e)
	}
	if s, e := a.Status(context.Background(), saved); e != nil || s.Exists {
		t.Fatal(s, e)
	}
}

func TestDuplicateAndRestartAreStatusOnly(t *testing.T) {
	for _, ambiguous := range []bool{false, true} {
		t.Run(map[bool]string{false: "settled", true: "ambiguous"}[ambiguous], func(t *testing.T) {
			b, _, w, f := setup(t, 30)
			f.createError = ambiguous
			first, e := b.Run(context.Background(), w)
			if e != nil {
				t.Fatal(e)
			}
			calls := f.count()
			s, e := ledger.Open(b.Store.Dir, false)
			if e != nil {
				t.Fatal(e)
			}
			a, e := NewOffline(s, f.target, f)
			if e != nil {
				t.Fatal(e)
			}
			restarted := broker.New(s, a)
			r, replayed, e := restarted.RunOnce(context.Background(), w)
			if e != nil || !replayed || r.Status != first.Status || r.Compute != first.Compute || f.count() != calls || f.creates != 1 {
				t.Fatal("duplicate contacted API or changed accounting", r, replayed, e)
			}
			if ambiguous {
				requireHeld(t, restarted, r)
			}
		})
	}
}

func TestChangedBindingAndRepeatedAttemptRefused(t *testing.T) {
	b, _, w, f := setup(t, 90)
	if _, e := b.Run(context.Background(), w); e != nil {
		t.Fatal(e)
	}
	cases := map[string]func(*provider.Request){
		"backend":                        func(r *provider.Request) { r.Placement.Backend = "docker" },
		"cluster":                        func(r *provider.Request) { r.Placement.ClusterID = strings.Repeat("c", 64) },
		"namespace":                      func(r *provider.Request) { r.Placement.Namespace = "other" },
		"namespace UID":                  func(r *provider.Request) { r.Placement.NamespaceUID = "other-uid" },
		"attempt":                        func(r *provider.Request) { r.Placement.AttemptID = "attempt-2" },
		"payload":                        func(r *provider.Request) { r.Command = []string{"different"} },
		"missing placement":              func(r *provider.Request) { r.Placement = nil },
		"same attempt under new request": func(r *provider.Request) { r.RequestID = "request-2" },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			changed := cloneRequest(w)
			change(&changed)
			calls := f.count()
			before, _ := os.ReadFile(filepath.Join(b.Store.Dir, "ledger.json"))
			if _, e := b.Run(context.Background(), changed); e == nil {
				t.Fatal("changed binding admitted")
			}
			after, _ := os.ReadFile(filepath.Join(b.Store.Dir, "ledger.json"))
			if string(before) != string(after) || f.count() != calls {
				t.Fatal("refusal mutated ledger or contacted API")
			}
		})
	}
}

func TestCancellationExpiryAndIdentityChangeBeforeDispatch(t *testing.T) {
	for _, mode := range []string{"cancel-watchdog", "cancel-final", "expiry-final", "context-final", "changed-cluster", "changed-namespace-uid", "missing-watchdog"} {
		t.Run(mode, func(t *testing.T) {
			b, a, w, f := setup(t, 30)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch mode {
			case "cancel-watchdog":
				b.StartWatchdog = func(id string) error { _, e := b.Cancel(id, "synthetic pre-dispatch cancellation"); return e }
			case "cancel-final":
				f.identityHook = func() {
					if _, e := b.Cancel(w.RequestID, "synthetic final cancellation"); e != nil {
						t.Fatal(e)
					}
				}
			case "expiry-final":
				f.identityHook = func() { a.now = func() time.Time { return time.Now().Add(time.Hour) } }
			case "context-final":
				f.identityHook = cancel
			case "changed-cluster":
				f.target.ClusterID = strings.Repeat("d", 64)
			case "changed-namespace-uid":
				f.target.NamespaceUID = "recreated-namespace"
			case "missing-watchdog":
				b.StartWatchdog = nil
			}
			r, e := b.Run(ctx, w)
			if e != nil || f.creates != 0 || record(t, b, w.RequestID).PlacementState.DispatchClaimed || r.Status == "succeeded" {
				t.Fatal("pre-dispatch refusal made effects", r, e, f.creates)
			}
			calls := f.count()
			if _, e = b.Run(context.Background(), w); e != nil || f.count() != calls {
				t.Fatal("refused slot replayed", e)
			}
		})
	}
}

func TestAmbiguousCreateLateAppearanceAndRestartNeverRefund(t *testing.T) {
	b, _, w, f := setup(t, 30)
	f.createError = true
	f.noCreateObject = true
	r, e := b.Run(context.Background(), w)
	if e != nil {
		t.Fatal(e)
	}
	requireHeld(t, b, r)
	s, e := ledger.Open(b.Store.Dir, false)
	if e != nil {
		t.Fatal(e)
	}
	a, e := NewOffline(s, f.target, f)
	if e != nil {
		t.Fatal(e)
	}
	restarted := broker.New(s, a)
	f.pod = new(Pod)
	*f.pod = f.podFor(f.intent, "late-owned-uid")
	r, e = restarted.Cancel(w.RequestID, "synthetic late cleanup")
	if e != nil {
		t.Fatal(e)
	}
	if f.pod != nil || f.deletes != 1 || record(t, restarted, w.RequestID).PlacementState.ResourceUID != "late-owned-uid" {
		t.Fatal("late owned Pod not bound/removed")
	}
	requireHeld(t, restarted, r)
	r, e = restarted.Reconcile(w.RequestID)
	if e != nil {
		t.Fatal(e)
	}
	requireHeld(t, restarted, r)
	calls := f.count()
	if _, e = restarted.Run(context.Background(), w); e != nil || f.count() != calls || f.creates != 1 {
		t.Fatal("ambiguous attempt replayed", e)
	}
}

func TestForeignUIDAndOwnershipRefuseLogsAndDeletion(t *testing.T) {
	for _, field := range []string{"uid", "owner", "attempt", "namespace-uid", "payload", "controller", "restarts"} {
		t.Run(field, func(t *testing.T) {
			b, _, w, f := setup(t, 30)
			f.waitHook = func() {
				switch field {
				case "uid":
					f.pod.UID = "foreign-uid"
				case "owner":
					f.pod.Owner = "foreign-owner"
				case "attempt":
					f.pod.AttemptID = "foreign-attempt"
				case "namespace-uid":
					f.pod.NamespaceUID = "foreign-namespace"
				case "payload":
					f.pod.PayloadHash = strings.Repeat("e", 64)
				case "controller":
					f.pod.Controller = true
				case "restarts":
					f.pod.Restarts = 1
				}
			}
			r, e := b.Run(context.Background(), w)
			if e != nil {
				t.Fatal(e)
			}
			requireHeld(t, b, r)
			if f.logs != 0 {
				t.Fatal("unverified Pod output read")
			}
			if field != "controller" && field != "restarts" && f.deletes != 0 {
				t.Fatal("foreign resource deleted")
			}
		})
	}
}

func TestCleanupFailureAndAPIAbsenceWithoutStopRemainLiable(t *testing.T) {
	for _, mode := range []string{"delete-error", "object-remains", "stop-unknown", "wait-error", "logs-error"} {
		t.Run(mode, func(t *testing.T) {
			b, _, w, f := setup(t, 30)
			switch mode {
			case "delete-error":
				f.deleteError = true
			case "object-remains":
				f.keepObject = true
			case "stop-unknown":
				f.stopUnknown = true
			case "wait-error":
				f.waitError = true
			case "logs-error":
				f.logError = true
			}
			r, e := b.Run(context.Background(), w)
			if e != nil {
				t.Fatal(e)
			}
			requireHeld(t, b, r)
			if mode == "stop-unknown" && (f.pod != nil || r.CleanupVerified) {
				t.Fatal("absence incorrectly proved stopped workload")
			}
			calls := f.count()
			if _, e = b.Run(context.Background(), w); e != nil || f.count() != calls || f.creates != 1 {
				t.Fatal("failed attempt retried", e)
			}
		})
	}
}

func TestCombinedOutputIsBoundedAndDiagnosticRetained(t *testing.T) {
	b, _, w, f := setup(t, 30)
	w.MaxOutputBytes = 64
	f.stdout = strings.Repeat("x", 40)
	f.stderr = strings.Repeat("y", 40)
	r, e := b.Run(context.Background(), w)
	if e != nil || r.Status != "failed" || !strings.Contains(r.Error, "output limit") || len(r.Stdout)+len(r.Stderr) != 64 || r.Compute.Charged != 30 || !r.CleanupVerified {
		t.Fatal(r, e)
	}
	saved := record(t, b, w.RequestID)
	if saved.OutputSHA256 == "" || saved.OutputBytes == 0 {
		t.Fatal("bounded diagnostics not retained")
	}
}

func TestWrongBackendAndHandleRefuseBeforeEffects(t *testing.T) {
	b, a, w, f := setup(t, 30)
	f.deleteError = true
	if _, e := b.Run(context.Background(), w); e != nil {
		t.Fatal(e)
	}
	r := record(t, b, w.RequestID)
	calls := f.count()
	foreign := r
	foreign.Owner = "foreign"
	if e := a.Teardown(context.Background(), foreign); e == nil {
		t.Fatal("foreign handle cleanup accepted")
	}
	if _, e := a.Status(context.Background(), foreign); e == nil {
		t.Fatal("foreign handle status accepted")
	}
	if _, e := a.Run(context.Background(), foreign, io.Discard, io.Discard); e == nil {
		t.Fatal("foreign handle run accepted")
	}
	if f.count() != calls {
		t.Fatal("foreign handle contacted API")
	}
	d := docker.New()
	d.Binary = filepath.Join(t.TempDir(), "must-not-exist")
	wrong := broker.New(b.Store, d)
	before, _ := os.ReadFile(filepath.Join(b.Store.Dir, "ledger.json"))
	if _, e := wrong.Run(context.Background(), w); e == nil {
		t.Fatal("wrong backend run accepted")
	}
	if _, e := wrong.Cancel(w.RequestID, "wrong backend"); e == nil {
		t.Fatal("wrong backend cancel accepted")
	}
	if _, e := wrong.Reconcile(w.RequestID); e == nil {
		t.Fatal("wrong backend reconcile accepted")
	}
	if e := wrong.Watch(context.Background(), w.RequestID); e == nil {
		t.Fatal("wrong backend watcher accepted")
	}
	if _, e := d.Estimate(w); e == nil {
		t.Fatal("Docker estimate accepted placement")
	}
	if e := d.Provision(context.Background(), w, provider.Reservation{}); e == nil || !strings.Contains(e.Error(), "different execution backend") {
		t.Fatal(e)
	}
	if _, e := d.Status(context.Background(), r); e == nil || !strings.Contains(e.Error(), "different execution backend") {
		t.Fatal(e)
	}
	if e := d.Teardown(context.Background(), r); e == nil || !strings.Contains(e.Error(), "different execution backend") {
		t.Fatal(e)
	}
	if _, e := d.Run(context.Background(), r, io.Discard, io.Discard); e == nil || !strings.Contains(e.Error(), "different execution backend") {
		t.Fatal(e)
	}
	after, _ := os.ReadFile(filepath.Join(b.Store.Dir, "ledger.json"))
	if string(before) != string(after) {
		t.Fatal("wrong backend changed ledger")
	}
	if _, e := os.Stat(filepath.Join(b.Store.Dir, "watch-"+w.RequestID+".ready")); !os.IsNotExist(e) {
		t.Fatal("wrong backend watcher marker created")
	}
}

func TestDurablePlacementCannotBeReboundRemovedOrRefunded(t *testing.T) {
	b, _, w, _ := setup(t, 30)
	if _, e := b.Run(context.Background(), w); e != nil {
		t.Fatal(e)
	}
	mutants := map[string]func(*ledger.Data){
		"remove":  func(d *ledger.Data) { delete(d.Runs, w.RequestID) },
		"version": func(d *ledger.Data) { d.Version = 4 },
		"target": func(d *ledger.Data) {
			r := d.Runs[w.RequestID]
			r.Request.Placement.NamespaceUID = "changed"
			r.PayloadHash = r.Request.Hash()
			d.Runs[w.RequestID] = r
		},
		"clear claim": func(d *ledger.Data) {
			r := d.Runs[w.RequestID]
			r.PlacementState.DispatchClaimed = false
			d.Runs[w.RequestID] = r
		},
		"foreign UID": func(d *ledger.Data) {
			r := d.Runs[w.RequestID]
			r.PlacementState.ResourceUID = "replacement"
			d.Runs[w.RequestID] = r
		},
		"clear stop": func(d *ledger.Data) {
			r := d.Runs[w.RequestID]
			r.PlacementState.Stopped = false
			d.Runs[w.RequestID] = r
		},
		"refund": func(d *ledger.Data) {
			r := d.Runs[w.RequestID]
			r.Compute.Charged = 0
			r.Compute.Provenance = "reserved"
			r.UnresolvedLiability = true
			r.CleanupVerified = false
			d.Runs[w.RequestID] = r
		},
	}
	for name, mutate := range mutants {
		t.Run(name, func(t *testing.T) {
			before, _ := os.ReadFile(filepath.Join(b.Store.Dir, "ledger.json"))
			if e := b.Store.Update(func(d *ledger.Data) error { mutate(d); return nil }); e == nil {
				t.Fatal("destructive placement mutation accepted")
			}
			after, _ := os.ReadFile(filepath.Join(b.Store.Dir, "ledger.json"))
			if string(before) != string(after) {
				t.Fatal("failed mutation changed durable state")
			}
		})
	}
	checkpoint := filepath.Join(t.TempDir(), "unqualified-checkpoint")
	if _, e := b.Store.Checkpoint(checkpoint); e == nil {
		t.Fatal("unqualified recovery enabled")
	}
	if _, e := os.Stat(checkpoint); !os.IsNotExist(e) {
		t.Fatal("refused checkpoint created output")
	}
}

func TestConcurrentDuplicateHasOneDispatch(t *testing.T) {
	b, _, w, f := setup(t, 30)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, e := b.Run(context.Background(), w); e != nil {
				t.Error(e)
			}
		}()
	}
	wg.Wait()
	if f.creates != 1 {
		t.Fatal("concurrent duplicate dispatch", f.creates)
	}
}

func TestDirectProvisionCannotReplayConsumedClaim(t *testing.T) {
	b, a, w, f := setup(t, 30)
	if _, e := b.Run(context.Background(), w); e != nil {
		t.Fatal(e)
	}
	r := record(t, b, w.RequestID)
	calls := f.count()
	if e := a.Provision(context.Background(), w, provider.Reservation{ID: r.RequestID, Amount: 30, Deadline: r.Deadline, Owner: r.Owner, Name: r.Name}); e == nil || errors.Is(e, provider.ErrNotDispatched) {
		t.Fatal("consumed create incorrectly retried or reset", e)
	}
	if f.count() != calls {
		t.Fatal("direct duplicate contacted API")
	}
}

func TestRestartAfterReservationOrDispatchClaimNeverCreates(t *testing.T) {
	for _, claimed := range []bool{false, true} {
		t.Run(map[bool]string{false: "reserved", true: "claimed"}[claimed], func(t *testing.T) {
			b, _, w, f := setup(t, 30)
			r, fresh, e := b.Reserve(w)
			if e != nil || !fresh {
				t.Fatal(e, fresh)
			}
			if claimed {
				e = b.Store.Update(func(d *ledger.Data) error {
					x := d.Runs[w.RequestID]
					x.CreateAttempted = true
					x.Status = "creating"
					x.PlacementState.DispatchClaimed = true
					d.Runs[w.RequestID] = x
					return nil
				})
				if e != nil {
					t.Fatal(e)
				}
			}
			store, e := ledger.Open(b.Store.Dir, false)
			if e != nil {
				t.Fatal(e)
			}
			a, e := NewOffline(store, f.target, f)
			if e != nil {
				t.Fatal(e)
			}
			restarted := broker.New(store, a)
			receipt, replayed, e := restarted.RunOnce(context.Background(), w)
			if e != nil || !replayed || f.count() != 0 || receipt.Compute.Reserved != r.Compute.Reserved {
				t.Fatal(receipt, replayed, e)
			}
			receipt, e = restarted.Cancel(w.RequestID, "interrupted synthetic owner")
			if e != nil || f.creates != 0 {
				t.Fatal(e)
			}
			if claimed {
				requireHeld(t, restarted, receipt)
			} else if receipt.UnresolvedLiability || receipt.Compute.Charged != 30 || !receipt.CleanupVerified || f.count() != 0 {
				t.Fatal("unclaimed slot refunded or made effects", receipt)
			}
		})
	}
}

func TestDeleteUIDPreconditionRefusesReplacementRace(t *testing.T) {
	b, _, w, f := setup(t, 30)
	f.deleteHook = func() { f.pod.UID = "replacement-after-get" }
	r, e := b.Run(context.Background(), w)
	if e != nil {
		t.Fatal(e)
	}
	requireHeld(t, b, r)
	if f.deletes != 0 || f.pod == nil || f.pod.UID != "replacement-after-get" {
		t.Fatal("UID precondition did not protect foreign replacement")
	}
}

func TestExecutionDeadlineCancelsWaitWithoutNewAttempt(t *testing.T) {
	b, _, w, f := setup(t, 30)
	w.TimeoutSeconds = 1
	f.waitUntilDeadline = true
	r, e := b.Run(context.Background(), w)
	if e != nil || r.Status != "failed" || r.Compute.Charged != 1 || !r.CleanupVerified || f.creates != 1 || f.logs != 0 {
		t.Fatal(r, e)
	}
	calls := f.count()
	if _, e = b.Run(context.Background(), w); e != nil || f.count() != calls {
		t.Fatal("expired attempt renewed", e)
	}
}

func TestKnownCreateCleanupCanSettleFullChargeAfterRestart(t *testing.T) {
	b, _, w, f := setup(t, 30)
	f.stopUnknown = true
	r, e := b.Run(context.Background(), w)
	if e != nil {
		t.Fatal(e)
	}
	requireHeld(t, b, r)
	if _, e = b.Cancel(w.RequestID, "stop evidence initially missing"); e != nil {
		t.Fatal(e)
	}
	store, e := ledger.Open(b.Store.Dir, false)
	if e != nil {
		t.Fatal(e)
	}
	a, e := NewOffline(store, f.target, f)
	if e != nil {
		t.Fatal(e)
	}
	restarted := broker.New(store, a)
	f.stopUnknown = false
	r, e = restarted.Reconcile(w.RequestID)
	if e != nil || !r.CleanupVerified || r.UnresolvedLiability || r.Compute.Charged != 30 || f.creates != 1 {
		t.Fatal(r, e)
	}
	u, e := store.Usage(w.ProjectID)
	if e != nil || u.Remaining != 0 || u.Charged != 30 {
		t.Fatal(u, e)
	}
}

func TestLegacyRecordAndNativeAccountRemainUnchanged(t *testing.T) {
	b, _, w, _ := setup(t, 90)
	account := ledger.NativeAccount{ID: "existing-native", Provider: "jetstream2", Unit: "milli_su", Scope: "synthetic-scope", Budget: 1000, Policy: json.RawMessage(`{"offline":true}`)}
	if e := b.Store.InitNativeAccount(account); e != nil {
		t.Fatal(e)
	}
	legacy := cloneRequest(w)
	legacy.Placement = nil
	legacy.RequestID = "legacy-request"
	oldBroker := broker.New(b.Store, docker.New())
	if _, fresh, e := oldBroker.Reserve(legacy); e != nil || !fresh {
		t.Fatal(fresh, e)
	} // Reserve only: zero Docker commands.
	oldRecord, e := json.Marshal(record(t, oldBroker, legacy.RequestID))
	if e != nil {
		t.Fatal(e)
	}
	if strings.Contains(string(oldRecord), "placement") {
		t.Fatal("legacy encoding changed")
	}
	if _, e = b.Run(context.Background(), w); e != nil {
		t.Fatal(e)
	}
	current, e := json.Marshal(record(t, oldBroker, legacy.RequestID))
	if e != nil {
		t.Fatal(e)
	}
	if string(current) != string(oldRecord) {
		t.Fatal("legacy record changed during offline reservation")
	}
	e = b.Store.View(func(d ledger.Data) error {
		raw, _ := json.Marshal(d.NativeAccounts[account.ID])
		expected, _ := json.Marshal(account)
		if string(raw) != string(expected) || d.Version != 5 {
			t.Fatal("native account changed or target not versioned")
		}
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
	if _, fresh, e := oldBroker.Reserve(legacy); e != nil || fresh {
		t.Fatal("legacy duplicate changed", fresh, e)
	}
}

func TestConcurrentDirectClaimCannotDenyAnExistingEffect(t *testing.T) {
	b, a, w, f := setup(t, 30)
	r, _, e := b.Reserve(w)
	if e != nil {
		t.Fatal(e)
	}
	e = b.Store.Update(func(d *ledger.Data) error {
		x := d.Runs[w.RequestID]
		x.CreateAttempted = true
		x.Status = "creating"
		d.Runs[w.RequestID] = x
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
	work := cloneRequest(w)
	work.InputDir = filepath.Join(b.Store.Dir, "snapshots", w.RequestID, "input")
	work.RecipeDir = filepath.Join(b.Store.Dir, "snapshots", w.RequestID, "recipe")
	reservation := provider.Reservation{ID: r.RequestID, Amount: r.Compute.Reserved, Deadline: r.Deadline, Owner: r.Owner, Name: r.Name}
	f.identityArrived = make(chan struct{}, 2)
	release := make(chan struct{})
	f.identityRelease = release
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { results <- a.Provision(context.Background(), work, reservation) }()
	}
	<-f.identityArrived
	<-f.identityArrived
	close(release)
	successes, failures := 0, 0
	for i := 0; i < 2; i++ {
		e := <-results
		if e == nil {
			successes++
		} else {
			failures++
			if errors.Is(e, provider.ErrNotDispatched) {
				t.Error("losing concurrent call falsely denied the persisted dispatch claim")
			}
		}
	}
	if successes != 1 || failures != 1 || f.creates != 1 {
		t.Fatal(successes, failures, f.creates)
	}
}

func TestAPICannotReturnNoEffectAfterDurableDispatch(t *testing.T) {
	b, _, w, f := setup(t, 30)
	f.createError = true
	f.createCause = provider.ErrNotDispatched
	r, e := b.Run(context.Background(), w)
	if e != nil {
		t.Fatal(e)
	}
	requireHeld(t, b, r)
	if f.creates != 1 || !record(t, b, w.RequestID).CreateAmbiguous {
		t.Fatal("API no-effect sentinel erased a possible create")
	}
}
