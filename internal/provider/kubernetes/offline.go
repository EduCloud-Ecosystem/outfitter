// Package kubernetes models a disabled single-Pod attempt using an injected API.
// There is no live client, credential reader, manifest renderer or CLI selector.
package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
	"github.com/EduCloud-Ecosystem/outfitter/internal/provider"
)

type Target struct {
	ClusterID, Namespace, NamespaceUID string
}

// Intent is a fake-protocol input, deliberately not a Kubernetes Pod manifest.
// Request directories are private snapshots; no artifact transfer is implemented.
type Intent struct {
	Name, Owner, RequestID, AttemptID, PayloadHash string
	Request                                        provider.Request
	Deadline                                       time.Time
	RestartPolicy                                  string
	Containers, InitContainers                     int
	Controller                                     bool
}

type Pod struct {
	Target
	Name, UID, Owner, RequestID, AttemptID, PayloadHash string
	Running, Controller                                 bool
	Containers, Restarts                                int
	ExitCode                                            *int
}

var ErrNotFound = errors.New("Pod not found at bound target")

// API is an offline protocol seam. Each method must use/refuse the exact target;
// name reuse is not permission to operate on a different UID. ConfirmStopped is
// deliberately stronger than Kubernetes object absence and has no live adapter.
type API interface {
	Identity(context.Context) (Target, error)
	Create(context.Context, Target, Intent) (Pod, error)
	Get(context.Context, Target, string) (Pod, error)
	Wait(context.Context, Target, string, string) (Pod, error)
	Logs(context.Context, Target, string, string, io.Writer, io.Writer) error
	Delete(context.Context, Target, string, string) error
	ConfirmStopped(context.Context, Target, string) (bool, error)
}

type Adapter struct {
	store  *ledger.Store
	target Target
	api    API
	now    func() time.Time
}

func NewOffline(store *ledger.Store, target Target, api API) (*Adapter, error) {
	p := provider.Placement{Backend: provider.OfflineKubernetes, ClusterID: target.ClusterID, Namespace: target.Namespace, NamespaceUID: target.NamespaceUID, AttemptID: "validation"}
	if store == nil || api == nil || p.Validate() != nil {
		return nil, errors.New("existing store, explicit target and injected offline API required")
	}
	return &Adapter{store: store, target: target, api: api, now: time.Now}, nil
}

func (*Adapter) Name() string { return provider.OfflineKubernetes }
func (*Adapter) Capabilities() provider.Capabilities {
	return provider.Capabilities{Isolation: []string{"reviewed-local"}}
}
func (a *Adapter) ValidatePlacement(p provider.Placement) error {
	if p.Validate() != nil || (Target{p.ClusterID, p.Namespace, p.NamespaceUID}) != a.target {
		return errors.New("offline cluster/namespace identity differs from bound request")
	}
	return nil
}
func (a *Adapter) Estimate(r provider.Request) (int64, error) {
	if e := provider.CheckPlacement(a, r); e != nil {
		return 0, e
	}
	return r.ReservationAmount(), r.Validate()
}
func (a *Adapter) current(id string) (provider.Record, error) {
	r, e := a.store.Record(id)
	if e == nil {
		e = provider.CheckPlacement(a, r.Request)
	}
	return r, e
}
func (a *Adapter) identity(ctx context.Context) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	target, e := a.api.Identity(ctx)
	if e != nil {
		return e
	}
	if target != a.target {
		return errors.New("observed cluster/namespace identity changed")
	}
	return nil
}
func owned(p Pod, r provider.Record, target Target) error {
	b, s := r.Request.Placement, r.PlacementState
	if b == nil || s == nil || p.Target != target || p.Name != r.Name || p.Owner != r.Owner || p.RequestID != r.RequestID || p.AttemptID != b.AttemptID || p.PayloadHash != r.PayloadHash || !provider.ValidID(p.UID) || (s.ResourceUID != "" && p.UID != s.ResourceUID) {
		return errors.New("foreign Pod identity or UID; refusing observation/effect")
	}
	return nil
}
func (a *Adapter) pin(r provider.Record, p Pod) (provider.Record, error) {
	if e := owned(p, r, a.target); e != nil {
		return r, e
	}
	e := a.store.Update(func(d *ledger.Data) error {
		x, ok := d.Runs[r.RequestID]
		if !ok || provider.CheckPlacement(a, x.Request) != nil || x.PayloadHash != r.PayloadHash || !x.PlacementState.DispatchClaimed {
			return errors.New("missing claimed placement")
		}
		if e := owned(p, x, a.target); e != nil {
			return e
		}
		x.PlacementState.ResourceUID = p.UID
		d.Runs[r.RequestID] = x
		r = x
		return nil
	})
	return r, e
}
func notDispatched(e error) error { return fmt.Errorf("%w: %v", provider.ErrNotDispatched, e) }

func attemptContext(ctx context.Context, deadline time.Time) (context.Context, context.CancelFunc) {
	// Reuse an equal/earlier caller deadline. A second timer at the same instant
	// can return before the broker's context observes expiry, misclassifying a
	// known deadline as an unknown API outcome.
	if existing, ok := ctx.Deadline(); ok && !existing.After(deadline) {
		return ctx, func() {}
	}
	return context.WithDeadline(ctx, deadline)
}

func sameHandle(current, supplied provider.Record) bool {
	return supplied.PlacementState != nil && current.RequestID == supplied.RequestID && current.ProjectID == supplied.ProjectID && current.Owner == supplied.Owner && current.Name == supplied.Name && current.PayloadHash == supplied.PayloadHash && current.Request.Hash() == supplied.Request.Hash() && (supplied.PlacementState.ResourceUID == "" || current.PlacementState.ResourceUID == supplied.PlacementState.ResourceUID)
}

func (a *Adapter) Provision(ctx context.Context, work provider.Request, v provider.Reservation) error {
	r, e := a.current(work.RequestID)
	if e != nil {
		return notDispatched(e)
	}
	if r.PlacementState.DispatchClaimed {
		// A second call cannot turn an earlier possible create into a no-effect result.
		return errors.New("offline attempt dispatch already consumed")
	}
	ctx, cancel := attemptContext(ctx, r.Deadline)
	defer cancel()
	original := work
	original.InputDir, original.RecipeDir = r.Request.InputDir, r.Request.RecipeDir
	if original.Hash() != r.PayloadHash || work.InputDir != filepath.Join(a.store.Dir, "snapshots", r.RequestID, "input") || work.RecipeDir != filepath.Join(a.store.Dir, "snapshots", r.RequestID, "recipe") || v.Owner != r.Owner || v.Name != r.Name || !v.Deadline.Equal(r.Deadline) || v.Amount != r.Compute.Reserved {
		return notDispatched(errors.New("request/reservation/snapshot identity differs"))
	}
	if e = v.Validate(work); e != nil {
		return notDispatched(e)
	}
	if e = a.identity(ctx); e != nil {
		return notDispatched(e)
	}
	// Linearize the irreversible dispatch claim after identity reads and before
	// any create. Never hold the ledger lock across a fake/API callback.
	e = a.store.Update(func(d *ledger.Data) error {
		if e := ledger.RequireAdmission(*d); e != nil {
			return e
		}
		x, ok := d.Runs[r.RequestID]
		if !ok || provider.CheckPlacement(a, x.Request) != nil || x.PayloadHash != r.PayloadHash || x.PlacementState.DispatchClaimed || !x.CreateAttempted || x.CreateFinished || x.CleanupVerified || x.CancelRequested || ctx.Err() != nil || !a.now().Before(x.Deadline) {
			return errors.New("dispatch refused: consumed, cancelled, expired or changed attempt")
		}
		x.PlacementState.DispatchClaimed = true
		d.Runs[r.RequestID] = x
		r = x
		return nil
	})
	if e != nil {
		latest, readErr := a.current(r.RequestID)
		if readErr != nil || latest.PlacementState.DispatchClaimed {
			return errors.New("dispatch claimed or state uncertain; no no-effect assertion")
		}
		return notDispatched(e)
	}
	p, e := a.api.Create(ctx, a.target, Intent{Name: r.Name, Owner: r.Owner, RequestID: r.RequestID, AttemptID: r.Request.Placement.AttemptID, PayloadHash: r.PayloadHash, Request: work, Deadline: r.Deadline, RestartPolicy: "Never", Containers: 1})
	if e != nil {
		// Do not forward a fake/transport's no-effect sentinel after our claim.
		return fmt.Errorf("offline create response ambiguous: %v", e)
	}
	_, e = a.pin(r, p)
	return e
}

func (a *Adapter) Run(ctx context.Context, supplied provider.Record, out, errout io.Writer) (int, error) {
	r, e := a.current(supplied.RequestID)
	if e != nil {
		return -1, e
	}
	if !sameHandle(r, supplied) || r.CleanupVerified || r.CreateAmbiguous || !r.CreateFinished || !r.ExecutionStarted || !r.PlacementState.DispatchClaimed || r.PlacementState.ResourceUID == "" {
		return -1, errors.New("run has no bound created UID")
	}
	ctx, cancel := attemptContext(ctx, r.Deadline)
	defer cancel()
	if e = a.identity(ctx); e != nil {
		return -1, e
	}
	p, e := a.api.Wait(ctx, a.target, r.Name, r.PlacementState.ResourceUID)
	if e != nil {
		return -1, e
	}
	if e = owned(p, r, a.target); e != nil {
		return -1, e
	}
	if p.Running || p.ExitCode == nil || p.Restarts != 0 || p.Controller || p.Containers != 1 {
		return -1, errors.New("single controller-free attempt completion is not established")
	}
	if e = a.identity(ctx); e != nil {
		return -1, e
	}
	if e = a.api.Logs(ctx, a.target, r.Name, p.UID, out, errout); e != nil {
		return -1, e
	}
	return *p.ExitCode, nil
}

func (a *Adapter) Status(ctx context.Context, supplied provider.Record) (provider.State, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var state provider.State
	r, e := a.current(supplied.RequestID)
	if e != nil {
		return state, e
	}
	if !sameHandle(r, supplied) {
		return state, errors.New("status binding mismatch")
	}
	if !r.PlacementState.DispatchClaimed || r.CleanupVerified {
		return state, nil
	}
	if e = a.identity(ctx); e != nil {
		return state, e
	}
	p, e := a.api.Get(ctx, a.target, r.Name)
	if errors.Is(e, ErrNotFound) {
		return state, nil // A status observation is not accounting settlement.
	}
	if e != nil {
		return state, e
	}
	if e = owned(p, r, a.target); e != nil {
		return state, e
	}
	state = provider.State{Exists: true, Running: p.Running, Owner: p.Owner, RequestID: p.RequestID}
	if p.ExitCode != nil {
		state.ExitCode = *p.ExitCode
	}
	return state, nil
}

func (a *Adapter) Teardown(ctx context.Context, supplied provider.Record) error {
	ctx, cancel := context.WithTimeout(ctx, 40*time.Second)
	defer cancel()
	r, e := a.current(supplied.RequestID)
	if e != nil {
		return e
	}
	if !sameHandle(r, supplied) {
		return errors.New("cleanup binding mismatch")
	}
	if !r.PlacementState.DispatchClaimed || r.CleanupVerified {
		return nil
	}
	if e = a.identity(ctx); e != nil {
		return e
	}
	p, e := a.api.Get(ctx, a.target, r.Name)
	if e != nil && !errors.Is(e, ErrNotFound) {
		return e
	}
	if e == nil {
		r, e = a.pin(r, p)
		if e != nil {
			return e
		}
		if e = a.identity(ctx); e != nil {
			return e
		}
		if e = a.api.Delete(ctx, a.target, r.Name, r.PlacementState.ResourceUID); e != nil {
			return e
		}
		if e = a.identity(ctx); e != nil {
			return e
		}
		_, e = a.api.Get(ctx, a.target, r.Name)
		if !errors.Is(e, ErrNotFound) {
			return errors.New("Pod absence not verified after UID-conditional deletion")
		}
	}
	if r.PlacementState.ResourceUID == "" {
		return errors.New("create outcome unknown; absence is provisional")
	}
	if e = a.identity(ctx); e != nil {
		return e
	}
	stopped, e := a.api.ConfirmStopped(ctx, a.target, r.PlacementState.ResourceUID)
	if e != nil || !stopped {
		return errors.New("Pod object absent but workload termination unverified")
	}
	return a.store.Update(func(d *ledger.Data) error {
		x := d.Runs[r.RequestID]
		if provider.CheckPlacement(a, x.Request) != nil || x.PayloadHash != r.PayloadHash || x.PlacementState.ResourceUID != r.PlacementState.ResourceUID {
			return errors.New("stopped observation identity mismatch")
		}
		x.PlacementState.Stopped = true
		d.Runs[r.RequestID] = x
		return nil
	})
}

var _ provider.Provisioner = (*Adapter)(nil)
