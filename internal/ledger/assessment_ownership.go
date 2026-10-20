package ledger

// The operation object is private live evidence for existing v7 journal rows.
// It is neither serialized nor reconstructed from an epoch, PID or free lease.
import (
	"os"
	"sync"
	"syscall"
	"time"

	x "github.com/EduCloud-Ecosystem/outfitter/internal/assessmentexec"
	"github.com/EduCloud-Ecosystem/outfitter/internal/owneddata"
)

type AssessmentBeginClock struct {
	Wall   time.Time
	TickMS int64
	Epoch  string
}

// This read-only BEGIN guard runs inside the existing authoritative journal
// lock. It must not perform IPC/provider/lease waits or mint completion proof.
type AssessmentBeginGuard func(Data) (AssessmentBeginClock, error)
type AssessmentRoots struct {
	Data, State *os.Root
	Failure     func(string) error
}
type AssessmentOperation struct{ state *assessmentOperationState }
type assessmentHandle struct {
	file              *os.File
	root              *os.Root
	locked, attempted bool
	result            error
	label             string
}
type assessmentOperationState struct {
	mu                                                  sync.Mutex
	store                                               *Store
	scope                                               x.ExecutionScope
	attempt                                             *x.AssessmentAttempt
	role, epoch                                         string
	roots                                               AssessmentRoots
	handles                                             []*assessmentHandle
	readyRead, readyWrite, watchWrite                   *os.File
	watchChild                                          *assessmentOperationState
	acquisitionStarted, acquired, fresh, ready, aborted bool
	watchReserved, watchLaunched                        bool
	closeAttempted, commitAttempted                     bool
	uncertain, removed                                  bool
	guard                                               AssessmentBeginGuard
	result                                              error
}
type assessmentCompletion struct {
	store       *Store
	binding     x.ScopeBinding
	role, epoch string
	removed     bool
}

func (p *assessmentCompletion) permitsAttempt(a x.AssessmentAttempt) bool {
	return p != nil && p.role == "execute" && p.binding == a.Binding && p.epoch == a.ClaimEpoch
}
func (p *assessmentCompletion) permitsScopeChanges(old, next x.ExecutionScope) bool {
	fields := []struct {
		a, b bool
		role string
	}{{old.PrepareOwnershipCloseConfirmed, next.PrepareOwnershipCloseConfirmed, "prepare"}, {old.WriterOwnershipCloseConfirmed, next.WriterOwnershipCloseConfirmed, "writer"}, {old.SealOwnershipCloseConfirmed, next.SealOwnershipCloseConfirmed, "seal"}, {old.WatchOwnershipCloseConfirmed, next.WatchOwnershipCloseConfirmed, "watch"}, {old.CleanupOwnershipCloseConfirmed, next.CleanupOwnershipCloseConfirmed, "cleanup"}}
	for _, f := range fields {
		if !f.a && f.b && (p == nil || p.role != f.role || p.binding != next.Binding() || p.epoch != assessmentRoleEpoch(next, f.role)) {
			return false
		}
	}
	if len(next.Observations) < len(old.Observations) {
		return false
	}
	for _, o := range next.Observations[len(old.Observations):] {
		if (o.WritersDrained == "observed" || o.ReadersDrained == "observed" || o.CollectorDrained == "observed" || o.ScopeAccessRevocation == "observed" || o.OwnedRemoval == "observed_absent") && (p == nil || p.role != "cleanup" || !p.removed || p.binding != next.Binding()) {
			return false
		}
	}
	return true
}
func assessmentRoleEpoch(s x.ExecutionScope, role string) string {
	if role == "watch" {
		if s.WatchEpoch != nil {
			return *s.WatchEpoch
		}
		return ""
	}
	if role == "cleanup" {
		if s.CleanupOwnerEpoch != nil {
			return *s.CleanupOwnerEpoch
		}
		return ""
	}
	return s.WriterEpoch
}
func newAssessmentOperation(store *Store, s x.ExecutionScope, a *x.AssessmentAttempt, role string, roots AssessmentRoots, guard AssessmentBeginGuard) *AssessmentOperation {
	epoch := assessmentRoleEpoch(s, role)
	if a != nil {
		epoch = a.ClaimEpoch
	}
	return &AssessmentOperation{&assessmentOperationState{store: store, scope: x.Clone(s), attempt: a, role: role, epoch: epoch, roots: roots, guard: guard}}
}
func cleanupQuiescentV7(a *x.Authority, s x.ExecutionScope) bool {
	if a == nil || !s.DenyLatched || !s.TransientOwnershipClosed(true) {
		return false
	}
	for _, r := range a.Attempts {
		if r.Binding.ScopeID == s.ScopeID && !r.SerialClear() {
			return false
		}
	}
	return true
}
func cleanupReadyV7(a *x.Authority, s x.ExecutionScope) bool {
	return s.CleanupOwnerEpoch == nil && cleanupQuiescentV7(a, s)
}
func (s *Store) AssessmentCleanupReadyV7(binding x.ScopeBinding) error {
	return s.View(func(d Data) error {
		if d.Version != 7 || d.AssessmentExecution == nil {
			return x.ErrDenied
		}
		row, ok := d.AssessmentExecution.Scopes[binding.ScopeID]
		if !ok || row.Binding() != binding {
			return x.ErrCollision
		}
		if !cleanupReadyV7(d.AssessmentExecution, row) {
			return x.ErrHeld
		}
		return nil
	})
}

// A failed fresh forward guard consumes only the existing absorbing denial.
// Bad or backward samples cannot move the retained journal clock backwards.
func denyAssessmentForwardV7(d *Data, row x.ExecutionScope, n AssessmentBeginClock) (x.ExecutionScope, error) {
	if row.DenyLatched {
		return row, nil
	}
	at, tick := row.WallHighWater, row.TickHighWaterMS
	sampled := owneddata.Stamp(n.Wall)
	_, stampErr := owneddata.Time(sampled, true)
	if stampErr == nil && n.Epoch == row.ClockEpoch && sampled >= at && n.TickMS >= tick && n.TickMS <= 9007199254740991 {
		at, tick = sampled, n.TickMS
	}
	next, e := x.AdvanceScope(row, x.ScopeDeny, at, tick, func(v *x.ExecutionScope) {
		v.DenyLatched = true
		v.ExecutionDenyLatched = true
		v.State = "cleaning"
		v.Reason = "io_uncertain"
	})
	if e == nil {
		d.AssessmentExecution.Scopes[row.ScopeID] = next
	}
	return next, e
}
func assessmentBeforeJournalV7(roots AssessmentRoots, event string) error {
	if roots.Failure != nil && roots.Failure("before-journal:"+event) != nil {
		return x.ErrHeld
	}
	return nil
}
func (s *Store) BeginAssessmentPrepareV7(row x.ExecutionScope, roots AssessmentRoots, guard AssessmentBeginGuard) (*AssessmentOperation, bool, error) {
	fresh := false
	e := s.Update(func(d *Data) error {
		if d.Version != 7 || d.AssessmentExecution == nil || guard == nil {
			return x.ErrDenied
		}
		n, e := guard(*d)
		if e != nil {
			return e
		}
		expires, _ := owneddata.Time(row.ExpiresAt, false)
		row.CreatedAt = owneddata.Stamp(n.Wall)
		row.WallHighWater = row.CreatedAt
		row.ClockEpoch = n.Epoch
		row.TickHighWaterMS = n.TickMS
		row.DeadlineTickMS = n.TickMS + expires.Sub(n.Wall).Milliseconds()
		if old, ok := d.AssessmentExecution.Scopes[row.ScopeID]; ok {
			if old.Binding() != row.Binding() || old.ExpiresAt != row.ExpiresAt || old.DeclaredUploadCeilingBytes != row.DeclaredUploadCeilingBytes || old.AdmittingOwnerEpoch != row.AdmittingOwnerEpoch {
				return x.ErrCollision
			}
			return nil
		}
		for _, v := range d.AssessmentExecution.Scopes {
			if v.AssessmentID == row.AssessmentID {
				return x.ErrCollision
			}
		}
		d.AssessmentExecution.Scopes[row.ScopeID] = row
		fresh = true
		return nil
	})
	if e != nil || !fresh {
		return nil, false, e
	}
	return newAssessmentOperation(s, row, nil, "prepare", roots, guard), true, nil
}
func (s *Store) BeginAssessmentScopeRoleV7(c x.Control, role string, roots AssessmentRoots, guard AssessmentBeginGuard) (x.ExecutionScope, *AssessmentOperation, error) {
	var out x.ExecutionScope
	refused := false
	event := x.ScopeWriterClaim
	switch role {
	case "writer":
	case "seal":
		event = x.ScopeSealBegin
	case "cleanup":
		event = x.ScopeCleanupBegin
	default:
		return out, nil, x.ErrInvalid
	}
	if role != "cleanup" {
		if e := assessmentBeforeJournalV7(roots, string(event)); e != nil {
			return out, nil, e
		}
	}
	e := s.Update(func(d *Data) error {
		if d.Version != 7 || d.AssessmentExecution == nil || guard == nil {
			return x.ErrDenied
		}
		row, ok := d.AssessmentExecution.Scopes[c.Binding.ScopeID]
		if !ok || row.Binding() != c.Binding {
			return x.ErrCollision
		}
		switch role {
		case "writer":
			if row.WriterStarted || row.State != "accepting" || !row.PrepareOwnershipCloseConfirmed || c.ExpectedGeneration == nil || row.Generation != *c.ExpectedGeneration || c.WriterEpoch == nil || row.WriterEpoch != *c.WriterEpoch {
				return x.ErrDenied
			}
		case "seal":
			if row.SealStarted || !row.WriterOwnershipCloseConfirmed || c.ExpectedGeneration == nil || row.Generation != *c.ExpectedGeneration || c.WriterEpoch == nil || row.WriterEpoch != *c.WriterEpoch {
				return x.ErrDenied
			}
		case "cleanup":
			if !cleanupReadyV7(d.AssessmentExecution, row) {
				return x.ErrHeld
			}
		default:
			return x.ErrInvalid
		}
		if role != "cleanup" && (row.DenyLatched || row.LeaseCloseUncertain || c.AdmittingOwnerEpoch == nil || row.AdmittingOwnerEpoch != *c.AdmittingOwnerEpoch) {
			return x.ErrDenied
		}
		n, e := guard(*d)
		if e != nil {
			if role == "cleanup" {
				return e
			}
			out, e = denyAssessmentForwardV7(d, row, n)
			refused = true
			return e
		}
		if role != "cleanup" {
			needed := int64(6)
			if role == "seal" {
				needed = 2
			}
			if x.ScopeNormalCapacity-row.NormalTransitionsUsed < needed {
				next, e := x.AdvanceScope(row, x.ScopeDeny, owneddata.Stamp(n.Wall), n.TickMS, func(v *x.ExecutionScope) {
					v.DenyLatched = true
					v.ExecutionDenyLatched = true
					v.State = "cleaning"
					v.Reason = "io_uncertain"
				})
				if e != nil {
					return e
				}
				d.AssessmentExecution.Scopes[row.ScopeID] = next
				refused = true
				return nil
			}
		}
		epoch := ""
		if role == "cleanup" {
			epoch, e = randomID()
			if e != nil {
				return e
			}
		}
		out, e = x.AdvanceScope(row, event, owneddata.Stamp(n.Wall), n.TickMS, func(v *x.ExecutionScope) {
			switch role {
			case "writer":
				v.WriterStarted = true
			case "seal":
				v.SealStarted = true
			case "cleanup":
				v.CleanupOwnerEpoch = &epoch
			}
		})
		if e != nil {
			return e
		}
		d.AssessmentExecution.Scopes[row.ScopeID] = out
		return nil
	})
	if e == nil && refused {
		e = x.ErrDenied
	}
	if e != nil {
		return out, nil, e
	}
	return out, newAssessmentOperation(s, out, nil, role, roots, guard), nil
}
func (s *Store) BeginAssessmentAttemptV7(c x.Control, roots AssessmentRoots, guard AssessmentBeginGuard) (x.AssessmentAttempt, *AssessmentOperation, bool, error) {
	var out x.AssessmentAttempt
	var parent x.ExecutionScope
	fresh := false
	refused := false
	if e := assessmentBeforeJournalV7(roots, "claim"); e != nil {
		return out, nil, false, e
	}
	epoch, e := randomID()
	if e != nil {
		return out, nil, false, e
	}
	e = s.Update(func(d *Data) error {
		if d.Version != 7 || d.AssessmentExecution == nil || guard == nil {
			return x.ErrDenied
		}
		auth := d.AssessmentExecution
		row, ok := auth.Scopes[c.Binding.ScopeID]
		if !ok || row.Binding() != c.Binding || c.Slot == nil || c.HandoffNonce == nil {
			return x.ErrCollision
		}
		if prior, ok := auth.Attempts[c.Slot.AttemptID]; ok {
			if prior.Slot != *c.Slot || prior.HandoffNonce != *c.HandoffNonce || prior.Binding != c.Binding {
				return x.ErrCollision
			}
			out = prior
			return nil
		}
		n, e := guard(*d)
		if e != nil {
			_, e = denyAssessmentForwardV7(d, row, n)
			refused = true
			return e
		}
		if c.ExpectedGeneration == nil || c.AdmittingOwnerEpoch == nil || row.Generation != *c.ExpectedGeneration || row.AdmittingOwnerEpoch != *c.AdmittingOwnerEpoch || row.State != "sealed" || row.DenyLatched || row.Manifest.Design.Sampling != "synthetic_fixture" || CanClaimNextSlotV7(auth, d.Runs, d.NativeRuns) != nil {
			return x.ErrDenied
		}
		if row.NormalTransitionsUsed >= x.ScopeNormalCapacity {
			next, e := x.AdvanceScope(row, x.ScopeDeny, owneddata.Stamp(n.Wall), n.TickMS, func(v *x.ExecutionScope) {
				v.DenyLatched = true
				v.ExecutionDenyLatched = true
				v.State = "cleaning"
				v.Reason = "io_uncertain"
			})
			if e != nil {
				return e
			}
			auth.Scopes[row.ScopeID] = next
			refused = true
			return nil
		}
		if c.Slot.Repetition < 1 || c.Slot.Repetition > int64(len(row.Manifest.Design.Seeds)) {
			return x.ErrDenied
		}
		seed := row.Manifest.Design.Seeds[c.Slot.Repetition-1]
		if seed < 0 || seed > 4294967295 {
			return x.ErrDenied
		}
		if Calculate(*d, row.ProjectID).Remaining < row.Limits.TimeoutSecondsPerAttempt {
			return x.ErrHeld
		}
		expires, _ := owneddata.Time(row.ExpiresAt, false)
		deadline := n.Wall.Truncate(time.Second).Add(time.Duration(row.Limits.TimeoutSecondsPerAttempt) * time.Second)
		if deadline.After(expires) || row.DeadlineTickMS-n.TickMS < row.Limits.TimeoutSecondsPerAttempt*1000 {
			return x.ErrDenied
		}
		for _, a := range auth.Attempts {
			if a.Binding.ScopeID == row.ScopeID && (n.TickMS < a.TickHighWaterMS || owneddata.Stamp(n.Wall) < a.WallHighWater) {
				return x.ErrDenied
			}
		}
		out = x.AssessmentAttempt{Schema: x.AttemptSchema, Binding: c.Binding, Slot: *c.Slot, BindingDigest: x.AttemptBindingDigest(c.Binding, *c.Slot, *c.HandoffNonce), HandoffNonce: *c.HandoffNonce, ClaimEpoch: epoch, ClaimedAt: owneddata.Stamp(n.Wall), Deadline: deadline.Format(owneddata.SecondUTC), ClockEpoch: n.Epoch, DeadlineTickMS: n.TickMS + deadline.Sub(n.Wall).Milliseconds(), Generation: 1, State: "claimed", Outcome: "unknown", FailureCode: "none", Compute: x.Compute{ReservedCPUSeconds: row.Limits.TimeoutSecondsPerAttempt, Provenance: "reserved"}, AccountingUnresolved: true, Metrics: x.UnknownMeasurements(), Quality: x.Quality{Status: "unknown", Source: "none", Checker: row.Capability.Checker}, CleanupObservations: []x.AttemptCleanupObservation{}, WallHighWater: owneddata.Stamp(n.Wall), TickHighWaterMS: n.TickMS}
		parent, e = x.CountSlotClaim(row, owneddata.Stamp(n.Wall), n.TickMS)
		if e != nil {
			return e
		}
		auth.Scopes[row.ScopeID] = parent
		auth.Attempts[out.Slot.AttemptID] = out
		fresh = true
		return nil
	})
	if e == nil && refused {
		e = x.ErrDenied
	}
	if e != nil || !fresh {
		return out, nil, false, e
	}
	return out, newAssessmentOperation(s, parent, &out, "execute", roots, guard), true, nil
}
func (o *AssessmentOperation) valid() (*assessmentOperationState, error) {
	if o == nil || o.state == nil || o.state.store == nil {
		return nil, x.ErrHeld
	}
	return o.state, nil
}
func (v *assessmentOperationState) file(f *os.File, e error) (*assessmentHandle, error) {
	var h *assessmentHandle
	if f != nil {
		h = &assessmentHandle{file: f}
		v.handles = append(v.handles, h)
	}
	if e != nil {
		v.uncertain = true
		return h, e
	}
	if h == nil {
		v.uncertain = true
		return nil, x.ErrHeld
	}
	return h, nil
}
func (v *assessmentOperationState) root(r *os.Root, e error) (*assessmentHandle, error) {
	var h *assessmentHandle
	if r != nil {
		h = &assessmentHandle{root: r}
		v.handles = append(v.handles, h)
	}
	if e != nil || h == nil {
		v.uncertain = true
		return h, x.ErrHeld
	}
	return h, nil
}
func (v *assessmentOperationState) fail(point string) error {
	if v.store.assessmentOwnershipFault != nil && v.store.assessmentOwnershipFault(point) != nil {
		return x.ErrHeld
	}
	if v.roots.Failure != nil && v.roots.Failure(point) != nil {
		return x.ErrHeld
	}
	return nil
}

func (v *assessmentOperationState) release(h *assessmentHandle) error {
	if h.attempted {
		return h.result
	}
	h.attempted = true
	var ue, ce error
	if h.file != nil {
		if h.locked {
			ue = v.fail("ownership-unlock:" + h.label)
			if ue == nil {
				ue = syscall.Flock(int(h.file.Fd()), syscall.LOCK_UN)
			}
		}
		ce = v.fail("ownership-close:" + h.label)
		if ce == nil {
			ce = h.file.Close()
		}
	} else if h.root != nil {
		ce = v.fail("ownership-close:root")
		if ce == nil {
			ce = h.root.Close()
		}
	}
	if ue != nil || ce != nil {
		h.result = x.ErrHeld
		v.uncertain = true
	}
	return h.result
}
func (v *assessmentOperationState) lease(root *os.Root, name string, id x.FilesystemIdentity, mode int) error {
	if root == nil {
		return x.ErrHeld
	}
	f, e := root.OpenFile(name, os.O_RDWR|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	h, e := v.file(f, e)
	if e != nil {
		return e
	}
	h.label = name
	if v.fail("ownership-after-open:"+name) != nil {
		return x.ErrHeld
	}
	st, e := f.Stat()
	var got x.FilesystemIdentity
	if e == nil {
		got, e = FileIdentity(st)
	}
	if e != nil || got != id || st.Mode().Perm()&0077 != 0 {
		return x.ErrHeld
	}
	if e = syscall.Flock(int(f.Fd()), mode|syscall.LOCK_NB); e != nil {
		return x.ErrHeld
	}
	h.locked = true
	st, e = root.Lstat(name)
	if e != nil {
		return x.ErrHeld
	}
	got, e = FileIdentity(st)
	if e != nil || got != id {
		return x.ErrHeld
	}
	return nil
}
func (o *AssessmentOperation) AcquireExecutionSet() error {
	v, e := o.valid()
	if e != nil {
		return e
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.acquisitionStarted || v.closeAttempted || v.role == "prepare" || v.role == "watch" {
		return x.ErrHeld
	}
	v.acquisitionStarted = true
	s := v.scope
	if v.store.ownedIdentity == nil || s.FilesystemIdentity.ExecutionLease == nil || s.FilesystemIdentity.WriterLease == nil {
		return x.ErrHeld
	}
	if e = v.lease(v.roots.State, "execution.lock", v.store.ownedIdentity.ExecutionLease, syscall.LOCK_SH); e != nil {
		return e
	}
	if e = v.lease(v.roots.State, "owned-"+s.ScopeID+"-assessment-execution.lock", *s.FilesystemIdentity.ExecutionLease, syscall.LOCK_EX); e != nil {
		return e
	}
	if e = v.lease(v.roots.State, "owned-"+s.ScopeID+"-writer.lock", *s.FilesystemIdentity.WriterLease, syscall.LOCK_EX); e != nil {
		return e
	}
	v.acquired = true
	return nil
}

// FreshCurrent samples the original guard again under the journal lock after
// acquisition. Only this live operation's exact initial claim is exempted.
func (o *AssessmentOperation) FreshCurrent(guard AssessmentBeginGuard) error {
	v, e := o.valid()
	if e != nil {
		return e
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	v.fresh = false
	if !v.acquired || v.closeAttempted || guard == nil {
		return x.ErrHeld
	}
	e = v.store.View(func(d Data) error {
		n, e := guard(d)
		if e != nil {
			return e
		}
		s, ok := d.AssessmentExecution.Scopes[v.scope.ScopeID]
		if !ok || s.Binding() != v.scope.Binding() || s.LeaseCloseUncertain || s.DenyLatched && v.role != "cleanup" || v.role == "cleanup" && !s.DenyLatched {
			return x.ErrDenied
		}
		if v.role == "cleanup" && (s.CleanupOwnerEpoch == nil || *s.CleanupOwnerEpoch != v.epoch || !cleanupQuiescentV7(d.AssessmentExecution, s)) {
			return x.ErrHeld
		}
		if v.attempt != nil {
			a, ok := d.AssessmentExecution.Attempts[v.attempt.Slot.AttemptID]
			if !ok || a.ClaimEpoch != v.epoch || a.DenyLatched || a.OwnershipCloseUncertain || a.OwnershipCloseConfirmed || n.Epoch != a.ClockEpoch || n.TickMS >= a.DeadlineTickMS || owneddata.Stamp(n.Wall) < a.WallHighWater || n.TickMS < a.TickHighWaterMS {
				return x.ErrDenied
			}
			copy := x.Clone(*d.AssessmentExecution)
			delete(copy.Attempts, a.Slot.AttemptID)
			if CanClaimNextSlotV7(&copy, d.Runs, d.NativeRuns) != nil {
				return x.ErrHeld
			}
		}
		return nil
	})
	if e == nil {
		v.fresh = true
	}
	return e
}

// Abort permanently suppresses forward success. The absorbing denial must be
// committed before an incomplete operation may publish ownership closure.
func (o *AssessmentOperation) Abort(at string, tick int64) error {
	v, e := o.valid()
	if e != nil {
		return e
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	v.aborted = true
	return v.store.Update(func(d *Data) error {
		s := d.AssessmentExecution.Scopes[v.scope.ScopeID]
		if s.DenyLatched {
			return nil
		}
		next, e := x.AdvanceScope(s, x.ScopeDeny, at, tick, func(n *x.ExecutionScope) {
			n.DenyLatched = true
			n.ExecutionDenyLatched = true
			n.State = "cleaning"
			n.Reason = "io_uncertain"
		})
		if e == nil {
			d.AssessmentExecution.Scopes[s.ScopeID] = next
		}
		return e
	})
}
func (o *AssessmentOperation) CloseAndCommitOwnership() error {
	v, e := o.valid()
	if e != nil {
		return e
	}
	v.mu.Lock()
	if v.closeAttempted {
		result := v.result
		v.mu.Unlock()
		return result
	}
	if v.role == "prepare" && !v.ready && !v.aborted && !v.closeAttempted {
		v.mu.Unlock()
		_ = o.Abort(v.scope.WallHighWater, v.scope.TickHighWaterMS)
		v.mu.Lock()
	}
	if v.role == "prepare" && v.aborted && v.watchChild != nil {
		child := v.watchChild
		child.mu.Lock()
		launched := child.watchLaunched
		if !launched {
			child.aborted = true
		} // atomically revoke launch before releasing ownership
		child.mu.Unlock()
		if !launched {
			v.mu.Unlock()
			cop := &AssessmentOperation{child}
			_ = cop.Abort(v.scope.WallHighWater, v.scope.TickHighWaterMS)
			_ = cop.CloseAndCommitOwnership()
			v.mu.Lock()
		}
	}
	defer v.mu.Unlock()
	if v.closeAttempted {
		return v.result
	}
	v.closeAttempted = true
	for i := len(v.handles) - 1; i >= 0; i-- {
		_ = v.release(v.handles[i])
	}
	if !v.aborted && (v.role == "prepare" && !v.ready || v.role != "prepare" && (!v.acquired || !v.fresh)) {
		v.uncertain = true
	}
	v.commitAttempted = true
	observationID := ""
	if v.role == "cleanup" && v.removed && !v.uncertain {
		observationID, e = randomID()
		if e != nil {
			v.result = x.ErrHeld
			return v.result
		}
	}
	positiveDated := false
	proof := &assessmentCompletion{store: v.store, binding: v.scope.Binding(), role: v.role, epoch: v.epoch, removed: v.removed}
	e = v.store.updateAssessment(func(d *Data) error {
		if d.Version != 7 || d.AssessmentExecution == nil {
			return x.ErrDenied
		}
		s, ok := d.AssessmentExecution.Scopes[v.scope.ScopeID]
		if !ok || s.Binding() != v.scope.Binding() || assessmentRoleEpoch(s, v.role) != v.epoch && v.role != "execute" {
			return x.ErrCollision
		}
		if v.role == "cleanup" && v.removed && !cleanupQuiescentV7(d.AssessmentExecution, s) {
			return x.ErrHeld
		}
		at, tick := s.WallHighWater, s.TickHighWaterMS
		clockKnown := false
		if v.guard != nil {
			n, ce := v.guard(*d)
			if ce == nil && n.Epoch == s.ClockEpoch && owneddata.Stamp(n.Wall) >= s.WallHighWater && n.TickMS >= s.TickHighWaterMS {
				at = owneddata.Stamp(n.Wall)
				tick = n.TickMS
				clockKnown = true
			}
		}
		if v.aborted && !s.DenyLatched {
			return x.ErrHeld
		}
		if v.role == "execute" {
			a, ok := d.AssessmentExecution.Attempts[v.attempt.Slot.AttemptID]
			if !ok || a.ClaimEpoch != v.epoch {
				return x.ErrCollision
			}
			event := x.AttemptOwnershipClosed
			if v.uncertain {
				event = x.AttemptOwnershipUncertain
			}
			next, e := x.AdvanceAttempt(a, s, event, a.WallHighWater, a.TickHighWaterMS, func(n *x.AssessmentAttempt) {
				if v.uncertain {
					n.OwnershipCloseUncertain = true
				} else {
					n.OwnershipCloseConfirmed = true
				}
			})
			if e == nil {
				d.AssessmentExecution.Attempts[a.Slot.AttemptID] = next
			}
			return e
		}
		event := x.ScopeLeaseUncertain
		if !v.uncertain {
			switch v.role {
			case "prepare":
				event = x.ScopePrepareClosed
			case "writer":
				event = x.ScopeWriterClosed
			case "seal":
				event = x.ScopeSealClosed
			case "watch":
				event = x.ScopeWatchClosed
			case "cleanup":
				event = x.ScopeCleanupClosed
			default:
				return x.ErrInvalid
			}
		}
		next, e := x.AdvanceScope(s, event, at, tick, func(n *x.ExecutionScope) {
			if v.uncertain {
				n.LeaseCloseUncertain = true
				return
			}
			switch v.role {
			case "prepare":
				n.PrepareOwnershipCloseConfirmed = true
			case "writer":
				n.WriterOwnershipCloseConfirmed = true
			case "seal":
				n.SealOwnershipCloseConfirmed = true
			case "watch":
				n.WatchOwnershipCloseConfirmed = true
			case "cleanup":
				n.CleanupOwnershipCloseConfirmed = true
				if v.removed && clockKnown {
					ob := x.ScopeObservation{Schema: x.ScopeObservationSchema, ObservationID: observationID, ScopeID: s.ScopeID, ScopeGeneration: s.Generation + 1, ObservedAt: at, Method: "synthetic_execution_scope_v1", Assurance: "synthetic", NewAdmission: "denied", WritersDrained: "observed", ReadersDrained: "observed", CollectorDrained: "observed", ScopeAccessRevocation: "observed", OwnedRemoval: "observed_absent", Reason: "none"}
					duplicate := false
					for _, prior := range s.Observations {
						prior.ObservationID = ""
						prior.ObservedAt = ""
						prior.ScopeGeneration = 0
						cmp := ob
						cmp.ObservationID = ""
						cmp.ObservedAt = ""
						cmp.ScopeGeneration = 0
						if prior == cmp {
							positiveDated = true
							duplicate = true
						}
					}
					if !duplicate && !s.ObservationsSaturated {
						if len(s.Observations) < x.MaxObservations {
							n.Observations = append(n.Observations, ob)
							positiveDated = true
						} else {
							n.ObservationsSaturated = true
						}
					}
				}
			}
		})
		if e == nil {
			d.AssessmentExecution.Scopes[s.ScopeID] = next
		}
		return e
	}, proof)
	if e != nil || v.uncertain || v.role == "cleanup" && v.removed && !positiveDated {
		v.result = x.ErrHeld
	}
	return v.result
}
