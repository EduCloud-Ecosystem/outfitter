package broker

import (
	"context"
	"io"
	"reflect"

	x "github.com/EduCloud-Ecosystem/outfitter/internal/assessmentexec"
	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
	"github.com/EduCloud-Ecosystem/outfitter/internal/owneddata"
)

// AssessmentData is a version-specific branch on the same broker. Production
// construction cannot prepare, ingest or seal, even when handed valid metadata.
func (b *Broker) AssessmentData(ctx context.Context, c x.Control, body io.Reader) (x.Status, error) {
	if c.Operation == "prepare" || c.Operation == "ingest" || c.Operation == "seal" {
		if !b.assessmentAvailable() {
			return x.Status{}, x.ErrDisabled
		}
	}
	if c.Validate() != nil || c.Operation == "execute" {
		return x.Status{}, x.ErrInvalid
	}
	s, _, e := b.assessmentRead(c.Binding)
	if c.Operation == "prepare" {
		return b.assessmentPrepare(ctx, c)
	}
	if e != nil {
		return x.Status{}, e
	}
	// Resolve the exact permanent attempt before any cancellation or cleanup.
	// A slot-qualified operation never widens into a scope-wide operation.
	if c.Slot != nil {
		if _, _, e = b.assessmentAttempt(c.Binding, *c.Slot); e != nil {
			return x.Status{}, e
		}
	}
	switch c.Operation {
	case "ingest":
		e = b.assessmentIngest(ctx, c, body)
	case "seal":
		e = b.assessmentSeal(c)
	case "cancel":
		if c.Slot == nil {
			_, e = b.assessmentDeny(c.Binding, *c.Reason)
			if e == nil {
				e = b.assessmentCleanup(ctx, c.Binding)
			}
		} else {
			_, e = b.assessmentChangeAttempt(c.Binding, *c.Slot, x.AttemptDeny, func(a *x.AssessmentAttempt) error {
				if !a.DenyLatched {
					a.DenyLatched = true
					a.FailureCode = *c.Reason
				}
				return nil
			})
			if e == nil {
				e = b.assessmentCleanupAttempt(ctx, c)
			}
		}
	case "reconcile":
		if c.Slot == nil {
			e = b.assessmentCleanup(ctx, c.Binding)
		} else {
			e = b.assessmentCleanupAttempt(ctx, c)
		}
	case "status":
	default:
		return x.Status{}, x.ErrInvalid
	}
	if e != nil {
		return x.Status{}, e
	}
	s, _, e = b.assessmentRead(c.Binding)
	if e != nil {
		return x.Status{}, e
	}
	var attempt *x.AssessmentAttempt
	if c.Slot != nil {
		_, a, ae := b.assessmentAttempt(c.Binding, *c.Slot)
		if ae != nil {
			return x.Status{}, ae
		}
		attempt = &a
	}
	code := "OK"
	if s.DenyLatched {
		code = "HELD"
	}
	return s.Status(c.Operation, code, "unknown", attempt), nil
}

func (b *Broker) assessmentPrepare(ctx context.Context, c x.Control) (x.Status, error) {
	var empty x.Status
	if !b.assessmentAvailable() || c.Validate() != nil || c.Operation != "prepare" {
		return empty, x.ErrDisabled
	}
	a := b.assessment
	o := a.owned
	n, e := b.assessmentNow()
	if e != nil {
		return empty, e
	}
	policy, e := o.policy(c.Binding.ProjectID, *c.AdmittingOwnerEpoch)
	if e != nil {
		return empty, x.ErrDenied
	}
	expires, _ := owneddata.Time(*c.ExpiresAt, false)
	if !expires.After(n.Wall) || expires.After(policy.ProjectEnd) || expires.After(policy.AuthorizationExpiry) || expires.After(policy.PolicyExpiry) || *c.DeclaredUploadCeilingBytes > policy.MaxUploadBytes || !reflect.DeepEqual(c.Binding.Workload, a.capability.Workload) || !reflect.DeepEqual(c.Binding.Checker, a.capability.Checker) || c.Binding.Capability.SHA256 != x.Digest(a.capability) || x.LimitsFor(*c.Manifest) != a.capability.Limits || assessmentRootMetadata(o.root, o.rootIdentity) != nil {
		return empty, x.ErrDenied
	}
	owner, e := pipeIdentity(o.ownerRead, false)
	if e != nil || livePipe(o.ownerRead, owner) != "observed_current" {
		return empty, x.ErrDenied
	}
	watchRead, e := pipeIdentity(o.watchRead, false)
	if e != nil {
		return empty, x.ErrHeld
	}
	watchWrite, e := pipeIdentity(o.watchWrite, true)
	if e != nil {
		return empty, x.ErrHeld
	}
	writer, e := ownedID()
	if e != nil {
		return empty, x.ErrHeld
	}
	s := x.ExecutionScope{Schema: x.ScopeSchema, ScopeID: c.Binding.ScopeID, AssessmentID: c.Binding.AssessmentID, ProjectID: c.Binding.ProjectID, WaypointAuthorityID: c.Binding.WaypointAuthorityID, BrokerAuthorityID: c.Binding.BrokerAuthorityID, ManifestSHA256: c.Binding.ManifestSHA256, ManifestExpiresAt: c.Manifest.ExpiresAt, BoundaryID: o.id, CreatedAt: owneddata.Stamp(n.Wall), ExpiresAt: *c.ExpiresAt, WallHighWater: owneddata.Stamp(n.Wall), Generation: 1, State: "preparing", Reason: "none", WriterEpoch: writer, Assurance: "synthetic", Observations: []x.ScopeObservation{}, DeclaredUploadCeilingBytes: *c.DeclaredUploadCeilingBytes, AdmittingOwnerEpoch: *c.AdmittingOwnerEpoch, ClockEpoch: n.Epoch, DeadlineTickMS: n.TickMS + expires.Sub(n.Wall).Milliseconds(), TickHighWaterMS: n.TickMS, FilesystemIdentity: x.ExecutionFilesystemBinding{Root: o.rootIdentity, OwnerRead: owner, WatchRead: watchRead, WatchWrite: watchWrite}, Manifest: *c.Manifest, ManifestSchema: c.Manifest.Schema, ExecutionProfile: c.Manifest.ExecutionProfile, Capability: a.capability, CapabilitySHA256: x.Digest(a.capability), Limits: a.capability.Limits, Slots: x.Clone(c.Manifest.Reservations)}
	if b.assessmentFault("before-journal:prepare") != nil {
		return empty, x.ErrHeld
	}
	guard := func(d ledger.Data) (ledger.AssessmentBeginClock, error) {
		n, e := b.assessmentNow()
		clock := ledger.AssessmentBeginClock{Wall: n.Wall, TickMS: n.TickMS, Epoch: n.Epoch}
		if e != nil || ctx.Err() != nil || d.Version != 7 || d.AssessmentExecution == nil || d.Authority.ID != c.Binding.BrokerAuthorityID || d.AssessmentExecution.Identity.WaypointAuthorityID != c.Binding.WaypointAuthorityID || assessmentRootMetadata(o.stateRoot, d.AssessmentExecution.Identity.Directory) != nil || assessmentRootMetadata(o.root, o.rootIdentity) != nil || livePipe(o.ownerRead, owner) != "observed_current" {
			return clock, x.ErrDenied
		}
		policy, e := o.policy(c.Binding.ProjectID, *c.AdmittingOwnerEpoch)
		if e != nil || !expires.After(n.Wall) || expires.After(policy.ProjectEnd) || expires.After(policy.AuthorizationExpiry) || expires.After(policy.PolicyExpiry) || *c.DeclaredUploadCeilingBytes > policy.MaxUploadBytes {
			return clock, x.ErrDenied
		}
		if _, ok := d.Projects[s.ProjectID]; !ok {
			return clock, x.ErrDenied
		}
		return clock, nil
	}
	ownerOp, fresh, e := b.Store.BeginAssessmentPrepareV7(s, b.assessmentRoots(), guard)
	if e != nil {
		return empty, assessmentError(e)
	}
	if b.assessmentFault("after-journal:prepare") != nil {
		return empty, x.ErrHeld
	}
	if !fresh {
		current, _, e := b.assessmentRead(c.Binding)
		return current.Status("prepare", "OK", "unknown", nil), e
	}
	completed := false
	defer func() {
		if !completed {
			n, _ := b.assessmentNow()
			_ = ownerOp.Abort(owneddata.Stamp(n.Wall), n.TickMS)
			_ = ownerOp.CloseAndCommitOwnership()
			_ = b.assessmentCleanup(context.WithoutCancel(ctx), c.Binding)
		}
	}()
	s, _, e = b.assessmentRead(c.Binding)
	if e != nil {
		return empty, e
	}
	if ctx.Err() != nil || a.storage.Admit(ctx, s) != nil || pairedWatchPipe(o.watchRead, o.watchWrite) != nil || b.assessmentFault("before-scope-create") != nil {
		return empty, x.ErrHeld
	}
	fs, createErr := ownerOp.CreateSetup()
	for _, part := range []struct {
		kind string
		id   *x.FilesystemIdentity
	}{{"writer", fs.WriterLease}, {"watch", fs.WatchLease}, {"scope", fs.Scope}, {"execution", fs.ExecutionLease}, {"work", fs.WorkRoot}, {"output", fs.OutputRoot}} {
		if part.id == nil {
			continue
		}
		_, e = b.assessmentChangeScope(c.Binding, x.ScopeIdentity, func(v *x.ExecutionScope) error {
			switch part.kind {
			case "writer":
				v.FilesystemIdentity.WriterLease = part.id
			case "watch":
				v.FilesystemIdentity.WatchLease = part.id
			case "scope":
				v.FilesystemIdentity.Scope = part.id
			case "execution":
				v.FilesystemIdentity.ExecutionLease = part.id
			case "work":
				v.FilesystemIdentity.WorkRoot = part.id
			case "output":
				v.FilesystemIdentity.OutputRoot = part.id
			}
			return nil
		})
		if e != nil {
			return empty, e
		}
	}
	if createErr != nil || b.assessmentFault("after-scope-create") != nil {
		return empty, x.ErrHeld
	}
	child, e := ownerOp.ReserveWatch(o.watchWrite, b.assessmentBeginGuard(c.Binding, false))
	if e != nil {
		return empty, e
	}
	if e = a.spawnWatch(child); e != nil {
		return empty, e
	}
	if e = ownerOp.ReadAndValidateReady(ctx); e != nil {
		return empty, e
	}
	if e = ownerOp.CloseAndCommitOwnership(); e != nil {
		return empty, e
	}
	completed = true
	s, e = b.assessmentFresh(c.Binding, nil)
	if e != nil {
		return empty, e
	}
	return s.Status("prepare", "OK", "observed_current", nil), nil
}

func (b *Broker) assessmentIngest(ctx context.Context, c x.Control, body io.Reader) (err error) {
	if body == nil {
		return x.ErrInvalid
	}
	s, e := b.assessmentFresh(c.Binding, nil)
	if e != nil {
		return e
	}
	if s.Generation != *c.ExpectedGeneration || s.AdmittingOwnerEpoch != *c.AdmittingOwnerEpoch || s.WriterEpoch != *c.WriterEpoch || s.State != "accepting" || s.WriterStarted || *c.ContentLength > s.DeclaredUploadCeilingBytes {
		return x.ErrDenied
	}
	s, owner, e := b.Store.BeginAssessmentScopeRoleV7(c, "writer", b.assessmentRoots(), b.assessmentBeginGuard(c.Binding, false))
	if e != nil {
		return assessmentError(e)
	}
	completed := false
	defer func() {
		if !completed {
			n, _ := b.assessmentNow()
			_ = owner.Abort(owneddata.Stamp(n.Wall), n.TickMS)
			_ = owner.CloseAndCommitOwnership()
		}
		_ = b.assessmentCleanup(context.WithoutCancel(ctx), c.Binding)
	}()
	if owner.AcquireExecutionSet() != nil || owner.FreshCurrent(b.assessmentBeginGuard(c.Binding, false)) != nil {
		return x.ErrHeld
	}
	var file *ledger.AssessmentFile
	closeUncertain := false
	defer func() {
		if file != nil {
			ce := file.CloseWithPoint("content-close:assessment-deferred")
			closeUncertain = closeUncertain || ce != nil
			_, _ = b.assessmentDeny(c.Binding, "io_uncertain")
			if closeUncertain {
				_, _ = b.assessmentChangeScope(c.Binding, x.ScopeContentUncertain, func(v *x.ExecutionScope) error { v.ContentCloseUncertain = true; return nil })
			} else {
				_, pe := b.assessmentChangeScope(c.Binding, x.ScopeContentClosed, func(v *x.ExecutionScope) error { v.ContentCloseConfirmed = true; return nil })
				if pe != nil {
					closeUncertain = true
					_, _ = b.assessmentChangeScope(c.Binding, x.ScopeContentUncertain, func(v *x.ExecutionScope) error { v.ContentCloseUncertain = true; return nil })
				}
			}
			err = x.ErrHeld
		}
		if err != nil {
			_, _ = b.assessmentDeny(c.Binding, "incomplete_upload")
		}
	}()
	file, identity, e := owner.OpenContent()
	if e != nil {
		return x.ErrHeld
	}
	s, e = b.assessmentChangeScope(c.Binding, x.ScopeContentIdentity, func(v *x.ExecutionScope) error { v.FilesystemIdentity.Content = &identity; return nil })
	if e != nil {
		return e
	}
	buf := make([]byte, 64<<10)
	defer clear(buf)
	remaining := *c.ContentLength
	empty := 0
	for remaining > 0 {
		if ctx.Err() != nil {
			return x.ErrDenied
		}
		if _, e = b.assessmentFresh(c.Binding, nil); e != nil {
			return e
		}
		want := int64(len(buf))
		if remaining < want {
			want = remaining
		}
		n, re := body.Read(buf[:want])
		if n < 0 || int64(n) > want {
			return x.ErrHeld
		}
		if n == 0 {
			empty++
			if empty > 8 {
				return x.ErrHeld
			}
		} else {
			empty = 0
		}
		if ctx.Err() != nil {
			return x.ErrDenied
		}
		if _, e = b.assessmentFresh(c.Binding, nil); e != nil {
			return e
		}
		if n > 0 {
			wn, we := file.Write(buf[:n])
			if we != nil || wn != n {
				return x.ErrHeld
			}
			remaining -= int64(n)
		}
		if re != nil {
			if re != io.EOF || remaining != 0 {
				return x.ErrHeld
			}
			break
		}
	}
	var one [1]byte
	n, re := body.Read(one[:])
	if n != 0 || re != io.EOF || ctx.Err() != nil {
		return x.ErrHeld
	}
	if _, e = b.assessmentFresh(c.Binding, nil); e != nil {
		return e
	}
	if file.Sync() != nil {
		return x.ErrHeld
	}
	if file.CloseWithPoint("content-close:assessment-main") != nil {
		closeUncertain = true
		_, _ = b.assessmentDeny(c.Binding, "io_uncertain")
		_, _ = b.assessmentChangeScope(c.Binding, x.ScopeContentUncertain, func(v *x.ExecutionScope) error { v.ContentCloseUncertain = true; return nil })
		return x.ErrHeld
	}
	file = nil
	_, e = b.assessmentChangeScope(c.Binding, x.ScopeContentClosed, func(v *x.ExecutionScope) error { v.ContentCloseConfirmed = true; return nil })
	if e != nil {
		closeUncertain = true
		_, _ = b.assessmentDeny(c.Binding, "io_uncertain")
		_, _ = b.assessmentChangeScope(c.Binding, x.ScopeContentUncertain, func(v *x.ExecutionScope) error { v.ContentCloseUncertain = true; return nil })
		return x.ErrHeld
	}
	if e = owner.CloseAndCommitOwnership(); e != nil {
		return e
	}
	completed = true
	_, e = b.assessmentChangeScope(c.Binding, x.ScopeUploadComplete, func(v *x.ExecutionScope) error {
		v.UploadComplete = true
		n := *c.ContentLength
		v.SealedLengthBytes = &n
		return nil
	})
	return e
}
func (b *Broker) assessmentSeal(c x.Control) (err error) {
	_, owner, e := b.Store.BeginAssessmentScopeRoleV7(c, "seal", b.assessmentRoots(), b.assessmentBeginGuard(c.Binding, false))
	if e != nil {
		return assessmentError(e)
	}
	completed := false
	defer func() {
		if !completed {
			n, _ := b.assessmentNow()
			_ = owner.Abort(owneddata.Stamp(n.Wall), n.TickMS)
			_ = owner.CloseAndCommitOwnership()
		}
	}()
	if owner.AcquireExecutionSet() != nil || owner.FreshCurrent(b.assessmentBeginGuard(c.Binding, false)) != nil {
		return x.ErrHeld
	}
	if e = owner.CloseAndCommitOwnership(); e != nil {
		return e
	}
	completed = true
	_, e = b.assessmentChangeScope(c.Binding, x.ScopeSeal, func(v *x.ExecutionScope) error {
		if !v.SealOwnershipCloseConfirmed || v.DenyLatched || v.LeaseCloseUncertain {
			return x.ErrHeld
		}
		v.State = "sealed"
		return nil
	})
	return e
}
