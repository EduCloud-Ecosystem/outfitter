package broker

import (
	"context"
	"io"
	"os"
	"time"

	x "github.com/EduCloud-Ecosystem/outfitter/internal/assessmentexec"
	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
	"github.com/EduCloud-Ecosystem/outfitter/internal/owneddata"
)

// These interfaces describe the reviewed, version-specific capability. They
// are private and have no production implementation/factory. In particular an
// ordinary Docker Request is never silently expanded to these larger limits.
type assessmentPhaseRequest struct {
	Binding          x.ScopeBinding
	Slot             x.Slot
	Phase, OwnerName string
	Code             x.CodeBinding
	Argv             []string
	Limits           x.Limits
	Deadline         time.Time
	Selection        assessmentSelection
}
type assessmentSelection struct {
	Method string
	Seed   int64
	Slot   x.Slot
}

func assessmentPhaseRequestFor(s x.ExecutionScope, a x.AssessmentAttempt, phase string) (assessmentPhaseRequest, error) {
	if s.Manifest.Design.Sampling != "synthetic_fixture" || a.Slot.Repetition < 1 || a.Slot.Repetition > int64(len(s.Manifest.Design.Seeds)) {
		return assessmentPhaseRequest{}, x.ErrDenied
	}
	seed := s.Manifest.Design.Seeds[a.Slot.Repetition-1]
	if seed < 0 || seed > 4294967295 {
		return assessmentPhaseRequest{}, x.ErrDenied
	}
	code, argv := s.Capability.Workload, s.Capability.WorkloadArgv
	if phase == "checker" {
		code, argv = s.Capability.Checker, s.Capability.CheckerArgv
	} else if phase != "workload" {
		return assessmentPhaseRequest{}, x.ErrInvalid
	}
	deadline, e := owneddata.Time(a.Deadline, false)
	if e != nil {
		return assessmentPhaseRequest{}, x.ErrInvalid
	}
	return assessmentPhaseRequest{Binding: a.Binding, Slot: a.Slot, Phase: phase, OwnerName: x.ResourceName(a.Binding.BrokerAuthorityID, a.Slot.AttemptID, phase), Code: code, Argv: append([]string{}, argv...), Limits: s.Limits, Deadline: deadline, Selection: assessmentSelection{Method: s.Manifest.Design.Sampling, Seed: seed, Slot: a.Slot}}, nil
}

type assessmentResourceObservation struct {
	Identity        x.DockerResourceIdentity
	Exists, Stopped bool
}
type assessmentProvider interface {
	Create(context.Context, assessmentPhaseRequest) (x.DockerResourceIdentity, error)
	Start(context.Context, x.DockerResourceIdentity, io.Reader, []byte, io.Writer, io.Writer) (assessmentCompletion, error)
	Inspect(context.Context, assessmentPhaseRequest) (assessmentResourceObservation, error)
	Stop(context.Context, x.DockerResourceIdentity) error
	Remove(context.Context, x.DockerResourceIdentity) error
}

// Times are observations supplied by the bound provider, never the wall clock
// sampled after a blocking Start call. Missing timing stays unknown.
type assessmentCompletion struct {
	ExitCode              int64
	StartedAt, FinishedAt *time.Time
}
type assessmentCollector interface {
	Observe(context.Context, x.DockerResourceIdentity) (x.MetricSet, error)
	Finish(context.Context) (x.MetricSet, error)
	Close() error
}
type assessmentStorage interface {
	// Admission must enforce the aggregate immutable partitions, including
	// unlinked/open storage. Free-space observations are not a capability.
	Admit(context.Context, x.ExecutionScope) error
	BeforePhase(context.Context, x.ExecutionScope, string) error
	AfterPhase(context.Context, x.ExecutionScope, string) error
	Remove(context.Context, x.ExecutionScope) error
}
type assessmentBoundary struct {
	owned      *ownedBoundary
	watchOwner *ledger.AssessmentOperation
	capability x.CapabilityMapping
	provider   assessmentProvider
	storage    assessmentStorage
	collector  func(context.Context, x.ExecutionScope) (assessmentCollector, error)
	// A real closeable transport is opened only after its journal marker. Tests
	// exercise actual pipes/files and uncertain closes; a receipt cannot supply it.
	transport    func(context.Context, x.ExecutionScope, string) (io.Closer, error)
	checkQuality func([]byte) (string, error)
	spawnWatch   func(*ledger.AssessmentOperation) error
}

func (b *Broker) assessmentAvailable() bool {
	a := b.assessment
	return a != nil && a.owned != nil && a.owned.clock != nil && a.owned.policy != nil && a.capability.Validate() == nil && a.provider != nil && a.storage != nil && a.collector != nil && a.transport != nil && a.checkQuality != nil && a.spawnWatch != nil
}
func (b *Broker) assessmentNow() (ownedClock, error) {
	if b.assessment == nil || b.assessment.owned == nil || b.assessment.owned.clock == nil {
		return ownedClock{}, x.ErrDisabled
	}
	n, e := b.assessment.owned.clock()
	n.Wall = n.Wall.UTC().Truncate(time.Microsecond)
	if e != nil || !owneddata.ID(n.Epoch) || !owneddata.Uint(n.TickMS) || n.Wall.IsZero() {
		return ownedClock{}, x.ErrHeld
	}
	return n, nil
}
func (b *Broker) assessmentFault(point string) error {
	if b.assessment != nil && b.assessment.owned != nil && b.assessment.owned.fail(point) != nil {
		return x.ErrHeld
	}
	return nil
}
func (a *assessmentBoundary) current(s x.ExecutionScope, n ownedClock) error {
	o := a.owned
	if s.DenyLatched || s.ExecutionDenyLatched || s.LeaseCloseUncertain || s.ContentCloseUncertain || s.BoundaryID != o.id || s.CapabilitySHA256 != x.Digest(a.capability) || ownedClockReason(s.OwnedProjection(), n) != "" || assessmentRootMetadata(o.root, s.FilesystemIdentity.Root) != nil {
		return x.ErrDenied
	}
	if livePipe(o.ownerRead, s.FilesystemIdentity.OwnerRead) != "observed_current" || s.WatchStarted && livePipe(o.watchRead, s.FilesystemIdentity.WatchRead) != "observed_current" {
		return x.ErrDenied
	}
	for _, part := range []struct {
		name string
		id   *x.FilesystemIdentity
	}{{"writer", s.FilesystemIdentity.WriterLease}, {"watch", s.FilesystemIdentity.WatchLease}, {"assessment-execution", s.FilesystemIdentity.ExecutionLease}} {
		if part.id != nil && sameRootEntry(o.stateRoot, leaseName(s.ScopeID, part.name), *part.id, false) != nil {
			return x.ErrHeld
		}
	}
	return nil
}
func assessmentRootMetadata(root *os.Root, id x.FilesystemIdentity) error {
	if root == nil || sameRootEntry(root, ".", id, true) != nil {
		return x.ErrHeld
	}
	st, e := os.Lstat(root.Name())
	if e != nil {
		return x.ErrHeld
	}
	got, e := ledger.FileIdentity(st)
	if e != nil || got != id || st.Mode()&os.ModeSymlink != 0 || st.Mode().Perm()&0077 != 0 {
		return x.ErrHeld
	}
	return nil
}
func (b *Broker) assessmentRoots() ledger.AssessmentRoots {
	return ledger.AssessmentRoots{Data: b.assessment.owned.root, State: b.assessment.owned.stateRoot, Failure: b.assessment.owned.fail}
}
func (b *Broker) assessmentBeginGuard(binding x.ScopeBinding, denied bool) ledger.AssessmentBeginGuard {
	return func(d ledger.Data) (ledger.AssessmentBeginClock, error) {
		n, e := b.assessmentNow()
		clock := ledger.AssessmentBeginClock{Wall: n.Wall, TickMS: n.TickMS, Epoch: n.Epoch}
		if e != nil || d.Version != 7 || d.AssessmentExecution == nil || d.Authority.ID != binding.BrokerAuthorityID || assessmentRootMetadata(b.assessment.owned.stateRoot, d.AssessmentExecution.Identity.Directory) != nil {
			return clock, x.ErrDenied
		}
		s, ok := d.AssessmentExecution.Scopes[binding.ScopeID]
		if !ok {
			return clock, x.ErrCollision
		}
		if s.Binding() != binding {
			return clock, x.ErrCollision
		}
		if denied {
			if !s.DenyLatched || s.LeaseCloseUncertain || assessmentRootMetadata(b.assessment.owned.root, s.FilesystemIdentity.Root) != nil {
				return clock, x.ErrHeld
			}
			return clock, nil
		}
		if b.assessment.current(s, n) != nil {
			return clock, x.ErrDenied
		}
		policy, e := b.assessment.owned.policy(s.ProjectID, s.AdmittingOwnerEpoch)
		if e != nil || !policy.ProjectEnd.After(n.Wall) || !policy.AuthorizationExpiry.After(n.Wall) || !policy.PolicyExpiry.After(n.Wall) {
			return clock, x.ErrDenied
		}
		return clock, nil
	}
}

func assessmentError(e error) error {
	if e == nil {
		return nil
	}
	switch e {
	case x.ErrInvalid, x.ErrDisabled, x.ErrDenied, x.ErrCollision, x.ErrCapacity, x.ErrHeld, x.ErrMissing, x.ErrStale, x.ErrBusy:
		return e
	}
	return x.ErrHeld
}
