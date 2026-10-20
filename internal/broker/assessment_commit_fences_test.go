package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	x "github.com/EduCloud-Ecosystem/outfitter/internal/assessmentexec"
	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
)

func executionIngestControl(f *executionFixture, s x.Status, n int64) x.Control {
	gen, writer := s.Generation, s.WriterEpoch
	return x.Control{Schema: x.ControlSchema, Operation: "ingest", Binding: f.c.Binding, ExpectedGeneration: &gen, AdmittingOwnerEpoch: f.c.AdmittingOwnerEpoch, WriterEpoch: &writer, ContentLength: &n}
}

// Pause the authentic watcher before it acquires its authority snapshot.
// Counter witnesses then compare only the operation under test; cleanup always
// releases the watcher before the enclosing fixture closes borrowed handles.
func executionPauseWatchBeforeSnapshot(t *testing.T, f *executionFixture) func() {
	t.Helper()
	entered, release := make(chan struct{}), make(chan struct{})
	var enteredOnce atomic.Bool
	var releaseOnce sync.Once
	prior := f.b.assessment.owned.fault
	f.b.assessment.owned.fault = func(point string) error {
		if point == "watch-before-snapshot" && enteredOnce.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
		if prior != nil {
			return prior(point)
		}
		return nil
	}
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	return func() { executionWait(t, entered) }
}
func TestExecutionPositiveCommitFencesRecheckCurrentAuthority(t *testing.T) {
	for _, target := range []x.Event{x.ScopeWatchReady, x.ScopeWriterClaim, x.ScopeUploadComplete, x.ScopeSeal} {
		for _, loss := range []string{"expiry", "owner", "watch"} {
			t.Run(string(target)+"/"+loss, func(t *testing.T) {
				f := newExecutionFixture(t)
				var shift atomic.Int64
				clock := f.b.assessment.owned.clock
				f.b.assessment.owned.clock = func() (ownedClock, error) {
					n, e := clock()
					n.Wall = n.Wall.Add(time.Duration(shift.Load()))
					n.TickMS += shift.Load() / int64(time.Millisecond)
					return n, e
				}
				var hit atomic.Bool
				fault := func(point string) error {
					if point == "before-journal:"+string(target) && hit.CompareAndSwap(false, true) {
						switch loss {
						case "expiry":
							shift.Store(int64(31 * time.Minute))
						case "owner":
							_ = f.ownerWrite.Close()
						case "watch":
							_ = f.b.assessment.owned.watchRead.Close()
						}
					}
					return nil
				}
				if target == x.ScopeWatchReady {
					f.b.assessment.owned.fault = fault
				}
				status, e := f.b.AssessmentData(context.Background(), f.c, nil)
				if target != x.ScopeWatchReady {
					if e != nil {
						t.Fatal("prepare", e)
					}
					payload := []byte("PRIVATE_INPUT_POISON\n")
					c := executionIngestControl(f, status, int64(len(payload)))
					if target != x.ScopeSeal {
						f.b.assessment.owned.fault = fault
					}
					status, e = f.b.AssessmentData(context.Background(), c, bytes.NewReader(payload))
					if target == x.ScopeSeal {
						if e != nil {
							t.Fatal("ingest", e)
						}
						c = executionIngestControl(f, status, 0)
						c.Operation = "seal"
						c.ContentLength = nil
						f.b.assessment.owned.fault = fault
						_, e = f.b.AssessmentData(context.Background(), c, nil)
					}
				}
				if e == nil || !hit.Load() {
					t.Fatal("positive transition passed lost authority", e)
				}
				scope, _, e := f.b.assessmentRead(f.c.Binding)
				if loss == "watch" && e != nil { // Loss of the bound descriptor may also deny reads.
					e = f.b.Store.View(func(d ledger.Data) error { scope = d.AssessmentExecution.Scopes[f.c.Binding.ScopeID]; return nil })
				}
				if e != nil || !scope.DenyLatched {
					t.Fatal("commit did not latch denial", e)
				}
				if target == x.ScopeWriterClaim && scope.WriterStarted || target == x.ScopeUploadComplete && scope.UploadComplete || target == x.ScopeSeal && scope.State == "sealed" || target == x.ScopeWatchReady && scope.State == "accepting" {
					t.Fatal("stale positive commit", target)
				}
				usage, e := f.b.Store.Usage(f.c.Binding.ProjectID)
				if e != nil || usage.Remaining != 60 || usage.Charged != 0 || usage.Reserved != 0 {
					t.Fatal("upload fabricated compute grant/charge", usage, e)
				}
			})
		}
	}
}
func TestExecutionMainCloseFailureSurvivesUncertaintyPublicationFault(t *testing.T) {
	for _, publication := range []string{"none", "before", "after"} {
		t.Run(publication, func(t *testing.T) {
			f := newExecutionFixture(t)
			status, e := f.b.AssessmentData(context.Background(), f.c, nil)
			if e != nil {
				t.Fatal(e)
			}
			contentFailure, publishFailure := false, false
			f.b.assessment.owned.fault = func(point string) error {
				if point == "content-close:assessment-main" {
					contentFailure = true
					return errors.New("synthetic retained open descriptor")
				}
				if !publishFailure && publication != "none" && point == publication+"-journal:"+string(x.ScopeContentUncertain) {
					publishFailure = true
					return errors.New("synthetic uncertainty publication failure")
				}
				return nil
			}
			body := []byte("PRIVATE_INPUT_POISON\n")
			c := executionIngestControl(f, status, int64(len(body)))
			if _, e = f.b.AssessmentData(context.Background(), c, bytes.NewReader(body)); e == nil {
				t.Fatal("uncertain close accepted")
			}
			if !contentFailure || publication != "none" && !publishFailure {
				t.Fatal("fault not reached")
			}
			scope, _, e := f.b.assessmentRead(c.Binding)
			if e != nil || !scope.ContentCloseUncertain || scope.ContentCloseConfirmed || scope.UploadComplete || !scope.DenyLatched {
				t.Fatal("later successful Close erased uncertainty", e)
			}
			cc := x.Control{Schema: x.ControlSchema, Operation: "reconcile", Binding: c.Binding}
			_, _ = f.b.AssessmentData(context.Background(), cc, nil)
			scope, _, e = f.b.assessmentRead(c.Binding)
			if e != nil {
				t.Fatal(e)
			}
			for _, obs := range scope.Observations {
				if obs.WritersDrained == "observed" || obs.OwnedRemoval == "observed_absent" {
					t.Fatal("uncertainty promoted to deletion/drain")
				}
			}
		})
	}
}

// This private fault fixture represents an otherwise valid authority whose
// forward bank is exhausted. It is not a production counter mutation API.
func executionSeedScopeNormalCounter(t *testing.T, f *executionFixture, used int64) x.Status {
	t.Helper()
	path := filepath.Join(f.b.Store.Dir, "ledger.json")
	raw, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	var d ledger.Data
	if e = json.Unmarshal(raw, &d); e != nil {
		t.Fatal(e)
	}
	scope := d.AssessmentExecution.Scopes[f.c.Binding.ScopeID]
	scope.NormalTransitionsUsed = used
	scope.Generation = 1 + scope.NormalTransitionsUsed + scope.SafetyTransitionsUsed
	d.AssessmentExecution.Scopes[scope.ScopeID] = scope
	raw, e = json.Marshal(d)
	if e != nil {
		t.Fatal(e)
	}
	tmp := filepath.Join(f.b.Store.Dir, "private-counter-fixture.tmp")
	if e = os.WriteFile(tmp, raw, 0600); e != nil {
		t.Fatal(e)
	}
	if e = os.Rename(tmp, path); e != nil {
		t.Fatal(e)
	}
	// Reopening goes through the actual serializer's independent static validator.
	if _, e = ledger.Open(f.b.Store.Dir, false); e != nil {
		t.Fatal("counter fixture invalid", e)
	}
	return scope.Status("status", "OK", "unknown", nil)
}
func TestExecutionNormalExhaustionPersistsSafetyDenial(t *testing.T) {
	for _, point := range []string{"writer-reserve-five", "writer-six-required-five-left", "slot-reserve-one"} {
		t.Run(point, func(t *testing.T) {
			f := newExecutionFixture(t)
			paused := executionPauseWatchBeforeSnapshot(t, f)
			var status x.Status
			var e error
			if strings.HasPrefix(point, "writer-") {
				status, e = f.b.AssessmentData(context.Background(), f.c, nil)
			} else {
				status = f.seal(t)
			}
			if e != nil {
				t.Fatal(e)
			}
			paused()
			prior, _, e := f.b.assessmentRead(f.c.Binding)
			if e != nil {
				t.Fatal(e)
			}
			// The historical used44 case is retained; used43 is the new exact
			// six-event writer boundary (five remaining must still refuse).
			normal := int64(44)
			if point == "writer-six-required-five-left" {
				normal = 43
			}
			if point == "slot-reserve-one" {
				normal = 48
			}
			status = executionSeedScopeNormalCounter(t, f, normal)
			if strings.HasPrefix(point, "writer-") {
				body := []byte("PRIVATE_INPUT_POISON\n")
				c := executionIngestControl(f, status, int64(len(body)))
				_, e = f.b.AssessmentData(context.Background(), c, bytes.NewReader(body))
			} else {
				_, e = f.b.RunAssessmentOnce(context.Background(), executionControl(f, status, 0))
			}
			if e == nil {
				t.Fatal("exhausted normal bank admitted forward work")
			}
			scope, _, e := f.b.assessmentRead(f.c.Binding)
			if e != nil || !scope.DenyLatched || !scope.ExecutionDenyLatched || scope.NormalTransitionsUsed != normal || scope.SafetyTransitionsUsed != prior.SafetyTransitionsUsed+1 {
				t.Fatal("exhausted normal refusal omitted permanent safety denial", e)
			}
			if strings.HasPrefix(point, "writer-") && scope.WriterStarted {
				t.Fatal("writer started")
			}
			usage, e := f.b.Store.Usage(f.c.Binding.ProjectID)
			if e != nil || usage.Remaining != 60 || usage.Reserved != 0 || usage.Charged != 0 {
				t.Fatal("counter refusal changed budget", e)
			}
			if f.p.starts != 0 || len(f.p.requests) != 0 {
				t.Fatal("counter refusal touched provider")
			}
		})
	}
}

// A real fixed BEGIN must retain denial even when its fresh clock cannot be
// published. These are valid pre-role states; no positive role is seeded.
func TestExecutionFixedFreshGuardFailureDeniesBeforeClaim(t *testing.T) {
	for _, role := range []string{"writer", "seal", "attempt"} {
		for _, loss := range []string{"clock-unavailable", "clock-backward"} {
			t.Run(role+"/"+loss, func(t *testing.T) {
				f := newExecutionFixture(t)
				paused := executionPauseWatchBeforeSnapshot(t, f)
				var status x.Status
				var e error
				if role == "attempt" {
					status = f.seal(t)
				} else {
					status, e = f.b.AssessmentData(context.Background(), f.c, nil)
					if e != nil {
						t.Fatal(e)
					}
					if role == "seal" {
						body := []byte("PRIVATE_INPUT_POISON\n")
						status, e = f.b.AssessmentData(context.Background(), executionIngestControl(f, status, int64(len(body))), bytes.NewReader(body))
						if e != nil {
							t.Fatal(e)
						}
					}
				}
				paused()
				before, _, e := f.b.assessmentRead(f.c.Binding)
				if e != nil {
					t.Fatal(e)
				}
				clock := f.b.assessment.owned.clock
				point := "before-journal:" + string(x.ScopeWriterClaim)
				if role == "seal" {
					point = "before-journal:" + string(x.ScopeSealBegin)
				}
				if role == "attempt" {
					point = "before-journal:claim"
				}
				var hit, opened atomic.Int32
				f.b.assessment.owned.fault = func(p string) error {
					if strings.HasPrefix(p, "ownership-after-open:") {
						opened.Add(1)
					}
					if p == point && hit.Add(1) == 1 {
						f.b.assessment.owned.clock = func() (ownedClock, error) {
							n, e := clock()
							if loss == "clock-unavailable" {
								return ownedClock{}, errors.New("synthetic clock unavailable")
							}
							n.Wall = n.Wall.Add(-time.Hour)
							n.TickMS = 0
							return n, e
						}
					}
					return nil
				}
				var op *ledger.AssessmentOperation
				if role == "attempt" {
					_, op, _, e = f.b.Store.BeginAssessmentAttemptV7(executionControl(f, status, 0), f.b.assessmentRoots(), f.b.assessmentBeginGuard(f.c.Binding, false))
				} else {
					_, op, e = f.b.Store.BeginAssessmentScopeRoleV7(executionIngestControl(f, status, 0), role, f.b.assessmentRoots(), f.b.assessmentBeginGuard(f.c.Binding, false))
				}
				if !errors.Is(e, x.ErrDenied) || op != nil || hit.Load() != 1 || opened.Load() != 0 {
					t.Fatal("failed guard gained owner/effects", e, hit.Load(), opened.Load())
				}
				var after x.ExecutionScope
				e = f.b.Store.View(func(d ledger.Data) error {
					after = d.AssessmentExecution.Scopes[f.c.Binding.ScopeID]
					if len(d.AssessmentExecution.Attempts) != 0 {
						t.Error("guard failure reserved an attempt")
					}
					return nil
				})
				if e != nil || !after.DenyLatched || !after.ExecutionDenyLatched || after.WallHighWater != before.WallHighWater || after.TickHighWaterMS != before.TickHighWaterMS || after.NormalTransitionsUsed != before.NormalTransitionsUsed || after.SafetyTransitionsUsed != before.SafetyTransitionsUsed+1 || after.WriterStarted != before.WriterStarted || after.SealStarted != before.SealStarted {
					t.Fatal("guard denial lost monotone held state", e)
				}
				u, e := f.b.Store.Usage(f.c.Binding.ProjectID)
				if e != nil || u.Reserved != 0 || u.Charged != 0 || u.Remaining != 60 {
					t.Fatal("failed guard changed budget", u, e)
				}
			})
		}
	}
}
