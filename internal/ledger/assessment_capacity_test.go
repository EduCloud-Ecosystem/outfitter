package ledger

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/EduCloud-Ecosystem/outfitter/internal/assessment"
	x "github.com/EduCloud-Ecosystem/outfitter/internal/assessmentexec"
)

func executionLiteralScope(t *testing.T, index int, slots int) x.ExecutionScope {
	t.Helper()
	raw, e := os.ReadFile("../assessmentexec/testdata/fresh-scope.literal.json")
	if e != nil {
		t.Fatal(e)
	}
	var s x.ExecutionScope
	if e = x.DecodeClosed(raw, &s, x.MaxUnitBytes); e != nil {
		t.Fatal(e)
	}
	s.ScopeID = fmt.Sprintf("%032x", 10000+index)
	s.AssessmentID = fmt.Sprintf("%032x", 20000+index)
	s.Manifest.AssessmentID = s.AssessmentID
	if slots == 6 {
		s.Manifest.Design.FittingSizes = s.Manifest.Design.FittingSizes[:1]
		s.Manifest.Design.SmallerDesign = &assessment.SmallerDesign{ApprovedByUser: true, Reason: "operator_selected"}
		s.Manifest.Ceilings.MaxAttempts = 6
		s.Manifest.Ceilings.CPUSeconds = 10800
	}
	s.Manifest.Reservations = []x.Slot{}
	for j, size := range append(append([]int64{}, s.Manifest.Design.FittingSizes...), s.Manifest.Design.HeldOutSize) {
		for rep := int64(1); rep <= s.Manifest.Design.Repetitions; rep++ {
			role := "fitting"
			if j == len(s.Manifest.Design.FittingSizes) {
				role = "held_out"
			}
			k := int64(index*100) + int64(len(s.Manifest.Reservations))
			s.Manifest.Reservations = append(s.Manifest.Reservations, x.Slot{AttemptID: fmt.Sprintf("%032x", 30000+k), ReservationID: fmt.Sprintf("%032x", 60000+k), StageID: "analysis", SampleRole: role, SampleSize: size, Repetition: rep})
		}
	}
	s.ManifestSHA256 = x.Digest(s.Manifest)
	s.Slots = x.Clone(s.Manifest.Reservations)
	s.Limits = x.LimitsFor(s.Manifest)
	s.Capability.Limits = s.Limits
	s.CapabilitySHA256 = x.Digest(s.Capability)
	if e = s.Validate(); e != nil {
		t.Fatal("literal scope", e)
	}
	return s
}

func TestExecution512ReservedUnitsKeepSafetyAndFrozenLiabilities(t *testing.T) {
	s, d := freshExecutionAuthority(t)
	_, legacy := freshOwnedFixture(t, filepath.Join(t.TempDir(), "separate-legacy-fixture"))
	// Fresh test seeding only. This does not mutate or migrate the v6 fixture.
	d.Runs = legacy.Runs
	d.NativeAccounts = legacy.NativeAccounts
	d.NativeRuns = legacy.NativeRuns
	p := d.Projects["22222222222222222222222222222222"]
	p.Budget = 1000000000
	d.Projects[p.ID] = p
	for i := 0; i < 40; i++ {
		slots := 12
		if i >= 38 {
			slots = 6
		}
		scope := executionLiteralScope(t, i, slots)
		d.AssessmentExecution.Scopes[scope.ScopeID] = scope
	}
	if n := len(d.Runs) + len(d.NativeRuns) + executionCount(d); n != 512 {
		t.Fatal(n)
	}
	if e := s.write(d); e != nil {
		t.Fatal("full finite seed", e)
	}
	if _, e := Open(s.Dir, false); e != nil {
		t.Fatal("full state unreadable", e)
	}
	reserved, e := assessmentReservedBytes(d)
	if e != nil || reserved > 64<<20 {
		t.Fatal(reserved, e)
	}
	raw, e := os.ReadFile(filepath.Join(s.Dir, "ledger.json"))
	if e != nil || len(raw) > 64<<20 {
		t.Fatal(len(raw), e)
	}
	t.Logf("512 units: actual=%d reserved-max=%d ceiling=%d", len(raw), reserved, 64<<20)
	beforeFrozen := executionFrozen(d)
	beforeUsage := Calculate(d, p.ID)
	extra := executionLiteralScope(t, 41, 12)
	if e = s.Update(func(d *Data) error { d.AssessmentExecution.Scopes[extra.ScopeID] = extra; return nil }); e == nil {
		t.Fatal("513-plus unit admission accepted")
	}
	after, _ := os.ReadFile(filepath.Join(s.Dir, "ledger.json"))
	if !bytes.Equal(raw, after) {
		t.Fatal("capacity refusal wrote authority")
	}
	id := executionLiteralScope(t, 0, 12).ScopeID
	update := func(event x.Event, fn func(*x.ExecutionScope)) {
		t.Helper()
		if e := s.Update(func(d *Data) error {
			old := d.AssessmentExecution.Scopes[id]
			next, e := x.AdvanceScope(old, event, old.WallHighWater, old.TickHighWaterMS, fn)
			if e == nil {
				d.AssessmentExecution.Scopes[id] = next
			}
			return e
		}); e != nil {
			t.Fatal(event, e)
		}
	}
	update(x.ScopeDeny, func(v *x.ExecutionScope) {
		v.DenyLatched = true
		v.ExecutionDenyLatched = true
		v.State = "cleaning"
		v.Reason = "cancelled"
	})
	update(x.ScopeUnresolved, func(v *x.ExecutionScope) { v.State = "cleanup_unresolved" })
	for i, reason := range []string{"none", "writer_active", "reader_active", "collector_active", "owner_mismatch", "io_uncertain", "lease_uncertain", "record_uncertain", "clock_uncertain"} {
		if e := s.Update(func(d *Data) error {
			v := d.AssessmentExecution.Scopes[id]
			o := x.ScopeObservation{Schema: x.ScopeObservationSchema, ObservationID: fmt.Sprintf("%032x", 90000+i), ScopeID: id, ObservedAt: v.WallHighWater, Method: "synthetic_execution_scope_v1", Assurance: "synthetic", NewAdmission: "denied", WritersDrained: "unknown", ReadersDrained: "unknown", CollectorDrained: "unknown", ScopeAccessRevocation: "unknown", OwnedRemoval: "unknown", Reason: reason}
			next, e := x.AddScopeObservation(v, o)
			if e == nil {
				d.AssessmentExecution.Scopes[id] = next
			}
			return e
		}); e != nil {
			t.Fatal(i, e)
		}
	}
	// A late exact identity remains publishable from the reserved safety bank
	// after observation saturation and at all 512 permanently consumed units.
	update(x.ScopeIdentity, func(v *x.ExecutionScope) {
		f := x.FilesystemIdentity{Device: "18446744073709551615", Inode: "18446744073709551615"}
		v.FilesystemIdentity.Scope = &f
	})
	if e = s.View(func(got Data) error {
		scope := got.AssessmentExecution.Scopes[id]
		if !scope.ObservationsSaturated || len(scope.Observations) != 8 || scope.SafetyTransitionsUsed != 12 || scope.FilesystemIdentity.Scope == nil {
			return errors.New("safety tail lost")
		}
		if !bytes.Equal(beforeFrozen, executionFrozen(got)) || !reflect.DeepEqual(beforeUsage, Calculate(got, p.ID)) {
			return errors.New("frozen liability/accounting changed")
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
}

func TestExecutionSerializerLostACKCountsCommittedTransition(t *testing.T) {
	for _, point := range []string{"before_temp", "before_file_sync", "before_rename", "after_rename", "before_directory_sync", "after_directory_sync"} {
		t.Run(point, func(t *testing.T) {
			s, _ := freshExecutionAuthority(t)
			scope := executionLiteralScope(t, 0, 12)
			if e := s.Update(func(d *Data) error { d.AssessmentExecution.Scopes[scope.ScopeID] = scope; return nil }); e != nil {
				t.Fatal(e)
			}
			s.assessmentWriteFault = func(p string) error {
				if p == point {
					return errors.New("injected persistence acknowledgement failure")
				}
				return nil
			}
			e := s.Update(func(d *Data) error {
				old := d.AssessmentExecution.Scopes[scope.ScopeID]
				next, e := x.AdvanceScope(old, x.ScopeDeny, old.WallHighWater, old.TickHighWaterMS, func(v *x.ExecutionScope) {
					v.DenyLatched = true
					v.ExecutionDenyLatched = true
					v.State = "cleaning"
					v.Reason = "cancelled"
				})
				if e == nil {
					d.AssessmentExecution.Scopes[scope.ScopeID] = next
				}
				return e
			})
			if e == nil {
				t.Fatal("injected write acknowledged")
			}
			s.assessmentWriteFault = nil
			reopened, e := Open(s.Dir, false)
			if e != nil {
				t.Fatal(e)
			}
			committed := point == "after_rename" || point == "before_directory_sync" || point == "after_directory_sync"
			if e = reopened.View(func(d Data) error {
				v := d.AssessmentExecution.Scopes[scope.ScopeID]
				want := int64(0)
				if committed {
					want = 1
				}
				if v.SafetyTransitionsUsed != want || v.Generation != 1+want || v.DenyLatched != committed {
					return errors.New("lost ACK rewound or omitted committed count")
				}
				return nil
			}); e != nil {
				t.Fatal(e)
			}
		})
	}
}

func TestExecutionInitialScopeCannotSmuggleNativeOrRawState(t *testing.T) {
	s, _ := freshExecutionAuthority(t)
	scope := executionLiteralScope(t, 0, 12)
	before, _ := os.ReadFile(filepath.Join(s.Dir, "ledger.json"))
	if e := s.Update(func(d *Data) error {
		d.AssessmentExecution.Scopes[scope.ScopeID] = scope
		if d.NativeAccounts == nil {
			d.NativeAccounts = map[string]NativeAccount{}
		}
		d.NativeAccounts["new-account"] = NativeAccount{ID: "new-account", Provider: "fixture", Unit: "unit", Scope: "fixture", Budget: 1, Policy: json.RawMessage(`{}`)}
		return nil
	}); e == nil {
		t.Fatal("assessment prepare introduced native grant")
	}
	after, _ := os.ReadFile(filepath.Join(s.Dir, "ledger.json"))
	if !bytes.Equal(before, after) {
		t.Fatal("refusal changed state")
	}
}
