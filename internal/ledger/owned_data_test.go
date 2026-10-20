package ledger

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/EduCloud-Ecosystem/outfitter/internal/owneddata"
	"github.com/EduCloud-Ecosystem/outfitter/internal/provider"
)

// This fresh fixture constructor is compiled only into the Go test binary.
// It uses the production validation/serialization path without exposing any
// version-six initializer or conversion in an ordinary executable.
func freshOwnedFixture(t *testing.T, dir string) (*Store, Data) {
	t.Helper()
	if e := os.Mkdir(dir, 0700); e != nil {
		t.Fatal(e)
	}
	var e error
	dir, e = filepath.EvalSymlinks(dir)
	if e != nil {
		t.Fatal(e)
	}
	// Publish the existing durable initialization tombstone before the fixture's
	// first journal. Losing ledger.json must not permit ordinary v2 creation.
	if e := writeExclusive(filepath.Join(dir, "initialized"), nil); e != nil {
		t.Fatal(e)
	}
	if e := syncDirectory(dir); e != nil {
		t.Fatal(e)
	}
	identity := func(name string, create bool) owneddata.FilesystemIdentity {
		var f *os.File
		var e error
		if create {
			f, e = os.OpenFile(filepath.Join(dir, name), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
		} else {
			f, e = os.Open(dir)
		}
		if e != nil {
			t.Fatal(e)
		}
		defer f.Close()
		st, e := f.Stat()
		if e != nil {
			t.Fatal(e)
		}
		id, e := FileIdentity(st)
		if e != nil {
			t.Fatal(e)
		}
		return id
	}
	i := &OwnedDataIdentity{WaypointAuthorityID: strings.Repeat("6", 32), Directory: identity("", false), JournalLease: identity("ledger.lock", true), ExecutionLease: identity("execution.lock", true)}
	scopes := map[string]owneddata.Scope{}
	d := Data{Version: 6, Owner: strings.Repeat("f", 32), Authority: &Authority{ID: strings.Repeat("5", 32), StatePath: dir, Admission: "active"}, OwnedDataIdentity: i, OwnedDataScopes: &scopes, Projects: map[string]Project{}, Runs: map[string]provider.Record{}, NativeAccounts: map[string]NativeAccount{}, NativeRuns: map[string]NativeRecord{}}
	for _, id := range []string{strings.Repeat("2", 32), strings.Repeat("3", 32)} {
		d.Projects[id] = Project{id, 1000}
	}
	at := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	for _, status := range []string{"charged", "unresolved"} {
		id := "prior-" + status
		w := provider.Request{SchemaVersion: 1, RequestID: id, ProjectID: strings.Repeat("2", 32), Image: "sha256:" + strings.Repeat("a", 64), Command: []string{"reviewed-fixture"}, InputDir: "/synthetic/input", RecipeDir: "/synthetic/recipe", CPUs: 1, MemoryMB: 256, PIDsLimit: 64, TimeoutSeconds: 30, MaxOutputBytes: 65536, Isolation: "reviewed-local", AllowSupervisedReaper: true}
		h := sha256.Sum256([]byte(d.Owner + ":" + id))
		r := provider.Record{Receipt: provider.Receipt{SchemaVersion: 1, Admitted: true, RequestID: id, ProjectID: w.ProjectID, Image: w.Image, Status: "unknown", CreatedAt: at, Deadline: at.Add(30 * time.Second), Compute: provider.Compute{Reserved: 30, Provenance: "reserved"}, UnresolvedLiability: true, TeardownRisk: "supervised external watchdog; no provider self-expiry"}, Request: w, PayloadHash: w.Hash(), Name: "outfitter-" + hex.EncodeToString(h[:16]), Owner: d.Owner}
		if status == "charged" {
			r.Compute.Charged = 30
			r.Compute.Provenance = "estimated_upper_bound"
			r.UnresolvedLiability = false
			r.CleanupVerified = true
			finish := at.Add(time.Second)
			r.FinishedAt = &finish
		}
		d.Runs[id] = r
	}
	d.NativeAccounts["native-account"] = NativeAccount{ID: "native-account", Provider: "synthetic", Unit: "native-unit", Scope: "fixture", Budget: 100, Policy: json.RawMessage(`{}`)}
	for _, id := range []string{"native-unresolved", "native-charged"} {
		r := NativeRecord{RequestID: id, AccountID: "native-account", Owner: d.Owner, ReservationID: strings.Repeat("1", 32), PayloadHash: strings.Repeat("a", 64), CreatedAt: at, Deadline: at.Add(time.Minute), Reserved: 10, Revision: 1, State: json.RawMessage(`{}`)}
		if id == "native-charged" {
			amount := int64(10)
			r.Settled = &amount
			r.CleanupVerified = true
			r.SettlementEvidence = strings.Repeat("b", 64)
		}
		d.NativeRuns[id] = r
	}
	s := &Store{Dir: dir}
	if e := s.write(d); e != nil {
		t.Fatal(e)
	}
	s, e = Open(dir, false)
	if e != nil {
		t.Fatal(e)
	}
	return s, d
}

func fixtureScope() owneddata.Scope {
	f := owneddata.FilesystemIdentity{Device: "1", Inode: "1"}
	return owneddata.Scope{ScopeID: strings.Repeat("4", 32), AssessmentID: "original-assessment", ProjectID: strings.Repeat("2", 32), WaypointAuthorityID: strings.Repeat("6", 32), ManifestSHA256: strings.Repeat("a", 64), ManifestExpiresAt: "2026-10-20T00:00:00Z", BoundaryID: strings.Repeat("b", 32), CreatedAt: "2026-10-19T12:00:00.000000Z", ExpiresAt: "2026-10-19T13:00:00Z", WallHighWater: "2026-10-19T12:00:00.000000Z", Generation: 1, State: "preparing", Reason: "none", WriterEpoch: strings.Repeat("8", 32), Assurance: "synthetic", Observations: []owneddata.Observation{}, DeclaredUploadCeilingBytes: 1024, AdmittingOwnerEpoch: strings.Repeat("7", 32), ClockEpoch: strings.Repeat("a", 32), DeadlineTickMS: 3601000, TickHighWaterMS: 1000, FilesystemIdentity: owneddata.FilesystemBinding{Root: f, OwnerRead: f, WatchRead: f, WatchWrite: f}}
}

func TestOwnedJournalFreezesMeaningfulAccounting(t *testing.T) {
	s, d := freshOwnedFixture(t, filepath.Join(t.TempDir(), "state"))
	before := frozenOwnedAccounting(d)
	x := fixtureScope()
	if e := s.Update(func(d *Data) error { (*d.OwnedDataScopes)[x.ScopeID] = x; return nil }); e != nil {
		t.Fatal(e)
	}
	if e := s.View(func(d Data) error {
		if !bytes.Equal(before, frozenOwnedAccounting(d)) {
			t.Fatal("accounting changed")
		}
		if u := Calculate(d, x.ProjectID); u.Reserved != 30 || u.Charged != 30 || u.Unknown != 30 || u.Remaining != 940 {
			t.Fatal(u)
		}
		if u := NativeCalculate(d, "native-account"); u.Reserved != 10 || u.Settled != 10 || u.Remaining != 80 {
			t.Fatal(u)
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	for _, mutate := range []func(*Data){
		func(d *Data) { p := d.Projects[x.ProjectID]; p.Budget++; d.Projects[x.ProjectID] = p },
		func(d *Data) { delete(d.Runs, "prior-unresolved") },
		func(d *Data) { delete(d.NativeRuns, "native-charged") },
		func(d *Data) { d.Version = 5 },
		func(d *Data) { delete(*d.OwnedDataScopes, x.ScopeID) },
		func(d *Data) {
			r := (*d.OwnedDataScopes)[x.ScopeID]
			r.ManifestSHA256 = strings.Repeat("c", 64)
			r.Generation++
			(*d.OwnedDataScopes)[x.ScopeID] = r
		},
	} {
		if e := s.Update(func(d *Data) error { mutate(d); return nil }); e == nil {
			t.Fatal("frozen state changed")
		}
	}
	if _, e := s.InitProject(x.ProjectID, 1000); e == nil {
		t.Fatal("v6 project init permitted")
	}
	if _, e := s.Checkpoint(filepath.Join(t.TempDir(), "checkpoint")); e == nil {
		t.Fatal("v6 checkpoint permitted")
	}
}

func TestOwnedMaximumFormsReservedBeforeWrite(t *testing.T) {
	s, d := freshOwnedFixture(t, filepath.Join(t.TempDir(), "state"))
	x := fixtureScope()
	(*d.OwnedDataScopes)[x.ScopeID] = x
	p, e := prepareLedgerWrite(d, s.Dir, maxRecordBytes, maxLedgerBytes)
	if e != nil {
		t.Fatal(e)
	}
	current, _ := json.Marshal(d)
	if p.maximumBytes <= len(current) {
		t.Fatal("future observation forms not reserved")
	}
	if _, e := prepareLedgerWrite(d, s.Dir, maxRecordBytes, p.maximumBytes-1); e == nil {
		t.Fatal("metadata ceiling bypass")
	}
	max := ownedMaximumData(d)
	if _, e := marshalCompact(max, maxRecordBytes, p.maximumBytes); e != nil {
		t.Fatal(e)
	}
}

// Construct permitted denial records independently of the production maximum
// helper. In particular, false has a longer JSON encoding than true.
func TestOwnedIndependentDenialFormsFitReservation(t *testing.T) {
	s, d := freshOwnedFixture(t, filepath.Join(t.TempDir(), "state"))
	accounting := frozenOwnedAccounting(d)
	initial := fixtureScope()
	(*d.OwnedDataScopes)[initial.ScopeID] = initial
	plan, e := prepareLedgerWrite(d, s.Dir, maxRecordBytes, maxLedgerBytes)
	if e != nil {
		t.Fatal(e)
	}
	denied := initial
	denied.Generation = 9007199254740991
	denied.TickHighWaterMS = 9007199254740991
	denied.State, denied.Reason, denied.DenyLatched = "cleanup_unresolved", "incomplete_upload", true
	epoch := strings.Repeat("9", 32)
	denied.WatchEpoch, denied.WatchStarted = &epoch, true
	identity := owneddata.FilesystemIdentity{Device: "18446744073709551615", Inode: "18446744073709551615"}
	denied.FilesystemIdentity.Scope, denied.FilesystemIdentity.WriterLease = &identity, &identity
	denied.FilesystemIdentity.WatchLease, denied.FilesystemIdentity.Content = &identity, &identity
	for j := 0; j < 8; j++ {
		denied.Observations = append(denied.Observations, owneddata.Observation{
			Schema: owneddata.ObservationSchema, ObservationID: fmt.Sprintf("%032x", j+1),
			ScopeID: denied.ScopeID, ScopeGeneration: 9007199254740991,
			ObservedAt: denied.CreatedAt, Method: "synthetic_local_scope_v1", Assurance: "synthetic",
			NewAdmission: "denied", WritersDrained: "observed", ScopeAccessRevocation: "observed",
			OwnedRemoval: "observed_absent", Reason: "record_uncertain",
		})
	}
	for _, flags := range []struct{ writer, complete, saturated bool }{
		{false, false, false}, {true, false, false}, {true, true, false}, {true, true, true},
	} {
		t.Run(fmt.Sprintf("writer=%t/complete=%t/saturated=%t", flags.writer, flags.complete, flags.saturated), func(t *testing.T) {
			x := denied
			x.WriterStarted, x.UploadComplete, x.ObservationsSaturated = flags.writer, flags.complete, flags.saturated
			x.ContentCloseConfirmed = flags.complete
			if e := x.Validate(); e != nil {
				t.Fatalf("independent failure form must be permitted: %v", e)
			}
			(*d.OwnedDataScopes)[x.ScopeID] = x
			encoded, e := json.Marshal(d)
			if e != nil {
				t.Fatal(e)
			}
			if len(encoded) > plan.maximumBytes {
				t.Fatalf("valid denial form needs %d bytes; reserved %d (short by %d)", len(encoded), plan.maximumBytes, len(encoded)-plan.maximumBytes)
			}
			if e := s.write(d); e != nil {
				t.Fatalf("reserved denial could not be written: %v", e)
			}
			if e := s.View(func(got Data) error {
				if !bytes.Equal(accounting, frozenOwnedAccounting(got)) {
					t.Fatal("denial changed charged or unresolved accounting")
				}
				return nil
			}); e != nil {
				t.Fatal(e)
			}
		})
	}
	(*d.OwnedDataScopes)[denied.ScopeID] = denied
	maxRecord, _ := json.Marshal(denied)
	maxData, _ := json.Marshal(d)
	(*d.OwnedDataScopes)[initial.ScopeID] = initial
	if _, e := prepareLedgerWrite(d, s.Dir, len(maxRecord), len(maxData)); e != nil {
		t.Fatalf("exact independently measured denial reserve refused: %v", e)
	}
	for _, bounds := range [][2]int{{len(maxRecord) - 1, len(maxData)}, {len(maxRecord), len(maxData) - 1}} {
		if _, e := prepareLedgerWrite(d, s.Dir, bounds[0], bounds[1]); e == nil {
			t.Fatalf("admission permitted without complete denial reserve: record=%d ledger=%d", bounds[0], bounds[1])
		}
	}
}

func TestOwnedFixtureSeedHelper(t *testing.T) {
	if os.Getenv("OUTFITTER_OWNED_TEST_SEED") == "" {
		return
	}
	freshOwnedFixture(t, os.Getenv("OUTFITTER_OWNED_TEST_SEED"))
	os.Exit(0)
}

func TestOwnedMissingJournalCannotReinitializeOrRecover(t *testing.T) {
	_, checkpoint, manifest, _ := recoveryFixture(t)
	for _, operation := range []string{"open", "create", "checkpoint", "activate", "restore"} {
		t.Run(operation, func(t *testing.T) {
			s, _ := freshOwnedFixture(t, filepath.Join(t.TempDir(), "state"))
			journal := filepath.Join(s.Dir, "ledger.json")
			if e := os.Remove(journal); e != nil {
				t.Fatal(e)
			}
			snapshot := func() map[string]string {
				t.Helper()
				entries, e := os.ReadDir(s.Dir)
				if e != nil {
					t.Fatal(e)
				}
				out := map[string]string{}
				for _, entry := range entries {
					b, e := os.ReadFile(filepath.Join(s.Dir, entry.Name()))
					if e != nil {
						t.Fatal(e)
					}
					st, e := os.Lstat(filepath.Join(s.Dir, entry.Name()))
					if e != nil {
						t.Fatal(e)
					}
					id, e := FileIdentity(st)
					if e != nil {
						t.Fatal(e)
					}
					out[entry.Name()] = fmt.Sprintf("%s:%s:%x", id.Device, id.Inode, b)
				}
				return out
			}
			before, _ := json.Marshal(snapshot())
			var e error
			output := filepath.Join(t.TempDir(), "checkpoint-output")
			switch operation {
			case "open":
				_, e = Open(s.Dir, false)
			case "create":
				_, e = Open(s.Dir, true)
			case "checkpoint":
				_, e = s.Checkpoint(output)
			case "activate":
				_, e = s.Activate(s.Dir, checkpoint, manifest.CheckpointSHA256)
			case "restore":
				_, e = Restore(s.Dir, checkpoint, manifest.CheckpointSHA256)
			}
			if e == nil {
				t.Error("missing assessment journal permitted authority replacement or recovery")
			}
			if _, e := os.Lstat(journal); !os.IsNotExist(e) {
				t.Error("missing assessment journal was replaced")
			}
			if _, e := os.Lstat(output); !os.IsNotExist(e) {
				t.Error("failed checkpoint created output")
			}
			after, _ := json.Marshal(snapshot())
			if !bytes.Equal(before, after) {
				t.Error("failed operation changed remaining authority objects")
			}
		})
	}
}

func TestOwnedCapacityRetainsDenialReservation(t *testing.T) {
	s, d := freshOwnedFixture(t, filepath.Join(t.TempDir(), "state"))
	accounting := frozenOwnedAccounting(d)
	count := MaxRecords - len(d.Runs) - len(d.NativeRuns)
	if e := s.Update(func(d *Data) error {
		for j := 0; j < count; j++ {
			x := fixtureScope()
			x.ScopeID = fmt.Sprintf("%032x", j+1)
			(*d.OwnedDataScopes)[x.ScopeID] = x
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(s.Dir, "ledger.json")
	before, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Update(func(d *Data) error {
		x := fixtureScope()
		x.ScopeID = strings.Repeat("f", 32)
		(*d.OwnedDataScopes)[x.ScopeID] = x
		return nil
	}); e == nil {
		t.Fatal("aggregate record cap bypassed")
	}
	after, e := os.ReadFile(path)
	if e != nil || !bytes.Equal(before, after) {
		t.Fatal("over-capacity admission changed journal")
	}
	if e = s.Update(func(d *Data) error {
		id := fmt.Sprintf("%032x", 1)
		x := (*d.OwnedDataScopes)[id]
		x.Generation++
		x.DenyLatched = true
		x.State = "cleanup_unresolved"
		x.Reason = "cancelled"
		x.Observations = []owneddata.Observation{{Schema: owneddata.ObservationSchema, ObservationID: strings.Repeat("e", 32), ScopeID: id, ScopeGeneration: x.Generation, ObservedAt: x.CreatedAt, Method: "synthetic_local_scope_v1", Assurance: "synthetic", NewAdmission: "denied", WritersDrained: "unknown", ScopeAccessRevocation: "unknown", OwnedRemoval: "unknown", Reason: "lease_uncertain"}}
		(*d.OwnedDataScopes)[id] = x
		return nil
	}); e != nil {
		t.Fatal("full intake cap blocked denial/observation", e)
	}
	if e = s.View(func(d Data) error {
		if !bytes.Equal(accounting, frozenOwnedAccounting(d)) {
			t.Fatal("capacity control changed prior liabilities")
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
}
