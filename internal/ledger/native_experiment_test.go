package ledger

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func nativeExperimentFixture(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	os.Chmod(dir, 0700)
	s, e := Open(dir, true)
	if e != nil {
		t.Fatal(e)
	}
	x := &NativeExperiment{Envelope: json.RawMessage(`{"fixture":true}`), SHA256: hash([]byte(`{"fixture":true}`)), RequestID: "one", ReservationID: strings.Repeat("a", 32)}
	if e = s.InitNativeAccount(NativeAccount{ID: "account", Provider: "fixture", Unit: "milli_su", Scope: "project", Budget: 100, Policy: json.RawMessage(`{}`), Experiment: x}); e != nil {
		t.Fatal(e)
	}
	if e = s.Update(func(d *Data) error {
		now := time.Now().UTC()
		a := d.NativeAccounts["account"]
		a.Experiment.Consumed = true
		d.NativeAccounts["account"] = a
		if d.NativeRuns == nil {
			d.NativeRuns = map[string]NativeRecord{}
		}
		d.NativeRuns["one"] = NativeRecord{ExperimentDigest: x.SHA256, RequestID: "one", AccountID: "account", Owner: d.Owner, ReservationID: x.ReservationID, PayloadHash: strings.Repeat("b", 64), CreatedAt: now, Deadline: now.Add(time.Minute), Reserved: 100, CleanupVerified: true, State: json.RawMessage(`{}`), Revision: 1}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	return s
}
func TestNativeExperimentSettlementDoesNotReplenish(t *testing.T) {
	s := nativeExperimentFixture(t)
	if e := s.SettleNative("one", 0, strings.Repeat("c", 64)); e != nil {
		t.Fatal(e)
	}
	before, _ := os.ReadFile(filepath.Join(s.Dir, "ledger.json"))
	changes := map[string]func(*Data){
		"remove_account": func(d *Data) { delete(d.NativeAccounts, "account"); delete(d.NativeRuns, "one") },
		"remove_envelope": func(d *Data) {
			a := d.NativeAccounts["account"]
			a.Experiment = nil
			d.NativeAccounts["account"] = a
			delete(d.NativeRuns, "one")
		},
		"reset_consumed": func(d *Data) { d.NativeAccounts["account"].Experiment.Consumed = false; delete(d.NativeRuns, "one") },
		"nested_envelope": func(d *Data) {
			x := d.NativeAccounts["account"].Experiment
			x.Envelope[2] = 'F'
			x.SHA256 = hash(x.Envelope)
			n := d.NativeRuns["one"]
			n.ExperimentDigest = x.SHA256
			d.NativeRuns["one"] = n
		},
		"reservation":   func(d *Data) { d.NativeAccounts["account"].Experiment.ReservationID = strings.Repeat("c", 32) },
		"request":       func(d *Data) { d.NativeAccounts["account"].Experiment.RequestID = "two" },
		"nested_policy": func(d *Data) { d.NativeAccounts["account"].Policy[0] = '[' },
		"second_reservation": func(d *Data) {
			n := d.NativeRuns["one"]
			n.RequestID = "two"
			n.Settled = nil
			n.SettlementEvidence = ""
			d.NativeRuns["two"] = n
		},
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			if e := s.Update(func(d *Data) error { change(d); return nil }); e == nil {
				t.Fatal("consumed experiment changed")
			}
			after, _ := os.ReadFile(filepath.Join(s.Dir, "ledger.json"))
			if !bytes.Equal(before, after) {
				t.Fatal("failed mutation persisted")
			}
		})
	}
	s, e := Open(s.Dir, false)
	if e != nil {
		t.Fatal(e)
	}
	s.View(func(d Data) error {
		if NativeCalculate(d, "account").Remaining != 100 || !d.NativeAccounts["account"].Experiment.Consumed {
			t.Fatal("financial headroom replenished attempt")
		}
		return nil
	})
}
func TestNativeExperimentReadTimeCrosslinksAndBounds(t *testing.T) {
	for _, mode := range []string{"marker", "record", "digest", "id", "envelope", "policy_size", "state_size"} {
		t.Run(mode, func(t *testing.T) {
			s := nativeExperimentFixture(t)
			var d Data
			s.View(func(v Data) error { d = v; return nil })
			switch mode {
			case "marker":
				d.NativeAccounts["account"].Experiment.Consumed = false
			case "record":
				delete(d.NativeRuns, "one")
			case "digest":
				n := d.NativeRuns["one"]
				n.ExperimentDigest = strings.Repeat("0", 64)
				d.NativeRuns["one"] = n
			case "id":
				d.NativeAccounts["account"].Experiment.RequestID = "other"
			case "envelope":
				d.NativeAccounts["account"].Experiment.Envelope[2] = 'F'
			case "policy_size":
				a := d.NativeAccounts["account"]
				a.Policy = json.RawMessage(`"` + strings.Repeat("x", 16384) + `"`)
				d.NativeAccounts["account"] = a
			case "state_size":
				n := d.NativeRuns["one"]
				n.State = json.RawMessage(`"` + strings.Repeat("x", 16384) + `"`)
				d.NativeRuns["one"] = n
			}
			raw, _ := json.Marshal(d)
			if e := os.WriteFile(filepath.Join(s.Dir, "ledger.json"), raw, 0600); e != nil {
				t.Fatal(e)
			}
			if _, e := Open(s.Dir, false); e == nil {
				t.Fatal("inconsistent authority opened")
			}
		})
	}
}

func TestNativeExperimentSettlementRecoveryPreservesAttempt(t *testing.T) {
	s := nativeExperimentFixture(t)
	if e := s.SettleNative("one", 0, strings.Repeat("c", 64)); e != nil {
		t.Fatal(e)
	}
	point := filepath.Join(t.TempDir(), "checkpoint")
	report, e := s.Checkpoint(point)
	if e != nil {
		t.Fatal(e)
	}
	dest := filepath.Join(t.TempDir(), "held")
	if _, e = Restore(dest, point, report.CheckpointSHA256); e != nil {
		t.Fatal(e)
	}
	target, e := Open(dest, false)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = target.Activate(s.Dir, point, report.CheckpointSHA256); e != nil {
		t.Fatal(e)
	}
	if e = s.View(func(d Data) error { return RequireAdmission(d) }); e == nil {
		t.Fatal("source not fenced")
	}
	if e = target.View(func(d Data) error {
		if !d.NativeAccounts["account"].Experiment.Consumed || len(d.NativeRuns) != 1 {
			t.Fatal("activation replenished attempt")
		}
		return RequireAdmission(d)
	}); e != nil {
		t.Fatal(e)
	}
	if e = target.Update(func(d *Data) error {
		d.NativeAccounts["account"].Experiment.Consumed = false
		delete(d.NativeRuns, "one")
		return nil
	}); e == nil {
		t.Fatal("activated target reset attempt")
	}
}

func TestNativeExperimentReviewStableEnvelopeBytes(t *testing.T) {
	for name, raw := range map[string]string{"whitespace": `{ "fixture": true }`, "less_than": `{"fixture":"<"}`, "greater_than": `{"fixture":">"}`, "ampersand": `{"fixture":"&"}`} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			os.Chmod(dir, 0700)
			s, e := Open(dir, true)
			if e != nil {
				t.Fatal(e)
			}
			before, e := os.ReadFile(filepath.Join(dir, "ledger.json"))
			if e != nil {
				t.Fatal(e)
			}
			x := &NativeExperiment{Envelope: json.RawMessage(raw), SHA256: hash([]byte(raw)), RequestID: "one", ReservationID: strings.Repeat("a", 32)}
			e = s.InitNativeAccount(NativeAccount{ID: "account", Provider: "fixture", Unit: "milli_su", Scope: "project", Budget: 100, Policy: json.RawMessage(`{}`), Experiment: x})
			if e == nil {
				readErr := s.View(func(Data) error { return nil })
				t.Fatalf("unstable envelope accepted; read result: %v", readErr)
			}
			after, e := os.ReadFile(filepath.Join(dir, "ledger.json"))
			if e != nil || !bytes.Equal(before, after) {
				t.Fatal("rejected configuration wrote ledger", e)
			}
			if _, e = Open(dir, false); e != nil {
				t.Fatal("rejected configuration damaged authority", e)
			}
		})
	}
	for name, raw := range map[string]string{"compact": `{"fixture":true}`, "escaped": `{"fixture":"\u003c\u003e\u0026"}`} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			os.Chmod(dir, 0700)
			s, e := Open(dir, true)
			if e != nil {
				t.Fatal(e)
			}
			x := &NativeExperiment{Envelope: json.RawMessage(raw), SHA256: hash([]byte(raw)), RequestID: "one", ReservationID: strings.Repeat("a", 32)}
			if e = s.InitNativeAccount(NativeAccount{ID: "account", Provider: "fixture", Unit: "milli_su", Scope: "project", Budget: 100, Policy: json.RawMessage(`{}`), Experiment: x}); e != nil {
				t.Fatal(e)
			}
			if _, e = Open(dir, false); e != nil {
				t.Fatal("stable envelope not readable", e)
			}
		})
	}
}
