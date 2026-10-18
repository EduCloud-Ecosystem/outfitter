package jetstream2

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
)

func experimentCandidate(t *testing.T) (*rescueTestRig, OfflineExperiment) {
	t.Helper()
	r := setupRescue(t)
	r.w.MaxSeconds = 4
	p, s, _, _, _ := compatibilityFixture(t)
	now := time.Now().UTC()
	p.NotBefore = now.Add(-time.Minute)
	p.ExpiresAt = now.Add(10 * time.Minute)
	p.MaxAgeSeconds = 60
	p.VCPUs = 2
	p.RAMMiB = 2048
	s.VCPUs = CompatibilityValue(int64(2))
	s.RAMMiB = CompatibilityValue(int64(2048))
	s.ObservedAt = now
	x := OfflineExperiment{Version: 1, EvidenceScope: "synthetic_offline", AuthorityID: r.j.policy.Rescue.AuthorityID, AccountID: r.j.accountID, Request: r.w, ReservationID: strings.Repeat("d", 32), NotBefore: now.Add(-time.Second), ExpiresAt: now.Add(5 * time.Minute), MaxLifetimeSeconds: 4, MaxAttempts: 1, Profile: p, Snapshot: s}
	bindExperiment(t, r, &x)
	return r, x
}
func bindExperiment(t *testing.T, r *rescueTestRig, x *OfflineExperiment) {
	t.Helper()
	compatibilityRebind(t, &x.Profile, &x.Snapshot, r.c.config, r.j.policy)
	report, e := EvaluateCompatibility(x.Profile, x.Snapshot, r.c.config, r.j.policy, time.Now().UTC())
	if e != nil {
		t.Fatal(e)
	}
	x.ConfigDigest, x.PolicyDigest, x.ProfileDigest, x.SnapshotDigest = report.ConfigDigest, report.PolicyDigest, report.ProfileDigest, report.SnapshotDigest
}
func configureExperiment(t *testing.T, r *rescueTestRig, x OfflineExperiment) {
	t.Helper()
	if e := ConfigureOfflineExperiment(r.j, x); e != nil {
		t.Fatal(e)
	}
}
func assertExperiment(t *testing.T, r *rescueTestRig, consumed bool) {
	t.Helper()
	e := r.j.Store.View(func(d ledger.Data) error {
		x := d.NativeAccounts[r.j.accountID].Experiment
		if x == nil || x.Consumed != consumed || len(d.NativeRuns) != map[bool]int{true: 1, false: 0}[consumed] {
			t.Fatal("experiment state mismatch")
		}
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
}
func TestOfflineExperimentLifecycle(t *testing.T) {
	r, x := experimentCandidate(t)
	configureExperiment(t, r, x)
	// r.j and r.c were opened before configuring the experiment.
	record := r.create(t)
	if record.ExperimentDigest == "" || record.ReservationID != x.ReservationID || record.Rescue.PortACK == "" || record.Rescue.ServerACK == "" || record.EvidenceScope != "synthetic_offline" || record.LiveReadiness != "BLOCKED" || record.GuestExecution != "NOT_RUN" {
		t.Fatal(record)
	}
	assertExperiment(t, r, true)
	if p, _ := r.counts(); p != 2 {
		t.Fatal("not exactly one port and server", p)
	}
	// An exact repeat must not even depend on resource preflight/authentication.
	r.c.token = ""
	r.c.endpoints = nil
	again, e := r.c.CreateWorker(context.Background(), r.w)
	if e != nil || digest(record) != digest(again) {
		t.Fatal("replay failed", e)
	}
	if p, _ := r.counts(); p != 2 {
		t.Fatal("replay dispatched")
	}
	if e = ConfigureOfflineExperiment(r.j, x); e != nil {
		t.Fatal("identical configuration not idempotent", e)
	}
	changed := r.w
	changed.RequestID = "different"
	if _, _, e = r.j.Admit(context.Background(), changed); e == nil {
		t.Fatal("second identity admitted")
	}
	j, e := OpenJournal(r.j.Store, r.c.config)
	if e != nil {
		t.Fatal(e)
	}
	if _, fresh, e := j.Admit(context.Background(), r.w); e != nil || fresh {
		t.Fatal("restart replenished attempt", e)
	}
	if _, _, e = j.Admit(context.Background(), changed); e == nil {
		t.Fatal("restart admitted second identity")
	}
	r.future(record)
	before, _ := os.ReadFile(filepath.Join(r.j.Store.Dir, "ledger.json"))
	if _, e = r.c.TeardownWorker(context.Background(), record); e == nil {
		t.Fatal("A cleanup")
	}
	if _, e = r.b.Sweep(context.Background()); e != nil {
		t.Fatal(e)
	}
	after, _ := os.ReadFile(filepath.Join(r.j.Store.Dir, "ledger.json"))
	if !bytes.Equal(before, after) {
		t.Fatal("B mutated accounting")
	}
	if p, d := r.counts(); p != 2 || d != 2 {
		t.Fatal(p, d)
	}
	if e = r.j.Store.SettleNative(r.w.RequestID, 0, strings.Repeat("a", 64)); e == nil {
		t.Fatal("B observation settled")
	}
	assertExperiment(t, r, true)
	assertHeld(t, r.j, 100)
}
func TestOfflineExperimentZeroEffectRefusals(t *testing.T) {
	cases := map[string]func(*OfflineExperiment){
		"before_start": func(x *OfflineExperiment) { x.NotBefore = time.Now().Add(time.Minute) },
		"expired": func(x *OfflineExperiment) {
			x.NotBefore = time.Now().Add(-time.Hour)
			x.ExpiresAt = time.Now().Add(-time.Second)
		},
		"deadline_exceeds_expiry": func(x *OfflineExperiment) { x.ExpiresAt = time.Now().Add(time.Second) },
		"profile_expired": func(x *OfflineExperiment) {
			x.Profile.NotBefore = time.Now().Add(-time.Hour)
			x.Profile.ExpiresAt = time.Now().Add(-time.Second)
		},
		"profile_future":   func(x *OfflineExperiment) { x.Profile.NotBefore = time.Now().Add(time.Minute) },
		"stale":            func(x *OfflineExperiment) { x.Snapshot.ObservedAt = time.Now().Add(-2 * time.Minute) },
		"future_snapshot":  func(x *OfflineExperiment) { x.Snapshot.ObservedAt = time.Now().Add(time.Minute) },
		"unknown":          func(x *OfflineExperiment) { x.Snapshot.Architecture = Observed[string]{} },
		"mismatch":         func(x *OfflineExperiment) { x.Snapshot.RAMMiB = CompatibilityValue(int64(1)) },
		"incomplete_rules": func(x *OfflineExperiment) { x.Snapshot.RulesComplete = CompatibilityValue(false) },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			r, x := experimentCandidate(t)
			change(&x)
			bindExperiment(t, r, &x)
			configureExperiment(t, r, x)
			if _, e := r.c.CreateWorker(context.Background(), r.w); e == nil {
				t.Fatal("refusal did not occur")
			}
			if _, fresh, e := r.j.Admit(context.Background(), r.w); e == nil || fresh {
				t.Fatal("direct native admission bypassed", e)
			}
			if p, d := r.counts(); p != 0 || d != 0 {
				t.Fatal(p, d)
			}
			assertExperiment(t, r, false)
		})
	}
}
func TestOfflineExperimentBindingsAndBounds(t *testing.T) {
	cases := map[string]func(*OfflineExperiment){
		"version": func(x *OfflineExperiment) { x.Version = 2 }, "mode": func(x *OfflineExperiment) { x.EvidenceScope = "live" },
		"authority": func(x *OfflineExperiment) { x.AuthorityID = "other" }, "account": func(x *OfflineExperiment) { x.AccountID = "other" },
		"config_digest": func(x *OfflineExperiment) { x.ConfigDigest = strings.Repeat("0", 64) }, "policy_digest": func(x *OfflineExperiment) { x.PolicyDigest = strings.Repeat("0", 64) },
		"profile_digest": func(x *OfflineExperiment) { x.ProfileDigest = strings.Repeat("0", 64) }, "snapshot_digest": func(x *OfflineExperiment) { x.SnapshotDigest = strings.Repeat("0", 64) },
		"profile_binding": func(x *OfflineExperiment) { x.Profile.ConfigDigest = strings.Repeat("0", 64) }, "snapshot_binding": func(x *OfflineExperiment) { x.Snapshot.ProfileDigest = strings.Repeat("0", 64) },
		"owner": func(x *OfflineExperiment) { x.Request.Owner = "foreign" }, "request": func(x *OfflineExperiment) { x.Request.RequestID = "../invalid" },
		"reservation": func(x *OfflineExperiment) { x.ReservationID = "bad" }, "max_attempts_zero": func(x *OfflineExperiment) { x.MaxAttempts = 0 }, "max_attempts_two": func(x *OfflineExperiment) { x.MaxAttempts = 2 },
		"lifetime": func(x *OfflineExperiment) { x.MaxLifetimeSeconds = 3 }, "lifetime_zero": func(x *OfflineExperiment) { x.Request.MaxSeconds = 0 }, "lifetime_over": func(x *OfflineExperiment) { x.MaxLifetimeSeconds = 3601 },
		"amount_zero": func(x *OfflineExperiment) { x.Request.CeilingMilliSU = 0 }, "amount_over": func(x *OfflineExperiment) { x.Request.CeilingMilliSU = 1000000001 },
		"time_zero": func(x *OfflineExperiment) { x.NotBefore = time.Time{} }, "reversed_time": func(x *OfflineExperiment) { x.ExpiresAt = x.NotBefore }, "approval": func(x *OfflineExperiment) { x.ExpiresAt = time.Now().Add(2 * time.Hour) },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			r, x := experimentCandidate(t)
			change(&x)
			if ConfigureOfflineExperiment(r.j, x) == nil {
				t.Fatal("invalid binding accepted")
			}
			if p, _ := r.counts(); p != 0 {
				t.Fatal(p)
			}
			r.j.Store.View(func(d ledger.Data) error {
				if d.NativeAccounts[r.j.accountID].Experiment != nil || len(d.NativeRuns) > 0 {
					t.Fatal("invalid binding persisted")
				}
				return nil
			})
		})
	}
	t.Run("bounded_snapshot", func(t *testing.T) {
		r, x := experimentCandidate(t)
		x.Snapshot.Architecture = CompatibilityValue(strings.Repeat("x", 4096))
		bindExperiment(t, r, &x)
		if ConfigureOfflineExperiment(r.j, x) == nil {
			t.Fatal("oversized snapshot")
		}
	})
	t.Run("too_many_rules", func(t *testing.T) {
		r, x := experimentCandidate(t)
		for len(x.Snapshot.Rules.value) < 9 {
			x.Snapshot.Rules.value = append(x.Snapshot.Rules.value, x.Snapshot.Rules.value[0])
		}
		bindExperiment(t, r, &x)
		if ConfigureOfflineExperiment(r.j, x) == nil {
			t.Fatal("too many rules")
		}
	})
	t.Run("changed_request", func(t *testing.T) {
		r, x := experimentCandidate(t)
		configureExperiment(t, r, x)
		for _, field := range []string{"id", "owner", "lifetime", "amount"} {
			w := r.w
			switch field {
			case "id":
				w.RequestID = "other"
			case "owner":
				w.Owner = "other"
			case "lifetime":
				w.MaxSeconds++
			case "amount":
				w.CeilingMilliSU++
			}
			if _, e := r.c.CreateWorker(context.Background(), w); e == nil {
				t.Fatal(field)
			}
		}
		assertExperiment(t, r, false)
	})
	t.Run("ordinary_constructor", func(t *testing.T) {
		r, x := experimentCandidate(t)
		configureExperiment(t, r, x)
		if _, e := New(r.c.config, r.transport, r.j); e == nil {
			t.Fatal("ordinary creator accepted")
		}
		r.c.offlineRescue = false
		if _, e := r.c.CreateWorker(context.Background(), r.w); e == nil {
			t.Fatal("missing offline enforcement accepted")
		}
		assertExperiment(t, r, false)
	})
}
func TestOfflineExperimentDelayedACK(t *testing.T) {
	for _, phase := range []string{"port", "server"} {
		for _, mode := range []string{"stale", "profile_expiry"} {
			t.Run(phase+"/"+mode, func(t *testing.T) {
				t.Parallel()
				r, x := experimentCandidate(t)
				now := time.Now()
				x.Snapshot.ObservedAt = now
				if mode == "stale" {
					x.Profile.MaxAgeSeconds = 1
				} else {
					x.Profile.ExpiresAt = now.Add(time.Second)
				}
				bindExperiment(t, r, &x)
				configureExperiment(t, r, x)
				// The durable ACK has passed signature/time checks before this delay; the
				// experiment must independently block dispatch after it becomes unusable.
				r.c.rescueFault = func(point string) error {
					if point == phase+"-ack-after" {
						time.Sleep(1100 * time.Millisecond)
					}
					return nil
				}
				record, e := r.c.CreateWorker(context.Background(), r.w)
				if e == nil || !strings.Contains(e.Error(), "EXPERIMENT_METADATA_NOT_MATCHED") {
					t.Fatal("metadata not rechecked after ACK", e)
				}
				want := 0
				if phase == "server" {
					want = 1
				}
				if p, _ := r.counts(); p != want {
					t.Fatal(p, want)
				}
				assertExperiment(t, r, true)
				assertHeld(t, r.j, 100)
				if record.Rescue.PortACK == "" || phase == "server" && record.Rescue.ServerACK == "" {
					t.Fatal("ACK not retained")
				}
				r.c.rescueFault = nil
				if _, e = r.c.CreateWorker(context.Background(), r.w); e != nil {
					t.Fatal("expired repeat not lookup-only", e)
				}
				if p, _ := r.counts(); p != want {
					t.Fatal("retry")
				}
			})
		}
	}
}
func TestOfflineExperimentStrictDecoder(t *testing.T) {
	_, x := experimentCandidate(t)
	raw, _ := json.Marshal(x)
	if _, e := DecodeOfflineExperiment(raw); e != nil {
		t.Fatal(e)
	}
	for name, change := range map[string]func([]byte) []byte{
		"duplicate": func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1)
		},
		"trailing": func(b []byte) []byte { return append(b, []byte(`{}`)...) }, "oversize": func(b []byte) []byte { return append(b, bytes.Repeat([]byte(" "), 8193)...) },
		"null_rule": func(b []byte) []byte { return bytes.Replace(b, []byte(`"rules":[{`), []byte(`"rules":[null,{`), 1) },
		"unknown":   func(b []byte) []byte { return append([]byte(`{"secret":"PRIVATE_SENTINEL",`), b[1:]...) },
	} {
		t.Run(name, func(t *testing.T) {
			value, e := DecodeOfflineExperiment(change(raw))
			if e == nil || !reflect.DeepEqual(value, OfflineExperiment{}) || strings.Contains(e.Error(), "PRIVATE_SENTINEL") {
				t.Fatal(e)
			}
		})
	}
	var fields map[string]json.RawMessage
	json.Unmarshal(raw, &fields)
	for field := range fields {
		t.Run("missing/"+field, func(t *testing.T) {
			next := map[string]json.RawMessage{}
			for k, v := range fields {
				if k != field {
					next[k] = v
				}
			}
			b, _ := json.Marshal(next)
			if _, e := DecodeOfflineExperiment(b); e == nil {
				t.Fatal("missing field accepted")
			}
		})
	}
}
func TestOfflineExperimentConcurrentProcesses(t *testing.T) {
	r, x := experimentCandidate(t)
	configureExperiment(t, r, x)
	env := rescueEnvSet(r.processEnv(t), "OUTFITTER_RESCUE_TEST_ROLE", "creator")
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cmd := exec.Command(os.Args[0], "-test.run=^TestOfflineRescueProcessHelper$")
			cmd.Env = env
			if out, e := cmd.CombinedOutput(); e != nil {
				t.Errorf("creator failed: %v %s", e, out)
			}
		}()
	}
	wg.Wait()
	if p, _ := r.counts(); p != 2 {
		t.Fatal("concurrent dispatch count", p)
	}
	assertExperiment(t, r, true)
}
