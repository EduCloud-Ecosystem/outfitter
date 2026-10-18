package jetstream2

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
)

func TestOfflineExperimentCrashBoundaries(t *testing.T) {
	type boundary struct {
		point string
		posts int
	}
	cases := []boundary{{"reservation-after", 0}}
	for _, phase := range []string{"port", "server"} {
		for _, stage := range []string{"intent-after", "scope-saved", "exchange-returned", "ack-after", "post-before", "post-after", "response-saved"} {
			n := 0
			if phase == "server" {
				n = 1
			}
			if stage == "post-after" || stage == "response-saved" {
				n++
			}
			cases = append(cases, boundary{phase + "-" + stage, n})
		}
	}
	for _, tc := range cases {
		t.Run(tc.point, func(t *testing.T) {
			t.Parallel()
			r, x := experimentCandidate(t)
			configureExperiment(t, r, x)
			cmd := exec.Command(os.Args[0], "-test.run=^TestOfflineRescueProcessHelper$")
			cmd.Env = rescueEnvSet(rescueEnvSet(r.processEnv(t), "OUTFITTER_RESCUE_TEST_ROLE", "creator"), "OUTFITTER_RESCUE_TEST_CRASH", tc.point)
			if out, e := cmd.CombinedOutput(); e == nil {
				t.Fatalf("crash not reached: %s", out)
			}
			if p, _ := r.counts(); p != tc.posts {
				t.Fatal(p, tc.posts)
			}
			assertExperiment(t, r, true)
			assertHeld(t, r.j, 100)
			record, e := r.j.Lookup(context.Background(), r.w.RequestID)
			if e != nil {
				t.Fatal(e)
			}
			if _, e = r.c.CreateWorker(context.Background(), r.w); e != nil {
				t.Fatal("restart replay", e)
			}
			changed := r.w
			changed.RequestID = "next"
			if _, e = r.c.CreateWorker(context.Background(), changed); e == nil {
				t.Fatal("second identity")
			}
			if p, _ := r.counts(); p != tc.posts {
				t.Fatal("crash resumed")
			}
			// B must use only its own signed scope after A becomes unavailable.
			if e = os.Rename(r.j.Store.Dir, r.j.Store.Dir+"-unavailable"); e != nil {
				t.Fatal(e)
			}
			if _, e = os.Stat(filepath.Join(r.b.store.dir, "port.scope")); os.IsNotExist(e) {
				if tc.posts != 0 {
					t.Fatal("effect without B scope")
				}
				return
			}
			r.future(record)
			raw, e := r.b.Sweep(context.Background())
			state, loadErr := r.b.store.load()
			if loadErr != nil {
				t.Fatal(loadErr)
			}
			unknown := state.server != nil && tc.posts == 1
			if (e != nil) != unknown {
				t.Fatal("unexpected B result", e)
			}
			if unknown && r.receipt(t, raw).Outcome != "UNKNOWN" {
				t.Fatal("false terminal absence")
			}
			want := tc.posts
			if unknown {
				want = 0
			}
			if _, d := r.counts(); d != want {
				t.Fatal("B cleanup count", d, want)
			}
			if _, e = os.Stat(filepath.Join(r.j.Store.Dir, "ledger.json")); !os.IsNotExist(e) {
				t.Fatal("B recreated A")
			}
		})
	}
}
func TestOfflineExperimentLostAndLateEffects(t *testing.T) {
	for _, phase := range []string{"port", "server"} {
		t.Run(phase, func(t *testing.T) {
			r, x := experimentCandidate(t)
			configureExperiment(t, r, x)
			r.f.api.lostPort = phase == "port"
			r.f.api.lostServer = phase == "server"
			record, e := r.c.CreateWorker(context.Background(), r.w)
			if e == nil {
				t.Fatal("missing lost response")
			}
			r.future(record)
			r.f.api.mu.Lock()
			r.f.api.port = false
			r.f.api.server = false
			r.f.api.mu.Unlock()
			raw, e := r.b.Sweep(context.Background())
			if e != nil || r.receipt(t, raw).Outcome != "OBSERVED_ABSENT" {
				t.Fatal(e)
			}
			r.b.store.now = func() time.Time { return record.Deadline.Add(time.Hour) }
			r.f.api.mu.Lock()
			r.f.api.port = true
			r.f.api.server = phase == "server"
			r.f.api.mu.Unlock()
			if _, e = r.b.Sweep(context.Background()); e != nil {
				t.Fatal(e)
			}
			want := 1
			if phase == "server" {
				want = 2
			}
			if p, d := r.counts(); p != want || d != want {
				t.Fatal(p, d)
			}
			if _, e = r.c.CreateWorker(context.Background(), r.w); e != nil {
				t.Fatal(e)
			}
			assertExperiment(t, r, true)
			assertHeld(t, r.j, 100)
			if e = r.j.Store.SettleNative(r.w.RequestID, 0, strings.Repeat("a", 64)); e == nil {
				t.Fatal("late effect refunded")
			}
		})
	}
}
func TestOfflineExperimentHeldRestoreTamper(t *testing.T) {
	r, x := experimentCandidate(t)
	configureExperiment(t, r, x)
	record := r.create(t)
	checkpoint := filepath.Join(t.TempDir(), "checkpoint")
	report, e := r.j.Store.Checkpoint(checkpoint)
	if e != nil {
		t.Fatal(e)
	}
	dest := filepath.Join(t.TempDir(), "held")
	if _, e = ledger.Restore(dest, checkpoint, report.CheckpointSHA256); e != nil {
		t.Fatal(e)
	}
	s, e := ledger.Open(dest, false)
	if e != nil {
		t.Fatal(e)
	}
	j, e := OpenJournal(s, r.c.config)
	if e != nil {
		t.Fatal(e)
	}
	c, e := NewOfflineRescueCreator(r.c.config, r.transport, j, r.c.rescueSender)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = c.CreateWorker(context.Background(), r.w); e == nil {
		t.Fatal("held replay admitted")
	}
	if _, e = s.Activate(r.j.Store.Dir, checkpoint, report.CheckpointSHA256); e == nil {
		t.Fatal("unsettled experiment activated")
	}
	if e = ConfigureOfflineExperiment(j, x); e == nil {
		t.Fatal("held experiment reconfigured")
	}
	var d ledger.Data
	if e = s.View(func(v ledger.Data) error { d = v; return nil }); e != nil {
		t.Fatal(e)
	}
	if !d.NativeAccounts[j.accountID].Experiment.Consumed {
		t.Fatal("held restore forgot attempt")
	}
	raw, _ := os.ReadFile(filepath.Join(checkpoint, "ledger.json"))
	tampered := bytes.Replace(raw, []byte(`"consumed":true`), []byte(`"consumed":false`), 1)
	if bytes.Equal(raw, tampered) {
		t.Fatal("tamper fixture failed")
	}
	os.WriteFile(filepath.Join(checkpoint, "ledger.json"), tampered, 0600)
	if _, e = ledger.Restore(filepath.Join(t.TempDir(), "tampered"), checkpoint, report.CheckpointSHA256); e == nil {
		t.Fatal("tampered checkpoint restored")
	}
	// A corrupt ledger does not authorize a new run or prevent B cleanup.
	os.WriteFile(filepath.Join(r.j.Store.Dir, "ledger.json"), []byte(`{}`), 0600)
	if _, e = r.c.CreateWorker(context.Background(), r.w); e == nil {
		t.Fatal("corrupt replay became create")
	}
	r.future(record)
	if _, e = r.b.Sweep(context.Background()); e != nil {
		t.Fatal(e)
	}
	if p, d := r.counts(); p != 2 || d != 2 {
		t.Fatal(p, d)
	}
	b, _ := json.Marshal(d.NativeAccounts[j.accountID].Experiment)
	if !bytes.Contains(b, []byte(`"consumed":true`)) {
		t.Fatal("restored marker missing")
	}
}

func TestOfflineExperimentFinalPOSTAndAdmissionRechecks(t *testing.T) {
	for _, point := range []string{"reservation-before", "port-post-before", "server-post-before"} {
		t.Run(point, func(t *testing.T) {
			t.Parallel()
			r, x := experimentCandidate(t)
			x.Profile.ExpiresAt = time.Now().Add(time.Second)
			bindExperiment(t, r, &x)
			configureExperiment(t, r, x)
			r.c.rescueFault = func(stage string) error {
				if stage == point {
					time.Sleep(1100 * time.Millisecond)
				}
				return nil
			}
			if _, e := r.c.CreateWorker(context.Background(), r.w); e == nil || !strings.Contains(e.Error(), "EXPERIMENT_METADATA_NOT_MATCHED") {
				t.Fatal("missing fresh effect/admission check", e)
			}
			consumed := point != "reservation-before"
			assertExperiment(t, r, consumed)
			want := 0
			if point == "server-post-before" {
				want = 1
			}
			if p, _ := r.counts(); p != want {
				t.Fatal(p, want)
			}
		})
	}
}
func TestOfflineExperimentEnvelopeExpiryDuringACK(t *testing.T) {
	r, x := experimentCandidate(t)
	r.w.MaxSeconds = 1
	x.Request = r.w
	x.ExpiresAt = time.Now().Add(1500 * time.Millisecond)
	configureExperiment(t, r, x)
	r.c.rescueFault = func(point string) error {
		if point == "port-ack-after" {
			time.Sleep(1600 * time.Millisecond)
		}
		return nil
	}
	if _, e := r.c.CreateWorker(context.Background(), r.w); e == nil {
		t.Fatal("expired envelope dispatched")
	}
	if p, _ := r.counts(); p != 0 {
		t.Fatal(p)
	}
	assertExperiment(t, r, true)
	assertHeld(t, r.j, 100)
}
func TestOfflineExperimentImmutableConfiguration(t *testing.T) {
	r, x := experimentCandidate(t)
	configureExperiment(t, r, x)
	changed := x
	changed.Request.RequestID = "second"
	changed.ReservationID = strings.Repeat("e", 32)
	if e := ConfigureOfflineExperiment(r.j, changed); e == nil {
		t.Fatal("experiment replaced")
	}
	for _, mode := range []string{"remove", "reset", "envelope", "reservation"} {
		t.Run(mode, func(t *testing.T) {
			if e := r.j.Store.Update(func(d *ledger.Data) error {
				a := d.NativeAccounts[r.j.accountID]
				switch mode {
				case "remove":
					a.Experiment = nil
				case "reset":
					a.Experiment.Consumed = true
				case "envelope":
					a.Experiment.Envelope[2] = 'V'
				case "reservation":
					a.Experiment.ReservationID = strings.Repeat("e", 32)
				}
				d.NativeAccounts[r.j.accountID] = a
				return nil
			}); e == nil {
				t.Fatal("experiment mutated")
			}
		})
	}
	assertExperiment(t, r, false)
	// Configuring after ordinary native admission cannot retroactively grant one.
	other, y := experimentCandidate(t)
	if _, _, e := other.j.Admit(context.Background(), other.w); e != nil {
		t.Fatal(e)
	}
	if e := ConfigureOfflineExperiment(other.j, y); e == nil {
		t.Fatal("used account configured")
	}
}
