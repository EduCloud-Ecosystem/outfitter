package jetstream2

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
)

func durableCloud(t *testing.T) (*Client, *apiFixture, *DurableJournal, WorkerRequest) {
	t.Helper()
	c, f, _ := setupCloud(t)
	s, e := ledger.Open(filepath.Join(t.TempDir(), "state"), true)
	if e != nil {
		t.Fatal(e)
	}
	p := Policy{Config: c.config, BudgetMilliSU: 1000, RateMilliSUPerHour: 1000, RateCheckedAt: "2026-10-17", RateSource: "https://docs.jetstream-cloud.org/general/vmsizes/", SupervisorID: "simulated-supervisor", ApprovalExpiresAt: time.Now().Add(time.Hour), AllowSupervisedException: true}
	j, e := ConfigureJournal(s, p)
	if e != nil {
		t.Fatal(e)
	}
	var owner string
	if e = s.View(func(d ledger.Data) error { owner = d.Owner; return nil }); e != nil {
		t.Fatal(e)
	}
	c.journal = j
	auth(t, c)
	return c, f, j, WorkerRequest{"request", owner, 1, 100}
}
func afterDeadline(r WorkerRecord) {
	if d := time.Until(r.Deadline); d > 0 {
		time.Sleep(d + 10*time.Millisecond)
	}
}
func reopen(t *testing.T, c *Client, j *DurableJournal) *DurableJournal {
	t.Helper()
	s, e := ledger.Open(j.Store.Dir, false)
	if e != nil {
		t.Fatal(e)
	}
	next, e := OpenJournal(s, c.config)
	if e != nil {
		t.Fatal(e)
	}
	c.journal = next
	return next
}
func assertHeld(t *testing.T, j *DurableJournal, amount int64) {
	t.Helper()
	if e := j.Store.View(func(d ledger.Data) error {
		u := ledger.NativeCalculate(d, j.accountID)
		if u.Reserved != amount || u.Settled != 0 {
			t.Fatalf("liability lost: %+v", u)
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
}

func TestDurableFaultRecovery(t *testing.T) {
	for _, mode := range []string{"lost-port", "lost-server", "worker-loss", "api-loss", "duplicate-effect", "foreign-resource", "partial-delete", "requester-loss", "late-port"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			c, f, j, w := durableCloud(t)
			f.lostPort = mode == "lost-port" || mode == "late-port"
			f.lostServer = mode == "lost-server"
			r, e := c.CreateWorker(context.Background(), w)
			if mode == "lost-port" || mode == "lost-server" || mode == "late-port" {
				if e == nil {
					t.Fatal("missing ambiguous error")
				}
			} else if e != nil {
				t.Fatal(e)
			}
			if mode == "worker-loss" {
				f.mu.Lock()
				f.server = false
				f.mu.Unlock()
			}
			j = reopen(t, c, j)
			f.mu.Lock()
			posts := f.postCount
			f.mu.Unlock()
			again, e := c.CreateWorker(context.Background(), w)
			if e != nil || again.ReservationID != r.ReservationID || !again.Deadline.Equal(r.Deadline) {
				t.Fatal(again, e)
			}
			f.mu.Lock()
			if f.postCount != posts {
				t.Fatal("duplicate create")
			}
			f.mu.Unlock()
			afterDeadline(r)
			if mode == "api-loss" || mode == "duplicate-effect" || mode == "foreign-resource" || mode == "partial-delete" || mode == "late-port" {
				f.mu.Lock()
				f.outage = mode == "api-loss"
				f.duplicate = mode == "duplicate-effect"
				f.foreign = mode == "foreign-resource"
				f.deletePortFail = mode == "partial-delete"
				if mode == "late-port" {
					f.port = false
				}
				f.mu.Unlock()
				if _, e = c.ReconcileExpired(context.Background(), w.RequestID); e == nil {
					t.Fatal("fault incorrectly accepted")
				}
				assertHeld(t, j, w.CeilingMilliSU)
				f.mu.Lock()
				if (mode == "foreign-resource" || mode == "duplicate-effect") && f.deletes != 0 {
					t.Fatal("unsafe delete")
				}
				if mode == "partial-delete" && (f.server || !f.port) {
					t.Fatal("partial deletion not exercised")
				}
				f.outage = false
				f.duplicate = false
				f.foreign = false
				f.deletePortFail = false
				if mode == "late-port" {
					f.port = true
				}
				f.mu.Unlock()
			}
			results, e := c.Reap(context.Background())
			if e != nil || len(results) != 1 || results[0].Error != "" || !results[0].Record.CleanupVerified {
				t.Fatal(results, e)
			}
			f.mu.Lock()
			if f.port || f.server || f.postCount != posts {
				t.Fatal("leak or duplicate creation")
			}
			deletes := f.deletes
			f.mu.Unlock()
			if _, e = c.Reap(context.Background()); e != nil {
				t.Fatal(e)
			}
			f.mu.Lock()
			if f.deletes != deletes {
				t.Fatal("non-idempotent cleanup")
			}
			f.mu.Unlock()
			assertHeld(t, j, w.CeilingMilliSU)
			if _, _, e = j.Admit(context.Background(), WorkerRequest{"another", w.Owner, 1, 100}); e == nil {
				t.Fatal("unsettled liability allowed another worker")
			}
			if strings.HasPrefix(mode, "lost-") || mode == "late-port" {
				if e = j.Store.SettleNative(w.RequestID, 0, strings.Repeat("a", 64)); e == nil {
					t.Fatal("ambiguous charges settled")
				}
			}
		})
	}
}
func TestNativeAdmissionAndSettlement(t *testing.T) {
	c, f, j, w := durableCloud(t)
	if _, e := j.Store.InitProject("local", 300); e != nil {
		t.Fatal(e)
	} // local accounting is a separate unit
	var wg sync.WaitGroup
	var mu sync.Mutex
	fresh := 0
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			request := w
			request.RequestID = fmt.Sprintf("request-%d", i)
			_, ok, e := j.Admit(context.Background(), request)
			if e == nil && ok {
				mu.Lock()
				fresh++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	if fresh != 1 {
		t.Fatalf("concurrent reservations: %d", fresh)
	}
	if f.postCount != 0 {
		t.Fatal("admission produced provider effects")
	}
	pending, e := j.Pending(context.Background())
	if e != nil || len(pending) != 1 {
		t.Fatal(pending, e)
	}
	r := pending[0]
	changed := r.Request
	changed.CeilingMilliSU++
	if _, _, e = j.Admit(context.Background(), changed); e == nil {
		t.Fatal("payload collision accepted")
	}
	changed = r.Request
	changed.Owner = "foreign"
	if _, _, e = j.Admit(context.Background(), changed); e == nil {
		t.Fatal("foreign owner accepted")
	}
	if e = j.Store.SettleNative(r.Request.RequestID, 1, strings.Repeat("a", 64)); e == nil {
		t.Fatal("settled before cleanup")
	}
	afterDeadline(r)
	if _, e = c.Reap(context.Background()); e != nil {
		t.Fatal(e)
	}
	if e = j.Store.SettleNative(r.Request.RequestID, 150, strings.Repeat("a", 64)); e != nil {
		t.Fatal(e)
	}
	if e = j.Store.SettleNative(r.Request.RequestID, 150, strings.Repeat("a", 64)); e != nil {
		t.Fatal("idempotent settlement", e)
	}
	if e = j.Store.SettleNative(r.Request.RequestID, 0, strings.Repeat("a", 64)); e == nil {
		t.Fatal("settlement changed")
	}
	if _, _, e = j.Admit(context.Background(), w); e == nil {
		t.Fatal("overrun ignored")
	}
	s, e := ledger.Open(j.Store.Dir, false)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.View(func(d ledger.Data) error {
		u := ledger.NativeCalculate(d, j.accountID)
		if !u.Overrun || u.Settled != 150 || u.Reserved != 0 || u.Remaining != 850 {
			t.Fatal(u)
		}
		local := ledger.Calculate(d, "local")
		if local.Reserved != 0 || local.Charged != 0 || local.Remaining != 300 {
			t.Fatal("units mixed", local)
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
}
func TestNativeHeldRestoreAndPolicy(t *testing.T) {
	c, _, j, w := durableCloud(t)
	p := j.policy
	p.BudgetMilliSU++
	if _, e := ConfigureJournal(j.Store, p); e == nil {
		t.Fatal("budget reset")
	}
	p = j.policy
	p.Config.NetworkID = "another-network"
	if _, e := ConfigureJournal(j.Store, p); e == nil {
		t.Fatal("network change reset allocation allowance")
	}
	p = j.policy
	p.AllowSupervisedException = false
	if _, e := ConfigureJournal(j.Store, p); e == nil {
		t.Fatal("unsupported self-expiry admitted")
	}
	r, _, e := j.Admit(context.Background(), w)
	if e != nil {
		t.Fatal(e)
	}
	point := filepath.Join(t.TempDir(), "checkpoint")
	snap, e := j.Store.Checkpoint(point)
	if e != nil {
		t.Fatal(e)
	}
	dest := filepath.Join(t.TempDir(), "replacement")
	report, e := ledger.Restore(dest, point, snap.CheckpointSHA256)
	if e != nil || report.Admission != "held" || len(report.NativeUnresolved) != 1 {
		t.Fatal(report, e)
	}
	replacement, e := ledger.Open(dest, false)
	if e != nil {
		t.Fatal(e)
	}
	jr, e := OpenJournal(replacement, c.config)
	if e != nil {
		t.Fatal(e)
	}
	restored, e := jr.Lookup(context.Background(), w.RequestID)
	if e != nil || restored.ReservationID != r.ReservationID || !restored.Deadline.Equal(r.Deadline) {
		t.Fatal(restored, e)
	}
	if _, _, e = jr.Admit(context.Background(), w); e == nil {
		t.Fatal("held admission")
	}
	if e = replacement.SettleNative(w.RequestID, 0, strings.Repeat("a", 64)); e == nil {
		t.Fatal("stale held settlement")
	}
	if _, e = replacement.Activate(j.Store.Dir, point, snap.CheckpointSHA256); e == nil {
		t.Fatal("activation with native liability")
	}
	afterDeadline(r)
	c.journal = jr
	if _, e = c.Reap(context.Background()); e != nil {
		t.Fatal("held cleanup", e)
	}
	assertHeld(t, jr, 100)
}
func TestNativeExpiredPermitAndCorruptState(t *testing.T) {
	c, f, j, w := durableCloud(t)
	w.MaxSeconds = 3600
	if _, e := c.CreateWorker(context.Background(), w); e == nil {
		t.Fatal("under-reservation accepted")
	}
	if f.postCount != 0 {
		t.Fatal("effect without budget")
	}
	w.MaxSeconds = 1
	r, _, e := j.Admit(context.Background(), w)
	if e != nil {
		t.Fatal(e)
	}
	r.PortAttempted = true
	r.PortID = "port"
	if e = j.Save(context.Background(), r); e != nil {
		t.Fatal(e)
	}
	stale := r
	stale.PortAttempted = false
	stale.PortID = ""
	if e = j.Save(context.Background(), stale); e == nil {
		t.Fatal("lost durable effect")
	}
	raw, e := os.ReadFile(filepath.Join(j.Store.Dir, "ledger.json"))
	if e != nil {
		t.Fatal(e)
	}
	raw = bytesReplace(raw, []byte(`"reserved":100`), []byte(`"reserved":0`))
	if e = os.WriteFile(filepath.Join(j.Store.Dir, "ledger.json"), raw, 0600); e != nil {
		t.Fatal(e)
	}
	if _, e = ledger.Open(j.Store.Dir, false); e == nil {
		t.Fatal("corrupt accounting accepted")
	}
}
func bytesReplace(b, old, new []byte) []byte {
	return []byte(strings.Replace(string(b), string(old), string(new), 1))
}

// Child processes trust only the fixture's explicit certificate. No live
// credentials, insecure TLS defaults or external provider endpoints are used.
func TestNativeProcessHelper(t *testing.T) {
	role := os.Getenv("OUTFITTER_NATIVE_TEST_ROLE")
	if role == "" {
		return
	}
	var cfg Config
	if e := json.Unmarshal([]byte(os.Getenv("OUTFITTER_NATIVE_TEST_CONFIG")), &cfg); e != nil {
		t.Fatal(e)
	}
	cert, e := base64.StdEncoding.DecodeString(os.Getenv("OUTFITTER_NATIVE_TEST_CERT"))
	if e != nil {
		t.Fatal(e)
	}
	parsed, e := x509.ParseCertificate(cert)
	if e != nil {
		t.Fatal(e)
	}
	pool := x509.NewCertPool()
	pool.AddCert(parsed)
	store, e := ledger.Open(os.Getenv("OUTFITTER_NATIVE_TEST_STATE"), false)
	if e != nil {
		t.Fatal(e)
	}
	j, e := OpenJournal(store, cfg)
	if e != nil {
		t.Fatal(e)
	}
	c, e := New(cfg, &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}, j)
	if e != nil {
		t.Fatal(e)
	}
	auth(t, c)
	if role == "create" {
		var owner string
		store.View(func(d ledger.Data) error { owner = d.Owner; return nil })
		_, e = c.CreateWorker(context.Background(), WorkerRequest{"request", owner, 2, 100})
		if e != nil {
			t.Fatal(e)
		}
	} else {
		rs, e := c.Reap(context.Background())
		if e != nil || len(rs) != 1 || rs[0].Error != "" || !rs[0].Record.CleanupVerified {
			t.Fatal(rs, e)
		}
	}
}
func TestCleanupAfterCreatorSIGKILLInSeparateProcess(t *testing.T) {
	c, f, j, _ := durableCloud(t)
	// Obtain the fixture certificate from the authenticated connection.
	resp, e := c.http.Get(f.url)
	if e != nil {
		t.Fatal(e)
	}
	cert := resp.TLS.PeerCertificates[0].Raw
	resp.Body.Close()
	cfg, _ := json.Marshal(c.config)
	env := append(os.Environ(), "OUTFITTER_NATIVE_TEST_CONFIG="+string(cfg), "OUTFITTER_NATIVE_TEST_CERT="+base64.StdEncoding.EncodeToString(cert), "OUTFITTER_NATIVE_TEST_STATE="+j.Store.Dir)
	block := make(chan struct{})
	created := make(chan struct{})
	f.mu.Lock()
	f.serverBlock = block
	f.created = created
	f.mu.Unlock()
	cmd := exec.Command(os.Args[0], "-test.run=^TestNativeProcessHelper$")
	cmd.Env = append(env, "OUTFITTER_NATIVE_TEST_ROLE=create")
	if e = cmd.Start(); e != nil {
		t.Fatal(e)
	}
	select {
	case <-created:
	case <-time.After(10 * time.Second):
		cmd.Process.Kill()
		close(block)
		cmd.Wait()
		t.Fatal("creator did not reach provider effect")
	}
	if e = cmd.Process.Kill(); e != nil {
		t.Fatal(e)
	}
	cmd.Wait()
	close(block)
	r, e := j.Lookup(context.Background(), "request")
	if e != nil || r.ServerID != "" || !r.ServerAttempted || r.PortID == "" {
		t.Fatal("process fault not exercised", r, e)
	}
	afterDeadline(r)
	reap := exec.Command(os.Args[0], "-test.run=^TestNativeProcessHelper$")
	reap.Env = append(env, "OUTFITTER_NATIVE_TEST_ROLE=reap")
	if out, e := reap.CombinedOutput(); e != nil {
		t.Fatalf("independent cleanup: %s %v", out, e)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.port || f.server || f.postCount != 2 || f.deletes != 2 {
		t.Fatal("independent cleanup failed")
	}
	r, e = j.Lookup(context.Background(), "request")
	if e != nil || !r.CleanupVerified || !r.CreateAmbiguous || !r.UnresolvedLiability {
		t.Fatal(r, e)
	}
}

func TestNativeObservationsDoNotSettleOrRefund(t *testing.T) {
	c, _, j, w := durableCloud(t)
	r, e := c.CreateWorker(context.Background(), w)
	if e != nil {
		t.Fatal(e)
	}
	for i, kind := range []string{"estimated", "provider_observed"} {
		o := ledger.NativeObservation{ID: fmt.Sprintf("sample-%d", i), Amount: 10, Provenance: kind, At: time.Now().UTC(), EvidenceSHA256: strings.Repeat("b", 64)}
		if e = j.Store.AppendNativeObservation(w.RequestID, o); e != nil {
			t.Fatal(e)
		}
		if e = j.Store.AppendNativeObservation(w.RequestID, o); e != nil {
			t.Fatal(e)
		}
		o.Amount++
		if e = j.Store.AppendNativeObservation(w.RequestID, o); e == nil {
			t.Fatal("observation overwritten")
		}
	}
	assertHeld(t, j, 100)
	afterDeadline(r)
	if _, e = c.Reap(context.Background()); e != nil {
		t.Fatal(e)
	}
	assertHeld(t, j, 100)
	if e = j.Store.SettleNative(w.RequestID, 12, strings.Repeat("c", 64)); e != nil {
		t.Fatal(e)
	}
	j = reopen(t, c, j)
	record, e := j.Lookup(context.Background(), w.RequestID)
	if e != nil || record.UnresolvedLiability || record.SettledMilliSU == nil || *record.SettledMilliSU != 12 {
		t.Fatal(record, e)
	}
	if e = j.Save(context.Background(), r); e == nil {
		t.Fatal("stale save erased settlement")
	}
	if e = j.Store.View(func(d ledger.Data) error {
		n := d.NativeRuns[w.RequestID]
		u := ledger.NativeCalculate(d, j.accountID)
		if len(n.Observations) != 2 || u.Reserved != 0 || u.Settled != 12 || u.Remaining != 988 {
			t.Fatal(n, u)
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
}
func TestNativeSettledCutoverPreservesAllowance(t *testing.T) {
	c, _, j, w := durableCloud(t)
	r, _, e := j.Admit(context.Background(), w)
	if e != nil {
		t.Fatal(e)
	}
	afterDeadline(r)
	if _, e = c.Reap(context.Background()); e != nil {
		t.Fatal(e)
	}
	if e = j.Store.SettleNative(w.RequestID, 25, strings.Repeat("d", 64)); e != nil {
		t.Fatal(e)
	}
	point := filepath.Join(t.TempDir(), "checkpoint")
	snap, e := j.Store.Checkpoint(point)
	if e != nil {
		t.Fatal(e)
	}
	dest := filepath.Join(t.TempDir(), "replacement")
	if _, e = ledger.Restore(dest, point, snap.CheckpointSHA256); e != nil {
		t.Fatal(e)
	}
	replacement, e := ledger.Open(dest, false)
	if e != nil {
		t.Fatal(e)
	}
	report, e := replacement.Activate(j.Store.Dir, point, snap.CheckpointSHA256)
	if e != nil || report.Admission != "active" || len(report.NativeAccounts) != 1 || report.NativeAccounts[0].Remaining != 975 {
		t.Fatal(report, e)
	}
	if _, _, e = j.Admit(context.Background(), w); e == nil {
		t.Fatal("fenced source admitted duplicate")
	}
	jr, e := OpenJournal(replacement, c.config)
	if e != nil {
		t.Fatal(e)
	}
	saved, fresh, e := jr.Admit(context.Background(), w)
	if e != nil || fresh || saved.ReservationID != r.ReservationID || saved.SettledMilliSU == nil || *saved.SettledMilliSU != 25 {
		t.Fatal(saved, fresh, e)
	}
}
func TestNativeExpiredApprovalRefusesEffects(t *testing.T) {
	c, f, j, w := durableCloud(t)
	s, e := ledger.Open(filepath.Join(t.TempDir(), "expired"), true)
	if e != nil {
		t.Fatal(e)
	}
	p := j.policy
	p.ApprovalExpiresAt = time.Now().Add(-time.Minute)
	j, e = ConfigureJournal(s, p)
	if e != nil {
		t.Fatal(e)
	}
	c.journal = j
	if e = s.View(func(d ledger.Data) error { w.Owner = d.Owner; return nil }); e != nil {
		t.Fatal(e)
	}
	if _, e = c.CreateWorker(context.Background(), w); e == nil {
		t.Fatal("expired approval admitted")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.postCount != 0 {
		t.Fatal("expired approval produced effects")
	}
}
func TestNativeCleanupRefusesIncompleteInventoryAndUnexpectedStorage(t *testing.T) {
	for _, mode := range []string{"pagination", "missing-list", "storage", "changed-config"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			c, f, _, w := durableCloud(t)
			r, e := c.CreateWorker(context.Background(), w)
			if e != nil {
				t.Fatal(e)
			}
			afterDeadline(r)
			f.mu.Lock()
			f.paginated = mode == "pagination"
			f.missingList = mode == "missing-list"
			f.storage = mode == "storage"
			f.mu.Unlock()
			if mode == "changed-config" {
				c.config.NetworkID = "different"
			}
			if _, e = c.ReconcileExpired(context.Background(), w.RequestID); e == nil {
				t.Fatal("unsafe inventory accepted")
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.deletes != 0 {
				t.Fatal("deleted before full ownership proof")
			}
		})
	}
}
func TestNativeEarlySupervisorAndEffectLock(t *testing.T) {
	c, f, j, w := durableCloud(t)
	w.MaxSeconds = 60
	r, _, e := j.Admit(context.Background(), w)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = c.ReconcileExpired(context.Background(), r.Request.RequestID); e == nil {
		t.Fatal("premature cleanup")
	}
	release, e := j.LockWorker(context.Background(), w.RequestID)
	if e != nil {
		t.Fatal(e)
	}
	defer release()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, e = j.LockWorker(ctx, w.RequestID); e == nil {
		t.Fatal("concurrent effect lease")
	}
	if f.deletes != 0 || f.postCount != 0 {
		t.Fatal("unexpected effects")
	}
}
