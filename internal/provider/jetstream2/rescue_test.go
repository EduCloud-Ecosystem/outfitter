package jetstream2

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
)

type rescueFixture struct {
	api     *apiFixture
	mu      sync.Mutex
	change  func(string, map[string]any)
	methods []string
}

func (f *rescueFixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rec := httptest.NewRecorder()
	f.api.ServeHTTP(rec, r)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.methods = append(f.methods, r.Method+" "+r.URL.Path)
	var data map[string]any
	raw := rec.Body.Bytes()
	if json.Unmarshal(raw, &data) == nil {
		// The old fixture deliberately omitted device_id when unattached. This
		// protocol fixture explicitly models the provider's required empty value.
		fill := func(p map[string]any) {
			if _, ok := p["device_id"]; !ok {
				p["device_id"] = ""
			}
		}
		if p, ok := data["port"].(map[string]any); ok && r.Method == "GET" {
			fill(p)
		}
		if ps, ok := data["ports"].([]any); ok {
			for _, p := range ps {
				fill(p.(map[string]any))
			}
		}
		if f.change != nil && r.Method == "GET" {
			f.change(r.URL.Path, data)
		}
		raw, _ = json.Marshal(data)
	}
	for key, values := range rec.Header() {
		w.Header()[key] = values
	}
	w.WriteHeader(rec.Code)
	_, _ = w.Write(raw)
}

type rescueTestRig struct {
	c          *Client
	b          *OfflineRescueObserver
	f          *rescueFixture
	j          *DurableJournal
	w          WorkerRequest
	aKey, bKey ed25519.PrivateKey
	transport  http.RoundTripper
}

type ackRejectJournal struct {
	*DurableJournal
	phase string
}

func (j *ackRejectJournal) Save(ctx context.Context, r WorkerRecord) error {
	if r.Rescue != nil && ((j.phase == "port" && r.Rescue.PortACK != "") || (j.phase == "server" && r.Rescue.ServerACK != "")) {
		return errors.New("injected A durable-save failure")
	}
	return j.DurableJournal.Save(ctx, r)
}

func fixtureKey(s string) ed25519.PrivateKey {
	seed := sha256.Sum256([]byte("PUBLIC OFFLINE FIXTURE ONLY " + s))
	return ed25519.NewKeyFromSeed(seed[:])
}
func setupRescue(t *testing.T) *rescueTestRig {
	t.Helper()
	f := &rescueFixture{api: &apiFixture{}}
	srv := httptest.NewTLSServer(f)
	t.Cleanup(srv.Close)
	f.api.url = srv.URL
	cfg := Config{AuthURL: srv.URL + "/v3", ProjectID: "project", Region: "region", ImageID: "image", ImageHash: "sha256:" + strings.Repeat("a", 64), FlavorID: "flavor", NetworkID: "network", SecurityGroupID: "group"}
	aKey, bKey := fixtureKey("A"), fixtureKey("B")
	s, e := ledger.Open(filepath.Join(t.TempDir(), "A"), true)
	if e != nil {
		t.Fatal(e)
	}
	var owner, authority string
	if e = s.View(func(d ledger.Data) error { owner = d.Owner; authority = d.Authority.ID; return nil }); e != nil {
		t.Fatal(e)
	}
	p := RescuePolicy{OfflineRescueMode, authority, "observer-B", base64.StdEncoding.EncodeToString(aKey.Public().(ed25519.PublicKey)), base64.StdEncoding.EncodeToString(bKey.Public().(ed25519.PublicKey))}
	j, e := ConfigureJournal(s, Policy{Config: cfg, BudgetMilliSU: 1000, RateMilliSUPerHour: 1000, RateCheckedAt: "2026-10-17", RateSource: "https://docs.jetstream-cloud.org/general/vmsizes/", SupervisorID: "observer-B", ApprovalExpiresAt: time.Now().Add(time.Hour), AllowSupervisedException: true, Rescue: &p})
	if e != nil {
		t.Fatal(e)
	}
	b, e := NewOfflineRescueObserver(filepath.Join(t.TempDir(), "B"), true, p, cfg, bKey, srv.Client().Transport)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { b.Close() })
	c, e := NewOfflineRescueCreator(cfg, srv.Client().Transport, j, &RescueSender{aKey, b.Accept})
	if e != nil {
		t.Fatal(e)
	}
	auth(t, c)
	if e = b.Authenticate(context.Background(), Credentials{"fixture", "fixture"}); e != nil {
		t.Fatal(e)
	}
	return &rescueTestRig{c, b, f, j, WorkerRequest{"request", owner, 10, 100}, aKey, bKey, srv.Client().Transport}
}
func (r *rescueTestRig) future(record WorkerRecord) {
	r.b.store.now = func() time.Time { return record.Deadline.Add(2 * time.Second) }
}
func (r *rescueTestRig) counts() (int, int) {
	r.f.api.mu.Lock()
	defer r.f.api.mu.Unlock()
	return r.f.api.postCount, r.f.api.deletes
}
func (r *rescueTestRig) create(t *testing.T) WorkerRecord {
	t.Helper()
	record, e := r.c.CreateWorker(context.Background(), r.w)
	if e != nil {
		t.Fatal(e)
	}
	return record
}
func (r *rescueTestRig) receipt(t *testing.T, raw []byte) RescueReceipt {
	t.Helper()
	st, e := r.b.store.load()
	if e != nil {
		t.Fatal(e)
	}
	rec, e := ValidateRescueReceipt(raw, st.portBytes, st.serverBytes, *r.j.policy.Rescue, r.c.config)
	if e != nil {
		t.Fatal(e)
	}
	return rec
}

func TestOfflineRescueLifecycleAndAuthority(t *testing.T) {
	r := setupRescue(t)
	record := r.create(t)
	if record.Rescue.PortACK == "" || record.Rescue.ServerACK == "" {
		t.Fatal("missing persisted phase ACKs")
	}
	before, e := os.ReadFile(filepath.Join(r.j.Store.Dir, "ledger.json"))
	if e != nil {
		t.Fatal(e)
	}
	r.future(record)
	if _, e = r.c.TeardownWorker(context.Background(), record); e == nil {
		t.Fatal("A deleted B-owned resources")
	}
	if _, e = r.c.ReconcileExpired(context.Background(), r.w.RequestID); e == nil {
		t.Fatal("A reaper accepted B-owned resources")
	}
	raw, e := r.b.Sweep(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	if rec := r.receipt(t, raw); rec.Outcome != "OBSERVED_ABSENT" {
		t.Fatal(rec)
	}
	posts, deletes := r.counts()
	if posts != 2 || deletes != 2 {
		t.Fatal(posts, deletes)
	}
	after, e := os.ReadFile(filepath.Join(r.j.Store.Dir, "ledger.json"))
	if e != nil || !bytes.Equal(before, after) {
		t.Fatal("B wrote A ledger", e)
	}
	assertHeld(t, r.j, 100)
	if e = r.j.Store.SettleNative(r.w.RequestID, 0, strings.Repeat("a", 64)); e == nil {
		t.Fatal("observational cleanup settled charges")
	}
	if _, _, e = r.j.Admit(context.Background(), WorkerRequest{"next", r.w.Owner, 1, 100}); e == nil {
		t.Fatal("new admission after observation")
	}
	if _, e = r.b.Sweep(context.Background()); e != nil {
		t.Fatal(e)
	}
	_, again := r.counts()
	if again != deletes {
		t.Fatal("duplicate deletion")
	}
	if _, e = os.Stat(filepath.Join(r.b.store.dir, "port.scope")); e != nil {
		t.Fatal("scope discarded")
	}
}

func TestOfflineRescueACKFailuresBlockActualCreator(t *testing.T) {
	for _, phase := range []string{"port", "server"} {
		for _, mode := range []string{"missing", "lost", "forged", "conflicting", "file-sync", "dir-sync", "A-save"} {
			t.Run(phase+"/"+mode, func(t *testing.T) {
				t.Parallel()
				r := setupRescue(t)
				r.c.rescueSender.Exchange = func(ctx context.Context, raw []byte) ([]byte, error) {
					var s RescueScope
					if e := rescueVerify(raw, "scope", r.j.policy.Rescue.SourceKey, &s); e != nil {
						return nil, e
					}
					if s.Phase != phase {
						return r.b.Accept(ctx, raw)
					}
					if mode == "missing" {
						return nil, errors.New("no ACK")
					}
					if mode == "file-sync" || mode == "dir-sync" {
						r.b.store.fault = func(point string) error {
							if point == "before-"+mode+":"+phase+".ack" {
								return errors.New("injected fsync failure")
							}
							return nil
						}
					}
					ack, e := r.b.Accept(ctx, raw)
					if e != nil {
						return nil, e
					}
					switch mode {
					case "lost":
						return nil, errors.New("ACK response lost")
					case "forged":
						ack[len(ack)-4] ^= 1
					case "conflicting":
						var a RescueACK
						_ = rescueVerify(ack, "ack", r.j.policy.Rescue.ObserverKey, &a)
						a.ScopeHash = strings.Repeat("f", 64)
						ack, _ = rescueSign("ack", a, r.bKey)
					case "A-save":
						r.c.journal = &ackRejectJournal{r.j, phase}
					}
					return ack, nil
				}
				if _, e := r.c.CreateWorker(context.Background(), r.w); e == nil {
					t.Fatal("fault accepted")
				}
				posts, _ := r.counts()
				want := 0
				if phase == "server" {
					want = 1
				}
				if posts != want {
					t.Fatal(posts, want)
				}
				r.b.store.fault = nil
				r.c.rescueFault = nil
				r.c.journal = r.j
				_, _ = r.c.CreateWorker(context.Background(), r.w)
				again, _ := r.counts()
				if again != posts {
					t.Fatal("ambiguous request retried")
				}
				assertHeld(t, r.j, 100)
			})
		}
	}
}

func TestOfflineRescueLateEffectsRemainEligibleAndUnsettled(t *testing.T) {
	for _, phase := range []string{"port", "server"} {
		t.Run(phase, func(t *testing.T) {
			r := setupRescue(t)
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
			if e != nil {
				t.Fatal(e)
			}
			if r.receipt(t, raw).Outcome != "OBSERVED_ABSENT" {
				t.Fatal("missing time-bounded absence")
			}
			if phase == "port" {
				if _, e = os.Stat(filepath.Join(r.b.store.dir, "server.scope")); !os.IsNotExist(e) {
					t.Fatal("lost port advanced to server")
				}
			}
			// A later sweep remains possible after an earlier absence/watch interval.
			r.b.store.now = func() time.Time { return record.Deadline.Add(time.Hour) }
			r.f.api.mu.Lock()
			r.f.api.port = true
			r.f.api.server = phase == "server"
			r.f.api.mu.Unlock()
			if _, e = r.b.Sweep(context.Background()); e != nil {
				t.Fatal(e)
			}
			posts, deletes := r.counts()
			want := 1
			if phase == "server" {
				want = 2
			}
			if posts != want || deletes != want {
				t.Fatal(posts, deletes)
			}
			assertHeld(t, r.j, 100)
			if e = r.j.Store.SettleNative(r.w.RequestID, 0, strings.Repeat("a", 64)); e == nil {
				t.Fatal("late effect settled")
			}
		})
	}
}

func TestOfflineRescueStrictDiscoveryAndFreshGET(t *testing.T) {
	for _, resource := range []string{"server", "port"} {
		fields := []string{"id", "name"}
		if resource == "server" {
			fields = append(fields, "tenant_id", "image", "flavor", "metadata", "os-extended-volumes:volumes_attached")
		} else {
			fields = append(fields, "project_id", "network_id", "device_id", "security_groups", "description")
		}
		for _, field := range fields {
			for _, where := range []string{"discovery", "fresh"} {
				for _, value := range []string{"missing", "null", "changed"} {
					t.Run(resource+"/"+field+"/"+where+"/"+value, func(t *testing.T) {
						t.Parallel()
						r := setupRescue(t)
						record := r.create(t)
						r.future(record)
						r.f.mu.Lock()
						r.f.change = func(path string, data map[string]any) {
							var objects []map[string]any
							if where == "fresh" {
								if strings.HasSuffix(path, "/"+resource) {
									if v, ok := data[resource].(map[string]any); ok {
										objects = append(objects, v)
									}
								}
							} else {
								key := resource + "s"
								if vs, ok := data[key].([]any); ok {
									for _, v := range vs {
										objects = append(objects, v.(map[string]any))
									}
								}
							}
							for _, v := range objects {
								switch value {
								case "missing":
									delete(v, field)
								case "null":
									v[field] = nil
								default:
									if field == "device_id" {
										v[field] = "foreign"
									} else {
										v[field] = []any{"foreign"}
									}
								}
							}
						}
						r.f.mu.Unlock()
						if _, e := r.b.Sweep(context.Background()); e == nil {
							t.Fatal("malformed required field accepted")
						}
						r.f.mu.Lock()
						defer r.f.mu.Unlock()
						for _, method := range r.f.methods {
							if method == "DELETE /compute/servers/server" && resource == "server" {
								t.Fatal("unsafe server deletion")
							}
							if method == "DELETE /network/v2.0/ports/port" {
								t.Fatal("unsafe port deletion")
							}
						}
						assertHeld(t, r.j, 100)
					})
				}
			}
		}
	}
}

func TestOfflineRescueInventoryRefusals(t *testing.T) {
	for _, mode := range []string{"duplicate", "foreign", "pagination", "missing-list", "null-list", "storage", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			r := setupRescue(t)
			record := r.create(t)
			r.future(record)
			r.f.api.mu.Lock()
			r.f.api.duplicate = mode == "duplicate"
			r.f.api.foreign = mode == "foreign"
			r.f.api.paginated = mode == "pagination"
			r.f.api.missingList = mode == "missing-list"
			r.f.api.storage = mode == "storage"
			r.f.api.mu.Unlock()
			r.f.mu.Lock()
			r.f.change = func(path string, data map[string]any) {
				if mode == "null-list" && path == "/compute/servers/detail" {
					data["servers"] = nil
				}
				if mode == "deadline" {
					if vs, ok := data["servers"].([]any); ok {
						for _, v := range vs {
							v.(map[string]any)["metadata"].(map[string]any)["outfitter_deadline"] = "2099-01-01T00:00:00Z"
						}
					}
				}
			}
			r.f.mu.Unlock()
			if _, e := r.b.Sweep(context.Background()); e == nil {
				t.Fatal("invalid inventory accepted")
			}
			_, deletes := r.counts()
			if deletes != 0 {
				t.Fatal("unsafe deletion")
			}
		})
	}
}

func TestOfflineRescueStorageAndReceiptFailures(t *testing.T) {
	for _, mode := range []string{"fifo", "symlink", "corrupt", "permissions", "quota", "intent-sync", "receipt-forgery", "copied-dir"} {
		t.Run(mode, func(t *testing.T) {
			r := setupRescue(t)
			record := r.create(t)
			r.future(record)
			path := filepath.Join(r.b.store.dir, "port.scope")
			switch mode {
			case "fifo", "symlink":
				if e := os.Remove(path); e != nil {
					t.Fatal(e)
				}
				if mode == "fifo" {
					if e := syscall.Mkfifo(path, 0600); e != nil {
						t.Fatal(e)
					}
				} else {
					if e := os.Symlink(filepath.Join(r.j.Store.Dir, "ledger.json"), path); e != nil {
						t.Fatal(e)
					}
				}
			case "corrupt":
				if e := os.WriteFile(path, []byte("{}"), 0600); e != nil {
					t.Fatal(e)
				}
			case "permissions":
				if e := os.Chmod(path, 0644); e != nil {
					t.Fatal(e)
				}
			case "quota":
				for i := 0; i < 2*rescueMaxSweeps+10; i++ {
					if e := os.WriteFile(filepath.Join(r.b.store.dir, strings.Repeat("x", i+1)), nil, 0600); e != nil {
						t.Fatal(e)
					}
				}
			case "intent-sync":
				r.b.store.fault = func(point string) error {
					if point == "before-file-sync:obs-001.intent" {
						return errors.New("disk unavailable")
					}
					return nil
				}
			case "receipt-forgery":
				raw, e := r.b.Sweep(context.Background())
				if e != nil {
					t.Fatal(e)
				}
				raw[len(raw)-4] ^= 1
				st, e := r.b.store.load()
				if e != nil {
					t.Fatal(e)
				}
				if _, e = ValidateRescueReceipt(raw, st.portBytes, st.serverBytes, *r.j.policy.Rescue, r.c.config); e == nil {
					t.Fatal("forged receipt accepted")
				}
				return
			case "copied-dir":
				dest := filepath.Join(t.TempDir(), "copy")
				if e := os.Mkdir(dest, 0700); e != nil {
					t.Fatal(e)
				}
				entries, e := os.ReadDir(r.b.store.dir)
				if e != nil {
					t.Fatal(e)
				}
				for _, v := range entries {
					raw, e := os.ReadFile(filepath.Join(r.b.store.dir, v.Name()))
					if e != nil {
						t.Fatal(e)
					}
					if e = os.WriteFile(filepath.Join(dest, v.Name()), raw, 0600); e != nil {
						t.Fatal(e)
					}
				}
				if copy, e := NewOfflineRescueObserver(dest, false, *r.j.policy.Rescue, r.c.config, r.bKey, r.transport); e == nil {
					copy.Close()
					t.Fatal("copied observer adopted")
				}
				return
			}
			if _, e := r.b.Sweep(context.Background()); e == nil {
				t.Fatal("storage fault ignored")
			}
			_, deletes := r.counts()
			if deletes != 0 {
				t.Fatal("deleted without durable valid scope/evidence")
			}
		})
	}
}

func TestOfflineRescueBoundsClocksAndTransport(t *testing.T) {
	t.Run("transport", func(t *testing.T) {
		r := setupRescue(t)
		if _, e := New(r.c.config, nil, r.j); e == nil {
			t.Fatal("ordinary constructor enabled rescue")
		}
		if _, e := NewOfflineRescueCreator(r.c.config, nil, r.j, r.c.rescueSender); e == nil {
			t.Fatal("default transport enabled")
		}
		cfg := r.c.config
		cfg.AuthURL = "https://provider.invalid/v3"
		if _, e := newRescueClient(cfg, r.transport, nil, true); e == nil {
			t.Fatal("non-loopback accepted")
		}
		if _, e := r.b.client.call(context.Background(), "POST", r.f.api.url+"/compute/servers", nil, nil); e == nil {
			t.Fatal("observer created resource")
		}
		if _, e := r.b.client.call(context.Background(), "GET", "https://example.invalid/", nil, nil); e == nil {
			t.Fatal("default route escaped")
		}
	})
	t.Run("clock-skew", func(t *testing.T) {
		r := setupRescue(t)
		r.b.store.now = func() time.Time { return time.Now().UTC().Add(3 * time.Second) }
		if _, e := r.c.CreateWorker(context.Background(), r.w); e == nil {
			t.Fatal("skew accepted")
		}
		posts, _ := r.counts()
		if posts != 0 {
			t.Fatal(posts)
		}
	})
	t.Run("clock-rollback", func(t *testing.T) {
		r := setupRescue(t)
		record := r.create(t)
		r.b.store.now = func() time.Time { return record.CreatedAt.Add(-time.Minute) }
		if _, e := r.b.Sweep(context.Background()); e == nil {
			t.Fatal("rollback accepted")
		}
	})
	t.Run("expiry-during-ACK", func(t *testing.T) {
		r := setupRescue(t)
		r.b.store.fault = func(point string) error {
			if point == "after-dir-sync:port.ack" {
				r.b.store.now = func() time.Time { return time.Now().UTC().Add(time.Hour) }
			}
			return nil
		}
		if _, e := r.c.CreateWorker(context.Background(), r.w); e == nil {
			t.Fatal("expired ACK accepted")
		}
		posts, _ := r.counts()
		if posts != 0 {
			t.Fatal(posts)
		}
	})
	t.Run("bounded-retention", func(t *testing.T) {
		r := setupRescue(t)
		record := r.create(t)
		r.future(record)
		for i := 0; i < rescueMaxSweeps; i++ {
			if _, e := r.b.Sweep(context.Background()); e != nil {
				t.Fatal(i, e)
			}
		}
		if _, e := r.b.Sweep(context.Background()); e == nil {
			t.Fatal("unbounded evidence growth")
		}
		if _, e := os.Stat(filepath.Join(r.b.store.dir, "server.scope")); e != nil {
			t.Fatal("unresolved scope evicted")
		}
		assertHeld(t, r.j, 100)
	})
}

func TestOfflineRescueConcurrentDuplicates(t *testing.T) {
	r := setupRescue(t)
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, e := r.c.CreateWorker(context.Background(), r.w); errs <- e }()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	posts, _ := r.counts()
	if posts != 2 {
		t.Fatal("duplicate effects", posts)
	}
}

// Guard against accidental protocol expansion or ambiguous JSON acceptance.
func TestOfflineRescueCanonicalProtocol(t *testing.T) {
	r := setupRescue(t)
	record := r.create(t)
	for _, raw := range [][]byte{append([]byte(" "), []byte(record.Rescue.PortScope)...), append([]byte(record.Rescue.PortScope), []byte("{}")...), []byte(strings.Repeat("x", rescueMaxBytes+1))} {
		if _, e := r.b.Accept(context.Background(), raw); e == nil {
			t.Fatal("ambiguous or oversized envelope accepted")
		}
	}
}
