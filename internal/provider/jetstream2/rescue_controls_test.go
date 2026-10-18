package jetstream2

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

type rescueAuditTransport struct {
	next    http.RoundTripper
	methods []string
}

func (t *rescueAuditTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	t.methods = append(t.methods, r.Method+" "+r.URL.Path)
	return t.next.RoundTrip(r)
}

func TestOfflineRescueObserverMethodAudit(t *testing.T) {
	r := setupRescue(t)
	record := r.create(t)
	r.future(record)
	guard := r.b.client.http.Transport.(rescueTransport)
	audit := &rescueAuditTransport{next: guard.next}
	guard.next = audit
	r.b.client.http.Transport = guard
	if e := r.b.Authenticate(context.Background(), Credentials{"fixture", "fixture"}); e != nil {
		t.Fatal(e)
	}
	if _, e := r.b.Sweep(context.Background()); e != nil {
		t.Fatal(e)
	}
	deletes := 0
	for _, method := range audit.methods {
		if strings.HasPrefix(method, "DELETE ") {
			deletes++
		}
		if !strings.HasPrefix(method, "GET ") && !strings.HasPrefix(method, "DELETE ") && method != "POST /v3/auth/tokens" {
			t.Fatal("observer exceeded API authority", method)
		}
	}
	if deletes != 2 {
		t.Fatal("audit missed cleanup", audit.methods)
	}
}

func TestOfflineRescueDiskFailuresAtEveryPhaseFile(t *testing.T) {
	for _, file := range []string{"port.scope", "port.ack", "server.scope", "server.ack"} {
		for _, point := range []string{"before-write:", "before-file-sync:", "before-dir-sync:"} {
			t.Run(point+file, func(t *testing.T) {
				r := setupRescue(t)
				r.b.store.fault = func(at string) error {
					if at == point+file {
						return errors.New("injected I/O error")
					}
					return nil
				}
				if _, e := r.c.CreateWorker(context.Background(), r.w); e == nil {
					t.Fatal("I/O failure accepted")
				}
				posts, _ := r.counts()
				want := 0
				if strings.HasPrefix(file, "server") {
					want = 1
				}
				if posts != want {
					t.Fatal(posts, want)
				}
				assertHeld(t, r.j, 100)
			})
		}
	}
}

func TestOfflineRescueMetadataPinsFreshAndDiscovery(t *testing.T) {
	for _, key := range []string{"outfitter_owner", "outfitter_request", "outfitter_reservation", "outfitter_deadline"} {
		for _, where := range []string{"discovery", "fresh"} {
			t.Run(key+"/"+where, func(t *testing.T) {
				r := setupRescue(t)
				record := r.create(t)
				r.future(record)
				r.f.mu.Lock()
				r.f.change = func(path string, data map[string]any) {
					alter := func(v map[string]any) { v["metadata"].(map[string]any)[key] = "wrong-but-present" }
					if where == "fresh" && path == "/compute/servers/server" {
						if v, ok := data["server"].(map[string]any); ok {
							alter(v)
						}
					}
					if where == "discovery" && path == "/compute/servers/detail" {
						for _, v := range data["servers"].([]any) {
							alter(v.(map[string]any))
						}
					}
				}
				r.f.mu.Unlock()
				if _, e := r.b.Sweep(context.Background()); e == nil {
					t.Fatal("changed identity accepted")
				}
				_, deletes := r.counts()
				if deletes != 0 {
					t.Fatal("unsafe deletion")
				}
			})
		}
	}
}

func TestOfflineRescueClockAndExpiryRaces(t *testing.T) {
	t.Run("rollback-before-cleanup", func(t *testing.T) {
		r := setupRescue(t)
		record := r.create(t)
		r.future(record)
		r.b.store.fault = func(point string) error {
			if point == "before-cleanup" {
				r.b.store.now = func() time.Time { return record.CreatedAt }
			}
			return nil
		}
		if _, e := r.b.Sweep(context.Background()); e == nil {
			t.Fatal("rollback during sweep accepted")
		}
		_, deletes := r.counts()
		if deletes != 0 {
			t.Fatal("deleted after rollback")
		}
	})
	t.Run("deadline-before-port-POST", func(t *testing.T) {
		r := setupRescue(t)
		r.w.MaxSeconds = 1
		r.c.rescueFault = func(point string) error {
			if point == "port-post-before" {
				time.Sleep(1100 * time.Millisecond)
			}
			return nil
		}
		if _, e := r.c.CreateWorker(context.Background(), r.w); e == nil {
			t.Fatal("expired POST accepted")
		}
		posts, _ := r.counts()
		if posts != 0 {
			t.Fatal("creation after durable deadline")
		}
		assertHeld(t, r.j, 100)
	})
	t.Run("distinct-trust-keys", func(t *testing.T) {
		r := setupRescue(t)
		p := *r.j.policy.Rescue
		p.ObserverKey = p.SourceKey
		if e := p.validate(r.c.config); e == nil {
			t.Fatal("A can authenticate its own ACK")
		}
	})
}

func TestOfflineRescueParentAndUntrustedProtocol(t *testing.T) {
	r := setupRescue(t)
	record := r.create(t)
	var s RescueScope
	if e := rescueVerify([]byte(record.Rescue.ServerScope), "scope", r.j.policy.Rescue.SourceKey, &s); e != nil {
		t.Fatal(e)
	}
	s.ParentHash = strings.Repeat("f", 64)
	raw, e := rescueSign("scope", s, r.aKey)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = r.b.Accept(context.Background(), raw); e == nil {
		t.Fatal("changed parent accepted")
	}
	var port RescueScope
	if e = rescueVerify([]byte(record.Rescue.PortScope), "scope", r.j.policy.Rescue.SourceKey, &port); e != nil {
		t.Fatal(e)
	}
	raw, e = rescueSign("scope", port, fixtureKey("untrusted"))
	if e != nil {
		t.Fatal(e)
	}
	if _, e = r.b.Accept(context.Background(), raw); e == nil {
		t.Fatal("untrusted source accepted")
	}
	for _, prefix := range []string{`{"unexpected":true,`, `{"kind":"scope",`} {
		raw = []byte(prefix + record.Rescue.PortScope[1:])
		if _, e = r.b.Accept(context.Background(), raw); e == nil {
			t.Fatal("unknown or duplicate field accepted")
		}
	}
}

func TestOfflineRescueReviewReceiptPersistence(t *testing.T) {
	for _, point := range []string{"before-write:", "before-file-sync:", "before-dir-sync:"} {
		t.Run(point, func(t *testing.T) {
			r := setupRescue(t)
			record := r.create(t)
			r.future(record)
			r.b.store.fault = func(at string) error {
				if at == point+"obs-001.receipt" {
					return errors.New("injected final receipt persistence failure")
				}
				return nil
			}
			raw, e := r.b.Sweep(context.Background())
			if e == nil || len(raw) != 0 {
				t.Fatal("non-durable signed receipt escaped", len(raw), e)
			}
			r.b.store.fault = nil
			if _, e = r.b.Sweep(context.Background()); e != nil {
				t.Fatal("retained intent failed later sweep", e)
			}
			assertHeld(t, r.j, 100)
		})
	}
}

func TestOfflineRescueReviewRollbackDuringACK(t *testing.T) {
	for _, phase := range []string{"port", "server"} {
		t.Run(phase, func(t *testing.T) {
			r := setupRescue(t)
			r.b.store.fault = func(at string) error {
				if at == "after-dir-sync:"+phase+".ack" {
					r.b.store.now = func() time.Time { return time.Now().UTC().Add(-time.Hour) }
				}
				return nil
			}
			if _, e := r.c.CreateWorker(context.Background(), r.w); e == nil {
				t.Fatal("rollback during ACK accepted")
			}
			posts, _ := r.counts()
			want := 0
			if phase == "server" {
				want = 1
			}
			if posts != want {
				t.Fatal("POST after observer rollback", posts, want)
			}
			assertHeld(t, r.j, 100)
		})
	}
}

func TestOfflineRescueReviewTransportRestrictions(t *testing.T) {
	r := setupRescue(t)
	if _, e := newRescueClient(r.c.config, http.DefaultTransport, nil, true); e == nil {
		t.Fatal("explicit default transport accepted")
	}
	if _, e := newRescueClient(r.c.config, http.DefaultTransport.(*http.Transport).Clone(), nil, true); e == nil {
		t.Fatal("environment proxy transport accepted")
	}
	cfg := r.c.config
	cfg.AuthURL += "/"
	c, e := newRescueClient(cfg, r.transport, nil, true)
	if e != nil {
		t.Fatal(e)
	}
	if e = c.Authenticate(context.Background(), Credentials{"fixture", "fixture"}); e != nil {
		t.Fatal("trailing slash authentication", e)
	}
}
