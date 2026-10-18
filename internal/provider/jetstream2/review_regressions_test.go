package jetstream2

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// Delay only after the successful port response has been durably saved. This
// isolates deadline expiry from an uncertain provider effect or ledger failure.
type delayedKnownPortJournal struct {
	*DurableJournal
	delayed bool
}

func (j *delayedKnownPortJournal) Save(ctx context.Context, r WorkerRecord) error {
	if e := j.DurableJournal.Save(ctx, r); e != nil {
		return e
	}
	if r.PortID != "" && !r.ServerAttempted && !j.delayed {
		j.delayed = true
		if remaining := time.Until(r.Deadline); remaining > 0 {
			time.Sleep(remaining + 10*time.Millisecond)
		}
	}
	return nil
}
func TestKnownPortDeadlineRetainsLiabilityUntilExplicitSettlement(t *testing.T) {
	c, f, j, w := durableCloud(t)
	c.journal = &delayedKnownPortJournal{DurableJournal: j}
	r, e := c.CreateWorker(context.Background(), w)
	if e == nil {
		t.Fatal("deadline did not prevent server creation")
	}
	f.mu.Lock()
	posts, port, server := f.postCount, f.port, f.server
	f.mu.Unlock()
	if posts != 1 || !port || server || r.PortID == "" || r.ServerAttempted {
		t.Fatalf("fault did not isolate a known port: posts=%d port=%v server=%v record=%+v", posts, port, server, r)
	}
	j = reopen(t, c, j)
	assertHeld(t, j, w.CeilingMilliSU)
	r, e = c.ReconcileExpired(context.Background(), w.RequestID)
	if e != nil || !r.CleanupVerified || !r.UnresolvedLiability {
		t.Fatalf("known port cleanup: %+v %v", r, e)
	}
	assertHeld(t, j, w.CeilingMilliSU)
	if e = j.Store.SettleNative(w.RequestID, 0, strings.Repeat("e", 64)); e != nil {
		t.Fatal("verified known effects cannot settle", e)
	}
	next := w
	next.RequestID = "after-known-port-expiry"
	if _, fresh, e := j.Admit(context.Background(), next); e != nil || !fresh {
		t.Fatal("explicit settlement did not restore usable allowance", fresh, e)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.port || f.server || f.postCount != 1 || f.deletes != 1 {
		t.Fatal("unexpected effects after cleanup or settlement")
	}
}

// Provider responses may omit or null an attachment field. Alter only the
// requested observation stage so each refusal is tested independently.
type incompleteAttachmentTransport struct {
	base     http.RoundTripper
	stage    string
	null     bool
	observed bool
}

func (tr *incompleteAttachmentTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	res, e := tr.base.RoundTrip(req)
	if e != nil || req.Method != "GET" || res.StatusCode != 200 {
		return res, e
	}
	stage := ""
	switch req.URL.Path {
	case "/compute/servers/detail":
		stage = "discovery"
	case "/compute/servers/server":
		stage = "fresh"
	}
	if stage == "" || (tr.stage != "both" && tr.stage != stage) {
		return res, nil
	}
	raw, e := io.ReadAll(res.Body)
	res.Body.Close()
	if e != nil {
		return nil, e
	}
	var payload map[string]any
	if e = json.Unmarshal(raw, &payload); e != nil {
		return nil, e
	}
	alter := func(s map[string]any) {
		tr.observed = true
		if tr.null {
			s["os-extended-volumes:volumes_attached"] = nil
		} else {
			delete(s, "os-extended-volumes:volumes_attached")
		}
	}
	if s, ok := payload["server"].(map[string]any); ok {
		alter(s)
	}
	if ss, ok := payload["servers"].([]any); ok {
		for _, v := range ss {
			alter(v.(map[string]any))
		}
	}
	raw, e = json.Marshal(payload)
	if e != nil {
		return nil, e
	}
	res.Body = io.NopCloser(bytes.NewReader(raw))
	res.ContentLength = int64(len(raw))
	return res, nil
}
func TestCleanupRefusesMissingOrNullAttachmentInventory(t *testing.T) {
	for _, stage := range []string{"discovery", "fresh", "both"} {
		for _, mode := range []string{"omitted", "null"} {
			t.Run(stage+"/"+mode, func(t *testing.T) {
				t.Parallel()
				c, f, j, w := durableCloud(t)
				r, e := c.CreateWorker(context.Background(), w)
				if e != nil {
					t.Fatal(e)
				}
				// The combined case also reproduces the reviewer's hidden retained-volume
				// scenario, while the individual cases isolate each observation boundary.
				if stage == "both" {
					f.mu.Lock()
					f.storage = true
					f.mu.Unlock()
				}
				transport := &incompleteAttachmentTransport{base: c.http.Transport, stage: stage, null: mode == "null"}
				c.http.Transport = transport
				r, e = c.TeardownWorker(context.Background(), r)
				if !transport.observed || e == nil || r.CleanupVerified || !r.UnresolvedLiability {
					t.Fatalf("unknown attachment evidence accepted: %+v %v", r, e)
				}
				f.mu.Lock()
				deletes, port, server := f.deletes, f.port, f.server
				f.mu.Unlock()
				if deletes != 0 || !port || !server {
					t.Fatal("resources removed before complete attachment evidence")
				}
				assertHeld(t, j, w.CeilingMilliSU)
				if e = j.Store.SettleNative(w.RequestID, 0, strings.Repeat("f", 64)); e == nil {
					t.Fatal("unknown storage allowed settlement")
				}
			})
		}
	}
}
