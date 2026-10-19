package kubernetes

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/EduCloud-Ecosystem/outfitter/internal/broker"
	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
	"github.com/EduCloud-Ecosystem/outfitter/internal/provider"
)

// Fake legacy provider: no Docker process or engine is involved.
type legacyReaperFake struct{ cleaned []string }

func (*legacyReaperFake) Name() string { return "docker" }
func (*legacyReaperFake) Capabilities() provider.Capabilities {
	return provider.Capabilities{Isolation: []string{"reviewed-local"}}
}
func (*legacyReaperFake) Estimate(w provider.Request) (int64, error) {
	return w.ReservationAmount(), w.Validate()
}
func (*legacyReaperFake) Provision(context.Context, provider.Request, provider.Reservation) error {
	return errors.New("unexpected provision during reaper test")
}
func (*legacyReaperFake) Run(context.Context, provider.Record, io.Writer, io.Writer) (int, error) {
	return -1, errors.New("unexpected run during reaper test")
}
func (*legacyReaperFake) Status(context.Context, provider.Record) (provider.State, error) {
	return provider.State{}, errors.New("unexpected status during reaper test")
}
func (p *legacyReaperFake) Teardown(_ context.Context, r provider.Record) error {
	if e := provider.CheckPlacement(p, r.Request); e != nil {
		return e
	}
	p.cleaned = append(p.cleaned, r.RequestID)
	return nil
}

func TestReapMixedAuthoritySelectsOnlyExactPlacement(t *testing.T) {
	for _, scenario := range []string{"docker-with-offline", "offline-with-docker", "other-cluster", "other-namespace-uid"} {
		t.Run(scenario, func(t *testing.T) {
			b, _, w, api := setup(t, 60)
			legacyProvider := &legacyReaperFake{}
			legacy := broker.New(b.Store, legacyProvider)
			old := cloneRequest(w)
			old.Placement = nil
			old.RequestID = "legacy-1"
			ownBroker, foreignBroker := b, legacy
			ownRequest, foreignRequest := w, old
			if scenario == "docker-with-offline" {
				ownBroker, foreignBroker = legacy, b
				ownRequest, foreignRequest = old, w
			} else if scenario == "other-cluster" || scenario == "other-namespace-uid" {
				target := api.target
				if scenario == "other-cluster" {
					target.ClusterID = strings.Repeat("c", 64)
				} else {
					target.NamespaceUID = "different-namespace-uid"
				}
				a, e := NewOffline(b.Store, target, &fakeAPI{target: target})
				if e != nil {
					t.Fatal(e)
				}
				foreignBroker = broker.New(b.Store, a)
				foreignRequest = cloneRequest(w)
				foreignRequest.RequestID = "foreign-1"
				foreignRequest.Placement.ClusterID = target.ClusterID
				foreignRequest.Placement.NamespaceUID = target.NamespaceUID
				foreignRequest.Placement.AttemptID = "foreign-attempt"
			}
			for _, admission := range []struct {
				b *broker.Broker
				w provider.Request
			}{{ownBroker, ownRequest}, {foreignBroker, foreignRequest}} {
				if _, fresh, e := admission.b.Reserve(admission.w); e != nil || !fresh {
					t.Fatal("reserve mixed authority", fresh, e)
				}
			}
			if e := b.Store.Update(func(d *ledger.Data) error {
				for id, r := range d.Runs {
					r.PID = 0 // Abandoned reservation; no process is started or killed.
					d.Runs[id] = r
				}
				return nil
			}); e != nil {
				t.Fatal(e)
			}
			before, e := json.Marshal(record(t, foreignBroker, foreignRequest.RequestID))
			if e != nil {
				t.Fatal(e)
			}
			receipts, e := ownBroker.Reap()
			if e != nil || len(receipts) != 1 {
				t.Fatalf("foreign placement blocked owned reaping: receipts=%d error=%v", len(receipts), e)
			}
			r := receipts[0]
			if r.RequestID != ownRequest.RequestID || !r.CleanupVerified || r.UnresolvedLiability || r.Compute.Charged != 30 {
				t.Fatal("owned attempt not cleaned with full charge", r)
			}
			if _, e := ownBroker.Cancel(foreignRequest.RequestID, "foreign direct cancel"); e == nil {
				t.Fatal("foreign direct cancellation accepted")
			}
			if _, e := ownBroker.Reconcile(foreignRequest.RequestID); e == nil {
				t.Fatal("foreign direct reconciliation accepted")
			}
			after, e := json.Marshal(record(t, foreignBroker, foreignRequest.RequestID))
			if e != nil || string(after) != string(before) {
				t.Fatal("foreign record or liability changed", e)
			}
			wantLegacyCleaned := 0
			if scenario == "docker-with-offline" {
				wantLegacyCleaned = 1
			}
			if len(legacyProvider.cleaned) != wantLegacyCleaned || api.count() != 0 {
				t.Fatal("unexpected provider effects", legacyProvider.cleaned, api.count())
			}
		})
	}
}
