package jetstream2

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
)

func TestOfflineExperimentReviewConfigurationDuringPreflight(t *testing.T) {
	r, x := experimentCandidate(t)
	hit := false
	r.c.rescueFault = func(point string) error {
		if point == "reservation-before" {
			hit = true
			return ConfigureOfflineExperiment(r.j, x)
		}
		return nil
	}
	got, err := r.c.CreateWorker(context.Background(), r.w)
	if err != nil || !hit || got.ExperimentDigest == "" {
		t.Fatalf("fresh configuration not enforced: %v", err)
	}
	assertExperiment(t, r, true)
	if p, _ := r.counts(); p != 2 {
		t.Fatal(p)
	}
}

func TestOfflineExperimentReviewAuthorityAndStateAtEffect(t *testing.T) {
	for _, phase := range []string{"port", "server"} {
		for _, change := range []string{"authority", "record"} {
			t.Run(phase+"/"+change, func(t *testing.T) {
				r, x := experimentCandidate(t)
				configureExperiment(t, r, x)
				hit := false
				r.c.rescueFault = func(point string) error {
					if point != phase+"-post-before" || hit {
						return nil
					}
					hit = true
					return r.j.Store.Update(func(d *ledger.Data) error {
						if change == "authority" {
							d.Authority.ID = strings.Repeat("e", 32)
							return nil
						}
						n := d.NativeRuns[r.w.RequestID]
						var w WorkerRecord
						if err := json.Unmarshal(n.State, &w); err != nil {
							return err
						}
						w.Status = "unknown"
						n.State, _ = json.Marshal(w)
						d.NativeRuns[r.w.RequestID] = n
						return nil
					})
				}
				_, err := r.c.CreateWorker(context.Background(), r.w)
				if err == nil || !hit {
					t.Fatalf("changed durable authority/state did not block: %v", err)
				}
				want := 0
				if phase == "server" {
					want = 1
				}
				if p, _ := r.counts(); p != want {
					t.Fatalf("posts %d want %d", p, want)
				}
				assertExperiment(t, r, true)
				if change == "authority" {
					if _, err = r.c.CreateWorker(context.Background(), r.w); err == nil {
						t.Fatal("foreign authority replay allowed")
					}
				}
			})
		}
	}
}
