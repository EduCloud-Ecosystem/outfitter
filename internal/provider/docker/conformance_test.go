package docker_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/EduCloud-Ecosystem/outfitter/internal/provider"
	"github.com/EduCloud-Ecosystem/outfitter/internal/provider/docker"
)

// exerciseLifecycle is provider-independent; each adapter supplies its configured instance.
func exerciseLifecycle(t *testing.T, p provider.Provisioner, image string) {
	t.Helper()
	root, e := filepath.EvalSymlinks(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	r := provider.Request{SchemaVersion: 1, RequestID: "conformance", ProjectID: "test", Image: image, Command: []string{"python", "-c", "import time; time.sleep(5)"}, InputDir: root, RecipeDir: root, CPUs: 1, MemoryMB: 128, PIDsLimit: 32, TimeoutSeconds: 15, MaxOutputBytes: 1024, Isolation: "reviewed-local", AllowSupervisedReaper: true}
	name := "outfitter-conformance-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	record := provider.Record{Receipt: provider.Receipt{RequestID: r.RequestID}, Name: name, Owner: "conformance"}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	defer p.Teardown(context.Background(), record)
	before, e := p.Status(ctx, record)
	if e != nil || before.Exists {
		t.Fatal(before, e)
	}
	cost, e := p.Estimate(r)
	if e != nil || cost != 15 {
		t.Fatal(cost, e)
	}
	after, e := p.Status(ctx, record)
	if e != nil || after.Exists {
		t.Fatal("estimate had provider effects", after, e)
	}
	for _, reservation := range []provider.Reservation{{}, {ID: r.RequestID, Amount: 15, Name: name, Owner: record.Owner, Deadline: time.Now().Add(-time.Second)}} {
		if p.Provision(ctx, r, reservation) == nil {
			t.Fatal("unreserved or expired creation accepted")
		}
	}
	state, e := p.Status(ctx, record)
	if e != nil || state.Exists {
		t.Fatal("refused request created resource", state, e)
	}
	reservation := provider.Reservation{ID: r.RequestID, Amount: 15, Name: name, Owner: record.Owner, Deadline: time.Now().Add(15 * time.Second)}
	strong := r
	strong.Isolation = "T-strong"
	if p.Provision(ctx, strong, reservation) == nil {
		t.Fatal("isolation silently downgraded")
	}
	if e = p.Provision(ctx, r, reservation); e != nil {
		t.Fatal(e)
	}
	done := make(chan struct{})
	go func() { p.Run(ctx, record, io.Discard, io.Discard); close(done) }()
	running := false
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		state, e = p.Status(ctx, record)
		if e == nil && state.Running {
			running = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !running {
		t.Fatal("placement never reached running", state, e)
	}
	if p.Capabilities().UsageSampling {
		t.Fatal("test expects explicitly unsupported usage sampling")
	}
	foreign := record
	foreign.Owner = "foreign"
	if p.Teardown(ctx, foreign) == nil {
		t.Fatal("foreign teardown accepted")
	}
	if e = p.Teardown(ctx, record); e != nil {
		t.Fatal(e)
	}
	if e = p.Teardown(ctx, record); e != nil {
		t.Fatal("repeated teardown", e)
	}
	state, e = p.Status(ctx, record)
	if e != nil || state.Exists {
		t.Fatal("cleanup unverified", state, e)
	}
	<-done
}
func TestDockerLifecycleConformance(t *testing.T) {
	image := os.Getenv("OUTFITTER_DOCKER_IMAGE")
	if image == "" {
		t.Skip("actual Docker lifecycle requires OUTFITTER_DOCKER_IMAGE immutable local Python image")
	}
	exerciseLifecycle(t, docker.New(), image)
}
func TestProviderSelfExpiryNotClaimed(t *testing.T) {
	if docker.New().Capabilities().SelfExpiry {
		t.Fatal("Docker cannot claim provider self-expiry")
	}
	t.Skip("Docker uses explicit supervised external watchdog; host/watchdog-loss conformance unsupported")
}
