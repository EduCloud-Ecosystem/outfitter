package broker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
	"github.com/EduCloud-Ecosystem/outfitter/internal/provider"
)

func corruptReviewLedger(t *testing.T, b *Broker, change func(*ledger.Data)) {
	t.Helper()
	path := filepath.Join(b.Store.Dir, "ledger.json")
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var data ledger.Data
	if err := json.Unmarshal(content, &data); err != nil {
		t.Fatal(err)
	}
	change(&data)
	content, err = json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, content, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestReviewPartialChargeCannotReleaseUnknownReservation(t *testing.T) {
	b, request, _ := setup(t, 30)
	if _, _, err := b.Reserve(request); err != nil {
		t.Fatal(err)
	}
	corruptReviewLedger(t, b, func(data *ledger.Data) {
		run := data.Runs[request.RequestID]
		run.Compute.Charged = 1
		data.Runs[request.RequestID] = run
	})
	reopened, err := ledger.Open(b.Store.Dir, false)
	if err != nil {
		return
	} // Refusing inconsistent durable accounting is safe.
	usage, err := reopened.Usage(request.ProjectID)
	if err != nil {
		return
	}
	if usage.Remaining != 0 {
		t.Fatalf("unresolved 30-second reservation regained %d seconds after partial charge: %+v", usage.Remaining, usage)
	}
}

func TestReviewReceiptProjectCannotRetargetCommittedReservation(t *testing.T) {
	b, request, _ := setup(t, 30)
	if _, err := b.Store.InitProject("another-project", 30); err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.Reserve(request); err != nil {
		t.Fatal(err)
	}
	corruptReviewLedger(t, b, func(data *ledger.Data) {
		run := data.Runs[request.RequestID]
		run.ProjectID = "another-project"
		data.Runs[request.RequestID] = run
	})
	reopened, err := ledger.Open(b.Store.Dir, false)
	if err != nil {
		return
	}
	usage, err := reopened.Usage(request.ProjectID)
	if err != nil {
		return
	}
	if usage.Remaining != 0 {
		t.Fatalf("request for %s was reattributed despite immutable payload hash; original allowance restored: %+v", request.ProjectID, usage)
	}
}

func TestReviewAcceptedBoundedOutputsCannotMakeLedgerUnreadable(t *testing.T) {
	b, request, fake := setup(t, 1000)
	request.MaxOutputBytes = 1048576
	fake.output = strings.Repeat("\x00", request.MaxOutputBytes)
	for i := 0; i < 12; i++ {
		request.RequestID = fmt.Sprintf("size-run-%d", i)
		receipt, err := b.Run(context.Background(), request)
		if err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
		if receipt.Status != "succeeded" {
			t.Fatalf("unexpected run %d status: %s", i, receipt.Status)
		}
		if _, err := ledger.Open(b.Store.Dir, false); err != nil {
			stat, _ := os.Stat(filepath.Join(b.Store.Dir, "ledger.json"))
			t.Fatalf("accepted %d individually bounded runs made %d-byte ledger unreadable: %v", i+1, stat.Size(), err)
		}
	}
}

func TestMissingDiagnosticsNeverBlocksAuthorityOrCleanup(t *testing.T) {
	b, r, _ := setup(t, 60)
	first, e := b.Run(context.Background(), r)
	if e != nil || !first.CleanupVerified {
		t.Fatal(first, e)
	}
	record, e := b.Store.Record(r.RequestID)
	if e != nil || record.OutputSHA256 == "" || record.Stdout != "" {
		t.Fatal(record, e)
	}
	if e = os.Remove(filepath.Join(b.Store.Dir, "outputs", record.OutputSHA256+".json")); e != nil {
		t.Fatal(e)
	}
	observed, e := b.Store.Receipt(r.RequestID)
	if e != nil || observed.OutputError == "" || !observed.CleanupVerified {
		t.Fatal(observed, e)
	}
	r.RequestID = "second"
	if _, fresh, e := b.Reserve(r); e != nil || !fresh {
		t.Fatal(fresh, e)
	}
	cancelled, e := b.Cancel(r.RequestID, "test cancellation")
	if e != nil || !cancelled.CleanupVerified {
		t.Fatal(cancelled, e)
	}
	if _, e = b.Reap(); e != nil {
		t.Fatal(e)
	}
	u, e := b.Store.Usage(r.ProjectID)
	if e != nil || u.Remaining != 0 {
		t.Fatal(u, e)
	}
}
func TestDiagnosticWriteFailureCannotBlockTerminalAccounting(t *testing.T) {
	b, r, _ := setup(t, 30)
	if e := os.WriteFile(filepath.Join(b.Store.Dir, "outputs"), []byte("unavailable output directory"), 0600); e != nil {
		t.Fatal(e)
	}
	out, e := b.Run(context.Background(), r)
	if e != nil || !out.CleanupVerified || out.UnresolvedLiability {
		t.Fatal(out, e)
	}
	receipt, e := b.Store.Receipt(r.RequestID)
	if e != nil || receipt.OutputError == "" || receipt.Compute.Charged != 30 {
		t.Fatal(receipt, e)
	}
}
func TestLegacyInlineDiagnosticsSpillWithoutReset(t *testing.T) {
	b, r, _ := setup(t, 30)
	before, e := b.Run(context.Background(), r)
	if e != nil {
		t.Fatal(e)
	}
	corruptReviewLedger(t, b, func(d *ledger.Data) {
		record := d.Runs[r.RequestID]
		record.Stdout = before.Stdout
		record.Stderr = before.Stderr
		record.OutputSHA256 = ""
		record.OutputBytes = 0
		d.Runs[r.RequestID] = record
	})
	reopened, e := ledger.Open(b.Store.Dir, false)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = reopened.InitProject(r.ProjectID, 30); e != nil {
		t.Fatal(e)
	}
	after, e := reopened.Receipt(r.RequestID)
	if e != nil || after.Stdout != before.Stdout || after.Compute != before.Compute {
		t.Fatal(after, e)
	}
	authority, e := reopened.Record(r.RequestID)
	if e != nil || authority.Stdout != "" || authority.OutputSHA256 == "" {
		t.Fatal(authority, e)
	}
	usage, e := reopened.Usage(r.ProjectID)
	if e != nil || usage.Remaining != 0 {
		t.Fatal(usage, e)
	}
}

func TestFullAuthorityCapacityStillAllowsTerminalCleanup(t *testing.T) {
	b, r, _ := setup(t, 100000)
	r.Command = []string{"python", strings.Repeat("\x01", 3500)}
	r.InputDir = "/" + strings.Repeat("a", 4095)
	r.RecipeDir = "/" + strings.Repeat("b", 4095)
	original, _, e := b.Reserve(r)
	if e != nil {
		t.Fatal(e)
	}
	e = b.Store.Update(func(d *ledger.Data) error {
		for i := 1; i < ledger.MaxRecords; i++ {
			record := original
			record.RequestID = fmt.Sprintf("capacity-%d", i)
			record.Request.RequestID = record.RequestID
			record.PayloadHash = record.Request.Hash()
			hash := sha256.Sum256([]byte(record.Owner + ":" + record.RequestID))
			record.Name = "outfitter-" + hex.EncodeToString(hash[:16])
			record.Status = "failed"
			record.CleanupVerified = true
			record.UnresolvedLiability = false
			record.Compute = provider.Compute{Reserved: 30, Charged: 30, Provenance: "estimated_upper_bound"}
			record.Error = strings.Repeat("\x02", 2048)
			now := time.Now().UTC()
			record.FinishedAt = &now
			d.Runs[record.RequestID] = record
		}
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
	stat, e := os.Stat(filepath.Join(b.Store.Dir, "ledger.json"))
	if e != nil || stat.Size() >= 64<<20 {
		t.Fatal(stat, e)
	}
	extra := r
	extra.RequestID = "over-capacity"
	if _, _, e = b.Reserve(extra); e == nil {
		t.Fatal("admitted beyond reserved metadata capacity")
	}
	cancelled, e := b.Cancel(r.RequestID, "cleanup remains available at capacity")
	if e != nil || !cancelled.CleanupVerified {
		t.Fatal(cancelled, e)
	}
	if _, e = ledger.Open(b.Store.Dir, false); e != nil {
		t.Fatal(e)
	}
}

type lateCreateReviewProvider struct {
	fake
	release  <-chan struct{}
	appeared chan struct{}
}

func (p *lateCreateReviewProvider) Provision(_ context.Context, _ provider.Request, _ provider.Reservation) error {
	go func() {
		<-p.release
		p.mu.Lock()
		p.created++
		p.exists = true
		p.mu.Unlock()
		close(p.appeared)
	}()
	return errors.New("creation response lost while provider may still complete")
}

func TestReviewAmbiguousCreationCannotSettleBeforeLateProviderCompletion(t *testing.T) {
	b, request, _ := setup(t, 30)
	release := make(chan struct{})
	p := &lateCreateReviewProvider{release: release, appeared: make(chan struct{})}
	b.Provider = p
	receipt, err := b.Run(context.Background(), request)
	if err != nil || !receipt.UnresolvedLiability {
		close(release)
		t.Fatal(receipt, err)
	}
	if _, err := b.change(request.RequestID, func(r *provider.Record) { r.PID = 0 }); err != nil {
		close(release)
		t.Fatal(err)
	}
	reconciled, err := b.Reconcile(request.RequestID)
	close(release)
	select {
	case <-p.appeared:
	case <-time.After(time.Second):
		t.Fatal("late creation test control timed out")
	}
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Reap(); err != nil {
		t.Fatal(err)
	}
	observed, err := p.Status(context.Background(), provider.Record{})
	if err != nil {
		t.Fatal(err)
	}
	if reconciled.CleanupVerified && !reconciled.UnresolvedLiability && observed.Exists {
		t.Fatal("ambiguous creation was marked cleaned and removed from reaping before the late resource appeared")
	}
}
