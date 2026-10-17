package broker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
)

func TestRecoveryRetainsUnknownReservationsWithoutReplayAndRequiresReconciliation(t *testing.T) {
	b, request, p := setup(t, 90)
	p.createError = true
	unknown, e := b.Run(context.Background(), request)
	if e != nil || !unknown.UnresolvedLiability {
		t.Fatal(unknown, e)
	}
	root := filepath.Dir(b.Store.Dir)
	cp := filepath.Join(root, "checkpoint")
	snapshot, e := b.Store.Checkpoint(cp)
	if e != nil {
		t.Fatal(e)
	}
	restored, e := ledger.Restore(filepath.Join(root, "restored"), cp, snapshot.CheckpointSHA256)
	if e != nil {
		t.Fatal(e)
	}
	if restored.Admission != "held" || len(restored.Unresolved) != 1 || restored.Projects[0].Unknown != 30 || restored.Projects[0].Remaining != 60 {
		t.Fatal(restored)
	}
	dest, e := ledger.Open(filepath.Join(root, "restored"), false)
	if e != nil {
		t.Fatal(e)
	}
	rb := New(dest, p)
	before := p.created
	refused, e := rb.Run(context.Background(), request)
	if e != nil || refused.Admitted || !refused.CleanupVerified || refused.Compute.Provenance != "not_admitted" || p.created != before {
		t.Fatal("held duplicate executed", refused, e)
	}
	request.RequestID = "new"
	if _, _, e = rb.Reserve(request); !errors.Is(e, ledger.ErrAdmission) {
		t.Fatal("held new reservation admitted", e)
	}
	if _, e = dest.Activate(b.Store.Dir, cp, snapshot.CheckpointSHA256); e == nil {
		t.Fatal("unknown provider liability activated")
	}
	sourceStatus, _ := b.Store.RecoveryStatus()
	if sourceStatus.Admission != "active" {
		t.Fatal(sourceStatus)
	}
	after, _ := dest.RecoveryStatus()
	if after.Projects[0] != restored.Projects[0] {
		t.Fatal("unknown reservation refunded", after)
	}
}
func TestRecoveryCompletedDiagnosticsAndBudgetSurviveCutover(t *testing.T) {
	b, request, p := setup(t, 90)
	original, e := b.Run(context.Background(), request)
	if e != nil {
		t.Fatal(e)
	}
	root := filepath.Dir(b.Store.Dir)
	cp := filepath.Join(root, "checkpoint")
	snapshot, e := b.Store.Checkpoint(cp)
	if e != nil {
		t.Fatal(e)
	}
	destination := filepath.Join(root, "restored")
	if _, e = ledger.Restore(destination, cp, snapshot.CheckpointSHA256); e != nil {
		t.Fatal(e)
	}
	dest, e := ledger.Open(destination, false)
	if e != nil {
		t.Fatal(e)
	}
	receipt, e := dest.Receipt(request.RequestID)
	if e != nil || receipt.Stdout != original.Stdout || receipt.Compute != original.Compute {
		t.Fatal(receipt, e)
	}
	if _, e = dest.Activate(b.Store.Dir, cp, snapshot.CheckpointSHA256); e != nil {
		t.Fatal(e)
	}
	if _, _, e = b.Reserve(request); !errors.Is(e, ledger.ErrAdmission) {
		t.Fatal("fenced duplicate admitted", e)
	}
	source, e := ledger.Open(b.Store.Dir, false)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = New(source, p).Reap(); e != nil {
		t.Fatal("fencing blocked cleanup", e)
	}
	rb := New(dest, p)
	rb.StartWatchdog = func(string) error { return nil }
	request.RequestID = "next"
	receipt, e = rb.Run(context.Background(), request)
	if e != nil || !receipt.CleanupVerified {
		t.Fatal(receipt, e)
	}
	usage, e := dest.Usage(request.ProjectID)
	if e != nil || usage.Charged != 60 || usage.Remaining != 30 {
		t.Fatal(usage, e)
	}
}
func TestRecoveryMissingOutputRefusesCompleteCheckpointWithoutBlockingAuthority(t *testing.T) {
	b, r, _ := setup(t, 90)
	if _, e := b.Run(context.Background(), r); e != nil {
		t.Fatal(e)
	}
	record, e := b.Store.Record(r.RequestID)
	if e != nil {
		t.Fatal(e)
	}
	if e = os.Remove(filepath.Join(b.Store.Dir, "outputs", record.OutputSHA256+".json")); e != nil {
		t.Fatal(e)
	}
	cp := filepath.Join(filepath.Dir(b.Store.Dir), "checkpoint")
	if _, e = b.Store.Checkpoint(cp); e == nil {
		t.Fatal("missing output silently omitted")
	}
	if _, e = os.Stat(filepath.Join(cp, "manifest.json")); !os.IsNotExist(e) {
		t.Fatal("partial checkpoint marked complete")
	}
	if _, e = b.Reap(); e != nil {
		t.Fatal("diagnostic failure blocked cleanup", e)
	}
	usage, e := b.Store.Usage(r.ProjectID)
	if e != nil || usage.Charged != 30 || usage.Remaining != 60 {
		t.Fatal(usage, e)
	}
}
