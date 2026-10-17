package ledger

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestRecoverySpecialFilesRefusedWithoutBlocking(t *testing.T) {
	for _, name := range []string{"manifest.json", "ledger.json", "output.json"} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), name)
			if e := syscall.Mkfifo(path, 0600); e != nil {
				t.Fatal(e)
			}
			done := make(chan error, 1)
			go func() { _, e := readBounded(path, 1024); done <- e }()
			select {
			case e := <-done:
				if e == nil {
					t.Fatal("FIFO accepted as a regular checkpoint file")
				}
			case <-time.After(time.Second):
				t.Fatal("special file blocked before validation")
			}
		})
	}
	if _, e := readBounded(os.DevNull, 1024); e == nil {
		t.Fatal("device accepted as a checkpoint file")
	}
}

func TestRecoveryRestoreRefusesFIFOManifestWithoutCreatingDestination(t *testing.T) {
	root := t.TempDir()
	checkpoint := filepath.Join(root, "checkpoint")
	if e := os.Mkdir(checkpoint, 0700); e != nil {
		t.Fatal(e)
	}
	if e := syscall.Mkfifo(filepath.Join(checkpoint, "manifest.json"), 0600); e != nil {
		t.Fatal(e)
	}
	destination := filepath.Join(root, "replacement")
	done := make(chan error, 1)
	go func() { _, e := Restore(destination, checkpoint, strings.Repeat("a", 64)); done <- e }()
	select {
	case e := <-done:
		if e == nil {
			t.Fatal("restore accepted FIFO manifest")
		}
	case <-time.After(time.Second):
		t.Fatal("restore blocked on FIFO manifest")
	}
	if _, e := os.Stat(destination); !os.IsNotExist(e) {
		t.Fatal("invalid checkpoint created destination", e)
	}
}

func recoveryFixture(t *testing.T) (*Store, string, RecoveryReport, *Store) {
	t.Helper()
	root := t.TempDir()
	source, e := Open(filepath.Join(root, "source"), true)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = source.InitProject("project", 90); e != nil {
		t.Fatal(e)
	}
	cp := filepath.Join(root, "checkpoint")
	manifest, e := source.Checkpoint(cp)
	if e != nil {
		t.Fatal(e)
	}
	destPath := filepath.Join(root, "destination")
	restored, e := Restore(destPath, cp, manifest.CheckpointSHA256)
	if e != nil {
		t.Fatal(e)
	}
	if restored.Admission != "held" || restored.Projects[0].Remaining != 90 || restored.AuthorityID == manifest.AuthorityID {
		t.Fatal(restored)
	}
	dest, e := Open(destPath, false)
	if e != nil {
		t.Fatal(e)
	}
	return source, cp, manifest, dest
}
func TestRecoveryMigratesLegacyInPlaceAndRejectsCopiedAuthority(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "source")
	source, e := Open(dir, true)
	if e != nil {
		t.Fatal(e)
	}
	source.InitProject("project", 90)
	var before Data
	if e = source.View(func(d Data) error { before = d; return nil }); e != nil {
		t.Fatal(e)
	}
	legacy := before
	legacy.Version = 1
	legacy.Authority = nil
	b, _ := json.Marshal(legacy)
	if e = os.WriteFile(filepath.Join(dir, "ledger.json"), b, 0600); e != nil {
		t.Fatal(e)
	}
	reopened, e := Open(dir, false)
	if e != nil {
		t.Fatal(e)
	}
	var migrated Data
	reopened.View(func(d Data) error { migrated = d; return nil })
	if migrated.Version != 2 || migrated.Owner != before.Owner || migrated.Projects["project"].Budget != 90 || migrated.Authority.StatePath != source.Dir {
		t.Fatal(migrated)
	}
	b, _ = os.ReadFile(filepath.Join(dir, "ledger.json"))
	// Prior binaries' strict decoder rejects the new authority field.
	var old struct {
		Version  int                `json:"version"`
		Owner    string             `json:"owner"`
		Projects map[string]Project `json:"projects"`
		Runs     map[string]any     `json:"runs"`
	}
	if e = decodeStrict(b, &old); e == nil {
		t.Fatal("old schema decoder accepted new authority")
	}
	copyPath := filepath.Join(root, "copy")
	os.Mkdir(copyPath, 0700)
	os.WriteFile(filepath.Join(copyPath, "ledger.json"), b, 0600)
	if _, e = Open(copyPath, true); e == nil {
		t.Fatal("copied authority opened at new path")
	}
	reopened2, e := Open(dir, false)
	if e != nil {
		t.Fatal(e)
	}
	after, _ := reopened2.RecoveryStatus()
	if after.AuthorityID != migrated.Authority.ID || after.Projects[0].Remaining != 90 {
		t.Fatal(after)
	}
}
func TestRecoveryHeldUntilCurrentSourceCutoverAndFenceIsOneWay(t *testing.T) {
	source, cp, manifest, dest := recoveryFixture(t)
	if _, e := dest.InitProject("project", 90); !errors.Is(e, ErrAdmission) {
		t.Fatal("held duplicate project admitted", e)
	}
	if _, e := dest.InitProject("new", 90); !errors.Is(e, ErrAdmission) {
		t.Fatal("held new project admitted", e)
	}
	if _, e := dest.Activate(filepath.Join(t.TempDir(), "missing"), cp, manifest.CheckpointSHA256); e == nil {
		t.Fatal("missing source activated")
	}
	activated, e := dest.Activate(source.Dir, cp, manifest.CheckpointSHA256)
	if e != nil || activated.Admission != "active" {
		t.Fatal(activated, e)
	}
	status, e := source.RecoveryStatus()
	if e != nil || status.Admission != "fenced" {
		t.Fatal(status, e)
	}
	if _, e = source.InitProject("project", 90); !errors.Is(e, ErrAdmission) {
		t.Fatal("fenced source admitted", e)
	}
	if _, e = dest.InitProject("another", 30); e != nil {
		t.Fatal(e)
	}
	again, e := dest.Activate(source.Dir, cp, manifest.CheckpointSHA256)
	if e != nil || again.Admission != "active" || len(again.Projects) != 2 {
		t.Fatal("idempotent retry altered destination", again, e)
	}
	otherPath := filepath.Join(filepath.Dir(source.Dir), "other")
	if _, e = Restore(otherPath, cp, manifest.CheckpointSHA256); e != nil {
		t.Fatal(e)
	}
	other, e := Open(otherPath, false)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = other.Activate(source.Dir, cp, manifest.CheckpointSHA256); e == nil {
		t.Fatal("second destination activated")
	}
	status, _ = other.RecoveryStatus()
	if status.Admission != "held" {
		t.Fatal(status)
	}
	if _, e = source.Checkpoint(filepath.Join(filepath.Dir(source.Dir), "fenced-copy")); !errors.Is(e, ErrAdmission) {
		t.Fatal("fenced authority made active checkpoint", e)
	}
}
func TestRecoveryCrashAfterFenceLeavesBothNonAdmittingAndRetryable(t *testing.T) {
	source, cp, manifest, dest := recoveryFixture(t)
	crash := errors.New("simulated loss after durable source fence")
	if _, e := dest.activate(source.Dir, cp, manifest.CheckpointSHA256, func() error { return crash }); !errors.Is(e, crash) {
		t.Fatal(e)
	}
	a, _ := source.RecoveryStatus()
	b, _ := dest.RecoveryStatus()
	if a.Admission != "fenced" || b.Admission != "held" {
		t.Fatal(a, b)
	}
	for _, s := range []*Store{source, dest} {
		if _, e := s.InitProject("project", 90); !errors.Is(e, ErrAdmission) {
			t.Fatal("admission during crash window", e)
		}
	}
	source, e := Open(source.Dir, false)
	if e != nil {
		t.Fatal(e)
	}
	dest, e = Open(dest.Dir, false)
	if e != nil {
		t.Fatal(e)
	}
	if r, e := dest.Activate(source.Dir, cp, manifest.CheckpointSHA256); e != nil || r.Admission != "active" {
		t.Fatal(r, e)
	}
}
func TestRecoveryStaleSourceOrChangedHeldDestinationCannotActivate(t *testing.T) {
	for _, changed := range []string{"source", "destination"} {
		t.Run(changed, func(t *testing.T) {
			source, cp, manifest, dest := recoveryFixture(t)
			if changed == "source" {
				source.InitProject("later", 30)
			} else {
				dest.Update(func(d *Data) error { d.Projects["project"] = Project{"project", 80}; return nil })
			}
			if _, e := dest.Activate(source.Dir, cp, manifest.CheckpointSHA256); e == nil {
				t.Fatal("changed authority activated")
			}
			a, _ := source.RecoveryStatus()
			b, _ := dest.RecoveryStatus()
			if a.Admission != "active" || b.Admission != "held" {
				t.Fatal(a, b)
			}
		})
	}
}
func TestRecoveryExecutionLeaseExcludesCutover(t *testing.T) {
	source, cp, manifest, dest := recoveryFixture(t)
	release, e := source.ExecutionLease()
	if e != nil {
		t.Fatal(e)
	}
	if _, e = dest.Activate(source.Dir, cp, manifest.CheckpointSHA256); e == nil {
		t.Fatal("cutover raced an execution")
	}
	status, _ := source.RecoveryStatus()
	if status.Admission != "active" {
		t.Fatal(status)
	}
	release()
	if _, e = dest.Activate(source.Dir, cp, manifest.CheckpointSHA256); e != nil {
		t.Fatal(e)
	}
}
func TestRecoveryConcurrentActivationHasAtMostOneAdmittingDestination(t *testing.T) {
	source, cp, manifest, first := recoveryFixture(t)
	secondPath := filepath.Join(filepath.Dir(source.Dir), "second")
	if _, e := Restore(secondPath, cp, manifest.CheckpointSHA256); e != nil {
		t.Fatal(e)
	}
	second, e := Open(secondPath, false)
	if e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	for _, s := range []*Store{first, second} {
		wg.Add(1)
		go func(s *Store) { defer wg.Done(); s.Activate(source.Dir, cp, manifest.CheckpointSHA256) }(s)
	}
	wg.Wait()
	a, _ := source.RecoveryStatus()
	b, _ := first.RecoveryStatus()
	c, _ := second.RecoveryStatus()
	active := 0
	for _, r := range []RecoveryReport{a, b, c} {
		if r.Admission == "active" {
			active++
		}
	}
	if active != 1 || a.Admission != "fenced" {
		t.Fatal(a, b, c)
	}
}
func TestRecoveryCheckpointTamperingAndOverwritesRefused(t *testing.T) {
	for _, mode := range []string{"digest", "ledger", "manifest", "extra", "symlink"} {
		t.Run(mode, func(t *testing.T) {
			source, cp, m, _ := recoveryFixture(t)
			if _, e := source.Checkpoint(cp); e == nil {
				t.Fatal("checkpoint overwrite")
			}
			digest := m.CheckpointSHA256
			switch mode {
			case "digest":
				digest = strings.Repeat("0", 64)
			case "ledger":
				os.WriteFile(filepath.Join(cp, "ledger.json"), []byte("{}"), 0600)
			case "manifest":
				os.Remove(filepath.Join(cp, "manifest.json"))
			case "extra":
				os.WriteFile(filepath.Join(cp, "unexpected"), []byte("x"), 0600)
			case "symlink":
				os.Symlink(filepath.Join(source.Dir, "ledger.json"), filepath.Join(cp, "link"))
			}
			path := filepath.Join(filepath.Dir(source.Dir), "bad-restore")
			if _, e := Restore(path, cp, digest); e == nil {
				t.Fatal("invalid checkpoint accepted")
			}
			if _, e := os.Stat(path); !os.IsNotExist(e) {
				t.Fatal("unverified checkpoint wrote destination", e)
			}
		})
	}
	source, cp, m, dest := recoveryFixture(t)
	if _, e := Restore(dest.Dir, cp, m.CheckpointSHA256); e == nil {
		t.Fatal("restore overwrote destination")
	}
	status, _ := source.RecoveryStatus()
	if status.Admission != "active" {
		t.Fatal(status)
	}
}
