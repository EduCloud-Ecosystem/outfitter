package ledger

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBudgetCannotResetOnReopen(t *testing.T) {
	dir := t.TempDir()
	os.Chmod(dir, 0700)
	s, e := Open(dir, true)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.InitProject("p", 30); e != nil {
		t.Fatal(e)
	}
	s, e = Open(dir, true)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.InitProject("p", 60); e == nil {
		t.Fatal("budget reset accepted")
	}
	u, e := s.Usage("p")
	if e != nil || u.Budget != 30 {
		t.Fatal(u, e)
	}
}
func TestMissingAndCorruptLedgerFailClosed(t *testing.T) {
	for _, mode := range []string{"missing", "corrupt"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			os.Chmod(dir, 0700)
			_, e := Open(dir, true)
			if e != nil {
				t.Fatal(e)
			}
			p := filepath.Join(dir, "ledger.json")
			if mode == "missing" {
				os.Remove(p)
			} else {
				os.WriteFile(p, []byte("{"), 0600)
			}
			if _, e = Open(dir, true); e == nil {
				t.Fatal("invalid state silently reset")
			}
		})
	}
}
func TestStateSymlinkRefused(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "real")
	os.Mkdir(target, 0700)
	link := filepath.Join(root, "link")
	os.Symlink(target, link)
	if _, e := Open(link, true); e == nil {
		t.Fatal("symlink state accepted")
	}
}
