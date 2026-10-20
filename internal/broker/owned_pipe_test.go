//go:build darwin || linux

package broker

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func ownedTestFIFO(t *testing.T, stage string) (*os.File, *os.File) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "lifeline")
	if e := syscall.Mkfifo(path, 0600); e != nil {
		t.Fatal(e)
	}
	r, e := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { r.Close() })
	w, e := os.OpenFile(path, os.O_WRONLY|syscall.O_NONBLOCK, 0)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { w.Close() })
	if stage == "renamed" || stage == "unlinked" {
		newPath := path + "-renamed"
		if e := os.Rename(path, newPath); e != nil {
			t.Fatal(e)
		}
		path = newPath
	}
	if stage == "unlinked" {
		if e := os.Remove(path); e != nil {
			t.Fatal(e)
		}
	}
	return r, w
}

func TestOwnedPipeIdentityRejectsFIFO(t *testing.T) {
	for _, name := range []string{"named", "renamed", "unlinked"} {
		t.Run(name, func(t *testing.T) {
			r, w := ownedTestFIFO(t, name)
			for _, endpoint := range []struct {
				name  string
				file  *os.File
				write bool
			}{{"read", r, false}, {"write", w, true}} {
				t.Run(endpoint.name, func(t *testing.T) {
					st, e := endpoint.file.Stat()
					if e != nil {
						t.Fatal(e)
					}
					raw := st.Sys().(*syscall.Stat_t)
					t.Logf("opened FIFO: mode=%#o device=%d inode=%d nlink=%d", raw.Mode, raw.Dev, raw.Ino, raw.Nlink)
					if _, e := pipeIdentity(endpoint.file, endpoint.write); e == nil {
						t.Fatal("filesystem FIFO accepted as anonymous lifeline")
					}
				})
			}
		})
	}
}

func TestOwnedPipeIdentityEndpoints(t *testing.T) {
	r, w, e := os.Pipe()
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	defer w.Close()
	readID, e := pipeIdentity(r, false)
	if e != nil {
		t.Fatal("anonymous read endpoint refused", e)
	}
	if _, e := pipeIdentity(w, true); e != nil {
		t.Fatal("anonymous write endpoint refused", e)
	}
	if _, e := pipeIdentity(r, true); e == nil {
		t.Fatal("read endpoint accepted as writer")
	}
	if _, e := pipeIdentity(w, false); e == nil {
		t.Fatal("write endpoint accepted as reader")
	}
	if got := livePipe(r, readID); got != "observed_current" {
		t.Fatal("live anonymous pair", got)
	}
	other, otherWriter, e := os.Pipe()
	if e != nil {
		t.Fatal(e)
	}
	defer other.Close()
	defer otherWriter.Close()
	if got := livePipe(other, readID); got != "unknown" {
		t.Fatal("unrelated live pipe matched bound identity", got)
	}
	w.Close()
	if got := livePipe(r, readID); got != "lost" {
		t.Fatal("closed writer not observed", got)
	}
	r.Close()
	if _, e := pipeIdentity(r, false); e == nil {
		t.Fatal("closed descriptor accepted")
	}
	if _, e := pipeIdentity(nil, false); e == nil {
		t.Fatal("nil descriptor accepted")
	}
}

func TestOwnedPipeIdentityPreservesRegularFileLinks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lease")
	f, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	if _, e := openedIdentity(f, "file"); e != nil {
		t.Fatal("private single-link file refused", e)
	}
	if _, e := pipeIdentity(f, false); e == nil {
		t.Fatal("regular file accepted as pipe")
	}
	link := path + "-hardlink"
	if e := os.Link(path, link); e != nil {
		t.Fatal(e)
	}
	if _, e := openedIdentity(f, "file"); e == nil {
		t.Fatal("multiply linked file accepted")
	}
	if e := os.Remove(path); e != nil {
		t.Fatal(e)
	}
	if e := os.Remove(link); e != nil {
		t.Fatal(e)
	}
	if _, e := openedIdentity(f, "file"); e == nil {
		t.Fatal("unlinked regular file accepted")
	}
}

func TestOwnedSeparateProcessesRefuseFIFOLifelines(t *testing.T) {
	for _, endpoint := range []string{"owner", "watch"} {
		for _, name := range []string{"named", "renamed", "unlinked"} {
			t.Run(endpoint+"/"+name, func(t *testing.T) {
				f := newOwnedProcessFixture(t)
				r, w := ownedTestFIFO(t, name)
				if endpoint == "owner" {
					f.ownerRead.Close()
					f.ownerRead = r
				} else {
					f.watchRead.Close()
					f.watchWrite.Close()
					f.watchRead, f.watchWrite = r, w
				}
				_, failure := f.invoke("prepare", f.prepare, nil, "")
				if failure.Code == "" {
					t.Fatal("filesystem FIFO authorized prepare")
				}
				entries, e := os.ReadDir(f.root)
				if e != nil || len(entries) != 0 {
					t.Fatal("FIFO prepare created content scope", e)
				}
				if f.scope().ScopeID != "" {
					t.Fatal("FIFO prepare changed scope journal")
				}
				if _, e := os.Stat(f.pid); !os.IsNotExist(e) {
					t.Fatal("FIFO prepare spawned watcher", e)
				}
			})
		}
	}
}
