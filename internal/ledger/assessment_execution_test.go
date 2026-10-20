package ledger

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	x "github.com/EduCloud-Ecosystem/outfitter/internal/assessmentexec"
	"github.com/EduCloud-Ecosystem/outfitter/internal/provider"
)

func freshExecutionAuthority(t *testing.T) (*Store, Data) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "fresh-seven")
	if e := os.Mkdir(dir, 0700); e != nil {
		t.Fatal(e)
	}
	var e error
	dir, e = filepath.EvalSymlinks(dir)
	if e != nil {
		t.Fatal(e)
	}
	identity := func(name string) x.FilesystemIdentity {
		path := dir
		if name != "" {
			path = filepath.Join(dir, name)
			if e := writeExclusive(path, nil); e != nil {
				t.Fatal(e)
			}
		}
		st, e := os.Stat(path)
		if e != nil {
			t.Fatal(e)
		}
		id, e := FileIdentity(st)
		if e != nil {
			t.Fatal(e)
		}
		return id
	}
	i := x.AuthorityIdentity{WaypointAuthorityID: strings.Repeat("4", 32), Directory: identity(""), JournalLease: identity("ledger.lock"), ExecutionLease: identity("execution.lock")}
	if e := writeExclusive(filepath.Join(dir, "initialized"), nil); e != nil {
		t.Fatal(e)
	}
	project := strings.Repeat("2", 32)
	d := Data{Version: 7, Owner: strings.Repeat("f", 32), Authority: &Authority{ID: strings.Repeat("5", 32), StatePath: dir, Admission: "active"}, Projects: map[string]Project{project: {ID: project, Budget: 21600}}, Runs: map[string]provider.Record{}, NativeAccounts: map[string]NativeAccount{}, NativeRuns: map[string]NativeRecord{}, AssessmentExecution: &x.Authority{Schema: x.AuthoritySchema, Identity: i, Scopes: map[string]x.ExecutionScope{}, Attempts: map[string]x.AssessmentAttempt{}}}
	s := &Store{Dir: dir}
	if e = s.write(d); e != nil {
		t.Fatal(e)
	}
	s, e = Open(dir, false)
	if e != nil {
		t.Fatal(e)
	}
	return s, d
}
func TestExecutionFreshSevenHasNoProductionGrantOrMigration(t *testing.T) {
	s, d := freshExecutionAuthority(t)
	before, e := os.ReadFile(filepath.Join(s.Dir, "ledger.json"))
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.InitProject(strings.Repeat("2", 32), 21600); e == nil {
		t.Fatal("ordinary idempotent init admitted v7")
	}
	for name, mutate := range map[string]func(*Data){
		"budget":    func(d *Data) { p := d.Projects[strings.Repeat("2", 32)]; p.Budget++; d.Projects[p.ID] = p },
		"version":   func(d *Data) { d.Version = 2; d.AssessmentExecution = nil },
		"authority": func(d *Data) { d.Authority.ID = strings.Repeat("a", 32) },
		"owner":     func(d *Data) { d.Owner = strings.Repeat("b", 32) },
		"identity":  func(d *Data) { d.AssessmentExecution.Identity.Directory.Inode = "999" },
	} {
		t.Run(name, func(t *testing.T) {
			if e := s.Update(func(d *Data) error { mutate(d); return nil }); e == nil {
				t.Fatal("mutated frozen v7 authority")
			}
			after, _ := os.ReadFile(filepath.Join(s.Dir, "ledger.json"))
			if !bytes.Equal(before, after) {
				t.Fatal("refusal changed persisted accounting")
			}
		})
	}
	if v, e := ReadOnlyVersion(s.Dir); e != nil || v != 7 {
		t.Fatal(v, e)
	}
	if u, e := s.Usage(strings.Repeat("2", 32)); e != nil || u.Budget != 21600 || u.Remaining != 21600 || u.Reserved != 0 || u.Charged != 0 || u.Unknown != 0 {
		t.Fatal(u, e)
	}
	if _, e := s.Checkpoint(filepath.Join(t.TempDir(), "checkpoint")); e == nil {
		t.Fatal("exported v7")
	}
	copyDir := filepath.Join(t.TempDir(), "copy")
	if e := os.Mkdir(copyDir, 0700); e != nil {
		t.Fatal(e)
	}
	copyData := d
	copyData.Authority = &Authority{ID: d.Authority.ID, StatePath: copyDir, Admission: "active"}
	raw, _ := json.Marshal(copyData)
	if e := os.WriteFile(filepath.Join(copyDir, "ledger.json"), raw, 0600); e != nil {
		t.Fatal(e)
	}
	if _, e := Open(copyDir, false); e == nil {
		t.Fatal("copied identity opened")
	}
	for _, file := range []string{"ledger.lock", "execution.lock", "initialized"} {
		if _, e := os.Stat(filepath.Join(copyDir, file)); !os.IsNotExist(e) {
			t.Fatal("read probe created copy object", file, e)
		}
	}
}
func TestExecutionNoopDoesNotRewriteAuthority(t *testing.T) {
	s, _ := freshExecutionAuthority(t)
	path := filepath.Join(s.Dir, "ledger.json")
	before, e := os.Stat(path)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Update(func(d *Data) error { return nil }); e != nil {
		t.Fatal(e)
	}
	after, e := os.Stat(path)
	if e != nil {
		t.Fatal(e)
	}
	if !os.SameFile(before, after) || !before.ModTime().Equal(after.ModTime()) {
		t.Fatal("no-op replaced journal")
	}
}
