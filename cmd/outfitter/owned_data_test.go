package main

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
	"github.com/EduCloud-Ecosystem/outfitter/internal/owneddata"
)

func ownedCLIBinary(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "outfitter")
	cmd := exec.Command("go", "build", "-p", "1", "-o", path, ".")
	cmd.Env = append(os.Environ(), "GOMAXPROCS=2")
	if output, e := cmd.CombinedOutput(); e != nil {
		t.Fatalf("build: %v %s", e, output)
	}
	return path
}

func TestOwnedCLIOnlyFixedCommandPosition(t *testing.T) {
	binary := ownedCLIBinary(t)
	state := filepath.Join(t.TempDir(), "state")
	cmd := exec.Command(binary, "--state-dir", state, "project-init", "--project-id", "owned-data", "--budget-cpu-seconds", "90")
	if out, e := cmd.CombinedOutput(); e != nil {
		t.Fatalf("ordinary project value hijacked: %v %s", e, out)
	}
	s, e := ledger.Open(state, false)
	if e != nil {
		t.Fatal(e)
	}
	u, e := s.Usage("owned-data")
	if e != nil || u.Remaining != 90 {
		t.Fatal("legacy project semantics changed", u, e)
	}
}

func TestOwnedCLIGuardDoesNotMigrateLegacy(t *testing.T) {
	binary := ownedCLIBinary(t)
	for _, command := range []string{"assessment-receipts", "consumer-preflight", "jetstream2-preflight"} {
		t.Run(command, func(t *testing.T) {
			s, e := ledger.Open(filepath.Join(t.TempDir(), "state"), true)
			if e != nil {
				t.Fatal(e)
			}
			if _, e = s.InitProject("legacy", 90); e != nil {
				t.Fatal(e)
			}
			path := filepath.Join(s.Dir, "ledger.json")
			raw, e := os.ReadFile(path)
			if e != nil {
				t.Fatal(e)
			}
			var d map[string]any
			if e = json.Unmarshal(raw, &d); e != nil {
				t.Fatal(e)
			}
			d["version"] = 1
			delete(d, "authority")
			before, _ := json.Marshal(d)
			if e = os.WriteFile(path, before, 0600); e != nil {
				t.Fatal(e)
			}
			cmd := exec.Command(binary, "--state-dir", s.Dir, command)
			if out, e := cmd.CombinedOutput(); e == nil {
				t.Fatalf("invalid invocation succeeded: %s", out)
			}
			after, e := os.ReadFile(path)
			if e != nil || !bytes.Equal(before, after) {
				t.Fatal("pure/refused command migrated legacy journal", e)
			}
		})
	}
}

func TestOwnedCLIContentGatePrecedesStdinAndState(t *testing.T) {
	binary := ownedCLIBinary(t)
	for _, operation := range []string{"prepare", "ingest", "seal"} {
		for _, extra := range []bool{false, true} {
			t.Run(operation+map[bool]string{true: "/extra", false: "/fixed"}[extra], func(t *testing.T) {
				state := filepath.Join(t.TempDir(), "absent-state")
				args := []string{"--state-dir", state, "owned-data", operation}
				if extra {
					args = append(args, "--enable", "PRIVATE_CALLER_VALUE")
				}
				cmd := exec.Command(binary, args...)
				cmd.Env = append(os.Environ(), "OUTFITTER_OWNED_DATA_ENABLED=true", "OUTFITTER_OWNED_TEST_HELPER=1", "OUTFITTER_OWNED_TEST_SEED="+state)
				input, e := cmd.StdinPipe()
				if e != nil {
					t.Fatal(e)
				}
				defer input.Close()
				var stdout, stderr bytes.Buffer
				cmd.Stdout = &stdout
				cmd.Stderr = &stderr
				if e = cmd.Start(); e != nil {
					t.Fatal(e)
				}
				done := make(chan error, 1)
				go func() { done <- cmd.Wait() }()
				select {
				case e = <-done:
					if e == nil || cmd.ProcessState.ExitCode() != 2 {
						t.Fatal("content gate exit changed")
					}
				case <-time.After(time.Second):
					_ = cmd.Process.Kill()
					<-done
					t.Fatal("production content gate read stdin")
				}
				want, _ := owneddata.CanonicalReply(owneddata.ErrorReply{Schema: owneddata.ErrorSchema, Code: "ASSESSMENT_UPLOAD_DISABLED"})
				if !bytes.Equal(stdout.Bytes(), want) || stderr.Len() != 0 {
					t.Fatal("content gate framing/raw output", stdout.String(), stderr.String())
				}
				if _, e := os.Lstat(state); !os.IsNotExist(e) {
					t.Fatal("production content gate created state", e)
				}
			})
		}
	}
}
