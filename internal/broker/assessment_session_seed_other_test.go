//go:build !linux

package broker

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func executionSeedSupported() bool { return false }
func executionSeedDriverSetup(_ string, _ bool) (executionSeedAssets, *os.File, *executionSeedChild, error) {
	return executionSeedAssets{}, nil, nil, executionSessionProtocolError
}
func executionSeedReadConfig(_ string) ([]byte, error) { return nil, executionSessionProtocolError }
func executionSeedPlatformEntry(_ string) int {
	// Refuse before opening even the request. This is not a legacy fallback.
	if executionSessionPrivatePipe(os.Stdout) != nil || executionSessionPrivatePipe(os.Stderr) != nil || os.Stdout.SetWriteDeadline(time.Now().Add(time.Second)) != nil {
		_ = executionSessionCloseFiles([9]*os.File{os.Stdin, os.Stdout, os.Stderr})
		return 2
	}
	_ = executionSessionWrite(os.Stdout, []byte("{\"code\":\"REFUSED\",\"schema\":\"outfitter.execution-session-seed-result/v1\"}\n"))
	_ = executionSessionCloseFiles([9]*os.File{os.Stdin, os.Stdout, os.Stderr})
	return 2
}
func TestExecutionSessionSeedUnsupportedTarget(t *testing.T) {
	executionSeedValidateVector(t, executionSeedVectorSource)
	parent, e := os.MkdirTemp("", "outfitter-unsupported-seed-")
	if e != nil {
		t.Fatal("private refusal parent")
	}
	var original *executionSeedChild
	// Unknown original Wait retains this root. Disposal here is test fixture
	// disposal only, never a core cleanup confirmation.
	t.Cleanup(func() {
		if original == nil || original.WaitKnown() {
			if os.RemoveAll(parent) != nil {
				t.Error("known refusal fixture disposal")
			}
		}
	})
	path := filepath.Join(parent, executionSeedRequestName)
	positiveEnd := time.Now().Add(10 * time.Second)
	reapEnd := positiveEnd.Add(2 * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), positiveEnd)
	defer cancel()
	input, e := os.Open("/dev/null")
	if e != nil {
		t.Fatal("fixed input unavailable")
	}
	outR, outW, e := os.Pipe()
	if e != nil {
		_ = input.Close()
		t.Fatal("private reply pipe")
	}
	errR, errW, e := os.Pipe()
	if e != nil {
		_ = input.Close()
		_ = outR.Close()
		_ = outW.Close()
		t.Fatal("private error pipe")
	}
	files := []*os.File{input, outR, outW, errR, errW}
	closed := map[*os.File]bool{}
	closeFile := func(f *os.File) error {
		if f == nil || closed[f] {
			return nil
		}
		closed[f] = true
		return f.Close()
	}
	finish := func() error {
		var e error
		for _, f := range files {
			if closeFile(f) != nil {
				e = executionSessionProtocolError
			}
		}
		return e
	}
	t.Cleanup(func() {
		if original != nil && !original.Finish() {
			t.Error("original refusal Wait unknown; fixture retained")
		}
		if finish() != nil {
			t.Error("checked unsupported endpoint close")
		}
	})
	out, e := executionSessionSpawnDuplicate(outW)
	if out != nil {
		files = append(files, out)
	}
	if e != nil {
		t.Fatal("private reply duplicate")
	}
	errout, e := executionSessionSpawnDuplicate(errW)
	if errout != nil {
		files = append(files, errout)
	}
	if e != nil {
		t.Fatal("private error duplicate")
	}
	if outR.SetReadDeadline(reapEnd) != nil || errR.SetReadDeadline(reapEnd) != nil {
		t.Fatal("private refusal deadline")
	}
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestExecutionPrivateSessionSeedHelper$")
	cmd.Env = []string{"OUTFITTER_PRIVATE_EXECUTION_SESSION_SEED=1", "OUTFITTER_PRIVATE_EXECUTION_SESSION_SEED_REQUEST=" + path, "GOMAXPROCS=2"}
	cmd.Stdin, cmd.Stdout, cmd.Stderr = input, out, errout
	cmd.WaitDelay = 250 * time.Millisecond
	if cmd.Start() != nil {
		t.Fatal("original unsupported child spawn")
	}
	for _, f := range []*os.File{input, outW, errW, out, errout} {
		if closeFile(f) != nil {
			t.Error("checked parent spawn close")
		}
	}
	original = executionSeedChildWait(cmd, cancel, finish, reapEnd)
	stdout, re := io.ReadAll(io.LimitReader(outR, 257))
	stderr, se := io.ReadAll(io.LimitReader(errR, 4097))
	left := time.Until(reapEnd)
	if left <= 0 {
		t.Fatal("original refusal Wait unavailable")
	}
	timer := time.NewTimer(left)
	defer timer.Stop()
	select {
	case <-original.done:
	case <-timer.C:
		t.Fatal("original refusal Wait unknown")
	}
	if re != nil || se != nil || original.waitErr == nil || original.exitCode != 2 || ctx.Err() != nil || len(stderr) != 0 || !bytes.Equal(stdout, []byte("{\"code\":\"REFUSED\",\"schema\":\"outfitter.execution-session-seed-result/v1\"}\n")) {
		clear(stdout)
		clear(stderr)
		t.Fatal("original unsupported child did not refuse cleanly")
	}
	if finish() != nil {
		t.Fatal("refusal endpoint close acknowledgement")
	}
	if _, e = os.Lstat(filepath.Join(parent, executionSeedTargetName)); !os.IsNotExist(e) {
		t.Fatal("unsupported child touched target")
	}
	if _, e = os.Lstat(path); !os.IsNotExist(e) {
		t.Fatal("unsupported child touched request fixture")
	}
}
