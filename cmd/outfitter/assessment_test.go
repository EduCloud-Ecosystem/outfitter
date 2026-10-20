package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestAssessmentExecutableHasNoStateOrEngineEffects(t *testing.T) {
	root := t.TempDir()
	binary := filepath.Join(root, "outfitter")
	if out, e := exec.Command("go", "build", "-o", binary, ".").CombinedOutput(); e != nil {
		t.Fatalf("build: %v %s", e, out)
	}
	work, bin := filepath.Join(root, "empty"), filepath.Join(root, "engines")
	for _, dir := range []string{work, bin} {
		if e := os.Mkdir(dir, 0700); e != nil {
			t.Fatal(e)
		}
	}
	for _, name := range []string{"docker", "podman", "kubectl", "openstack"} {
		if e := os.WriteFile(filepath.Join(bin, name), []byte("#!/bin/sh\necho called > engine-called\nexit 99\n"), 0700); e != nil {
			t.Fatal(e)
		}
	}
	for _, fixture := range []string{"", "v2/", "v2/maximum-", "v2/maximum-settled-", "v2/unknown-held-", "v2/unknown-settled-"} {
		request, e := os.ReadFile("../../internal/assessment/testdata/" + fixture + "validate-request.canonical.json")
		if e != nil {
			t.Fatal(e)
		}
		expected, e := os.ReadFile("../../internal/assessment/testdata/" + fixture + "validate-result.canonical.json")
		if e != nil {
			t.Fatal(e)
		}
		for _, args := range [][]string{{"assessment-receipts"}, {"--state-dir", filepath.Join(work, "state"), "assessment-receipts"}, {"assessment-receipts", "--state-dir", "SECRET"}} {
			cmd := exec.Command(binary, args...)
			cmd.Dir = work
			cmd.Env = []string{"PATH=" + bin}
			cmd.Stdin = bytes.NewReader(request)
			var out, errout bytes.Buffer
			cmd.Stdout = &out
			cmd.Stderr = &errout
			e = cmd.Run()
			if len(args) == 1 {
				if e != nil || !bytes.Equal(out.Bytes(), expected) {
					t.Fatalf("pure executable: %v %s", e, out.Bytes())
				}
			} else if e == nil || out.String() != "{\"error\":\"INVALID_ASSESSMENT_ARGUMENTS\"}\n" {
				t.Fatal("state arguments accepted/leaked", e, out.String())
			}
			if errout.Len() != 0 {
				t.Fatal("stderr leaked", errout.String())
			}
			entries, e := os.ReadDir(work)
			if e != nil || len(entries) != 0 {
				t.Fatal("command touched state or engine", e, entries)
			}
		}
		if fixture != "" {
			// The real binary must refuse old dispatch and false trust without any
			// output beyond a fixed error, engine call, or state creation.
			for _, bad := range [][]byte{
				bytes.ReplaceAll(request, []byte(`"operation":"validate_v2"`), []byte(`"operation":"validate"`)),
				bytes.ReplaceAll(request, []byte(`"authenticated":false`), []byte(`"authenticated":true`)),
				bytes.ReplaceAll(request, []byte(`"input_bytes":5000000000`), []byte(`"input_bytes":5000000001`)),
			} {
				cmd := exec.Command(binary, "assessment-receipts")
				cmd.Dir, cmd.Env, cmd.Stdin = work, []string{"PATH=" + bin}, bytes.NewReader(bad)
				var out, errout bytes.Buffer
				cmd.Stdout, cmd.Stderr = &out, &errout
				if e := cmd.Run(); e == nil || errout.Len() != 0 || len(out.Bytes()) > 80 || !bytes.HasPrefix(out.Bytes(), []byte(`{"error":"INVALID_`)) {
					t.Fatal("real CLI accepted or leaked invalid v2 request", e, out.String(), errout.String())
				}
				entries, e := os.ReadDir(work)
				if e != nil || len(entries) != 0 {
					t.Fatal("refusal touched state or engine", e, entries)
				}
			}
		}
	}
}
