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
	request, e := os.ReadFile("../../internal/assessment/testdata/validate-request.json")
	if e != nil {
		t.Fatal(e)
	}
	expected, e := os.ReadFile("../../internal/assessment/testdata/validate-result.canonical.json")
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
}
