package main

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	x "github.com/EduCloud-Ecosystem/outfitter/internal/assessmentexec"
	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
)

type executionNeverRead struct{}

func (executionNeverRead) Read([]byte) (int, error) {
	panic("production assessment execution read content")
}
func executionCLIBinding() x.ScopeBinding {
	code := x.CodeBinding{RecipeID: "summary", CodeSHA256: strings.Repeat("a", 64), EnvironmentID: "fixture", Image: "sha256:" + strings.Repeat("b", 64), InterpreterID: "python", InterpreterVersion: "3.12+fixture", InterpreterIdentitySHA256: strings.Repeat("c", 64)}
	return x.ScopeBinding{BrokerAuthorityID: strings.Repeat("1", 32), WaypointAuthorityID: strings.Repeat("2", 32), ScopeID: strings.Repeat("3", 32), AssessmentID: strings.Repeat("4", 32), ProjectID: strings.Repeat("5", 32), ManifestSchema: "educloud.benchmark-manifest/v2", ManifestSHA256: strings.Repeat("d", 64), ExecutionProfile: x.ProfileRef{ID: "cpu-python-r-bounded", Version: 1}, Capability: x.CapabilityRef{ID: "fixture", Version: 1, SHA256: strings.Repeat("e", 64)}, Workload: code, Checker: code}
}
func TestExecutionCLIContentRefusalPrecedesInputAndState(t *testing.T) {
	for _, op := range []string{"prepare", "ingest", "seal", "execute"} {
		for _, extra := range []bool{false, true} {
			t.Run(op+map[bool]string{false: "/fixed", true: "/flag"}[extra], func(t *testing.T) {
				state := filepath.Join(t.TempDir(), "absent")
				args := []string{"--state-dir", state, "assessment-execution", op}
				if extra {
					args = append(args, "--enable", "true")
				}
				var out bytes.Buffer
				if e := executeAssessmentExecutionCommand(args, executionNeverRead{}, &out); e != errReportedOwned {
					t.Fatal(e)
				}
				if out.String() != "{\"code\":\"ASSESSMENT_DISABLED\",\"schema\":\"outfitter.assessment-execution-error/v1\"}\n" {
					t.Fatal("unexpected fixed refusal", out.String())
				}
				if _, e := os.Stat(state); !os.IsNotExist(e) {
					t.Fatal("refusal created state", e)
				}
			})
		}
	}
}
func TestExecutionCLIMetadataRefusalCannotMigrateLegacy(t *testing.T) {
	for _, op := range []string{"status", "cancel", "reconcile"} {
		t.Run(op, func(t *testing.T) {
			s, e := ledger.Open(filepath.Join(t.TempDir(), "state"), true)
			if e != nil {
				t.Fatal(e)
			}
			path := filepath.Join(s.Dir, "ledger.json")
			raw, e := os.ReadFile(path)
			if e != nil {
				t.Fatal(e)
			}
			var data map[string]any
			if e = json.Unmarshal(raw, &data); e != nil {
				t.Fatal(e)
			}
			data["version"] = 1
			delete(data, "authority")
			before, e := json.Marshal(data)
			if e != nil {
				t.Fatal(e)
			}
			if e = os.WriteFile(path, before, 0600); e != nil {
				t.Fatal(e)
			}
			stat, _ := os.Stat(path)
			c := x.Control{Schema: x.ControlSchema, Operation: op, Binding: executionCLIBinding()}
			if op == "cancel" {
				reason := "cancelled"
				c.Reason = &reason
			}
			control, e := json.Marshal(c)
			if e != nil {
				t.Fatal(e)
			}
			var out bytes.Buffer
			if e = executeAssessmentExecutionCommand([]string{"--state-dir", s.Dir, "assessment-execution", op}, bytes.NewReader(control), &out); e != errReportedOwned {
				t.Fatal(e)
			}
			after, e := os.ReadFile(path)
			next, _ := os.Stat(path)
			if e != nil || !bytes.Equal(before, after) || !os.SameFile(stat, next) {
				t.Fatal("metadata refusal migrated legacy journal", e)
			}
			if !strings.Contains(out.String(), "ASSESSMENT_DENIED") {
				t.Fatal(out.String())
			}
		})
	}
}
func TestExecutionCLIEnvelopeOperationMismatch(t *testing.T) {
	c := x.Control{Schema: x.ControlSchema, Operation: "status", Binding: executionCLIBinding()}
	raw, _ := json.Marshal(c)
	state := filepath.Join(t.TempDir(), "absent")
	if e := executeAssessmentExecutionCommand([]string{"--state-dir", state, "assessment-execution", "reconcile"}, bytes.NewReader(raw), io.Discard); e != errReportedOwned {
		t.Fatal(e)
	}
	if _, e := os.Stat(state); !os.IsNotExist(e) {
		t.Fatal("mismatched operation touched state")
	}
}
