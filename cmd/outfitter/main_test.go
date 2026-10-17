package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/EduCloud-Ecosystem/outfitter/internal/provider/jetstream2"
	"testing"
)

func TestFailedPreflightReportSignalsNonzeroExit(t *testing.T) {
	for _, check := range []string{"active pinned image checksum", "free instances quota"} {
		t.Run(check, func(t *testing.T) {
			var out bytes.Buffer
			report := jetstream2.Preflight{ResourceChecksPassed: false, Checks: []jetstream2.Check{{Name: check, Passed: false}}}
			e := emitPreflight(&out, report)
			if !errors.Is(e, errReportedPreflight) {
				t.Fatal("failed resource checks did not return exit failure", e)
			}
			var decoded jetstream2.Preflight
			if e = json.Unmarshal(out.Bytes(), &decoded); e != nil || decoded.ResourceChecksPassed {
				t.Fatal(out.String(), e)
			}
		})
	}
}
func TestSuccessfulPreflightReportsSuccess(t *testing.T) {
	var out bytes.Buffer
	if e := emitPreflight(&out, jetstream2.Preflight{ResourceChecksPassed: true}); e != nil {
		t.Fatal(e)
	}
}
