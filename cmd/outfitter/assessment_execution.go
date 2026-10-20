package main

import (
	"context"
	"flag"
	"io"

	x "github.com/EduCloud-Ecosystem/outfitter/internal/assessmentexec"
	"github.com/EduCloud-Ecosystem/outfitter/internal/broker"
	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
)

func isAssessmentExecutionCommand(args []string) bool {
	i := ownedCommandPosition(args)
	return i >= 0 && args[i] == "assessment-execution"
}
func executeAssessmentExecutionCommand(args []string, in io.Reader, out io.Writer) error {
	fail := func(e error) error {
		code := x.ErrHeld.Error()
		for _, known := range []error{x.ErrInvalid, x.ErrDisabled, x.ErrDenied, x.ErrCollision, x.ErrCapacity} {
			if e == known {
				code = e.Error()
			}
		}
		raw, _ := x.EncodeReply(x.Error{Schema: x.ErrorSchema, Code: code})
		_, _ = out.Write(raw)
		return errReportedOwned
	}
	i := ownedCommandPosition(args)
	if i < 0 || args[i] != "assessment-execution" || i+1 >= len(args) {
		return fail(x.ErrInvalid)
	}
	op := args[i+1]
	// Fixed trusted argv classification happens before flag parsing, state probes
	// and stdin consumption. No environment/config/header enables content work.
	switch op {
	case "prepare", "ingest", "seal", "execute":
		return fail(x.ErrDisabled)
	}
	f := flag.NewFlagSet("assessment-execution", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	state := f.String("state-dir", "", "")
	if f.Parse(args) != nil || len(f.Args()) != 2 || *state == "" || (op != "status" && op != "cancel" && op != "reconcile") {
		return fail(x.ErrInvalid)
	}
	raw, e := io.ReadAll(io.LimitReader(in, x.MaxControlBytes+1))
	if e != nil {
		return fail(x.ErrInvalid)
	}
	c, e := x.DecodeControl(raw, op)
	if e != nil {
		return fail(e)
	}
	v, e := ledger.ReadOnlyVersion(*state)
	if e != nil {
		return fail(x.ErrHeld)
	}
	if v != 7 {
		return fail(x.ErrDenied)
	}
	s, e := ledger.Open(*state, false)
	if e != nil {
		return fail(x.ErrHeld)
	}
	status, e := broker.New(s, nil).AssessmentData(context.Background(), c, nil)
	if e != nil {
		return fail(e)
	}
	reply, e := x.EncodeReply(status)
	if e != nil {
		return fail(e)
	}
	n, e := out.Write(reply)
	if e != nil || n != len(reply) {
		return errReportedOwned
	}
	return nil
}
