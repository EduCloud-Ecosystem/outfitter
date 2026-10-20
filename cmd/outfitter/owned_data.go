package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"strings"

	"github.com/EduCloud-Ecosystem/outfitter/internal/assessment"
	"github.com/EduCloud-Ecosystem/outfitter/internal/broker"
	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
	"github.com/EduCloud-Ecosystem/outfitter/internal/owneddata"
)

func isOwnedCommand(args []string) bool {
	i := ownedCommandPosition(args)
	return i >= 0 && args[i] == "owned-data"
}

func ownedCommandPosition(args []string) int {
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			if i+1 < len(args) {
				return i + 1
			}
			return -1
		}
		if arg == "--state-dir" || arg == "-state-dir" {
			i++
			if i >= len(args) {
				return -1
			}
			continue
		}
		if strings.HasPrefix(arg, "--state-dir=") || strings.HasPrefix(arg, "-state-dir=") {
			continue
		}
		if strings.HasPrefix(arg, "-") {
			return -1
		}
		return i
	}
	return -1
}

func executeOwnedCommand(args []string, in io.Reader, out io.Writer) error {
	replyError := func(e error) error {
		code := "ASSESSMENT_UNRESOLVED"
		for _, known := range []error{owneddata.ErrInvalid, owneddata.ErrMissing, owneddata.ErrDisabled, owneddata.ErrDenied, owneddata.ErrCollision, owneddata.ErrStale, owneddata.ErrBusy} {
			if errors.Is(e, known) {
				code = known.Error()
				break
			}
		}
		raw, _ := owneddata.CanonicalReply(owneddata.ErrorReply{Schema: owneddata.ErrorSchema, Code: code})
		_, _ = out.Write(raw)
		return errReportedOwned
	}
	// The content gate is based solely on fixed argv. Extra enabling-looking
	// arguments cannot cause stdin/state reads or expose raw flag diagnostics.
	i := ownedCommandPosition(args)
	if i < 0 || args[i] != "owned-data" || i+1 >= len(args) {
		return replyError(owneddata.ErrInvalid)
	}
	operation := args[i+1]
	if owneddata.OneOf(operation, "prepare", "ingest", "seal") {
		return replyError(owneddata.ErrDisabled)
	}
	root := flag.NewFlagSet("owned-data", flag.ContinueOnError)
	root.SetOutput(io.Discard)
	state := root.String("state-dir", "", "")
	if root.Parse(args) != nil || len(root.Args()) != 2 || *state == "" || !owneddata.OneOf(operation, "status", "cancel", "reconcile") {
		return replyError(owneddata.ErrInvalid)
	}
	c, e := assessment.ReadOwnedControl(operation, in)
	if e != nil {
		return replyError(owneddata.ErrInvalid)
	}
	version, e := ledger.ReadOnlyVersion(*state)
	if e != nil {
		return replyError(owneddata.ErrUnresolved)
	}
	if version != 6 {
		return replyError(owneddata.ErrDenied)
	}
	s, e := ledger.Open(*state, false)
	if e != nil {
		return replyError(owneddata.ErrUnresolved)
	}
	status, e := broker.New(s, nil).OwnedData(context.Background(), c, nil)
	if e != nil {
		return replyError(e)
	}
	raw, e := owneddata.CanonicalReply(status)
	if e != nil {
		return replyError(owneddata.ErrUnresolved)
	}
	if n, e := out.Write(raw); e != nil || n != len(raw) {
		return errReportedOwned
	}
	return nil
}
