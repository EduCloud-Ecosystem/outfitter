package broker

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"time"

	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
	"github.com/EduCloud-Ecosystem/outfitter/internal/owneddata"
)

func readOwnedReady(ctx context.Context, r io.ReadCloser) (owneddata.Ready, error) {
	var ready owneddata.Ready
	f, ok := r.(*os.File)
	if !ok {
		return ready, owneddata.ErrUnresolved
	}
	if _, e := pipeIdentity(f, false); e != nil {
		return ready, e
	}
	if f.SetReadDeadline(time.Now().Add(3*time.Second)) != nil {
		return ready, owneddata.ErrUnresolved
	}
	stop := context.AfterFunc(ctx, func() { _ = f.Close() })
	defer stop()
	raw, e := io.ReadAll(io.LimitReader(f, owneddata.MaxReplyBytes+1))
	if e != nil || ctx.Err() != nil || len(raw) == 0 || len(raw) > owneddata.MaxReplyBytes || raw[len(raw)-1] != '\n' || bytes.Count(raw, []byte{'\n'}) != 1 || owneddata.ClosedJSON(raw[:len(raw)-1], &ready, owneddata.MaxReplyBytes) != nil || ready.Validate() != nil {
		return ready, owneddata.ErrUnresolved
	}
	canonical, e := owneddata.CanonicalReply(ready)
	if e != nil || !bytes.Equal(raw, canonical) {
		return ready, owneddata.ErrUnresolved
	}
	return ready, nil
}
func readyFor(s owneddata.Scope, authority string) owneddata.Ready {
	watch := ""
	if s.WatchEpoch != nil {
		watch = *s.WatchEpoch
	}
	return owneddata.Ready{Schema: owneddata.ReadySchema, ScopeID: s.ScopeID, AuthorityID: authority, WaypointAuthorityID: s.WaypointAuthorityID, ProjectID: s.ProjectID, ManifestSHA256: s.ManifestSHA256, DeclaredUploadCeilingBytes: s.DeclaredUploadCeilingBytes, AdmittingOwnerEpoch: s.AdmittingOwnerEpoch, WriterEpoch: s.WriterEpoch, WatchEpoch: watch, Generation: s.Generation, ExpiresAt: s.ExpiresAt, ClockEpoch: s.ClockEpoch, DeadlineTickMS: s.DeadlineTickMS}
}
func readyMatches(r owneddata.Ready, s owneddata.Scope, authority string) bool {
	return r.Validate() == nil && r == readyFor(s, authority) && s.State == "accepting" && !s.DenyLatched && s.WatchStarted
}

func (b *Broker) ownedWatch(ctx context.Context, id string) error {
	var c owneddata.Control
	e := b.Store.View(func(d ledger.Data) error {
		if d.Version != 6 {
			return owneddata.ErrDenied
		}
		s, ok := (*d.OwnedDataScopes)[id]
		if !ok {
			return owneddata.ErrMissing
		}
		c = owneddata.Control{ScopeID: id, WaypointAuthorityID: s.WaypointAuthorityID, ExpectedBrokerAuthorityID: d.Authority.ID, AdmittingOwnerEpoch: s.AdmittingOwnerEpoch, WriterEpoch: s.WriterEpoch}
		return nil
	})
	if e != nil {
		return ownedError(e)
	}
	s, authority, e := b.ownedRead(c)
	if e != nil {
		return e
	}
	x := b.owned
	if x == nil {
		_, _ = b.ownedDeny(c, "restart")
		_, _ = b.ownedCleanup(c)
		return owneddata.ErrDisabled
	}
	defer func() {
		if x.watchWrite != nil {
			_ = x.watchWrite.Close()
		}
		if x.readyWrite != nil {
			_ = x.readyWrite.Close()
		}
	}()
	fail := func(reason string) error {
		_, _ = b.ownedDeny(c, reason)
		_, _ = b.ownedCleanup(c)
		return owneddata.ErrUnresolved
	}
	if !s.WatchStarted || s.WatchEpoch == nil || s.State != "preparing" || s.DenyLatched {
		return fail("restart")
	}
	writeID, e := pipeIdentity(x.watchWrite, true)
	if e != nil || writeID != s.FilesystemIdentity.WatchWrite {
		return fail("owner_mismatch")
	}
	if _, e = pipeIdentity(x.readyWrite, true); e != nil {
		return fail("owner_mismatch")
	}
	if livePipe(x.ownerRead, s.FilesystemIdentity.OwnerRead) != "observed_current" {
		return fail("restart")
	}
	release, e := x.lease(s, "watch", true)
	if e != nil {
		return fail("restart")
	}
	defer release()
	n, e := b.ownedCurrent(c, s, false)
	if e != nil {
		return fail("restart")
	}
	s, e = b.ownedCommit(c, "watch-ready", false, n, func(v *owneddata.Scope, n ownedClock) error {
		if v.DenyLatched || v.State != "preparing" || v.WatchEpoch == nil || *v.WatchEpoch != *s.WatchEpoch || ownedClockReason(*v, n) != "" {
			return owneddata.ErrDenied
		}
		v.State = "accepting"
		applyOwnedClock(v, n)
		return nil
	})
	if e != nil {
		return fail("io_uncertain")
	}
	ack, e := owneddata.CanonicalReply(readyFor(s, authority))
	if e != nil {
		return fail("io_uncertain")
	}
	if x.fail("before-ready-ack") != nil || x.readyWrite.SetWriteDeadline(time.Now().Add(time.Second)) != nil {
		return fail("io_uncertain")
	}
	written, e := x.readyWrite.Write(ack)
	closeErr := x.readyWrite.Close()
	x.readyWrite = nil
	if e != nil || closeErr != nil || written != len(ack) || x.fail("after-ready-ack") != nil {
		return fail("io_uncertain")
	}
	// The watcher remembers every observed clock advance even between journal
	// transitions. Healthy ticks never change the generation acknowledged above.
	high := n
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return fail("restart")
		case <-ticker.C:
		}
		s, _, e = b.ownedRead(c)
		if e != nil {
			return owneddata.ErrUnresolved
		}
		reason := ""
		if s.DenyLatched {
			reason = s.Reason
		} else {
			n, e = b.ownedNow()
			if e != nil {
				reason = "clock_rollback"
			} else if n.Epoch != high.Epoch || n.Wall.Before(high.Wall) || n.TickMS < high.TickMS {
				reason = "clock_rollback"
			} else {
				reason = ownedClockReason(s, n)
				high = n
			}
			if reason == "" && livePipe(x.ownerRead, s.FilesystemIdentity.OwnerRead) != "observed_current" {
				reason = "restart"
			}
			if reason == "" && (x.checkRoot() != nil || x.checkLeases(s) != nil) {
				reason = "owner_mismatch"
			}
			if reason == "" && s.WriterStarted && !s.UploadComplete {
				drain, err := x.lease(s, "writer", true)
				if err == nil {
					drain()
					reason = "incomplete_upload"
				} else if !errors.Is(err, owneddata.ErrBusy) {
					reason = "owner_mismatch"
				}
			}
		}
		if reason == "" {
			continue
		}
		if _, e = b.ownedDeny(c, reason); e != nil {
			return owneddata.ErrUnresolved
		}
		status, e := b.ownedCleanup(c)
		if e != nil {
			return e
		}
		latest, _, e := b.ownedRead(c)
		if e != nil {
			return e
		}
		if latest.ObservationsSaturated {
			return owneddata.ErrUnresolved
		}
		// A recorded synthetic absence permits this observer to exit, never a
		// trusted cleanup or reopening claim. At saturation it remains unresolved.
		for _, o := range status.Observations {
			if o.ScopeGeneration == status.Generation && o.OwnedRemoval == "observed_absent" {
				return nil
			}
		}
	}
}
