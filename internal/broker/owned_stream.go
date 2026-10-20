package broker

import (
	"context"
	"errors"
	"io"
	"os"

	"github.com/EduCloud-Ecosystem/outfitter/internal/owneddata"
)

func (b *Broker) ownedIngest(ctx context.Context, c owneddata.Control, body io.Reader) (out owneddata.Status, err error) {
	s, authority, e := b.ownedRead(c)
	if e != nil {
		return out, e
	}
	if body == nil || !owneddata.Uint(c.ContentLength) || c.ContentLength == 0 || c.ContentLength > s.DeclaredUploadCeilingBytes || c.WriterEpoch != s.WriterEpoch {
		return out, owneddata.ErrInvalid
	}
	if c.ExpectedGeneration != s.Generation {
		return out, owneddata.ErrStale
	}
	if s.State != "accepting" || s.WriterStarted {
		return out, owneddata.ErrDenied
	}
	if _, e = b.ownedCurrent(c, s, true); e != nil {
		return out, e
	}
	execution, e := b.Store.ExecutionLease()
	if e != nil {
		return out, owneddata.ErrUnresolved
	}
	writer, e := b.owned.lease(s, "writer", false)
	if e != nil {
		execution()
		return out, ownedError(e)
	}
	var content *os.File
	attempted := false
	closeUncertain := false
	defer func() {
		if content != nil {
			closeErr := b.owned.closeContent(content, "deferred")
			closeUncertain = closeUncertain || closeErr != nil
			// A later successful close cannot erase an earlier failed close whose
			// uncertainty publication itself failed. Never publish confirmation then.
			_, proofErr := b.ownedCloseProof(c, closeUncertain)
			if closeErr != nil || proofErr != nil {
				err = owneddata.ErrUnresolved
				_, _ = b.ownedDeny(c, "io_uncertain")
			}
		}
		writer()
		execution()
		if attempted && err != nil {
			_, _ = b.ownedDeny(c, "incomplete_upload")
			_, _ = b.ownedCleanup(c)
		}
	}()
	n, e := b.ownedCurrent(c, s, true)
	if e != nil {
		return out, e
	}
	s, e = b.ownedCommit(c, "writer-claim", true, n, func(v *owneddata.Scope, n ownedClock) error {
		if v.Generation != c.ExpectedGeneration {
			return owneddata.ErrStale
		}
		if v.DenyLatched || v.WriterStarted || v.State != "accepting" || ownedClockReason(*v, n) != "" {
			return owneddata.ErrDenied
		}
		attempted = true
		v.WriterStarted = true
		applyOwnedClock(v, n)
		return nil
	})
	if e != nil {
		if attempted {
			_, _ = b.ownedDeny(c, "io_uncertain")
		}
		return out, e
	}
	check := func() error {
		if ctx.Err() != nil {
			return owneddata.ErrDenied
		}
		var e error
		s, authority, e = b.ownedRead(c)
		if e != nil {
			return e
		}
		previous := n
		n, e = b.ownedCurrent(c, s, true)
		if e != nil {
			return e
		}
		if n.Epoch != previous.Epoch || n.Wall.Before(previous.Wall) || n.TickMS < previous.TickMS {
			_, _ = b.ownedDeny(c, "clock_rollback")
			return owneddata.ErrDenied
		}
		if s.State != "accepting" || !s.WriterStarted || s.UploadComplete {
			return owneddata.ErrDenied
		}
		return nil
	}
	if e = check(); e != nil {
		return out, e
	}
	var identity owneddata.FilesystemIdentity
	content, identity, e = b.owned.openContent(s, authority)
	if e != nil {
		_, _ = b.ownedDeny(c, "io_uncertain")
		return out, e
	}
	s, e = b.ownedCommit(c, "content-identity", true, n, func(v *owneddata.Scope, n ownedClock) error {
		if v.DenyLatched || !v.WriterStarted || v.FilesystemIdentity.Content != nil || ownedClockReason(*v, n) != "" {
			return owneddata.ErrDenied
		}
		v.FilesystemIdentity.Content = &identity
		applyOwnedClock(v, n)
		return nil
	})
	if e != nil {
		_, _ = b.ownedDeny(c, "io_uncertain")
		return out, e
	} // The file exists, but no body byte precedes this ACK.
	checkContent := func() error {
		if e := check(); e != nil {
			return e
		}
		id, e := openedIdentity(content, "file")
		if e != nil || s.FilesystemIdentity.Content == nil || id != *s.FilesystemIdentity.Content {
			return owneddata.ErrUnresolved
		}
		r, e := b.owned.openScope(s, authority)
		if e != nil {
			return e
		}
		defer r.Close()
		return sameRootEntry(r, "payload", id, false)
	}
	buffer := make([]byte, owneddata.MaxChunkBytes)
	remaining := c.ContentLength
	emptyReads := 0
	for remaining > 0 {
		if e = checkContent(); e != nil {
			return out, e
		}
		want := int64(len(buffer))
		if remaining < want {
			want = remaining
		}
		count, readErr := body.Read(buffer[:want])
		if count < 0 || count > int(want) {
			return out, owneddata.ErrUnresolved
		}
		if e = checkContent(); e != nil {
			return out, e
		}
		if count > 0 {
			if b.owned.fail("before-content-write") != nil {
				return out, owneddata.ErrUnresolved
			}
			written, writeErr := content.Write(buffer[:count])
			if writeErr != nil || written != count {
				return out, owneddata.ErrUnresolved
			}
			remaining -= int64(count)
			emptyReads = 0
		} else {
			emptyReads++
			if emptyReads >= 8 {
				return out, owneddata.ErrUnresolved
			}
		}
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) || remaining != 0 {
				return out, owneddata.ErrUnresolved
			}
			break
		}
	}
	// A framed body must end at exactly the declared byte count. EOF is supplied
	// by the trusted launcher closing broker stdin, not by an HTTP connection.
	if e = checkContent(); e != nil {
		return out, e
	}
	var extra [1]byte
	count, endErr := body.Read(extra[:])
	if count != 0 || endErr != io.EOF {
		return out, owneddata.ErrUnresolved
	}
	if e = checkContent(); e != nil {
		return out, e
	}
	if b.owned.fail("before-content-sync") != nil || content.Sync() != nil {
		return out, owneddata.ErrUnresolved
	}
	if e = checkContent(); e != nil {
		return out, e
	}
	e = b.owned.closeContent(content, "main")
	if e != nil {
		closeUncertain = true
		_, _ = b.ownedCloseProof(c, true)
		return out, owneddata.ErrUnresolved
	}
	content = nil
	if _, e = b.ownedCloseProof(c, false); e != nil {
		_, _ = b.ownedDeny(c, "io_uncertain")
		return out, owneddata.ErrUnresolved
	}
	if b.owned.fail("after-content-close") != nil {
		return out, owneddata.ErrUnresolved
	}
	if e = check(); e != nil {
		return out, e
	}
	s, e = b.ownedCommit(c, "upload-complete", true, n, func(v *owneddata.Scope, n ownedClock) error {
		if v.DenyLatched || v.State != "accepting" || !v.WriterStarted || v.UploadComplete || !v.ContentCloseConfirmed || v.ContentCloseUncertain || ownedClockReason(*v, n) != "" {
			return owneddata.ErrDenied
		}
		v.UploadComplete = true
		applyOwnedClock(v, n)
		return nil
	})
	if e != nil {
		_, _ = b.ownedDeny(c, "io_uncertain")
		return out, e
	}
	return ownedStatus(s, authority), nil
}

func (b *Broker) ownedSeal(c owneddata.Control) (owneddata.Status, error) {
	s, authority, e := b.ownedRead(c)
	if e != nil {
		return owneddata.Status{}, e
	}
	if c.ExpectedGeneration != s.Generation {
		return owneddata.Status{}, owneddata.ErrStale
	}
	if c.WriterEpoch != s.WriterEpoch {
		return owneddata.Status{}, owneddata.ErrDenied
	}
	if _, e = b.ownedCurrent(c, s, true); e != nil {
		return owneddata.Status{}, e
	}
	release, e := b.owned.lease(s, "writer", true)
	if e != nil {
		return owneddata.Status{}, ownedError(e)
	}
	defer release()
	s, authority, e = b.ownedRead(c)
	if e != nil {
		return owneddata.Status{}, e
	}
	n, e := b.ownedCurrent(c, s, true)
	if e != nil {
		return owneddata.Status{}, e
	}
	if !s.WriterStarted || !s.UploadComplete || !s.ContentCloseConfirmed || s.ContentCloseUncertain {
		_, _ = b.ownedDeny(c, "incomplete_upload")
		return owneddata.Status{}, owneddata.ErrDenied
	}
	if s.State == "sealed" {
		return ownedStatus(s, authority), nil
	}
	s, e = b.ownedCommit(c, "seal", true, n, func(v *owneddata.Scope, n ownedClock) error {
		if v.Generation != c.ExpectedGeneration {
			return owneddata.ErrStale
		}
		if v.State != "accepting" || v.DenyLatched || !v.UploadComplete || ownedClockReason(*v, n) != "" {
			return owneddata.ErrDenied
		}
		v.State = "sealed"
		applyOwnedClock(v, n)
		return nil
	})
	if e != nil {
		_, _ = b.ownedDeny(c, "io_uncertain")
		return owneddata.Status{}, e
	}
	return ownedStatus(s, authority), nil
}

// Close evidence is eligible after denial/expiry, while the same writer lease
// is still held. An uncertain close is sticky and overrides any confirmation.
func (b *Broker) ownedCloseProof(c owneddata.Control, uncertain bool) (owneddata.Scope, error) {
	point := "close-confirmed"
	if uncertain {
		point = "close-uncertain"
	}
	s, e := b.ownedChange(c, point, func(v *owneddata.Scope) error {
		if !v.WriterStarted {
			return owneddata.ErrDenied
		}
		if uncertain {
			v.ContentCloseUncertain = true
			if !v.DenyLatched {
				v.DenyLatched = true
				v.State = "cleaning"
				v.Reason = "io_uncertain"
			}
		} else {
			v.ContentCloseConfirmed = true
		}
		return nil
	})
	if e != nil && !uncertain {
		_, _ = b.ownedCloseProof(c, true)
	}
	return s, e
}
