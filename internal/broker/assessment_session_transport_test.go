package broker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
	"syscall"
	"time"

	x "github.com/EduCloud-Ecosystem/outfitter/internal/assessmentexec"
)

const (
	executionSessionLifetime    = 120 * time.Second
	executionSessionStageTime   = 5 * time.Second
	executionSessionReplyTime   = 10 * time.Second
	executionSessionExecuteTime = 40 * time.Second
	executionSessionACKBytes    = 53
)

var executionSessionProtocolError = errors.New("private session unresolved")

// A lane has exactly one reader/writer worker and at most one inert stage. It
// shares no lock with the other lane across Broker or pipe waits.
type executionSessionLane struct {
	kind          byte
	input, output *os.File
	sequence      uint64
	maximum       uint64
}

type executionSessionTransport struct {
	context         context.Context
	cancel          context.CancelFunc
	deadline        time.Time
	primary, safety executionSessionLane
	mu              sync.Mutex
	failure         error
	// Test-only fixed constructor supplies a pure plan check and the existing
	// Broker dispatch. Neither callback is a public capability or close proof.
	validate   func(byte, uint64, x.Control) error
	dispatch   func(context.Context, x.Control) (any, error)
	finish     func(byte) bool
	afterReply func(x.Control, error) error
	interrupt  []*os.File
	stop       func() bool
	stopOnce   sync.Once
	stopDone   chan struct{}
	// Negative deterministic unit barrier only; fixed owner leaves it nil.
	beforeDeadline func(*os.File, bool)
}

func executionSessionMinDeadline(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

// These checks never call File.Fd. Parents set O_NONBLOCK before spawn so Go's
// stdio wrappers are pollable when the runtime first constructs them.
func executionSessionPrivatePipe(file *os.File) error {
	if file == nil {
		return executionSessionProtocolError
	}
	info, e := file.Stat()
	if e != nil || info.Mode()&os.ModeNamedPipe == 0 {
		return executionSessionProtocolError
	}
	raw, e := file.SyscallConn()
	if e != nil {
		return e
	}
	var nativeErr error
	e = raw.Control(func(fd uintptr) {
		flags, _, errno := syscall.Syscall(syscall.SYS_FCNTL, fd, syscall.F_GETFL, 0)
		if errno != 0 {
			nativeErr = errno
			return
		}
		if flags&syscall.O_NONBLOCK == 0 {
			nativeErr = executionSessionProtocolError
			return
		}
		flags, _, errno = syscall.Syscall(syscall.SYS_FCNTL, fd, syscall.F_GETFD, 0)
		if errno != 0 {
			nativeErr = errno
			return
		}
		_, _, errno = syscall.Syscall(syscall.SYS_FCNTL, fd, syscall.F_SETFD, flags|syscall.FD_CLOEXEC)
		if errno != 0 {
			nativeErr = errno
		}
	})
	if e != nil {
		return e
	}
	return nativeErr
}

func newExecutionSessionTransport(parent context.Context, primaryIn, primaryOut, safetyIn, safetyOut, body *os.File, deadline time.Time) (*executionSessionTransport, error) {
	ctx, cancel := context.WithDeadline(parent, deadline)
	s := &executionSessionTransport{context: ctx, cancel: cancel, deadline: deadline,
		primary:   executionSessionLane{kind: 'P', input: primaryIn, output: primaryOut, maximum: 5},
		safety:    executionSessionLane{kind: 'S', input: safetyIn, output: safetyOut, maximum: 10},
		interrupt: []*os.File{primaryIn, primaryOut, safetyIn, safetyOut}}
	if body != nil {
		s.interrupt = append(s.interrupt, body)
	}
	for _, f := range s.interrupt {
		if e := executionSessionPrivatePipe(f); e != nil {
			cancel()
			return nil, e
		}
		if e := f.SetDeadline(deadline); e != nil {
			cancel()
			return nil, e
		}
	}
	s.stopDone = make(chan struct{})
	s.stop = context.AfterFunc(ctx, func() { defer close(s.stopDone); s.abort(executionSessionProtocolError) })
	return s, nil
}

func (s *executionSessionTransport) stopTimer() {
	s.stopOnce.Do(func() {
		if s.stop != nil && !s.stop() {
			<-s.stopDone
		}
	})
}

func (s *executionSessionTransport) abort(e error) {
	s.mu.Lock()
	if s.failure == nil {
		s.failure = e
	}
	s.mu.Unlock()
	s.cancel()
	// Deadline cancellation interrupts pollable originals, not aliases or raw
	// blocking reads. Closing borrowed/core handles while workers run is forbidden.
	for _, f := range s.interrupt {
		_ = f.SetDeadline(time.Now())
	}
}

func executionSessionWrite(file *os.File, b []byte) error {
	for len(b) != 0 {
		n, e := file.Write(b)
		if n > 0 {
			b = b[n:]
		}
		if e != nil {
			return e
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

// Cancellation may have set an immediate deadline just before this setter.
// Recheck after setup so a later deadline cannot erase that wakeup. If cancel
// happens after the check, abort's immediate deadline interrupts the next I/O.
func (s *executionSessionTransport) readDeadline(file *os.File, deadline time.Time) error {
	if s.beforeDeadline != nil {
		s.beforeDeadline(file, false)
	}
	if e := file.SetReadDeadline(deadline); e != nil {
		return e
	}
	return s.context.Err()
}

func (s *executionSessionTransport) writeDeadline(file *os.File, deadline time.Time) error {
	if s.beforeDeadline != nil {
		s.beforeDeadline(file, true)
	}
	if e := file.SetWriteDeadline(deadline); e != nil {
		return e
	}
	return s.context.Err()
}

// This rejects input already queued at a protocol boundary. It is not proof
// that a peer consumed our ACK/reply, or a timestamp for future concurrent
// input. The fixed parent must also obey frame/ACK/trigger/reply ordering.
func executionSessionNoQueuedInput(file *os.File) error {
	raw, e := file.SyscallConn()
	if e != nil {
		return e
	}
	var result error
	e = raw.Control(func(fd uintptr) {
		flags, _, errno := syscall.Syscall(syscall.SYS_FCNTL, fd, syscall.F_GETFL, 0)
		if errno != 0 || flags&syscall.O_NONBLOCK == 0 {
			result = executionSessionProtocolError
			return
		}
		var one [1]byte
		n, readErr := syscall.Read(int(fd), one[:])
		if n > 0 || readErr != syscall.EAGAIN && readErr != syscall.EWOULDBLOCK {
			result = executionSessionProtocolError
		}
	})
	if e != nil {
		return e
	}
	return result
}

func (s *executionSessionTransport) readStage(l *executionSessionLane) (x.Control, bool, error) {
	var c x.Control
	// Idle is bounded by the original session lifetime. Once the first prefix
	// byte arrives, the single stage deadline covers the remaining frame, ACK
	// and trigger. Fragmentation never resets that deadline.
	if e := s.readDeadline(l.input, s.deadline); e != nil {
		return c, false, e
	}
	var prefix [4]byte
	n, e := io.ReadFull(l.input, prefix[:1])
	if e == io.EOF && n == 0 && s.finish != nil && s.finish(l.kind) {
		return c, true, nil
	}
	if e != nil {
		return c, false, e
	}
	stageDeadline := executionSessionMinDeadline(time.Now().Add(executionSessionStageTime), s.deadline)
	if e = s.readDeadline(l.input, stageDeadline); e != nil {
		return c, false, e
	}
	if e = s.writeDeadline(l.output, stageDeadline); e != nil {
		return c, false, e
	}
	if _, e = io.ReadFull(l.input, prefix[1:]); e != nil {
		return c, false, e
	}
	l.sequence++
	if l.sequence > l.maximum {
		return c, false, executionSessionProtocolError
	}
	length := binary.BigEndian.Uint32(prefix[:])
	if length < 2 || length > x.MaxControlBytes {
		return c, false, executionSessionProtocolError
	}
	frame := make([]byte, int(length))
	defer clear(frame)
	if _, e = io.ReadFull(l.input, frame); e != nil {
		return c, false, e
	}
	if frame[len(frame)-1] != '\n' || bytes.ContainsAny(frame[:len(frame)-1], "\r\n") || x.DecodeClosed(frame[:len(frame)-1], &c, x.MaxControlBytes) != nil || c.Validate() != nil {
		return c, false, executionSessionProtocolError
	}
	if s.validate == nil || s.validate(l.kind, l.sequence, c) != nil {
		return c, false, executionSessionProtocolError
	}
	if e = executionSessionNoQueuedInput(l.input); e != nil {
		return c, false, e
	}
	var ack [executionSessionACKBytes]byte
	copy(ack[:8], "EXACK001")
	ack[8] = l.kind
	binary.BigEndian.PutUint64(ack[9:17], l.sequence)
	binary.BigEndian.PutUint32(ack[17:21], length)
	digest := sha256.Sum256(frame)
	copy(ack[21:], digest[:])
	if e = executionSessionWrite(l.output, ack[:]); e != nil {
		return c, false, e
	}
	// ACK is inert. Read exactly this trigger; no read-ahead can authorize a
	// next frame, and no Broker call is made before this point.
	var trigger [1]byte
	if _, e = io.ReadFull(l.input, trigger[:]); e != nil {
		return c, false, e
	}
	if trigger[0] != 1 || !time.Now().Before(stageDeadline) || s.context.Err() != nil {
		return c, false, executionSessionProtocolError
	}
	if e = executionSessionNoQueuedInput(l.input); e != nil {
		return c, false, e
	}
	return c, false, nil
}

func (s *executionSessionTransport) runLane(l *executionSessionLane) error {
	for {
		c, done, e := s.readStage(l)
		if e != nil {
			return e
		}
		if done {
			return nil
		}
		if s.dispatch == nil {
			return executionSessionProtocolError
		}
		limit := executionSessionReplyTime
		if c.Operation == "execute" {
			limit = executionSessionExecuteTime
		}
		deadline := executionSessionMinDeadline(time.Now().Add(limit), s.deadline)
		if e = s.writeDeadline(l.output, deadline); e != nil {
			return e
		}
		ctx, cancel := context.WithDeadline(s.context, deadline)
		value, callErr := s.dispatch(ctx, c)
		cancel()
		if callErr != nil {
			value = x.Error{Schema: x.ErrorSchema, Code: assessmentError(callErr).Error()}
		}
		if e = executionSessionNoQueuedInput(l.input); e != nil {
			return e
		}
		raw, e := x.EncodeReply(value)
		if e != nil {
			return e
		}
		if e = executionSessionWrite(l.output, raw); e != nil {
			return e
		}
		if s.afterReply != nil {
			if e = s.afterReply(c, callErr); e != nil {
				return e
			}
		}
	}
}

func (s *executionSessionTransport) run() error {
	var workers sync.WaitGroup
	workers.Add(2)
	for _, lane := range []*executionSessionLane{&s.primary, &s.safety} {
		go func(l *executionSessionLane) {
			defer workers.Done()
			if e := s.runLane(l); e != nil {
				s.abort(e)
			}
		}(lane)
	}
	workers.Wait()
	s.stopTimer()
	s.cancel()
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.failure
}
