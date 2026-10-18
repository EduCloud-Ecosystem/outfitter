package jetstream2

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const rescueMaxSweeps = 64
const rescueReceiptBytes = 4096

var rescueObservationName = regexp.MustCompile(`^obs-([0-9]{3})\.(intent|receipt)$`)

type rescueBinding struct {
	Version    int          `json:"version"`
	Directory  string       `json:"directory"`
	Policy     RescuePolicy `json:"policy"`
	ConfigHash string       `json:"config_hash"`
	At         time.Time    `json:"at"`
}
type rescueStore struct {
	root   *os.Root
	dir    string
	policy RescuePolicy
	config Config
	key    ed25519.PrivateKey
	now    func() time.Time
	fault  func(string) error // deterministic offline fault injection; never CLI-configurable
}
type rescueState struct {
	port, server           *RescueScope
	portBytes, serverBytes []byte
	portACK, serverACK     []byte
	latest                 time.Time
	sequence               int
	knownPort, knownServer string
}

func openRescueStore(dir string, create bool, p RescuePolicy, c Config, key ed25519.PrivateKey) (*rescueStore, error) {
	if e := p.validate(c); e != nil {
		return nil, e
	}
	if len(key) != ed25519.PrivateKeySize || base64.StdEncoding.EncodeToString(key.Public().(ed25519.PublicKey)) != p.ObserverKey {
		return nil, errors.New("observer key mismatch")
	}
	if create {
		if e := os.Mkdir(dir, 0700); e != nil && !os.IsExist(e) {
			return nil, e
		}
	}
	st, e := os.Lstat(dir)
	if e != nil || !st.IsDir() || st.Mode()&os.ModeSymlink != 0 || st.Mode().Perm() != 0700 {
		return nil, errors.New("rescue directory must be private and nonsymlink")
	}
	dir, e = filepath.EvalSymlinks(dir)
	if e != nil {
		return nil, e
	}
	dir, e = filepath.Abs(dir)
	if e != nil {
		return nil, e
	}
	root, e := os.OpenRoot(dir)
	if e != nil {
		return nil, e
	}
	s := &rescueStore{root: root, dir: dir, policy: p, config: c, key: append(ed25519.PrivateKey(nil), key...), now: func() time.Time { return time.Now().UTC() }}
	e = s.lock(context.Background(), func() error {
		_, e := s.read("binding.json", rescueReceiptBytes)
		if os.IsNotExist(e) && create {
			entries, err := s.entries()
			if err != nil {
				return err
			}
			if len(entries) != 1 || entries[0] != "scope.lock" {
				return errors.New("refuse initialization of nonempty rescue directory")
			}
			b, err := rescueSign("binding", rescueBinding{1, dir, p, digest(c), s.now()}, key)
			if err != nil {
				return err
			}
			if err = s.writeOnce("binding.json", b); err != nil {
				return err
			}
		} else if e != nil {
			return e
		}
		_, e = s.load()
		return e
	})
	if e != nil {
		root.Close()
		return nil, e
	}
	return s, nil
}
func (s *rescueStore) fail(point string) error {
	if s.fault != nil {
		return s.fault(point)
	}
	return nil
}
func (s *rescueStore) open(name string, flags int) (*os.File, error) {
	f, e := s.root.OpenFile(name, flags|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0600)
	if e != nil {
		return nil, e
	}
	st, e := f.Stat()
	if e != nil || !st.Mode().IsRegular() || st.Mode().Perm() != 0600 || st.Size() > rescueMaxBytes {
		f.Close()
		return nil, errors.New("invalid private rescue file")
	}
	return f, nil
}
func (s *rescueStore) read(name string, max int) ([]byte, error) {
	f, e := s.open(name, os.O_RDONLY)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	b, e := io.ReadAll(io.LimitReader(f, int64(max+1)))
	if e != nil || len(b) > max {
		return nil, errors.New("unavailable or oversized rescue file")
	}
	return b, nil
}
func (s *rescueStore) sync(f *os.File, name string) error {
	if e := s.fail("before-file-sync:" + name); e != nil {
		return e
	}
	if e := f.Sync(); e != nil {
		return e
	}
	if e := s.fail("after-file-sync:" + name); e != nil {
		return e
	}
	d, e := s.root.Open(".")
	if e != nil {
		return e
	}
	defer d.Close()
	if e = s.fail("before-dir-sync:" + name); e != nil {
		return e
	}
	if e = d.Sync(); e != nil {
		return e
	}
	return s.fail("after-dir-sync:" + name)
}
func (s *rescueStore) writeOnce(name string, b []byte) error {
	if len(b) == 0 || len(b) > rescueMaxBytes {
		return errors.New("invalid rescue write size")
	}
	if e := s.fail("before-write:" + name); e != nil {
		return e
	}
	f, e := s.open(name, os.O_RDWR|os.O_CREATE|os.O_EXCL)
	if os.IsExist(e) {
		old, err := s.read(name, rescueMaxBytes)
		if err != nil {
			return err
		}
		if !bytes.Equal(old, b) {
			return errors.New("immutable rescue file conflict")
		}
		f, e = s.open(name, os.O_RDWR)
		if e != nil {
			return e
		}
		defer f.Close()
		return s.sync(f, name)
	}
	if e != nil {
		return e
	}
	defer f.Close()
	if _, e = f.Write(b); e != nil {
		return e
	}
	if e = s.fail("after-write:" + name); e != nil {
		return e
	}
	return s.sync(f, name)
}
func (s *rescueStore) lock(ctx context.Context, fn func() error) error {
	f, e := s.open("scope.lock", os.O_RDWR|os.O_CREATE)
	if e != nil {
		return e
	}
	defer f.Close()
	for {
		e = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if e == nil {
			break
		}
		if e != syscall.EWOULDBLOCK && e != syscall.EAGAIN {
			return e
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	if e = ctx.Err(); e != nil {
		return e
	}
	return fn()
}
func (s *rescueStore) entries() ([]string, error) {
	d, e := s.root.Open(".")
	if e != nil {
		return nil, e
	}
	defer d.Close()
	es, e := d.ReadDir(2*rescueMaxSweeps + 8)
	if e != nil && e != io.EOF {
		return nil, e
	}
	if len(es) > 2*rescueMaxSweeps+6 {
		return nil, errors.New("rescue storage quota exceeded")
	}
	out := []string{}
	for _, v := range es {
		out = append(out, v.Name())
	}
	return out, nil
}
func (s *rescueStore) load() (rescueState, error) {
	var out rescueState
	names, e := s.entries()
	if e != nil {
		return out, e
	}
	files := map[string][]byte{}
	for _, name := range names {
		max := rescueReceiptBytes
		if name == "port.scope" || name == "server.scope" {
			max = rescueMaxBytes
		} else if name != "scope.lock" && name != "binding.json" && name != "port.ack" && name != "server.ack" && !rescueObservationName.MatchString(name) {
			return out, errors.New("unexpected rescue entry")
		}
		b, err := s.read(name, max)
		if err != nil {
			return out, err
		}
		files[name] = b
	}
	var binding rescueBinding
	if e = rescueVerify(files["binding.json"], "binding", s.policy.ObserverKey, &binding); e != nil {
		return out, e
	}
	if binding.Version != 1 || binding.Directory != s.dir || binding.Policy != s.policy || binding.ConfigHash != digest(s.config) || binding.At.IsZero() {
		return out, errors.New("rescue directory binding mismatch; no copied-store takeover")
	}
	out.latest = binding.At
	for _, phase := range []string{"port", "server"} {
		b, ok := files[phase+".scope"]
		if !ok {
			if len(files[phase+".ack"]) != 0 {
				return out, errors.New("ACK without scope")
			}
			continue
		}
		var scope RescueScope
		if e = rescueVerify(b, "scope", s.policy.SourceKey, &scope); e != nil {
			return out, e
		}
		if e = scope.validate(s.policy, s.config); e != nil {
			return out, e
		}
		if scope.Phase != phase {
			return out, errors.New("scope phase filename mismatch")
		}
		if phase == "port" {
			out.port = &scope
			out.portBytes = b
		} else {
			out.server = &scope
			out.serverBytes = b
		}
		ack := files[phase+".ack"]
		if len(ack) > 0 {
			if e = verifyACK(ack, b, scope, time.Time{}, false); e != nil {
				return out, e
			}
			var a RescueACK
			_ = rescueVerify(ack, "ack", s.policy.ObserverKey, &a)
			if a.At.After(out.latest) {
				out.latest = a.At
			}
			if phase == "port" {
				out.portACK = ack
			} else {
				out.serverACK = ack
			}
		}
	}
	if out.server != nil {
		if out.port == nil || len(out.portACK) == 0 {
			return out, errors.New("server phase lacks durably acknowledged parent")
		}
		if e = checkRescueParent(*out.port, out.portBytes, *out.server); e != nil {
			return out, e
		}
	}
	for name, b := range files {
		match := rescueObservationName.FindStringSubmatch(name)
		if match == nil {
			continue
		}
		n, _ := strconv.Atoi(match[1])
		if n < 1 || n > rescueMaxSweeps {
			return out, errors.New("observation quota exceeded")
		}
		r, err := ValidateRescueReceipt(b, out.portBytes, out.serverBytes, s.policy, s.config)
		if err != nil {
			return out, err
		}
		if r.Sequence != n || (match[2] == "intent") != (r.Outcome == "SWEEP_INTENT") {
			return out, errors.New("observation filename mismatch")
		}
		if r.PortID != "" {
			if out.knownPort != "" && out.knownPort != r.PortID {
				return out, errors.New("conflicting observed port IDs")
			}
			out.knownPort = r.PortID
		}
		if r.ServerID != "" {
			if out.knownServer != "" && out.knownServer != r.ServerID {
				return out, errors.New("conflicting observed server IDs")
			}
			out.knownServer = r.ServerID
		}
		if match[2] == "receipt" && len(files[fmt.Sprintf("obs-%03d.intent", n)]) == 0 {
			return out, errors.New("receipt lacks durable sweep intent")
		}
		if n > out.sequence {
			out.sequence = n
		}
		if r.At.After(out.latest) {
			out.latest = r.At
		}
	}
	if s.now().Before(out.latest) {
		return out, errors.New("observer clock rollback")
	}
	return out, nil
}
func checkRescueParent(parent RescueScope, raw []byte, child RescueScope) error {
	if child.ParentHash != rescueHash(raw) {
		return errors.New("rescue parent hash mismatch")
	}
	child.Phase = "port"
	child.PortID = ""
	child.ParentHash = ""
	if child != parent {
		return errors.New("server phase changed parent scope")
	}
	return nil
}
func (s *rescueStore) accept(ctx context.Context, raw []byte) ([]byte, error) {
	var ack []byte
	e := s.lock(ctx, func() error {
		state, e := s.load()
		if e != nil {
			return e
		}
		var scope RescueScope
		if e = rescueVerify(raw, "scope", s.policy.SourceKey, &scope); e != nil {
			return e
		}
		if e = scope.validate(s.policy, s.config); e != nil {
			return e
		}
		now := s.now()
		if !now.Before(scope.Deadline) || scope.CreatedAt.After(now.Add(rescueClockTolerance)) {
			return errors.New("scope expired or clock disagreement")
		}
		if state.port != nil && (state.port.ReservationID != scope.ReservationID || state.port.Request != scope.Request) {
			return errors.New("retained scope prevents another reservation")
		}
		if scope.Phase == "server" {
			if state.port == nil || len(state.portACK) == 0 {
				return errors.New("server scope lacks parent ACK")
			}
			if e = checkRescueParent(*state.port, state.portBytes, scope); e != nil {
				return e
			}
		}
		if e = s.writeOnce(scope.Phase+".scope", raw); e != nil {
			return e
		}
		ack, e = s.read(scope.Phase+".ack", rescueReceiptBytes)
		if os.IsNotExist(e) {
			ack, e = rescueSign("ack", RescueACK{1, s.policy.ObserverID, scope.ReservationID, scope.Phase, rescueHash(raw), scope.ParentHash, now}, s.key)
		}
		if e != nil {
			return e
		}
		if e = verifyACK(ack, raw, scope, now, false); e != nil {
			return e
		}
		if e = s.writeOnce(scope.Phase+".ack", ack); e != nil {
			return e
		}
		if !s.now().Before(scope.Deadline) {
			return errors.New("scope expired during durable acceptance")
		}
		return nil
	})
	if e != nil {
		return nil, e
	}
	return ack, nil
}

func ValidateRescueReceipt(raw, port, server []byte, p RescuePolicy, c Config) (RescueReceipt, error) {
	var r RescueReceipt
	if len(raw) > rescueReceiptBytes {
		return r, errors.New("oversized observational receipt")
	}
	if e := rescueVerify(raw, "receipt", p.ObserverKey, &r); e != nil {
		return r, e
	}
	var scope RescueScope
	if e := rescueVerify(port, "scope", p.SourceKey, &scope); e != nil {
		return r, e
	}
	if e := scope.validate(p, c); e != nil {
		return r, e
	}
	if scope.Phase != "port" || r.Version != 1 || r.ObserverID != p.ObserverID || r.ReservationID != scope.ReservationID || r.PortScopeHash != rescueHash(port) || r.Sequence < 1 || r.Sequence > rescueMaxSweeps || r.At.Before(scope.Deadline) {
		return r, errors.New("receipt binding mismatch")
	}
	if r.ServerScopeHash != "" {
		var child RescueScope
		if e := rescueVerify(server, "scope", p.SourceKey, &child); e != nil {
			return r, e
		}
		if e := child.validate(p, c); e != nil {
			return r, e
		}
		if child.Phase != "server" || r.ServerScopeHash != rescueHash(server) {
			return r, errors.New("receipt server scope mismatch")
		}
		if e := checkRescueParent(scope, port, child); e != nil {
			return r, e
		}
	}
	if (r.PortID != "" && !ref.MatchString(r.PortID)) || (r.ServerID != "" && (!ref.MatchString(r.ServerID) || r.ServerScopeHash == "")) {
		return r, errors.New("invalid receipt resource identity")
	}
	if !strings.Contains("|SWEEP_INTENT|UNKNOWN|OBSERVED_ABSENT|", "|"+r.Outcome+"|") {
		return r, errors.New("receipt cannot assert terminal cleanup")
	}
	return r, nil
}
