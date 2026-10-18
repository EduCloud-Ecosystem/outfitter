package ledger

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
)

var ErrAdmission = errors.New("authority is held or permanently fenced; new admission refused")

type Authority struct {
	ID        string  `json:"authority_id"`
	StatePath string  `json:"state_path"`
	Admission string  `json:"admission"`
	Restore   *Origin `json:"restore,omitempty"`
	Fence     *Fence  `json:"fence,omitempty"`
}
type Origin struct {
	CheckpointSHA256  string `json:"checkpoint_sha256"`
	SourceAuthorityID string `json:"source_authority_id"`
	SourceStatePath   string `json:"source_state_path"`
	AuthoritySHA256   string `json:"authority_sha256"`
}
type Fence struct {
	DestinationAuthorityID string `json:"destination_authority_id"`
	DestinationStatePath   string `json:"destination_state_path"`
	CheckpointSHA256       string `json:"checkpoint_sha256"`
}
type Unresolved struct {
	RequestID string    `json:"request_id"`
	ProjectID string    `json:"project_id"`
	Status    string    `json:"status"`
	Reserved  int64     `json:"reserved_cpu_seconds"`
	Deadline  time.Time `json:"deadline"`
}
type RecoveryReport struct {
	NativeAccounts    []NativeUsage  `json:"native_accounts,omitempty"`
	NativeUnresolved  []NativeRecord `json:"native_unresolved,omitempty"`
	SchemaVersion     int            `json:"schema_version"`
	AuthorityID       string         `json:"authority_id"`
	StatePath         string         `json:"state_path"`
	Admission         string         `json:"admission"`
	AuthoritySHA256   string         `json:"authority_sha256"`
	CheckpointSHA256  string         `json:"checkpoint_sha256,omitempty"`
	SourceAuthorityID string         `json:"source_authority_id,omitempty"`
	SourceStatePath   string         `json:"source_state_path,omitempty"`
	Fence             *Fence         `json:"fence,omitempty"`
	Projects          []Usage        `json:"projects"`
	Unresolved        []Unresolved   `json:"unresolved"`
}
type fileReference struct {
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
}
type checkpointManifest struct {
	SchemaVersion     int                      `json:"schema_version"`
	CreatedAt         time.Time                `json:"created_at"`
	SourceAuthorityID string                   `json:"source_authority_id"`
	SourceStatePath   string                   `json:"source_state_path"`
	AuthoritySHA256   string                   `json:"authority_sha256"`
	Files             map[string]fileReference `json:"files"`
}
type checkpoint struct {
	dir      string
	digest   string
	manifest checkpointManifest
	data     Data
}

func randomID() (string, error) {
	var b [16]byte
	if _, e := rand.Read(b[:]); e != nil {
		return "", e
	}
	return hex.EncodeToString(b[:]), nil
}
func validHex(s string, n int) bool {
	b, e := hex.DecodeString(s)
	return e == nil && len(b) == n && s == strings.ToLower(s)
}
func validPath(p string) bool { return len(p) <= 4096 && filepath.IsAbs(p) && filepath.Clean(p) == p }
func validateAuthority(d Data) error {
	if d.Version == 1 {
		if d.Authority != nil {
			return errors.New("legacy ledger has unexpected authority")
		}
		return nil
	}
	a := d.Authority
	if a == nil || !validHex(a.ID, 16) || !validPath(a.StatePath) {
		return errors.New("invalid authority identity or path")
	}
	if a.Restore != nil {
		r := a.Restore
		if !validHex(r.CheckpointSHA256, 32) || !validHex(r.AuthoritySHA256, 32) || !validHex(r.SourceAuthorityID, 16) || !validPath(r.SourceStatePath) || r.SourceStatePath == a.StatePath || r.SourceAuthorityID == a.ID {
			return errors.New("invalid restoration provenance")
		}
	}
	switch a.Admission {
	case "active":
		if a.Fence != nil {
			return errors.New("active authority cannot have a fence")
		}
	case "held":
		if a.Restore == nil || a.Fence != nil {
			return errors.New("invalid held authority")
		}
	case "fenced":
		f := a.Fence
		if f == nil || !validHex(f.DestinationAuthorityID, 16) || !validHex(f.CheckpointSHA256, 32) || !validPath(f.DestinationStatePath) || f.DestinationStatePath == a.StatePath || f.DestinationAuthorityID == a.ID {
			return errors.New("invalid one-way fence")
		}
	default:
		return errors.New("invalid admission mode")
	}
	return nil
}
func RequireAdmission(d Data) error {
	if (d.Version != 2 && d.Version != 3) || d.Authority == nil || d.Authority.Admission != "active" {
		return ErrAdmission
	}
	return nil
}
func dataHash(d Data) string { b, _ := json.Marshal(d); return hash(b) }
func hash(b []byte) string   { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func report(d Data) RecoveryReport {
	a := d.Authority
	r := RecoveryReport{SchemaVersion: 1, AuthorityID: a.ID, StatePath: a.StatePath, Admission: a.Admission, AuthoritySHA256: dataHash(d), Fence: a.Fence, Projects: []Usage{}, Unresolved: []Unresolved{}}
	if a.Restore != nil {
		r.CheckpointSHA256 = a.Restore.CheckpointSHA256
		r.SourceAuthorityID = a.Restore.SourceAuthorityID
		r.SourceStatePath = a.Restore.SourceStatePath
	}
	ids := make([]string, 0, len(d.Projects))
	for id := range d.Projects {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		r.Projects = append(r.Projects, Calculate(d, id))
	}
	ids = ids[:0]
	for id := range d.Runs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		x := d.Runs[id]
		if x.Active() {
			r.Unresolved = append(r.Unresolved, Unresolved{x.RequestID, x.ProjectID, x.Status, x.Compute.Reserved, x.Deadline})
		}
	}
	for id := range d.NativeAccounts {
		r.NativeAccounts = append(r.NativeAccounts, NativeCalculate(d, id))
	}
	sort.Slice(r.NativeAccounts, func(i, j int) bool { return r.NativeAccounts[i].AccountID < r.NativeAccounts[j].AccountID })
	for _, n := range d.NativeRuns {
		if n.Settled == nil {
			r.NativeUnresolved = append(r.NativeUnresolved, n)
		}
	}
	sort.Slice(r.NativeUnresolved, func(i, j int) bool { return r.NativeUnresolved[i].RequestID < r.NativeUnresolved[j].RequestID })
	return r
}
func (s *Store) RecoveryStatus() (RecoveryReport, error) {
	var out RecoveryReport
	e := s.View(func(d Data) error {
		if (d.Version != 2 && d.Version != 3) || d.Authority == nil {
			return errors.New("reopen legacy authority to migrate before recovery")
		}
		out = report(d)
		return nil
	})
	return out, e
}

// Shared leases cover the complete admitting broker process, including effects.
// Recovery takes nonblocking exclusive leases before either authority lock.
func (s *Store) ExecutionLease() (func(), error) { return s.executionLease(false) }
func (s *Store) executionLease(exclusive bool) (func(), error) {
	fd, e := syscall.Open(filepath.Join(s.Dir, "execution.lock"), syscall.O_RDWR|syscall.O_CREAT|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0600)
	if e != nil {
		return nil, e
	}
	mode := syscall.LOCK_SH
	if exclusive {
		mode = syscall.LOCK_EX
	}
	if e = syscall.Flock(fd, mode|syscall.LOCK_NB); e != nil {
		syscall.Close(fd)
		return nil, errors.New("execution or recovery is in progress; refusing cutover/admission")
	}
	return func() { syscall.Flock(fd, syscall.LOCK_UN); syscall.Close(fd) }, nil
}
func syncDirectory(dir string) error {
	f, e := os.Open(dir)
	if e != nil {
		return e
	}
	defer f.Close()
	return f.Sync()
}
func exclusiveDirectory(dir string) (string, error) {
	p, e := filepath.Abs(dir)
	if e != nil {
		return "", e
	}
	parent, e := filepath.EvalSymlinks(filepath.Dir(p))
	if e != nil {
		return "", e
	}
	p = filepath.Join(parent, filepath.Base(p))
	if !validPath(p) {
		return "", errors.New("invalid destination path")
	}
	if e = os.Mkdir(p, 0700); e != nil {
		return "", errors.New("destination must be a new directory; existing or partial state is preserved")
	}
	if e = syncDirectory(parent); e != nil {
		return "", e
	}
	return p, nil
}
func privateDirectory(dir string) (string, error) {
	p, e := filepath.Abs(dir)
	if e != nil {
		return "", e
	}
	st, e := os.Lstat(p)
	if e != nil {
		return "", e
	}
	if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 || st.Mode().Perm()&0077 != 0 {
		return "", errors.New("recovery directory must be private and not a symbolic link")
	}
	return filepath.EvalSymlinks(p)
}
func writeExclusive(path string, b []byte) error {
	f, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	_, e = f.Write(b)
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	return e
}
func readBounded(path string, max int64) ([]byte, error) {
	// Inspect special files without waiting for a writer/device before fstat can
	// reject them. O_NONBLOCK has no effect on supported regular file reads.
	fd, e := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC|syscall.O_NONBLOCK, 0)
	if e != nil {
		return nil, e
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	st, e := f.Stat()
	if e != nil {
		return nil, e
	}
	if !st.Mode().IsRegular() || st.Size() > max || st.Size() < 0 {
		return nil, errors.New("invalid bounded recovery file")
	}
	b, e := io.ReadAll(io.LimitReader(f, max+1))
	if e != nil {
		return nil, e
	}
	if int64(len(b)) > max {
		return nil, errors.New("oversized recovery file")
	}
	return b, nil
}
func decodeStrict(b []byte, out any) error {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if e := dec.Decode(out); e != nil {
		return e
	}
	if dec.Decode(new(any)) != io.EOF {
		return errors.New("trailing recovery data")
	}
	return nil
}
func outputReferences(d Data) map[string]fileReference {
	files := map[string]fileReference{}
	for _, r := range d.Runs {
		if r.OutputSHA256 != "" {
			files["outputs/"+r.OutputSHA256+".json"] = fileReference{r.OutputSHA256, r.OutputBytes}
		}
	}
	return files
}
func readOutput(dir, name string, ref fileReference) ([]byte, error) {
	st, e := os.Lstat(filepath.Join(dir, "outputs"))
	if e != nil {
		return nil, e
	}
	if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("invalid output directory")
	}
	b, e := readBounded(filepath.Join(dir, filepath.FromSlash(name)), (6<<20)+512)
	if e != nil {
		return nil, e
	}
	if int64(len(b)) != ref.Bytes || hash(b) != ref.SHA256 {
		return nil, errors.New("missing or corrupt checkpoint output")
	}
	return b, nil
}

func (s *Store) Checkpoint(output string) (RecoveryReport, error) {
	var out RecoveryReport
	e := s.lock(func() error {
		d, e := s.read()
		if e != nil {
			return e
		}
		if e = RequireAdmission(d); e != nil {
			return e
		}
		dir, e := exclusiveDirectory(output)
		if e != nil {
			return e
		}
		m := checkpointManifest{SchemaVersion: 1, CreatedAt: time.Now().UTC(), SourceAuthorityID: d.Authority.ID, SourceStatePath: s.Dir, AuthoritySHA256: dataHash(d), Files: outputReferences(d)}
		if len(m.Files) > 0 {
			if e = os.Mkdir(filepath.Join(dir, "outputs"), 0700); e != nil {
				return e
			}
		}
		for name, ref := range m.Files {
			b, e := readOutput(s.Dir, name, ref)
			if e != nil {
				return e
			}
			if e = writeExclusive(filepath.Join(dir, filepath.FromSlash(name)), b); e != nil {
				return e
			}
		}
		if len(m.Files) > 0 {
			if e = syncDirectory(filepath.Join(dir, "outputs")); e != nil {
				return e
			}
		}
		b, e := json.Marshal(d)
		if e != nil {
			return e
		}
		if e = writeExclusive(filepath.Join(dir, "ledger.json"), b); e != nil {
			return e
		}
		m.Files["ledger.json"] = fileReference{hash(b), int64(len(b))}
		manifest, e := json.Marshal(m)
		if e != nil {
			return e
		}
		if e = writeExclusive(filepath.Join(dir, "manifest.json"), manifest); e != nil {
			return e
		}
		if e = syncDirectory(dir); e != nil {
			return e
		}
		out = report(d)
		out.CheckpointSHA256 = hash(manifest)
		out.SourceAuthorityID = d.Authority.ID
		out.SourceStatePath = s.Dir
		return nil
	})
	return out, e
}
func loadCheckpoint(dir, digest string) (checkpoint, error) {
	var c checkpoint
	if !validHex(digest, 32) {
		return c, errors.New("a separately retained checkpoint SHA-256 is required")
	}
	p, e := privateDirectory(dir)
	if e != nil {
		return c, e
	}
	b, e := readBounded(filepath.Join(p, "manifest.json"), 256<<10)
	if e != nil {
		return c, e
	}
	if hash(b) != digest {
		return c, errors.New("checkpoint integrity identity does not match")
	}
	var m checkpointManifest
	if e = decodeStrict(b, &m); e != nil {
		return c, e
	}
	if m.SchemaVersion != 1 || m.CreatedAt.IsZero() || len(m.Files) > MaxRecords+1 || len(m.Files) == 0 {
		return c, errors.New("invalid checkpoint manifest")
	}
	b, e = readBounded(filepath.Join(p, "ledger.json"), maxLedgerBytes)
	if e != nil {
		return c, e
	}
	if ref, ok := m.Files["ledger.json"]; !ok || hash(b) != ref.SHA256 || int64(len(b)) != ref.Bytes {
		return c, errors.New("checkpoint ledger integrity failure")
	}
	var d Data
	if e = decodeStrict(b, &d); e != nil {
		return c, e
	}
	if e = validate(d); e != nil {
		return c, e
	}
	if e = RequireAdmission(d); e != nil {
		return c, e
	}
	if d.Authority.ID != m.SourceAuthorityID || d.Authority.StatePath != m.SourceStatePath || dataHash(d) != m.AuthoritySHA256 {
		return c, errors.New("checkpoint authority identity mismatch")
	}
	expected := outputReferences(d)
	if len(expected)+1 != len(m.Files) {
		return c, errors.New("unexpected checkpoint file references")
	}
	for name, ref := range expected {
		if m.Files[name] != ref {
			return c, errors.New("checkpoint output reference mismatch")
		}
		if _, e = readOutput(p, name, ref); e != nil {
			return c, e
		}
	}
	// Refuse extra files and symlinks, not just the files listed in the manifest.
	e = filepath.WalkDir(p, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(p, path)
		name := filepath.ToSlash(rel)
		if name == "." {
			return nil
		}
		if entry.IsDir() {
			if name != "outputs" {
				return errors.New("unexpected checkpoint directory")
			}
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return errors.New("checkpoint links are refused")
		}
		if name != "manifest.json" {
			if _, ok := m.Files[name]; !ok {
				return errors.New("unexpected checkpoint file")
			}
		}
		return nil
	})
	if e != nil {
		return c, e
	}
	return checkpoint{p, digest, m, d}, nil
}

func Restore(destination, dir, digest string) (RecoveryReport, error) {
	c, e := loadCheckpoint(dir, digest)
	if e != nil {
		return RecoveryReport{}, e
	}
	path, e := exclusiveDirectory(destination)
	if e != nil {
		return RecoveryReport{}, e
	}
	s := &Store{Dir: path}
	id, e := randomID()
	if e != nil {
		return RecoveryReport{}, e
	}
	d := c.data
	d.Authority = &Authority{ID: id, StatePath: path, Admission: "held", Restore: &Origin{digest, c.manifest.SourceAuthorityID, c.manifest.SourceStatePath, c.manifest.AuthoritySHA256}}
	if e = writeExclusive(filepath.Join(path, "initialized"), nil); e != nil {
		return RecoveryReport{}, e
	}
	refs := outputReferences(d)
	if len(refs) > 0 {
		if e = os.Mkdir(filepath.Join(path, "outputs"), 0700); e != nil {
			return RecoveryReport{}, e
		}
	}
	for name, ref := range refs {
		b, e := readOutput(c.dir, name, ref)
		if e != nil {
			return RecoveryReport{}, e
		}
		if e = writeExclusive(filepath.Join(path, filepath.FromSlash(name)), b); e != nil {
			return RecoveryReport{}, e
		}
	}
	if len(refs) > 0 {
		if e = syncDirectory(filepath.Join(path, "outputs")); e != nil {
			return RecoveryReport{}, e
		}
	}
	if e = s.lock(func() error { return s.write(d) }); e != nil {
		return RecoveryReport{}, e
	}
	return report(d), nil
}

func (s *Store) Activate(source, dir, digest string) (RecoveryReport, error) {
	return s.activate(source, dir, digest, nil)
}
func (s *Store) activate(source, dir, digest string, afterFence func() error) (RecoveryReport, error) {
	var out RecoveryReport
	c, e := loadCheckpoint(dir, digest)
	if e != nil {
		return out, e
	}
	src, e := Open(source, false)
	if e != nil {
		return out, e
	}
	if src.Dir == s.Dir || src.Dir != c.manifest.SourceStatePath {
		return out, errors.New("source does not match checkpoint")
	}
	stores := []*Store{src, s}
	sort.Slice(stores, func(i, j int) bool { return stores[i].Dir < stores[j].Dir })
	for _, st := range stores {
		release, e := st.executionLease(true)
		if e != nil {
			return out, e
		}
		defer release()
	}
	e = stores[0].lock(func() error {
		return stores[1].lock(func() error {
			current, e := src.read()
			if e != nil {
				return e
			}
			dest, e := s.read()
			if e != nil {
				return e
			}
			origin := dest.Authority.Restore
			if origin == nil || *origin != (Origin{digest, c.manifest.SourceAuthorityID, c.manifest.SourceStatePath, c.manifest.AuthoritySHA256}) {
				return errors.New("destination was not restored from this checkpoint")
			}
			fence := Fence{dest.Authority.ID, s.Dir, digest}
			if current.Authority.Admission == "fenced" {
				if current.Authority.Fence == nil || *current.Authority.Fence != fence {
					return errors.New("source is permanently fenced to another destination")
				}
				if dest.Authority.Admission == "active" {
					out = report(dest)
					return nil
				}
			} else if current.Authority.Admission != "active" {
				return errors.New("source is not an admitting authority")
			}
			if dest.Authority.Admission != "held" {
				return errors.New("destination must remain held until activation")
			}
			normalized := current
			authority := *current.Authority
			authority.Admission = "active"
			authority.Fence = nil
			normalized.Authority = &authority
			if dataHash(normalized) != c.manifest.AuthoritySHA256 {
				return errors.New("checkpoint is stale relative to current source authority")
			}
			normalized = dest
			normalized.Authority = c.data.Authority
			if dataHash(normalized) != c.manifest.AuthoritySHA256 {
				return errors.New("held destination accounting changed after restoration")
			}
			for _, r := range current.Runs {
				if r.Active() {
					return fmt.Errorf("request %s remains unresolved; reconcile source and take a fresh checkpoint", r.RequestID)
				}
			}
			for _, r := range current.NativeRuns {
				if r.Settled == nil {
					return fmt.Errorf("native request %s remains unresolved", r.RequestID)
				}
			}
			current.Authority.Admission = "fenced"
			current.Authority.Fence = &fence
			if e = src.write(current); e != nil {
				return e
			}
			if afterFence != nil {
				if e = afterFence(); e != nil {
					return e
				}
			}
			dest.Authority.Admission = "active"
			if e = s.write(dest); e != nil {
				return e
			}
			out = report(dest)
			return nil
		})
	})
	return out, e
}
