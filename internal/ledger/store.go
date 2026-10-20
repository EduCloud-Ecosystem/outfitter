// Package ledger implements a private local-filesystem transaction store.
package ledger

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"time"

	"github.com/EduCloud-Ecosystem/outfitter/internal/assessmentexec"
	"github.com/EduCloud-Ecosystem/outfitter/internal/owneddata"
	"github.com/EduCloud-Ecosystem/outfitter/internal/provider"
)

const MaxRecords = 512
const MaxProjects = 512
const maxRecordBytes = 96 << 10
const maxLedgerBytes = 64 << 20

type Project struct {
	ID     string `json:"project_id"`
	Budget int64  `json:"budget_cpu_seconds"`
}
type Data struct {
	AssessmentExecution *assessmentexec.Authority   `json:"assessment_execution,omitempty"`
	OwnedDataScopes     *map[string]owneddata.Scope `json:"owned_data_scopes,omitempty"`
	OwnedDataIdentity   *OwnedDataIdentity          `json:"owned_data_identity,omitempty"`
	NativeAccounts      map[string]NativeAccount    `json:"native_accounts,omitempty"`
	NativeRuns          map[string]NativeRecord     `json:"native_runs,omitempty"`
	Version             int                         `json:"version"`
	Owner               string                      `json:"owner"`
	Authority           *Authority                  `json:"authority,omitempty"`
	Projects            map[string]Project          `json:"projects"`
	Runs                map[string]provider.Record  `json:"runs"`
}
type Store struct {
	Dir           string
	ownedIdentity *OwnedDataIdentity
	// Only package tests install persistence faults. A returned post-rename
	// error may mean a counted transaction committed; it never permits replay.
	assessmentWriteFault     func(string) error
	assessmentOwnershipFault func(string) error
}
type Usage struct {
	ProjectID string `json:"project_id"`
	Budget    int64  `json:"budget_cpu_seconds"`
	Reserved  int64  `json:"reserved_cpu_seconds"`
	Charged   int64  `json:"charged_cpu_seconds"`
	Remaining int64  `json:"remaining_cpu_seconds"`
	Unknown   int64  `json:"unknown_cpu_seconds"`
}

func Open(dir string, create bool) (*Store, error) {
	p, e := filepath.Abs(dir)
	if e != nil {
		return nil, e
	}
	if create {
		if e = os.MkdirAll(p, 0700); e != nil {
			return nil, e
		}
	}
	st, e := os.Lstat(p)
	if e != nil {
		return nil, e
	}
	if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 || st.Mode().Perm()&0077 != 0 {
		return nil, errors.New("state directory must be a private 0700 directory")
	}
	real, e := filepath.EvalSymlinks(p)
	if e != nil {
		return nil, errors.New("state directory cannot contain symbolic links")
	}
	p = real
	s := &Store{Dir: p}
	// Pin fresh assessment-only authority objects before a lock operation could
	// create a missing lease. Legacy initialization/migration remains unchanged.
	if _, statErr := os.Lstat(filepath.Join(p, "ledger.json")); statErr == nil {
		d, readErr := s.read()
		if readErr != nil {
			return nil, readErr
		}
		if d.Version == 6 {
			identity := *d.OwnedDataIdentity
			s.ownedIdentity = &identity
		}
		if d.Version == 7 {
			s.ownedIdentity = executionIdentity(d)
		}
	}
	e = s.lock(func() error {
		path := filepath.Join(p, "ledger.json")
		if _, e := os.Lstat(path); e == nil {
			d, e := s.read()
			if e != nil {
				return e
			}
			if d.Version == 1 {
				id, e := randomID()
				if e != nil {
					return e
				}
				d.Version = 2
				d.Authority = &Authority{ID: id, StatePath: p, Admission: "active"}
				return s.write(d)
			}
			return nil
		} else if !os.IsNotExist(e) {
			return e
		}
		if !create {
			return errors.New("ledger is missing; refusing fresh accounting")
		}
		marker := filepath.Join(p, "initialized")
		f, e := os.OpenFile(marker, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			return errors.New("ledger missing after initialization; restore current accounting before continuing")
		}
		if e = f.Sync(); e != nil {
			f.Close()
			return e
		}
		f.Close()
		var b [16]byte
		if _, e = rand.Read(b[:]); e != nil {
			return e
		}
		id, e := randomID()
		if e != nil {
			return e
		}
		return s.write(Data{Version: 2, Owner: hex.EncodeToString(b[:]), Authority: &Authority{ID: id, StatePath: p, Admission: "active"}, Projects: map[string]Project{}, Runs: map[string]provider.Record{}})
	})
	return s, e
}
func (s *Store) lock(fn func() error) error {
	if s.ownedIdentity != nil {
		return s.ownedLock(fn)
	}
	fd, e := syscall.Open(filepath.Join(s.Dir, "ledger.lock"), syscall.O_RDWR|syscall.O_CREAT|syscall.O_NOFOLLOW, 0600)
	if e != nil {
		return e
	}
	defer syscall.Close(fd)
	if e = syscall.Flock(fd, syscall.LOCK_EX); e != nil {
		return e
	}
	defer syscall.Flock(fd, syscall.LOCK_UN)
	return fn()
}
func (s *Store) read() (Data, error) {
	var d Data
	p := filepath.Join(s.Dir, "ledger.json")
	st, e := os.Lstat(p)
	if e != nil {
		return d, e
	}
	if !st.Mode().IsRegular() || st.Size() > 64<<20 {
		return d, errors.New("invalid ledger file")
	}
	f, e := os.OpenFile(p, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if e != nil {
		return d, e
	}
	defer f.Close()
	opened, e := f.Stat()
	if e != nil || !opened.Mode().IsRegular() || !os.SameFile(st, opened) {
		return d, owneddata.ErrUnresolved
	}
	raw, e := io.ReadAll(io.LimitReader(f, maxLedgerBytes+1))
	if e != nil || len(raw) > maxLedgerBytes {
		return d, errors.New("invalid ledger file")
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if e = dec.Decode(&d); e != nil {
		return d, fmt.Errorf("invalid ledger: %w", e)
	}
	if dec.Decode(new(any)) != io.EOF {
		return d, errors.New("trailing ledger data")
	}
	if d.Version == 6 || d.Version == 7 {
		if e := owneddata.ClosedJSON(raw, &d, maxLedgerBytes); e != nil {
			return d, e
		}
	}
	if d.Version == 7 {
		var envelope struct {
			AssessmentExecution json.RawMessage `json:"assessment_execution"`
		}
		if json.Unmarshal(raw, &envelope) != nil || assessmentexec.DecodeClosed(envelope.AssessmentExecution, d.AssessmentExecution, maxLedgerBytes) != nil {
			return d, assessmentexec.ErrInvalid
		}
	}
	if e = validate(d); e != nil {
		return d, e
	}
	if d.Version >= 2 && d.Authority.StatePath != s.Dir {
		return d, errors.New("ledger belongs to a different state path; use held restore")
	}
	if d.Version == 6 {
		if e := s.checkOwnedIdentity(d.OwnedDataIdentity); e != nil {
			return d, e
		}
	}
	if d.Version == 7 {
		if e := s.checkOwnedIdentity(executionIdentity(d)); e != nil {
			return d, e
		}
	}
	return d, nil
}
func validate(d Data) error {
	if (d.Version != 1 && d.Version != 2 && d.Version != 3 && d.Version != 4 && d.Version != 5 && d.Version != 6 && d.Version != 7) || len(d.Owner) != 32 || d.Projects == nil || d.Runs == nil || len(d.Projects)+len(d.NativeAccounts) > MaxProjects || len(d.Runs)+len(d.NativeRuns)+ownedCount(d)+executionCount(d) > MaxRecords {
		return errors.New("incomplete or over-capacity ledger")
	}
	if e := validatePlacements(d); e != nil {
		return e
	}
	if e := validateNative(d); e != nil {
		return e
	}
	if e := validateAuthority(d); e != nil {
		return e
	}
	if e := validateOwnedData(d); e != nil {
		return e
	}
	if e := validateAssessmentExecution(d); e != nil {
		return e
	}
	if decoded, e := hex.DecodeString(d.Owner); e != nil || len(decoded) != 16 {
		return errors.New("invalid ledger owner")
	}
	for id, p := range d.Projects {
		if p.ID != id || !provider.ValidID(id) || p.Budget <= 0 || p.Budget > 1000000000 {
			return errors.New("invalid project ledger")
		}
	}
	allowed := map[string]bool{"reserved": true, "creating": true, "running": true, "succeeded": true, "failed": true, "cancelled": true, "unknown": true}
	for id, r := range d.Runs {
		if r.Request.Consumer != nil && d.Version < 4 {
			return errors.New("consumer binding requires authority version 4")
		}
		if e := r.Request.Validate(); e != nil {
			return errors.New("invalid persisted request")
		}
		h := sha256.Sum256([]byte(d.Owner + ":" + id))
		name := "outfitter-" + hex.EncodeToString(h[:16])
		if r.RequestID != id || r.Request.RequestID != id || r.ProjectID != r.Request.ProjectID || r.Image != r.Request.Image || r.Owner != d.Owner || r.Name != name || r.PayloadHash != r.Request.Hash() || !r.Admitted || r.SchemaVersion != 1 || !allowed[r.Status] || r.CreatedAt.IsZero() || !r.Deadline.Equal(r.CreatedAt.Add(time.Duration(r.Request.TimeoutSeconds)*time.Second)) {
			return errors.New("reservation identity or lifecycle metadata mismatch")
		}
		if _, ok := d.Projects[r.ProjectID]; !ok {
			return errors.New("orphan reservation")
		}
		if r.Compute.Reserved != r.Request.ReservationAmount() || len(r.Error) > 2048 || len(r.OutputError) > 256 || r.TeardownRisk != "supervised external watchdog; no provider self-expiry" {
			return errors.New("invalid reservation amount or diagnostic bound")
		}
		if r.CreateAmbiguous && !r.UnresolvedLiability {
			return errors.New("ambiguous creation cannot settle from an absence observation")
		}
		if r.UnresolvedLiability {
			if r.Compute.Charged != 0 || r.Compute.Provenance != "reserved" {
				return errors.New("unresolved liability must retain its whole reservation")
			}
		} else {
			if r.Compute.Charged != r.Compute.Reserved || r.Compute.Provenance != "estimated_upper_bound" || !r.CleanupVerified || r.Status == "reserved" || r.Status == "creating" || r.Status == "running" {
				return errors.New("settled reservation requires full conservative charge and verified cleanup")
			}
		}
		if r.OutputSHA256 != "" {
			decoded, e := hex.DecodeString(r.OutputSHA256)
			if e != nil || len(decoded) != 32 || r.OutputSHA256 != strings.ToLower(r.OutputSHA256) || r.OutputBytes <= 0 || r.OutputBytes > (6<<20)+512 {
				return errors.New("invalid output reference")
			}
		} else if r.OutputBytes != 0 {
			return errors.New("missing output digest")
		}
		if len(r.Stdout)+len(r.Stderr) > r.Request.MaxOutputBytes {
			return errors.New("retained output exceeds request limit")
		}
	}
	for id := range d.Projects {
		if Calculate(d, id).Remaining < 0 {
			return errors.New("budget ledger overdrawn")
		}
	}
	return nil
}

func (s *Store) write(d Data) error {
	fault := func(point string) error {
		if d.Version == 7 && s.assessmentWriteFault != nil {
			return s.assessmentWriteFault(point)
		}
		return nil
	}
	// Prepare every possible diagnostic outcome before creating sidecars. Later
	// I/O failure can still leave an orphan; uncertain accounting is never refunded.
	plan, e := prepareLedgerWrite(d, s.Dir, maxRecordBytes, maxLedgerBytes)
	if e != nil {
		return e
	}
	for id, output := range plan.outputs {
		if e := s.persistPlannedOutput(output); e != nil {
			plan.compact.Runs[id] = diagnosticFailure(plan.compact.Runs[id])
		}
	}
	b, e := marshalCompact(plan.compact, maxRecordBytes, maxLedgerBytes)
	if e != nil {
		return e
	}
	if len(b) > plan.maximumBytes {
		return errors.New("authority serialization exceeded prepared diagnostic outcomes")
	}
	if e = fault("before_temp"); e != nil {
		return e
	}
	f, e := os.CreateTemp(s.Dir, ".ledger-")
	if e != nil {
		return e
	}
	name := f.Name()
	defer os.Remove(name)
	if e = f.Chmod(0600); e == nil {
		_, e = f.Write(b)
	}
	if e == nil {
		e = fault("before_file_sync")
	}
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		return e
	}
	if e = fault("before_rename"); e != nil {
		return e
	}
	if e = os.Rename(name, filepath.Join(s.Dir, "ledger.json")); e != nil {
		return e
	}
	if e = fault("after_rename"); e != nil {
		return e
	}
	dir, e := os.Open(s.Dir)
	if e != nil {
		return e
	}
	defer dir.Close()
	if e = fault("before_directory_sync"); e != nil {
		return e
	}
	if e = dir.Sync(); e != nil {
		return e
	}
	return fault("after_directory_sync")
}
func (s *Store) Update(fn func(*Data) error) error {
	return s.updateAssessment(fn, nil)
}

func (s *Store) updateAssessment(fn func(*Data) error, completion *assessmentCompletion) error {
	if completion != nil && completion.store != s {
		return assessmentexec.ErrDenied
	}
	return s.lock(func() error {
		d, e := s.read()
		if e != nil {
			return e
		}
		experiments := nativeExperimentGuards(d)
		placements := placementGuards(d)
		owned := captureOwnedGuard(d)
		execution := captureExecutionGuard(d)
		if e = fn(&d); e != nil {
			return e
		}
		if e = preserveNativeExperiments(experiments, d); e != nil {
			return e
		}
		if e = preservePlacements(placements, d); e != nil {
			return e
		}
		if e = preserveOwnedGuard(owned, d); e != nil {
			return e
		}
		if e = preserveExecutionGuard(execution, d, completion); e != nil {
			return e
		}
		if d.Version == 7 && execution.authority != nil && reflect.DeepEqual(*execution.authority, *d.AssessmentExecution) {
			return nil
		}
		return s.write(d)
	})
}
func (s *Store) View(fn func(Data) error) error {
	return s.lock(func() error {
		d, e := s.read()
		if e != nil {
			return e
		}
		return fn(d)
	})
}
func (s *Store) InitProject(id string, budget int64) (Usage, error) {
	var out Usage
	if !provider.ValidID(id) || budget <= 0 || budget > 1000000000 {
		return out, errors.New("invalid project ID or finite budget")
	}
	e := s.Update(func(d *Data) error {
		if e := RequireAdmission(*d); e != nil {
			return e
		}
		if _, ok := d.Projects[id]; !ok && len(d.Projects)+len(d.NativeAccounts) >= MaxProjects {
			return errors.New("project metadata capacity exhausted")
		}
		if p, ok := d.Projects[id]; ok && p.Budget != budget {
			return errors.New("existing project budget is immutable")
		}
		d.Projects[id] = Project{id, budget}
		out = Calculate(*d, id)
		return nil
	})
	return out, e
}
func Calculate(d Data, id string) Usage {
	u := Usage{ProjectID: id, Budget: d.Projects[id].Budget}
	for _, r := range d.Runs {
		if r.ProjectID != id {
			continue
		}
		u.Charged += r.Compute.Charged
		if r.Compute.Charged == 0 {
			u.Reserved += r.Compute.Reserved
		}
		if r.UnresolvedLiability {
			u.Unknown += r.Compute.Reserved
		}
	}
	if d.AssessmentExecution != nil {
		for _, a := range d.AssessmentExecution.Attempts {
			if a.Binding.ProjectID != id {
				continue
			}
			u.Charged += a.Compute.ChargedCPUSeconds
			if a.AccountingUnresolved {
				u.Reserved += a.Compute.ReservedCPUSeconds
				u.Unknown += a.Compute.ReservedCPUSeconds
			}
		}
	}
	u.Remaining = u.Budget - u.Charged - u.Reserved
	return u
}
func (s *Store) Usage(id string) (Usage, error) {
	var u Usage
	e := s.View(func(d Data) error {
		if _, ok := d.Projects[id]; !ok {
			return errors.New("unknown project")
		}
		u = Calculate(d, id)
		return nil
	})
	return u, e
}
func (s *Store) Record(id string) (provider.Record, error) {
	var r provider.Record
	e := s.View(func(d Data) error {
		var ok bool
		r, ok = d.Runs[id]
		if !ok {
			return errors.New("unknown request")
		}
		return nil
	})
	return r, e
}

// Diagnostic bytes are never required to read authority, cancel or reap a run.
func (s *Store) saveOutput(data []byte, name string) error {
	dir := filepath.Join(s.Dir, "outputs")
	if e := os.MkdirAll(dir, 0700); e != nil {
		return e
	}
	if st, e := os.Lstat(dir); e != nil || !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
		return errors.New("invalid diagnostic directory")
	}
	f, e := os.CreateTemp(dir, ".output-")
	if e != nil {
		return e
	}
	temp := f.Name()
	defer os.Remove(temp)
	if _, e = f.Write(data); e == nil {
		e = f.Sync()
	}
	closeErr := f.Close()
	if e == nil {
		e = closeErr
	}
	if e != nil {
		return e
	}
	if e = os.Rename(temp, filepath.Join(dir, name+".json")); e != nil {
		return e
	}
	directory, e := os.Open(dir)
	if e != nil {
		return e
	}
	defer directory.Close()
	if e = directory.Sync(); e != nil {
		return e
	}
	return nil
}
func (s *Store) Receipt(id string) (provider.Receipt, error) {
	r, e := s.Record(id)
	if e != nil {
		return r.Receipt, e
	}
	if r.OutputSHA256 == "" {
		return r.Receipt, nil
	}
	unavailable := func() (provider.Receipt, error) {
		r.Stdout = ""
		r.Stderr = ""
		r.OutputError = "execution diagnostics missing, corrupt or oversized; authority accounting remains intact"
		return r.Receipt, nil
	}
	path := filepath.Join(s.Dir, "outputs", r.OutputSHA256+".json")
	stat, e := os.Lstat(path)
	if e != nil || !stat.Mode().IsRegular() || stat.Size() != r.OutputBytes {
		return unavailable()
	}
	fd, e := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if e != nil {
		return unavailable()
	}
	file := os.NewFile(uintptr(fd), path)
	defer file.Close()
	data, e := io.ReadAll(io.LimitReader(file, r.OutputBytes+1))
	if e != nil || int64(len(data)) != r.OutputBytes {
		return unavailable()
	}
	digest := sha256.Sum256(data)
	if hex.EncodeToString(digest[:]) != r.OutputSHA256 {
		return unavailable()
	}
	var output struct {
		Stdout string `json:"stdout"`
		Stderr string `json:"stderr"`
	}
	if e = json.Unmarshal(data, &output); e != nil || len(output.Stdout)+len(output.Stderr) > r.Request.MaxOutputBytes {
		return unavailable()
	}
	r.Stdout = output.Stdout
	r.Stderr = output.Stderr
	return r.Receipt, nil
}
