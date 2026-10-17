// Package ledger implements a private local-filesystem transaction store.
package ledger

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/EduCloud-Ecosystem/outfitter/internal/provider"
)

type Project struct {
	ID     string `json:"project_id"`
	Budget int64  `json:"budget_cpu_seconds"`
}
type Data struct {
	Version  int                        `json:"version"`
	Owner    string                     `json:"owner"`
	Projects map[string]Project         `json:"projects"`
	Runs     map[string]provider.Record `json:"runs"`
}
type Store struct{ Dir string }
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
	s := &Store{p}
	e = s.lock(func() error {
		path := filepath.Join(p, "ledger.json")
		if _, e := os.Lstat(path); e == nil {
			_, e = s.read()
			return e
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
		return s.write(Data{1, hex.EncodeToString(b[:]), map[string]Project{}, map[string]provider.Record{}})
	})
	return s, e
}
func (s *Store) lock(fn func() error) error {
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
	f, e := os.Open(p)
	if e != nil {
		return d, e
	}
	defer f.Close()
	dec := json.NewDecoder(io.LimitReader(f, 64<<20))
	dec.DisallowUnknownFields()
	if e = dec.Decode(&d); e != nil {
		return d, fmt.Errorf("invalid ledger: %w", e)
	}
	if dec.Decode(new(any)) != io.EOF {
		return d, errors.New("trailing ledger data")
	}
	if d.Version != 1 || len(d.Owner) != 32 || d.Projects == nil || d.Runs == nil {
		return d, errors.New("incomplete ledger")
	}
	for id, p := range d.Projects {
		if p.ID != id || !provider.ValidID(id) || p.Budget <= 0 || p.Budget > 1000000000 {
			return d, errors.New("invalid project ledger")
		}
	}
	for id, r := range d.Runs {
		if r.RequestID != id || r.Owner != d.Owner || r.PayloadHash != r.Request.Hash() || r.Compute.Reserved != r.Request.ReservationAmount() || r.Compute.Charged < 0 || r.Compute.Charged > r.Compute.Reserved {
			return d, errors.New("invalid reservation ledger")
		}
		if _, ok := d.Projects[r.ProjectID]; !ok {
			return d, errors.New("orphan reservation")
		}
	}
	for id := range d.Projects {
		u := Calculate(d, id)
		if u.Remaining < 0 {
			return d, errors.New("budget ledger overdrawn")
		}
	}
	return d, nil
}
func (s *Store) write(d Data) error {
	b, e := json.MarshalIndent(d, "", "  ")
	if e != nil {
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
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		return e
	}
	if e = os.Rename(name, filepath.Join(s.Dir, "ledger.json")); e != nil {
		return e
	}
	dir, e := os.Open(s.Dir)
	if e != nil {
		return e
	}
	defer dir.Close()
	return dir.Sync()
}
func (s *Store) Update(fn func(*Data) error) error {
	return s.lock(func() error {
		d, e := s.read()
		if e != nil {
			return e
		}
		if e = fn(&d); e != nil {
			return e
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
