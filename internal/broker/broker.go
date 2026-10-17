// Package broker performs reserve-before-effects execution and owned reconciliation.
package broker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
	"github.com/EduCloud-Ecosystem/outfitter/internal/provider"
)

var ErrBudget = errors.New("project CPU-second budget exhausted")

type Broker struct {
	Store         *ledger.Store
	Provider      provider.Provisioner
	StartWatchdog func(string) error
}

func New(s *ledger.Store, p provider.Provisioner) *Broker { return &Broker{Store: s, Provider: p} }
func (b *Broker) Reserve(w provider.Request) (provider.Record, bool, error) {
	var r provider.Record
	fresh := false
	ceiling, e := b.Provider.Estimate(w)
	if e != nil {
		return r, false, e
	}
	if ceiling != w.ReservationAmount() {
		return r, false, errors.New("provider estimate is not a bounded local CPU reservation")
	}
	caps := b.Provider.Capabilities()
	supported := false
	for _, tier := range caps.Isolation {
		if tier == w.Isolation {
			supported = true
		}
	}
	if !supported || (!caps.SelfExpiry && !w.AllowSupervisedReaper) {
		return r, false, errors.New("required provider capability not declared")
	}
	e = b.Store.Update(func(d *ledger.Data) error {
		if e := ledger.RequireAdmission(*d); e != nil {
			return e
		}
		if old, ok := d.Runs[w.RequestID]; ok {
			if old.PayloadHash != w.Hash() {
				return errors.New("request ID payload collision")
			}
			r = old
			return nil
		}
		if len(d.Runs) >= ledger.MaxRecords {
			return errors.New("authority metadata capacity exhausted; new admission refused")
		}
		if _, ok := d.Projects[w.ProjectID]; !ok {
			return errors.New("project must be initialized before admission")
		}
		u := ledger.Calculate(*d, w.ProjectID)
		if u.Remaining < ceiling {
			return ErrBudget
		}
		all, project := 0, 0
		for _, x := range d.Runs {
			if x.Active() {
				all++
				if x.ProjectID == w.ProjectID {
					project++
				}
			}
		}
		if all >= 4 || project >= 2 {
			return errors.New("active placement limit reached; reconcile unresolved work first")
		}
		now := time.Now().UTC()
		h := sha256.Sum256([]byte(d.Owner + ":" + w.RequestID))
		name := "outfitter-" + hex.EncodeToString(h[:16])
		r = provider.Record{Receipt: provider.Receipt{Admitted: true, SchemaVersion: 1, RequestID: w.RequestID, ProjectID: w.ProjectID, Status: "reserved", Image: w.Image, CreatedAt: now, Deadline: now.Add(time.Duration(w.TimeoutSeconds) * time.Second), UnresolvedLiability: true, Compute: provider.Compute{Reserved: ceiling, Provenance: "reserved"}, TeardownRisk: "supervised external watchdog; no provider self-expiry"}, Request: w, PayloadHash: w.Hash(), Name: name, Owner: d.Owner, PID: os.Getpid()}
		d.Runs[w.RequestID] = r
		fresh = true
		return nil
	})
	return r, fresh, e
}
func (b *Broker) change(id string, fn func(*provider.Record)) (provider.Record, error) {
	var r provider.Record
	e := b.Store.Update(func(d *ledger.Data) error {
		var ok bool
		r, ok = d.Runs[id]
		if !ok {
			return errors.New("unknown request")
		}
		fn(&r)
		d.Runs[id] = r
		return nil
	})
	return r, e
}
func (b *Broker) Run(ctx context.Context, w provider.Request) (provider.Receipt, error) {
	release, leaseErr := b.Store.ExecutionLease()
	if leaseErr != nil {
		return provider.Receipt{}, leaseErr
	}
	defer release()
	r, fresh, e := b.Reserve(w)
	if e != nil {
		if errors.Is(e, ErrBudget) || errors.Is(e, ledger.ErrAdmission) {
			return provider.Receipt{SchemaVersion: 1, RequestID: w.RequestID, ProjectID: w.ProjectID, Status: "failed", Error: e.Error(), CleanupVerified: true, Image: w.Image, Compute: provider.Compute{Provenance: "not_admitted"}}, nil
		}
		return r.Receipt, e
	}
	if !fresh {
		return b.Store.Receipt(r.RequestID)
	}
	// Snapshots are private host artifacts, never writable by the workload.
	in, recipe, e := snapshot(b.Store.Dir, w)
	if e != nil {
		return b.finish(r.RequestID, "failed", "input snapshot refused: "+e.Error(), nil, "", "", false)
	}
	if b.StartWatchdog == nil {
		return b.finish(r.RequestID, "failed", "no independent watchdog configured", nil, "", "", false)
	}
	if e = b.StartWatchdog(w.RequestID); e != nil {
		return b.finish(r.RequestID, "failed", "watchdog did not become ready", nil, "", "", false)
	}
	r, e = b.change(w.RequestID, func(x *provider.Record) {
		if !x.CancelRequested && time.Now().Before(x.Deadline) {
			x.Status = "creating"
			x.CreateAttempted = true
		}
	})
	if e != nil {
		return r.Receipt, e
	}
	if !r.CreateAttempted {
		return b.finish(w.RequestID, "cancelled", "cancelled or expired before creation", nil, "", "", false)
	}
	work := w
	work.InputDir = in
	work.RecipeDir = recipe
	runctx, cancel := context.WithDeadline(ctx, r.Deadline)
	defer cancel()
	v := provider.Reservation{ID: r.RequestID, Amount: r.Compute.Reserved, Deadline: r.Deadline, Owner: r.Owner, Name: r.Name}
	e = b.Provider.Provision(runctx, work, v)
	r, saveErr := b.change(w.RequestID, func(x *provider.Record) {
		x.CreateFinished = true
		if e != nil {
			x.CreateAmbiguous = true
		}
	})
	if saveErr != nil {
		_ = b.Provider.Teardown(context.Background(), r)
		return r.Receipt, saveErr
	}
	if e != nil {
		return b.finish(w.RequestID, "unknown", "provider creation failed or response ambiguous", nil, "", "", true)
	}
	if r.CancelRequested || runctx.Err() != nil {
		return b.finish(w.RequestID, "cancelled", "cancelled before execution", nil, "", "", false)
	}
	now := time.Now().UTC()
	r, e = b.change(w.RequestID, func(x *provider.Record) { x.Status = "running"; x.ExecutionStarted = true; x.StartedAt = &now })
	if e != nil {
		return r.Receipt, e
	}
	c := &capture{max: w.MaxOutputBytes, cancel: cancel}
	code, runErr := b.Provider.Run(runctx, r, c.writer(false), c.writer(true))
	stdout, stderr, exceeded := c.result()
	status, problem := "succeeded", ""
	var exit *int
	if code >= 0 {
		exit = &code
	}
	switch {
	case exceeded:
		status = "failed"
		problem = "combined output limit exceeded"
	case runctx.Err() != nil:
		status = "failed"
		problem = "execution deadline or cancellation reached"
	case runErr != nil:
		status = "unknown"
		problem = "execution observation failed"
	case code != 0:
		status = "failed"
		problem = fmt.Sprintf("workload exited with code %d", code)
	}
	return b.finish(w.RequestID, status, problem, exit, stdout, stderr, runErr != nil && runctx.Err() == nil && !exceeded)
}
func (b *Broker) finish(id, status, problem string, exit *int, out, errout string, uncertain bool) (provider.Receipt, error) {
	r, e := b.Store.Record(id)
	if e != nil {
		return r.Receipt, e
	}
	if r.CleanupVerified && !r.UnresolvedLiability {
		return b.Store.Receipt(id)
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	cleanupErr := b.Provider.Teardown(cleanupCtx, r)
	// A successful query plus removal is required. It cannot settle ambiguous create.
	clean := cleanupErr == nil
	r, e = b.change(id, func(x *provider.Record) {
		if x.CleanupVerified && !x.UnresolvedLiability {
			return
		}
		if x.CancelRequested {
			status = "cancelled"
		}
		if cleanupErr != nil {
			status = "unknown"
			problem = "cleanup unverified; reconcile required"
		}
		if x.CreateAmbiguous {
			uncertain = true
			status = "unknown"
		}
		x.Status = status
		if len(problem) > 2048 {
			problem = problem[:2048]
		}
		x.Error = problem
		x.ExitCode = exit
		x.Stdout = out
		x.Stderr = errout
		x.CleanupVerified = clean && !x.CreateAmbiguous && (!x.CreateAttempted || x.CreateFinished)
		x.UnresolvedLiability = uncertain || !x.CleanupVerified
		if !x.UnresolvedLiability {
			x.Compute.Charged = x.Compute.Reserved
			x.Compute.Provenance = "estimated_upper_bound"
		} else {
			x.Compute.Provenance = "reserved"
		}
		now := time.Now().UTC()
		x.FinishedAt = &now
	})
	if e != nil {
		return r.Receipt, e
	}
	return b.Store.Receipt(id)
}
func (b *Broker) Cancel(id, reason string) (provider.Receipt, error) {
	if len(reason) > 2048 {
		reason = reason[:2048]
	}
	r, e := b.change(id, func(x *provider.Record) {
		if !x.CleanupVerified {
			x.CancelRequested = true
			x.Error = reason
		}
	})
	if e != nil {
		return r.Receipt, e
	}
	if r.CleanupVerified {
		return b.Store.Receipt(id)
	}
	// Creation still in progress can race a late provider response: retain uncertainty.
	return b.finish(id, "cancelled", reason, r.ExitCode, r.Stdout, r.Stderr, r.CreateAttempted && !r.CreateFinished)
}
func Alive(pid int) bool {
	if pid <= 0 {
		return false
	}
	e := syscall.Kill(pid, 0)
	return e == nil || e == syscall.EPERM
}
func (b *Broker) Reconcile(id string) (provider.Receipt, error) {
	r, e := b.Store.Record(id)
	if e != nil {
		return r.Receipt, e
	}
	if r.CleanupVerified && !r.UnresolvedLiability {
		return b.Store.Receipt(id)
	}
	if Alive(r.PID) && time.Now().Before(r.Deadline) && !r.CancelRequested {
		return r.Receipt, errors.New("request is still owned by a live broker; cancel it first")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	e = b.Provider.Teardown(ctx, r)
	if e != nil {
		return b.finish(id, "unknown", "reconciliation could not verify cleanup", r.ExitCode, r.Stdout, r.Stderr, true)
	}
	// Never credit capacity for uncertain execution; settle the full ceiling.
	r, e = b.change(id, func(x *provider.Record) {
		if x.CreateAmbiguous || (x.CreateAttempted && !x.CreateFinished) {
			x.Status = "unknown"
			x.Error = "creation response unresolved; absence is provisional; liability and supervised cleanup remain active"
			return
		}
		x.CleanupVerified = true
		x.UnresolvedLiability = false
		x.Compute.Charged = x.Compute.Reserved
		x.Compute.Provenance = "estimated_upper_bound"
		if x.Status == "reserved" || x.Status == "creating" || x.Status == "running" {
			x.Status = "unknown"
		}
		if x.Status == "unknown" {
			x.Error = "execution outcome unknown; full ceiling charged; owned cleanup verified"
		}
		now := time.Now().UTC()
		x.FinishedAt = &now
	})
	return r.Receipt, e
}
func (b *Broker) Reap() ([]provider.Receipt, error) {
	var ids []string
	e := b.Store.View(func(d ledger.Data) error {
		for id, r := range d.Runs {
			if r.Active() && (!time.Now().Before(r.Deadline) || !Alive(r.PID) || r.CancelRequested) {
				ids = append(ids, id)
			}
		}
		return nil
	})
	if e != nil {
		return nil, e
	}
	result := []provider.Receipt{}
	for _, id := range ids {
		r, e := b.Cancel(id, "expired or abandoned request reaped")
		if e != nil {
			return result, e
		}
		result = append(result, r)
	}
	return result, nil
}
func (b *Broker) Watch(ctx context.Context, id string) error {
	r, e := b.Store.Record(id)
	if e != nil {
		return e
	}
	ready := filepath.Join(b.Store.Dir, "watch-"+id+".ready")
	if e = os.WriteFile(ready, []byte("ready\n"), 0600); e != nil {
		return e
	}
	defer os.Remove(ready)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			r, e = b.Store.Record(id)
			if e != nil {
				return e
			}
			if r.CleanupVerified && !r.UnresolvedLiability {
				return nil
			}
			if !time.Now().Before(r.Deadline) || !Alive(r.PID) || r.CancelRequested {
				if _, e = b.Cancel(id, "watchdog deadline, cancellation or broker loss"); e != nil {
					return e
				}
				// Retry ambiguous cleanup while the host remains healthy; no automatic refund.
				if !Alive(r.PID) {
					_, e = b.Reconcile(id)
					if e != nil {
						return e
					}
				}
			}
		}
	}
}

func snapshot(root string, r provider.Request) (string, string, error) {
	dest := filepath.Join(root, "snapshots", r.RequestID)
	if e := os.MkdirAll(filepath.Dir(dest), 0700); e != nil {
		return "", "", e
	}
	if e := os.Mkdir(dest, 0700); e != nil {
		return "", "", e
	}
	remaining := int64(16 << 20)
	count := 0
	copyTree := func(src, target string) error {
		real, e := filepath.EvalSymlinks(src)
		if e != nil {
			return errors.New("input path contains symlink or is missing")
		}
		st, e := os.Lstat(src)
		if e != nil || !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
			return errors.New("input is not a directory")
		}
		src = real
		return filepath.WalkDir(src, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			count++
			if count > 1024 {
				return errors.New("too many input files")
			}
			rel, e := filepath.Rel(src, path)
			if e != nil {
				return e
			}
			to := filepath.Join(target, rel)
			if entry.Type()&os.ModeSymlink != 0 {
				return errors.New("symlink input refused")
			}
			if entry.IsDir() {
				return os.Mkdir(to, 0755)
			}
			info, e := entry.Info()
			if e != nil {
				return e
			}
			if !info.Mode().IsRegular() {
				return errors.New("special input file refused")
			}
			if info.Size() > remaining {
				return errors.New("input size exceeds 16 MiB")
			}
			fd, e := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW, 0)
			if e != nil {
				return e
			}
			in := os.NewFile(uintptr(fd), path)
			defer in.Close()
			out, e := os.OpenFile(to, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0444)
			if e != nil {
				return e
			}
			n, e := io.Copy(out, io.LimitReader(in, remaining+1))
			closeErr := out.Close()
			remaining -= n
			if e != nil {
				return e
			}
			if closeErr != nil {
				return closeErr
			}
			if remaining < 0 {
				return errors.New("input grew beyond size cap")
			}
			return nil
		})
	}
	in, recipe := filepath.Join(dest, "input"), filepath.Join(dest, "recipe")
	if e := copyTree(r.InputDir, in); e != nil {
		return "", "", e
	}
	if e := copyTree(r.RecipeDir, recipe); e != nil {
		return "", "", e
	}
	return in, recipe, nil
}

type capture struct {
	mu       sync.Mutex
	max      int
	used     int
	out, err strings.Builder
	exceeded bool
	cancel   context.CancelFunc
}
type captureWriter struct {
	c      *capture
	stderr bool
}

func (c *capture) writer(stderr bool) io.Writer { return captureWriter{c, stderr} }
func (w captureWriter) Write(p []byte) (int, error) {
	c := w.c
	c.mu.Lock()
	defer c.mu.Unlock()
	n := len(p)
	remaining := c.max - c.used
	if n > remaining {
		p = p[:remaining]
		c.exceeded = true
		c.cancel()
	}
	if w.stderr {
		c.err.Write(p)
	} else {
		c.out.Write(p)
	}
	c.used += len(p)
	if c.exceeded {
		return len(p), errors.New("output limit exceeded")
	}
	return n, nil
}
func (c *capture) result() (string, string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.out.String(), c.err.String(), c.exceeded
}
