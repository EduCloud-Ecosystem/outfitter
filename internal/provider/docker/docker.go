// Package docker is the reviewed-local Docker CLI adapter. It never constructs shell commands.
package docker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/EduCloud-Ecosystem/outfitter/internal/provider"
)

type Adapter struct{ Binary string }

func New() *Adapter             { return &Adapter{Binary: "docker"} }
func (a *Adapter) Name() string { return "docker" }
func (a *Adapter) Capabilities() provider.Capabilities {
	return provider.Capabilities{Isolation: []string{"reviewed-local"}}
}
func (a *Adapter) Estimate(r provider.Request) (int64, error) {
	if e := r.Validate(); e != nil {
		return 0, e
	}
	return r.ReservationAmount(), nil
}
func (a *Adapter) command(ctx context.Context, args ...string) ([]byte, error) {
	c, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(c, a.Binary, args...)
	var out, errout bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errout
	e := cmd.Run()
	if e != nil {
		return nil, fmt.Errorf("Docker %s failed: %s", args[0], truncate(errout.String(), 1024))
	}
	return out.Bytes(), nil
}
func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return strings.TrimSpace(s)
}
func (a *Adapter) Provision(ctx context.Context, r provider.Request, v provider.Reservation) error {
	if e := r.Validate(); e != nil {
		return e
	}
	if e := v.Validate(r); e != nil {
		return e
	}
	// Local-only immutable images: never fetch dependencies during a workload.
	imageInfo, e := a.command(ctx, "image", "inspect", r.Image)
	if e != nil {
		return e
	}
	var images []struct {
		Config struct{ Volumes map[string]any }
	}
	if e = json.Unmarshal(imageInfo, &images); e != nil || len(images) != 1 {
		return errors.New("invalid immutable image inspection")
	}
	if len(images[0].Config.Volumes) > 0 {
		return errors.New("image declares implicit writable volumes; refuse unowned storage")
	}
	args := []string{"create", "--pull", "never", "--name", v.Name, "--label", "org.educloud.outfitter.owner=" + v.Owner, "--label", "org.educloud.outfitter.request=" + r.RequestID, "--network", "none", "--restart", "no", "--user", "65532:65532", "--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges:true", "--cpus", strconv.FormatFloat(r.CPUs, 'f', -1, 64), "--memory", fmt.Sprintf("%dm", r.MemoryMB), "--memory-swap", fmt.Sprintf("%dm", r.MemoryMB), "--pids-limit", strconv.Itoa(r.PIDsLimit), "--ulimit", fmt.Sprintf("cpu=%d:%d", r.TimeoutSeconds, r.TimeoutSeconds), "--ulimit", "nofile=1024:1024", "--log-driver", "none", "--no-healthcheck", "--tmpfs", "/tmp:rw,noexec,nosuid,nodev,size=16777216,mode=1777", "--mount", "type=bind,source=" + r.InputDir + ",target=/input,readonly", "--mount", "type=bind,source=" + r.RecipeDir + ",target=/recipe,readonly", "--workdir", "/tmp", "--env", "HOME=/tmp", "--env", "TMPDIR=/tmp", "--env", "PYTHONDONTWRITEBYTECODE=1", "--entrypoint", r.Command[0], r.Image}
	args = append(args, r.Command[1:]...)
	_, e = a.command(ctx, args...)
	return e
}
func (a *Adapter) Run(ctx context.Context, r provider.Record, out, errout io.Writer) (int, error) {
	cmd := exec.CommandContext(ctx, a.Binary, "start", "--attach", r.Name)
	cmd.Stdout = out
	cmd.Stderr = errout
	e := cmd.Run()
	if ctx.Err() != nil {
		return -1, ctx.Err()
	}
	s, se := a.Status(ctx, r)
	if se != nil {
		return -1, se
	}
	if !s.Exists {
		return -1, errors.New("container disappeared before exit observation")
	}
	if s.Running {
		return -1, errors.New("container remains running after attach")
	}
	if e != nil && s.ExitCode == 0 {
		return -1, errors.New("Docker attach failed without workload failure")
	}
	return s.ExitCode, nil
}
func (a *Adapter) Status(ctx context.Context, r provider.Record) (provider.State, error) {
	var result provider.State
	// Enumerating names distinguishes a successful absence query from daemon errors.
	b, e := a.command(ctx, "container", "ls", "--all", "--filter", "name=^/"+r.Name+"$", "--format", "{{.Names}}")
	if e != nil {
		return result, e
	}
	found := false
	for _, name := range strings.Fields(string(b)) {
		if name == r.Name {
			found = true
		}
	}
	if !found {
		return result, nil
	}
	b, e = a.command(ctx, "container", "inspect", r.Name)
	if e != nil {
		// A concurrent owned reaper may remove it between enumeration and inspect.
		names, probeErr := a.command(ctx, "container", "ls", "--all", "--filter", "name=^/"+r.Name+"$", "--format", "{{.Names}}")
		if probeErr == nil && strings.TrimSpace(string(names)) == "" {
			return result, nil
		}
		return result, e
	}
	var objects []struct {
		Config struct{ Labels map[string]string }
		State  struct {
			Running  bool
			ExitCode int
		}
	}
	if e = json.Unmarshal(b, &objects); e != nil || len(objects) != 1 {
		return result, errors.New("invalid Docker inspection response")
	}
	result = provider.State{Exists: true, Running: objects[0].State.Running, ExitCode: objects[0].State.ExitCode, Owner: objects[0].Config.Labels["org.educloud.outfitter.owner"], RequestID: objects[0].Config.Labels["org.educloud.outfitter.request"]}
	if e = provider.CheckOwnership(r, result); e != nil {
		return result, e
	}
	return result, nil
}
func (a *Adapter) Teardown(ctx context.Context, r provider.Record) error {
	s, e := a.Status(ctx, r)
	if e != nil {
		return e
	}
	if !s.Exists {
		return nil
	}
	_, removeErr := a.command(ctx, "container", "rm", "--force", r.Name)
	// Another owned reaper can already be deleting this resource. A transient
	// "removal in progress" is not final failure, nor is it verified absence.
	until := time.Now().Add(5 * time.Second)
	for {
		observed, probeErr := a.Status(ctx, r)
		if probeErr == nil && !observed.Exists {
			return nil
		}
		if probeErr != nil {
			return probeErr
		}
		if time.Now().After(until) {
			if removeErr != nil {
				return removeErr
			}
			return errors.New("resource remains after teardown")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}
