package broker

import (
	"bytes"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/EduCloud-Ecosystem/outfitter/internal/assessment"
	x "github.com/EduCloud-Ecosystem/outfitter/internal/assessmentexec"
	"github.com/EduCloud-Ecosystem/outfitter/internal/owneddata"
)

//go:embed testdata/execution-session-a2-seed-vectors.json
var executionSeedVectorSource []byte

const executionSeedRequestName = "a2-seed-request.json"
const executionSeedTargetName = "a2-fixture"
const executionSeedSchema = "outfitter.execution-session-seed-request/v1"
const executionSeedResultSchema = "outfitter.execution-session-seed-result/v1"

type executionSeedRequest struct {
	Schema              string `json:"schema"`
	ScratchParent       string `json:"scratch_parent"`
	WaypointAuthorityID string `json:"waypoint_authority_id"`
	ProjectID           string `json:"project_id"`
	ScopeID             string `json:"scope_id"`
	OwnerEpoch          string `json:"owner_epoch"`
	SampledAt           string `json:"sampled_at"`
	SessionDeadline     string `json:"session_deadline"`
	AuthorizationExpiry string `json:"authorization_expiry"`
	ProjectExpiry       string `json:"project_expiry"`
	PolicyExpiry        string `json:"policy_expiry"`
	ManifestSHA256      string `json:"manifest_sha256"`
}
type executionSeedResult struct {
	Schema string `json:"schema"`
	Code   string `json:"code"`
}
type executionSeedAssets struct {
	Config  executionSessionConfig
	Binding x.ScopeBinding
}

func executionSeedPaths(parent string) bool {
	if !utf8.ValidString(parent) || len(parent) > 990 || strings.ContainsRune(parent, 0) || !filepath.IsAbs(parent) || filepath.Clean(parent) != parent || parent == string(filepath.Separator) {
		return false
	}
	for _, suffix := range []string{executionSeedRequestName, "a2-fixture", "a2-fixture/state", "a2-fixture/data", "a2-fixture/state/initialized", "a2-fixture/state/ledger.lock", "a2-fixture/state/execution.lock", "a2-fixture/state/ledger.json", "a2-fixture/private-bootstrap.json", "a2-fixture/fixed-binding.json"} {
		if len(filepath.Join(parent, suffix)) > 1024 {
			return false
		}
	}
	return true
}
func executionSeedPrepare(r executionSeedRequest, now time.Time) (executionSeedAssets, time.Time, error) {
	var a executionSeedAssets
	if r.Schema != executionSeedSchema || !executionSeedPaths(r.ScratchParent) || !owneddata.ID(r.WaypointAuthorityID) || !owneddata.ID(r.ProjectID) || !owneddata.ID(r.ScopeID) || !owneddata.ID(r.OwnerEpoch) || !owneddata.Hex(r.ManifestSHA256, 32) {
		return a, time.Time{}, executionSessionProtocolError
	}
	s, e := owneddata.Time(r.SampledAt, true)
	d, de := owneddata.Time(r.SessionDeadline, true)
	if e != nil || de != nil || !d.Equal(s.Add(executionSessionLifetime)) || now.Before(s) {
		return a, time.Time{}, executionSessionProtocolError
	}
	var m assessment.ManifestV2
	if x.DecodeClosed(executionSessionManifestSource, &m, 65536) != nil {
		return a, time.Time{}, executionSessionProtocolError
	}
	m.ProjectID = r.ProjectID
	m.ExpiresAt = s.Add(time.Hour).UTC().Truncate(time.Second).Format(owneddata.SecondUTC)
	if x.Digest(m) != r.ManifestSHA256 {
		return a, time.Time{}, executionSessionProtocolError
	}
	c := executionSessionConfig{StateRoot: filepath.Join(r.ScratchParent, executionSeedTargetName, "state"), DataRoot: filepath.Join(r.ScratchParent, executionSeedTargetName, "data"), WaypointAuthorityID: r.WaypointAuthorityID, ScopeID: r.ScopeID, OwnerEpoch: r.OwnerEpoch, AuthorizationExpiry: r.AuthorizationExpiry, ProjectExpiry: r.ProjectExpiry, PolicyExpiry: r.PolicyExpiry, Manifest: m}
	if c.bindSessionDeadline(s) != nil || c.SessionDeadline != r.SessionDeadline {
		return a, time.Time{}, executionSessionProtocolError
	}
	_, scope, e := c.grants(now)
	if e != nil || !scope.After(now) {
		return a, time.Time{}, executionSessionProtocolError
	}
	end := executionSessionMinDeadline(s.Add(10*time.Second), d)
	for _, text := range []string{c.AuthorizationExpiry, c.ProjectExpiry, c.PolicyExpiry, c.ScopeExpiresAt, c.Manifest.ExpiresAt} {
		cap, e := owneddata.Time(text, false)
		if e != nil {
			return a, time.Time{}, executionSessionProtocolError
		}
		end = executionSessionMinDeadline(end, cap)
	}
	if !now.Before(end) {
		return a, end, executionSessionProtocolError
	}
	_, b := executionSessionBindings(c)
	return executionSeedAssets{c, b}, end, nil
}
func executionSeedDecode(raw []byte, path string, now time.Time) (executionSeedRequest, executionSeedAssets, time.Time, error) {
	var r executionSeedRequest
	if len(raw) < 2 || len(raw) > 8192 || raw[len(raw)-1] != '\n' || raw[0] != '{' || raw[len(raw)-2] != '}' || bytes.IndexByte(raw[:len(raw)-1], '\n') >= 0 || x.DecodeClosed(raw[:len(raw)-1], &r, 8191) != nil || path != filepath.Join(r.ScratchParent, executionSeedRequestName) {
		return r, executionSeedAssets{}, time.Time{}, executionSessionProtocolError
	}
	a, end, e := executionSeedPrepare(r, now)
	return r, a, end, e
}
func executionSeedRequestFor(parent, waypoint, project, scope, owner string, s time.Time, auth, projectEnd, policy time.Time) (executionSeedRequest, error) {
	s = s.UTC().Truncate(time.Microsecond)
	var m assessment.ManifestV2
	if x.DecodeClosed(executionSessionManifestSource, &m, 65536) != nil {
		return executionSeedRequest{}, executionSessionProtocolError
	}
	m.ProjectID = project
	m.ExpiresAt = s.Add(time.Hour).Truncate(time.Second).Format(owneddata.SecondUTC)
	r := executionSeedRequest{executionSeedSchema, parent, waypoint, project, scope, owner, owneddata.Stamp(s), owneddata.Stamp(s.Add(executionSessionLifetime)), auth.UTC().Truncate(time.Second).Format(owneddata.SecondUTC), projectEnd.UTC().Truncate(time.Second).Format(owneddata.SecondUTC), policy.UTC().Truncate(time.Second).Format(owneddata.SecondUTC), x.Digest(m)}
	_, _, e := executionSeedPrepare(r, s)
	return r, e
}
func executionSeedEncode(v any, limit int) ([]byte, error) {
	b, e := assessment.Canonical(v)
	if e != nil || len(b)+1 > limit {
		return nil, executionSessionProtocolError
	}
	return append(b, '\n'), nil
}
func executionSeedResultCode(raw []byte) (string, bool) {
	var r executionSeedResult
	if len(raw) < 2 || len(raw) > 256 || raw[len(raw)-1] != '\n' || raw[0] != '{' || raw[len(raw)-2] != '}' || bytes.IndexByte(raw[:len(raw)-1], '\n') >= 0 || x.DecodeClosed(raw[:len(raw)-1], &r, 255) != nil || r.Schema != executionSeedResultSchema || !owneddata.OneOf(r.Code, "SEEDED", "REFUSED", "HELD") {
		return "unknown", false
	}
	return r.Code, true
}
func executionSeedResultRead(raw []byte) bool {
	code, valid := executionSeedResultCode(raw)
	return valid && code == "SEEDED"
}

// This is only the original test child/Wait lifetime, not a core ownership
// receipt. Its captured negative deadline is never renewed by later teardown.
type executionSeedChild struct {
	cmd      *exec.Cmd
	cancel   context.CancelFunc
	wake     func() error
	deadline time.Time
	done     chan struct{}
	waitErr  error
	exitCode int
}

func executionSeedChildWait(cmd *exec.Cmd, cancel context.CancelFunc, wake func() error, deadline time.Time) *executionSeedChild {
	return executionSeedChildWaitPublication(cmd, cancel, wake, deadline, nil, nil)
}

// The optional TEST-only gate delays publication AFTER the same actual Wait.
// It cannot produce a successful observation, restart the child or extend its
// captured finish deadline. The negative control always releases it.
func executionSeedChildWaitPublication(cmd *exec.Cmd, cancel context.CancelFunc, wake func() error, deadline time.Time, gate <-chan struct{}, returned chan<- struct{}) *executionSeedChild {
	c := &executionSeedChild{cmd: cmd, cancel: cancel, wake: wake, deadline: deadline, done: make(chan struct{}), exitCode: -1}
	go func() {
		c.waitErr = cmd.Wait()
		if cmd.ProcessState != nil {
			c.exitCode = cmd.ProcessState.ExitCode()
		}
		if returned != nil {
			close(returned)
		}
		if gate != nil {
			<-gate
		}
		close(c.done)
	}()
	return c
}
func (c *executionSeedChild) WaitKnown() bool {
	if c == nil {
		return true
	}
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}
func (c *executionSeedChild) Finish() bool {
	if c == nil {
		return true
	}
	c.cancel()
	if c.wake != nil {
		_ = c.wake()
	}
	if c.WaitKnown() {
		return true
	}
	left := time.Until(c.deadline)
	if left <= 0 {
		return false
	}
	timer := time.NewTimer(left)
	defer timer.Stop()
	select {
	case <-c.done:
		return true
	case <-timer.C:
		return false
	}
}

// No fallback on an unsupported platform. The entry owns stdio only and never
// converts an error/exit/complete-looking root into an accepted fixture.
func TestExecutionPrivateSessionSeedHelper(t *testing.T) {
	if os.Getenv("OUTFITTER_PRIVATE_EXECUTION_SESSION_SEED") != "1" {
		return
	}
	code := executionSeedPlatformEntry(os.Getenv("OUTFITTER_PRIVATE_EXECUTION_SESSION_SEED_REQUEST"))
	os.Exit(code)
}

// Seed vectors are inert fixture identities, never runtime input/output hashes.
type executionSeedVector struct {
	Schema            string               `json:"schema"`
	Request           executionSeedRequest `json:"request"`
	ManifestCanonical string               `json:"manifest_canonical"`
	ManifestSHA256    string               `json:"manifest_sha256"`
	BindingCanonical  string               `json:"binding_canonical"`
	BindingSHA256     string               `json:"binding_sha256"`
}

func executionSeedValidateVector(t *testing.T, raw []byte) {
	t.Helper()
	var v executionSeedVector
	if x.DecodeClosed(raw, &v, 16384) != nil || v.Schema != "outfitter.execution-session-seed-vector/v1" {
		t.Fatal("closed seed vector refused")
	}
	s, e := owneddata.Time(v.Request.SampledAt, true)
	if e != nil {
		t.Fatal("seed vector clock")
	}
	a, _, e := executionSeedPrepare(v.Request, s)
	if e != nil {
		t.Fatal("seed vector request")
	}
	m, e := assessment.Canonical(a.Config.Manifest)
	if e != nil || !bytes.Equal(m, []byte(v.ManifestCanonical)) || x.Digest(a.Config.Manifest) != v.ManifestSHA256 || v.Request.ManifestSHA256 != v.ManifestSHA256 {
		t.Fatal("manifest vector bytes differ")
	}
	b, e := assessment.Canonical(a.Binding)
	if e != nil || !bytes.Equal(b, []byte(v.BindingCanonical)) || x.Digest(a.Binding) != v.BindingSHA256 {
		t.Fatal("binding vector bytes differ")
	}
	// Decode the literals independently of the producer's typed values.
	var literalBinding x.ScopeBinding
	if json.Unmarshal([]byte(v.BindingCanonical), &literalBinding) != nil || literalBinding != a.Binding {
		t.Fatal("binding literal correlation")
	}
}

// Content-free identity of the exact prebuilt executable passed to Cmd. This is
// artifact association, not an embedded VCS stamp or compiler attestation.
func executionSeedExecutableIdentity(t *testing.T) {
	t.Helper()
	f, e := os.Open(os.Args[0])
	if e != nil {
		t.Fatal("exact seed executable unavailable")
	}
	before, se := f.Stat()
	if se != nil || !before.Mode().IsRegular() || before.Size() < 1 || before.Size() > 256<<20 {
		_ = f.Close()
		t.Fatal("seed executable shape")
	}
	h := sha256.New()
	n, re := io.Copy(h, io.LimitReader(f, (256<<20)+1))
	after, ae := f.Stat()
	ce := f.Close()
	if re != nil || ae != nil || ce != nil || n != before.Size() || n != after.Size() || !os.SameFile(before, after) || !before.ModTime().Equal(after.ModTime()) {
		t.Fatal("seed executable identity changed or close unknown")
	}
	t.Logf("external seed executable: sha256=%s bytes=%d", hex.EncodeToString(h.Sum(nil)), n)
}
