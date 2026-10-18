package jetstream2

// This protocol is deliberately unavailable from the CLI. Constructors require
// injected loopback fixtures; signatures are not live-host or IAM qualification.
import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const OfflineRescueMode = "offline-rescue-v1"
const rescueMaxBytes = 32768
const rescueClockTolerance = time.Second

type RescuePolicy struct {
	Mode        string `json:"mode"`
	AuthorityID string `json:"authority_id"`
	ObserverID  string `json:"observer_id"`
	SourceKey   string `json:"source_key"`
	ObserverKey string `json:"observer_key"`
}

func (p RescuePolicy) validate(c Config) error {
	if p.Mode != OfflineRescueMode || !ref.MatchString(p.AuthorityID) || !ref.MatchString(p.ObserverID) {
		return errors.New("invalid offline cleanup policy")
	}
	if _, e := rescueKey(p.SourceKey); e != nil {
		return e
	}
	if _, e := rescueKey(p.ObserverKey); e != nil {
		return e
	}
	if p.SourceKey == p.ObserverKey {
		return errors.New("source and observer require distinct trust keys")
	}
	_, e := loopbackOrigin(c.AuthURL)
	return e
}

func rescueKey(s string) (ed25519.PublicKey, error) {
	b, e := base64.StdEncoding.DecodeString(s)
	if e != nil || len(b) != ed25519.PublicKeySize {
		return nil, errors.New("invalid separately provisioned trust key")
	}
	return ed25519.PublicKey(b), nil
}

type RescueRecord struct {
	Policy      RescuePolicy `json:"policy"`
	PortScope   string       `json:"port_scope,omitempty"`
	PortACK     string       `json:"port_ack,omitempty"`
	ServerScope string       `json:"server_scope,omitempty"`
	ServerACK   string       `json:"server_ack,omitempty"`
}

type RescueScope struct {
	Version       int           `json:"version"`
	Phase         string        `json:"phase"`
	Policy        RescuePolicy  `json:"policy"`
	AccountID     string        `json:"account_id"`
	Request       WorkerRequest `json:"request"`
	RequestHash   string        `json:"request_hash"`
	Config        Config        `json:"config"`
	ConfigHash    string        `json:"config_hash"`
	ReservationID string        `json:"reservation_id"`
	CreatedAt     time.Time     `json:"created_at"`
	Deadline      time.Time     `json:"deadline"`
	Name          string        `json:"name"`
	PortID        string        `json:"port_id"`
	ParentHash    string        `json:"parent_hash"`
}

type RescueACK struct {
	Version       int       `json:"version"`
	ObserverID    string    `json:"observer_id"`
	ReservationID string    `json:"reservation_id"`
	Phase         string    `json:"phase"`
	ScopeHash     string    `json:"scope_hash"`
	ParentHash    string    `json:"parent_hash"`
	At            time.Time `json:"at"`
}

// Receipt validation is observational only. There is intentionally no ingestion
// API, ledger pointer, settlement, activation or cleanup-complete outcome.
type RescueReceipt struct {
	Version         int       `json:"version"`
	ObserverID      string    `json:"observer_id"`
	ReservationID   string    `json:"reservation_id"`
	PortScopeHash   string    `json:"port_scope_hash"`
	ServerScopeHash string    `json:"server_scope_hash"`
	Sequence        int       `json:"sequence"`
	At              time.Time `json:"at"`
	Outcome         string    `json:"outcome"`
	PortID          string    `json:"port_id"`
	ServerID        string    `json:"server_id"`
}

type rescueEnvelope struct {
	Kind      string `json:"kind"`
	Payload   []byte `json:"payload"`
	Signature []byte `json:"signature"`
}

func rescueHash(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func rescueMessage(kind string, payload []byte) []byte {
	return append([]byte("outfitter-offline-rescue-v1\x00"+kind+"\x00"), payload...)
}
func rescueSign(kind string, value any, key ed25519.PrivateKey) ([]byte, error) {
	if len(key) != ed25519.PrivateKeySize {
		return nil, errors.New("fixture signing key required")
	}
	p, e := json.Marshal(value)
	if e != nil {
		return nil, e
	}
	b, e := json.Marshal(rescueEnvelope{kind, p, ed25519.Sign(key, rescueMessage(kind, p))})
	if len(b) > rescueMaxBytes {
		return nil, errors.New("rescue envelope too large")
	}
	return b, e
}
func rescueCanonical(b []byte, value any) error {
	if len(b) == 0 || len(b) > rescueMaxBytes {
		return errors.New("invalid rescue object size")
	}
	if e := strict(b, value); e != nil {
		return e
	}
	canonical, e := json.Marshal(value)
	if e != nil || !bytes.Equal(b, canonical) {
		return errors.New("noncanonical rescue object")
	}
	return nil
}
func rescueVerify(b []byte, kind string, public string, value any) error {
	var env rescueEnvelope
	if e := rescueCanonical(b, &env); e != nil {
		return e
	}
	key, e := rescueKey(public)
	if e != nil {
		return e
	}
	if env.Kind != kind || !ed25519.Verify(key, rescueMessage(kind, env.Payload), env.Signature) {
		return errors.New("rescue signature or kind mismatch")
	}
	return rescueCanonical(env.Payload, value)
}
func (s RescueScope) validate(p RescuePolicy, c Config) error {
	if s.Version != 1 || s.Policy != p || s.Config != c || s.ConfigHash != digest(c) || s.AccountID != accountID(c) || s.RequestHash != digest(s.Request) {
		return errors.New("rescue scope binding mismatch")
	}
	if !ref.MatchString(s.Request.RequestID) || !ref.MatchString(s.Request.Owner) || s.Request.MaxSeconds < 1 || s.Request.MaxSeconds > 3600 || s.Request.CeilingMilliSU < 1 || s.Request.CeilingMilliSU > 1000000000 || s.CreatedAt.IsZero() || !s.Deadline.Equal(s.CreatedAt.Add(time.Duration(s.Request.MaxSeconds)*time.Second)) {
		return errors.New("invalid finite rescue scope")
	}
	b, e := hex.DecodeString(s.ReservationID)
	h := sha256.Sum256([]byte(s.Request.Owner + ":" + s.Request.RequestID))
	if e != nil || len(b) != 16 || s.ReservationID != hex.EncodeToString(b) || s.Name != "outfitter-"+hex.EncodeToString(h[:16]) {
		return errors.New("invalid rescue identity")
	}
	if (s.Phase == "port" && (s.PortID != "" || s.ParentHash != "")) || (s.Phase == "server" && (!ref.MatchString(s.PortID) || len(s.ParentHash) != 64)) || (s.Phase != "port" && s.Phase != "server") {
		return errors.New("invalid rescue phase")
	}
	return nil
}
func scopeFor(c Config, r WorkerRecord, phase string) RescueScope {
	s := RescueScope{Version: 1, Phase: phase, Policy: r.Rescue.Policy, AccountID: accountID(c), Request: r.Request, RequestHash: digest(r.Request), Config: c, ConfigHash: digest(c), ReservationID: r.ReservationID, CreatedAt: r.CreatedAt, Deadline: r.Deadline, Name: r.Name}
	if phase == "server" {
		s.PortID = r.PortID
		s.ParentHash = rescueHash([]byte(r.Rescue.PortScope))
	}
	return s
}
func verifyACK(raw []byte, scope []byte, s RescueScope, now time.Time, live bool) error {
	var a RescueACK
	if e := rescueVerify(raw, "ack", s.Policy.ObserverKey, &a); e != nil {
		return e
	}
	if a.Version != 1 || a.ObserverID != s.Policy.ObserverID || a.ReservationID != s.ReservationID || a.Phase != s.Phase || a.ScopeHash != rescueHash(scope) || a.ParentHash != s.ParentHash || a.At.Before(s.CreatedAt.Add(-rescueClockTolerance)) || !a.At.Before(s.Deadline) {
		return errors.New("ACK binding or expiry mismatch")
	}
	if live && (now.Sub(a.At) > rescueClockTolerance || a.At.Sub(now) > rescueClockTolerance || !now.Before(s.Deadline)) {
		return errors.New("ACK clock disagreement or expiry")
	}
	return nil
}

// RescueSender has no default network path. Exchange is a fixture handoff to B.
type RescueSender struct {
	Key      ed25519.PrivateKey
	Exchange func(context.Context, []byte) ([]byte, error)
}

func loopbackOrigin(raw string) (string, error) {
	if e := httpsURL(raw); e != nil {
		return "", e
	}
	u, _ := url.Parse(raw)
	ip := net.ParseIP(u.Hostname())
	if ip == nil || !ip.IsLoopback() {
		return "", errors.New("offline rescue requires a literal loopback HTTPS origin")
	}
	return u.Scheme + "://" + u.Host, nil
}

type rescueTransport struct {
	next     http.RoundTripper
	origin   string
	observer bool
	authPath string
}

func (t rescueTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	origin, e := loopbackOrigin(r.URL.String())
	if e != nil || origin != t.origin {
		return nil, errors.New("offline rescue transport refused endpoint")
	}
	if t.observer && r.Method != "GET" && r.Method != "DELETE" && !(r.Method == "POST" && r.URL.Path == t.authPath) {
		return nil, errors.New("observer resource mutation refused")
	}
	return t.next.RoundTrip(r)
}
func newRescueClient(c Config, t http.RoundTripper, j Journal, observer bool) (*Client, error) {
	if t == nil || t == http.DefaultTransport {
		return nil, errors.New("explicit fixture transport required")
	}
	if transport, ok := t.(*http.Transport); ok && transport.Proxy != nil {
		return nil, errors.New("offline fixture transport cannot discover or use proxies")
	}
	origin, e := loopbackOrigin(c.AuthURL)
	if e != nil {
		return nil, e
	}
	u, _ := url.Parse(c.AuthURL)
	client, e := newClient(c, rescueTransport{t, origin, observer, strings.TrimRight(u.Path, "/") + "/auth/tokens"}, j)
	if e == nil {
		client.offlineRescue = true
	}
	return client, e
}
func NewOfflineRescueCreator(c Config, t http.RoundTripper, j *DurableJournal, sender *RescueSender) (*Client, error) {
	if j == nil || j.policy.Rescue == nil || sender == nil || sender.Exchange == nil || len(sender.Key) != ed25519.PrivateKeySize {
		return nil, errors.New("offline rescue journal and sender required")
	}
	if base64.StdEncoding.EncodeToString(sender.Key.Public().(ed25519.PublicKey)) != j.policy.Rescue.SourceKey {
		return nil, errors.New("source signing key mismatch")
	}
	client, e := newRescueClient(c, t, j, false)
	if e == nil {
		client.rescueSender = sender
	}
	return client, e
}

func (c *Client) rescuePhase(ctx context.Context, r *WorkerRecord, phase string) error {
	if r.Rescue == nil {
		return nil
	}
	if !c.offlineRescue || c.rescueSender == nil {
		return errors.New("offline rescue sender missing")
	}
	s := scopeFor(c.config, *r, phase)
	scope, e := rescueSign("scope", s, c.rescueSender.Key)
	if e != nil {
		return e
	}
	if phase == "port" {
		r.Rescue.PortScope = string(scope)
	} else {
		r.Rescue.ServerScope = string(scope)
	}
	if e = c.journal.Save(ctx, *r); e != nil {
		return e
	}
	if e = c.rescuePoint(phase + "-scope-saved"); e != nil {
		return e
	}
	ack, e := c.rescueSender.Exchange(ctx, scope)
	if pointErr := c.rescuePoint(phase + "-exchange-returned"); pointErr != nil {
		return pointErr
	}
	if e != nil {
		return errors.New("rescue durable ACK unavailable")
	}
	if e = verifyACK(ack, scope, s, time.Now().UTC(), true); e != nil {
		return e
	}
	if e = c.rescuePoint(phase + "-ack-before"); e != nil {
		return e
	}
	if phase == "port" {
		r.Rescue.PortACK = string(ack)
	} else {
		r.Rescue.ServerACK = string(ack)
	}
	if e = c.journal.Save(ctx, *r); e != nil {
		return e
	}
	return c.rescuePoint(phase + "-ack-after")
}

func (c *Client) rescuePoint(point string) error {
	if c.offlineRescue && c.rescueFault != nil {
		return c.rescueFault(point)
	}
	return nil
}

func validateRescueRecord(p *RescuePolicy, c Config, r WorkerRecord) error {
	if p == nil {
		if r.Rescue != nil {
			return errors.New("unexpected cleanup assignment")
		}
		return nil
	}
	if r.Rescue == nil || r.Rescue.Policy != *p || r.CleanupVerified {
		return errors.New("immutable rescue assignment or cleanup state mismatch")
	}
	x := r.Rescue
	if (x.PortScope != "" && !r.PortAttempted) || (x.PortACK != "" && x.PortScope == "") || (x.ServerScope != "" && (!r.ServerAttempted || x.PortACK == "")) || (x.ServerACK != "" && x.ServerScope == "") || (r.PortID != "" && x.PortACK == "") || (r.ServerID != "" && x.ServerACK == "") {
		return errors.New("rescue phase prerequisite missing")
	}
	for _, item := range []struct{ phase, scope, ack string }{{"port", x.PortScope, x.PortACK}, {"server", x.ServerScope, x.ServerACK}} {
		if item.scope == "" {
			continue
		}
		var s RescueScope
		if e := rescueVerify([]byte(item.scope), "scope", p.SourceKey, &s); e != nil {
			return e
		}
		if e := s.validate(*p, c); e != nil {
			return e
		}
		if s != scopeFor(c, r, item.phase) {
			return errors.New("signed scope differs from durable request")
		}
		if item.ack != "" {
			if e := verifyACK([]byte(item.ack), []byte(item.scope), s, time.Time{}, false); e != nil {
				return e
			}
		}
	}
	return nil
}
