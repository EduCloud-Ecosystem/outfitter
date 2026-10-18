package jetstream2

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"net/http"
	"time"
)

// OfflineRescueObserver owns only signed scope and observational files. It never
// receives A's ledger. It cannot be constructed with a default/live transport.
type OfflineRescueObserver struct {
	store  *rescueStore
	client *Client
}

func NewOfflineRescueObserver(dir string, create bool, p RescuePolicy, c Config, key ed25519.PrivateKey, t http.RoundTripper) (*OfflineRescueObserver, error) {
	client, e := newRescueClient(c, t, nil, true)
	if e != nil {
		return nil, e
	}
	store, e := openRescueStore(dir, create, p, c, key)
	if e != nil {
		return nil, e
	}
	return &OfflineRescueObserver{store, client}, nil
}
func (b *OfflineRescueObserver) Close() error { return b.store.root.Close() }
func (b *OfflineRescueObserver) Authenticate(ctx context.Context, c Credentials) error {
	return b.client.Authenticate(ctx, c)
}
func (b *OfflineRescueObserver) Accept(ctx context.Context, scope []byte) ([]byte, error) {
	return b.store.accept(ctx, scope)
}

type rescuePort struct {
	ID             string   `json:"id"`
	Name           string   `json:"name"`
	ProjectID      string   `json:"project_id"`
	NetworkID      string   `json:"network_id"`
	Description    string   `json:"description"`
	DeviceID       *string  `json:"device_id"`
	SecurityGroups []string `json:"security_groups"`
}
type rescueServer struct {
	ID        string            `json:"id"`
	Name      string            `json:"name"`
	ProjectID string            `json:"tenant_id"`
	Metadata  map[string]string `json:"metadata"`
	Image     struct {
		ID string `json:"id"`
	} `json:"image"`
	Flavor struct {
		ID string `json:"id"`
	} `json:"flavor"`
	Volumes []any `json:"os-extended-volumes:volumes_attached"`
}

func rescueDescription(s RescueScope) string {
	return "outfitter:" + s.Request.Owner + ":" + s.Request.RequestID + ":" + s.ReservationID
}
func (p rescuePort) validate(s RescueScope, portID, serverID string) error {
	if !ref.MatchString(p.ID) || (portID != "" && p.ID != portID) || p.Name != s.Name || p.ProjectID != s.Config.ProjectID || p.NetworkID != s.Config.NetworkID || p.Description != rescueDescription(s) || p.DeviceID == nil || (*p.DeviceID != "" && *p.DeviceID != serverID) || len(p.SecurityGroups) != 1 || p.SecurityGroups[0] != s.Config.SecurityGroupID {
		return errors.New("rescue port ownership or required fields mismatch")
	}
	return nil
}
func (v rescueServer) validate(s RescueScope, serverID string) error {
	if s.Phase != "server" || !ref.MatchString(v.ID) || (serverID != "" && v.ID != serverID) || v.Name != s.Name || v.ProjectID != s.Config.ProjectID || v.Image.ID != s.Config.ImageID || v.Flavor.ID != s.Config.FlavorID || v.Metadata["outfitter_owner"] != s.Request.Owner || v.Metadata["outfitter_request"] != s.Request.RequestID || v.Metadata["outfitter_reservation"] != s.ReservationID || v.Metadata["outfitter_deadline"] != s.Deadline.UTC().Format(time.RFC3339) || v.Volumes == nil || len(v.Volumes) != 0 {
		return errors.New("rescue server ownership or required fields mismatch")
	}
	return nil
}

func (b *OfflineRescueObserver) discover(ctx context.Context, st rescueState) (string, string, error) {
	var ps struct {
		Ports []rescuePort `json:"ports"`
		Links []any        `json:"ports_links"`
	}
	var ss struct {
		Servers []rescueServer `json:"servers"`
		Links   []any          `json:"servers_links"`
	}
	if e := b.client.get(ctx, "network", "/v2.0/ports", &ps); e != nil {
		return "", "", e
	}
	if e := b.client.get(ctx, "compute", "/servers/detail", &ss); e != nil {
		return "", "", e
	}
	if ps.Ports == nil || ss.Servers == nil || len(ps.Links) > 0 || len(ss.Links) > 0 || len(ps.Ports) > 1000 || len(ss.Servers) > 1000 {
		return "", "", errors.New("incomplete rescue inventory")
	}
	s := *st.port
	portID, serverID := st.knownPort, st.knownServer
	if st.server != nil {
		if portID != "" && portID != st.server.PortID {
			return "", "", errors.New("known port conflict")
		}
		portID = st.server.PortID
	}
	serverCount, portCount := 0, 0
	for _, v := range ss.Servers {
		if v.ID != serverID && v.Name != s.Name && v.Metadata["outfitter_reservation"] != s.ReservationID {
			continue
		}
		if st.server == nil {
			return "", "", errors.New("server outside port-only scope")
		}
		if e := v.validate(*st.server, serverID); e != nil {
			return "", "", e
		}
		serverCount++
		serverID = v.ID
	}
	for _, p := range ps.Ports {
		if p.ID != portID && p.Name != s.Name && p.Description != rescueDescription(s) {
			continue
		}
		if e := p.validate(s, portID, serverID); e != nil {
			return "", "", e
		}
		portCount++
		portID = p.ID
	}
	if serverCount > 1 || portCount > 1 {
		return "", "", errors.New("duplicate rescue effects")
	}
	return portID, serverID, nil
}
func (b *OfflineRescueObserver) serverPresent(ctx context.Context, s RescueScope, id string) (bool, error) {
	var response struct {
		Server rescueServer `json:"server"`
	}
	e := b.client.get(ctx, "compute", "/servers/"+id, &response)
	var api apiError
	if errors.As(e, &api) && api.Status == 404 {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	return true, response.Server.validate(s, id)
}
func (b *OfflineRescueObserver) portPresent(ctx context.Context, s RescueScope, id, serverID string) (bool, error) {
	var response struct {
		Port rescuePort `json:"port"`
	}
	e := b.client.get(ctx, "network", "/v2.0/ports/"+id, &response)
	var api apiError
	if errors.As(e, &api) && api.Status == 404 {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	return true, response.Port.validate(s, id, serverID)
}
func (b *OfflineRescueObserver) remove(ctx context.Context, service, path string) error {
	ep, e := b.client.endpoint(service, path)
	if e != nil {
		return e
	}
	_, e = b.client.call(ctx, "DELETE", ep, nil, nil)
	return e
}
func (b *OfflineRescueObserver) cleanup(ctx context.Context, st rescueState, pid, sid string, intentAt time.Time) error {
	checkClock := func() error {
		now := b.store.now()
		if now.Before(intentAt) || now.Before(st.port.Deadline.Add(rescueClockTolerance)) {
			return errors.New("observer clock changed before deletion")
		}
		return nil
	}
	if e := checkClock(); e != nil {
		return e
	}
	if sid != "" {
		if st.server == nil {
			return errors.New("missing server phase")
		}
		present, e := b.serverPresent(ctx, *st.server, sid)
		if e != nil {
			return e
		}
		if present {
			if e = checkClock(); e != nil {
				return e
			}
			if e = b.remove(ctx, "compute", "/servers/"+sid); e != nil {
				return e
			}
			present, e = b.serverPresent(ctx, *st.server, sid)
			if e != nil {
				return e
			}
			if present {
				return errors.New("server absence not observed")
			}
		}
	}
	if pid != "" {
		present, e := b.portPresent(ctx, *st.port, pid, sid)
		if e != nil {
			return e
		}
		if present {
			if st.server != nil && sid == "" {
				return errors.New("unknown server effect retains owned port")
			}
			if e = checkClock(); e != nil {
				return e
			}
			if e = b.remove(ctx, "network", "/v2.0/ports/"+pid); e != nil {
				return e
			}
			present, e = b.portPresent(ctx, *st.port, pid, sid)
			if e != nil {
				return e
			}
			if present {
				return errors.New("port absence not observed")
			}
		}
	}
	return nil
}

// Sweep always retains scopes. OBSERVED_ABSENT is time-bounded and never an
// accounting or terminal claim. There is no watch-window completion transition.
func (b *OfflineRescueObserver) Sweep(ctx context.Context) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var signed []byte
	e := b.store.lock(ctx, func() error {
		st, e := b.store.load()
		if e != nil {
			return e
		}
		if st.port == nil {
			return errors.New("no retained rescue scope")
		}
		if b.store.now().Before(st.port.Deadline.Add(rescueClockTolerance)) {
			return errors.New("cleanup expiry or clock tolerance not reached")
		}
		if st.sequence >= rescueMaxSweeps {
			return errors.New("observation capacity exhausted; unresolved scopes retained")
		}
		pid, sid, discoveryErr := b.discover(ctx, st)
		if now := b.store.now(); now.Before(st.latest) || now.Before(st.port.Deadline.Add(rescueClockTolerance)) {
			return errors.New("observer clock changed during discovery")
		}
		r := RescueReceipt{Version: 1, ObserverID: b.store.policy.ObserverID, ReservationID: st.port.ReservationID, PortScopeHash: rescueHash(st.portBytes), Sequence: st.sequence + 1, At: b.store.now(), Outcome: "SWEEP_INTENT", PortID: pid, ServerID: sid}
		if st.server != nil {
			r.ServerScopeHash = rescueHash(st.serverBytes)
		}
		intent, e := rescueSign("receipt", r, b.store.key)
		if e != nil {
			return e
		}
		if e = b.store.writeOnce(fmt.Sprintf("obs-%03d.intent", r.Sequence), intent); e != nil {
			return e
		}
		if e = b.store.fail("before-cleanup"); e != nil {
			return e
		}
		cleanupErr := discoveryErr
		if cleanupErr == nil {
			cleanupErr = b.cleanup(ctx, st, pid, sid, r.At)
		}
		r.Outcome = "OBSERVED_ABSENT"
		if cleanupErr != nil {
			r.Outcome = "UNKNOWN"
		}
		now := b.store.now()
		if now.Before(r.At) {
			return errors.New("observer clock rollback during sweep")
		}
		r.At = now
		candidate, e := rescueSign("receipt", r, b.store.key)
		if e != nil {
			return e
		}
		if e = b.store.writeOnce(fmt.Sprintf("obs-%03d.receipt", r.Sequence), candidate); e != nil {
			return e
		}
		signed = candidate
		return cleanupErr
	})
	return signed, e
}
