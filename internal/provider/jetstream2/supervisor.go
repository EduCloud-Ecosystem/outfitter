package jetstream2

import (
	"context"
	"errors"
	"time"
)

type CleanupResult struct {
	RequestID string       `json:"request_id"`
	Record    WorkerRecord `json:"record"`
	Error     string       `json:"error,omitempty"`
}

// Reap is a single bounded sweep; an external service invokes it periodically.
// It does not start guests, create resources or depend on the original caller.
func (c *Client) Reap(ctx context.Context) ([]CleanupResult, error) {
	j, ok := c.journal.(interface {
		Pending(context.Context) ([]WorkerRecord, error)
	})
	if !ok {
		return nil, errors.New("durable supervisor journal required")
	}
	pending, e := j.Pending(ctx)
	if e != nil {
		return nil, e
	}
	results := []CleanupResult{}
	for _, r := range pending {
		if time.Now().Before(r.Deadline) {
			continue
		}
		next, e := c.ReconcileExpired(ctx, r.Request.RequestID)
		result := CleanupResult{RequestID: r.Request.RequestID, Record: next}
		if e != nil {
			result.Error = e.Error()
		}
		results = append(results, result)
	}
	return results, nil
}
func (c *Client) ReconcileExpired(ctx context.Context, id string) (WorkerRecord, error) {
	var r WorkerRecord
	if c.journal == nil {
		return r, errors.New("durable journal required")
	}
	release, e := c.lockWorker(ctx, id)
	if e != nil {
		return r, e
	}
	defer release()
	r, e = c.journal.Lookup(ctx, id)
	if e != nil {
		return r, e
	}
	if r.AccountingStatus == "provider_settled" {
		return r, nil
	}
	if time.Now().Before(r.Deadline) {
		return r, errors.New("request has not expired")
	}
	if r.ConfigHash != digest(c.config) {
		return r, errors.New("supervisor configuration mismatch")
	}
	// A process can die after POST and before saving the returned ID or ambiguity.
	r.CreateAmbiguous = r.CreateAmbiguous || (r.PortAttempted && r.PortID == "") || (r.ServerAttempted && r.ServerID == "")
	r.CleanupVerified = false
	if e = c.journal.Save(ctx, r); e != nil {
		return r, e
	}
	return c.teardownWorker(ctx, r)
}

type inventoryPort struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	ProjectID   string `json:"project_id"`
	NetworkID   string `json:"network_id"`
	Description string `json:"description"`
	DeviceID    string `json:"device_id"`
}
type inventoryServer struct {
	ID       string            `json:"id"`
	Name     string            `json:"name"`
	TenantID string            `json:"tenant_id"`
	Metadata map[string]string `json:"metadata"`
	Image    struct {
		ID string `json:"id"`
	} `json:"image"`
	Flavor struct {
		ID string `json:"id"`
	} `json:"flavor"`
	Volumes []any `json:"os-extended-volumes:volumes_attached"`
}

func (c *Client) discover(ctx context.Context, r WorkerRecord) (WorkerRecord, error) {
	var ports struct {
		Ports []inventoryPort `json:"ports"`
		Links []any           `json:"ports_links"`
	}
	var servers struct {
		Servers []inventoryServer `json:"servers"`
		Links   []any             `json:"servers_links"`
	}
	if e := c.get(ctx, "network", "/v2.0/ports", &ports); e != nil {
		return r, e
	}
	if e := c.get(ctx, "compute", "/servers/detail", &servers); e != nil {
		return r, e
	}
	if ports.Ports == nil || servers.Servers == nil {
		return r, errors.New("incomplete inventory response")
	}
	if len(ports.Links) > 0 || len(servers.Links) > 0 || len(ports.Ports) > 1000 || len(servers.Servers) > 1000 {
		return r, errors.New("incomplete or oversized inventory; cleanup refused")
	}
	var ps []inventoryPort
	var ss []inventoryServer
	for _, p := range ports.Ports {
		if p.Name != r.Name && p.ID != r.PortID && p.Description != portDescription(r) {
			continue
		}
		if !ref.MatchString(p.ID) || p.ProjectID != r.ProjectID || p.NetworkID != c.config.NetworkID || p.Description != portDescription(r) || p.Name != r.Name {
			return r, errors.New("discovered port ownership mismatch")
		}
		ps = append(ps, p)
	}
	for _, s := range servers.Servers {
		if s.Name != r.Name && s.ID != r.ServerID && s.Metadata["outfitter_reservation"] != r.ReservationID {
			continue
		}
		if !ref.MatchString(s.ID) || s.TenantID != r.ProjectID || s.Metadata["outfitter_owner"] != r.Request.Owner || s.Metadata["outfitter_request"] != r.Request.RequestID || s.Metadata["outfitter_reservation"] != r.ReservationID || s.Name != r.Name || s.Image.ID != r.ImageID || s.Flavor.ID != r.FlavorID || len(s.Volumes) > 0 {
			return r, errors.New("discovered server ownership or storage mismatch")
		}
		ss = append(ss, s)
	}
	if len(ps) > 1 || len(ss) > 1 {
		return r, errors.New("duplicate provider effects; manual inventory required")
	}
	if len(ps) == 1 {
		if !r.PortAttempted || (r.PortID != "" && r.PortID != ps[0].ID) {
			return r, errors.New("port not corroborated by durable attempt")
		}
		r.PortID = ps[0].ID
	}
	if len(ss) == 1 {
		if !r.ServerAttempted || (r.ServerID != "" && r.ServerID != ss[0].ID) {
			return r, errors.New("server not corroborated by durable attempt")
		}
		r.ServerID = ss[0].ID
	}
	if len(ps) == 1 && ps[0].DeviceID != "" && ps[0].DeviceID != r.ServerID {
		return r, errors.New("owned port attached to an unrecorded server")
	}
	if r.ServerAttempted && r.ServerID == "" {
		return r, errors.New("server creation remains ambiguous; later inventory required")
	}
	return r, nil
}
