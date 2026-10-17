package jetstream2

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

// Journal is an integration requirement, not an in-memory budget fallback.
// Admit must atomically commit/reuse a durable native-unit reservation and an
// accepted independent-supervisor assignment before returning Fresh=true.
type Journal interface {
	Admit(context.Context, WorkerRequest) (WorkerRecord, bool, error)
	Save(context.Context, WorkerRecord) error
	Lookup(context.Context, string) (WorkerRecord, error)
}
type WorkerRequest struct {
	RequestID      string `json:"request_id"`
	Owner          string `json:"owner"`
	MaxSeconds     int    `json:"max_seconds"`
	CeilingMilliSU int64  `json:"ceiling_milli_su"`
}
type WorkerRecord struct {
	Request             WorkerRequest `json:"request"`
	ProjectID           string        `json:"project_id"`
	Region              string        `json:"region"`
	ImageID             string        `json:"image_id"`
	FlavorID            string        `json:"flavor_id"`
	ReservationID       string        `json:"reservation_id"`
	ReservedMilliSU     int64         `json:"reserved_milli_su"`
	Deadline            time.Time     `json:"deadline"`
	SupervisorApproved  bool          `json:"supervisor_approved"`
	Name                string        `json:"name"`
	PortID              string        `json:"port_id"`
	ServerID            string        `json:"server_id"`
	Status              string        `json:"status"`
	CleanupVerified     bool          `json:"cleanup_verified"`
	AccountingStatus    string        `json:"accounting_status"`
	UnresolvedLiability bool          `json:"unresolved_liability"`
}

func (c *Client) CreateWorker(ctx context.Context, r WorkerRequest) (WorkerRecord, error) {
	var record WorkerRecord
	if c.journal == nil {
		return record, errors.New("live creation disabled: durable native-unit journal and accepted supervisor are required")
	}
	if !ref.MatchString(r.RequestID) || !ref.MatchString(r.Owner) || r.MaxSeconds < 1 || r.MaxSeconds > 3600 || r.CeilingMilliSU < 1 || r.CeilingMilliSU > 1000000000 {
		return record, errors.New("invalid bounded worker request")
	}
	preflight, e := c.Preflight(ctx)
	if e != nil {
		return record, e
	}
	if !preflight.ResourceChecksPassed {
		return record, errors.New("provider resource checks failed")
	}
	record, fresh, e := c.journal.Admit(ctx, r)
	if e != nil {
		return record, e
	}
	if !fresh {
		return record, nil
	}
	if record.Request != r || record.ReservationID == "" || record.ReservedMilliSU < r.CeilingMilliSU || !record.SupervisorApproved || !record.Deadline.After(time.Now()) || record.Deadline.After(time.Now().Add(time.Duration(r.MaxSeconds+1)*time.Second)) || record.ProjectID != c.config.ProjectID || record.Region != c.config.Region || record.ImageID != c.config.ImageID || record.FlavorID != c.config.FlavorID {
		return record, errors.New("missing, mismatched or expired durable reservation/supervisor permit")
	}
	// Bound all provider mutation calls to the already durable lifetime.
	mutationCtx, cancelMutation := context.WithDeadline(ctx, record.Deadline)
	defer cancelMutation()
	checkDeadline := func() error {
		if mutationCtx.Err() != nil || !time.Now().Before(record.Deadline) {
			return errors.New("durable reservation expired before provider creation")
		}
		return nil
	}
	hash := sha256.Sum256([]byte(r.Owner + ":" + r.RequestID))
	record.Name = "outfitter-" + hex.EncodeToString(hash[:16])
	record.Status = "creating"
	record.AccountingStatus = "reserved_unsettled"
	record.UnresolvedLiability = true
	if e = c.journal.Save(ctx, record); e != nil {
		return record, e
	}
	unknown := func(cause error) (WorkerRecord, error) {
		record.Status = "unknown"
		record.UnresolvedLiability = true
		if saveErr := c.journal.Save(context.Background(), record); saveErr != nil {
			return record, errors.New("provider outcome and durable resource recording unresolved")
		}
		return record, cause
	}
	endpoint, e := c.endpoint("network", "/v2.0/ports")
	if e != nil {
		return unknown(e)
	}
	description := "outfitter:" + r.Owner + ":" + r.RequestID
	var port struct{ Port struct{ ID string } }
	if e = checkDeadline(); e != nil {
		return unknown(e)
	}
	_, e = c.call(mutationCtx, "POST", endpoint, map[string]any{"port": map[string]any{"name": record.Name, "network_id": c.config.NetworkID, "project_id": c.config.ProjectID, "security_groups": []string{c.config.SecurityGroupID}, "admin_state_up": true, "description": description}}, &port)
	if e != nil {
		return unknown(errors.New("port creation response unknown; do not retry creation"))
	}
	if !ref.MatchString(port.Port.ID) {
		return unknown(errors.New("port creation did not return a valid ID"))
	}
	record.PortID = port.Port.ID
	if e = c.journal.Save(ctx, record); e != nil {
		return unknown(e)
	}
	endpoint, e = c.endpoint("compute", "/servers")
	if e != nil {
		return unknown(e)
	}
	var server struct{ Server struct{ ID string } }
	if e = checkDeadline(); e != nil {
		return unknown(e)
	}
	_, e = c.call(mutationCtx, "POST", endpoint, map[string]any{"server": map[string]any{"name": record.Name, "imageRef": c.config.ImageID, "flavorRef": c.config.FlavorID, "networks": []map[string]string{{"port": record.PortID}}, "metadata": map[string]string{"outfitter_owner": r.Owner, "outfitter_request": r.RequestID, "outfitter_deadline": record.Deadline.UTC().Format(time.RFC3339)}}}, &server)
	if e != nil {
		return unknown(errors.New("server creation response unknown; do not retry creation"))
	}
	if !ref.MatchString(server.Server.ID) {
		return unknown(errors.New("server creation did not return a valid ID"))
	}
	record.ServerID = server.Server.ID
	record.Status = "provisioning"
	if e = c.journal.Save(ctx, record); e != nil {
		return unknown(e)
	}
	return record, nil
}
func (c *Client) server(ctx context.Context, r WorkerRecord) (bool, string, error) {
	if !ref.MatchString(r.ServerID) {
		return false, "", errors.New("missing recorded server ID")
	}
	var result struct {
		Server struct {
			ID       string
			Status   string
			TenantID string `json:"tenant_id"`
			Metadata map[string]string
			Volumes  []any `json:"os-extended-volumes:volumes_attached"`
		}
	}
	e := c.get(ctx, "compute", "/servers/"+r.ServerID, &result)
	var api apiError
	if errors.As(e, &api) && api.Status == 404 {
		return false, "deleted", nil
	}
	if e != nil {
		return false, "", e
	}
	s := result.Server
	if s.ID != r.ServerID || s.TenantID != c.config.ProjectID || s.Metadata["outfitter_owner"] != r.Request.Owner || s.Metadata["outfitter_request"] != r.Request.RequestID {
		return true, "", errors.New("server ownership mismatch")
	}
	if len(s.Volumes) > 0 {
		return true, "", errors.New("unexpected attached storage; manual inventory required")
	}
	return true, s.Status, nil
}
func (c *Client) InspectWorker(ctx context.Context, r WorkerRecord) (string, error) {
	exists, status, e := c.server(ctx, r)
	if e != nil {
		return "unknown", e
	}
	if !exists {
		return "deleted", nil
	}
	return status, nil
}
func (c *Client) TeardownWorker(ctx context.Context, r WorkerRecord) (WorkerRecord, error) {
	if c.journal == nil || r.ReservationID == "" || r.ProjectID != c.config.ProjectID {
		return r, errors.New("owned journal record required for teardown")
	}
	stored, e := c.journal.Lookup(ctx, r.Request.RequestID)
	if e != nil || stored.ServerID != r.ServerID || stored.PortID != r.PortID || stored.Request != r.Request || stored.ReservationID != r.ReservationID {
		return r, errors.New("resource IDs do not match durable journal")
	}
	r = stored
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	fail := func(e error) (WorkerRecord, error) {
		r.CleanupVerified = false
		r.UnresolvedLiability = true
		r.Status = "unknown"
		if saveErr := c.journal.Save(context.Background(), r); saveErr != nil {
			return r, errors.New("cleanup and durable recording unresolved")
		}
		return r, e
	}
	if r.ServerID == "" {
		return fail(errors.New("server creation outcome unknown; inventory exact request before teardown"))
	}
	exists, _, e := c.server(ctx, r)
	if e != nil {
		return fail(e)
	}
	if exists {
		endpoint, _ := c.endpoint("compute", "/servers/"+r.ServerID)
		if _, e = c.call(ctx, "DELETE", endpoint, nil, nil); e != nil {
			return fail(e)
		}
		for {
			exists, _, e = c.server(ctx, r)
			if e != nil {
				return fail(e)
			}
			if !exists {
				break
			}
			select {
			case <-ctx.Done():
				return fail(errors.New("server deletion not confirmed"))
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	if !ref.MatchString(r.PortID) {
		return fail(errors.New("missing recorded port ID"))
	}
	var port struct {
		Port struct {
			ID          string
			ProjectID   string `json:"project_id"`
			NetworkID   string `json:"network_id"`
			Description string
		}
	}
	e = c.get(ctx, "network", "/v2.0/ports/"+r.PortID, &port)
	var api apiError
	if e != nil && !(errors.As(e, &api) && api.Status == 404) {
		return fail(e)
	}
	if e == nil {
		p := port.Port
		if p.ID != r.PortID || p.ProjectID != c.config.ProjectID || p.NetworkID != c.config.NetworkID || p.Description != "outfitter:"+r.Request.Owner+":"+r.Request.RequestID {
			return fail(errors.New("port ownership mismatch"))
		}
		endpoint, _ := c.endpoint("network", "/v2.0/ports/"+r.PortID)
		if _, e = c.call(ctx, "DELETE", endpoint, nil, nil); e != nil {
			return fail(e)
		}
		e = c.get(ctx, "network", "/v2.0/ports/"+r.PortID, &port)
		if !errors.As(e, &api) || api.Status != 404 {
			return fail(fmt.Errorf("port absence not confirmed"))
		}
	}
	r.Status = "deleted"
	r.CleanupVerified = true
	r.UnresolvedLiability = true
	r.AccountingStatus = "reserved_unsettled"
	// Financial settlement belongs to the native-unit journal, never a deletion response.
	if e = c.journal.Save(ctx, r); e != nil {
		return fail(e)
	}
	return r, nil
}
