package jetstream2

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// This journal deliberately models the integration seam only. It is NOT a
// production journal and is never used by the executable.
type memoryJournal struct {
	expireAfterPort bool
	mu              sync.Mutex
	config          Config
	records         map[string]WorkerRecord
	denied          bool
	supervisor      bool
	saveDelay       time.Duration
	saveCount       int
	lifetime        time.Duration
}

func (j *memoryJournal) Admit(_ context.Context, r WorkerRequest) (WorkerRecord, bool, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.denied {
		return WorkerRecord{}, false, errors.New("budget denied")
	}
	if old, ok := j.records[r.RequestID]; ok {
		if old.Request != r {
			return old, false, errors.New("payload collision")
		}
		return old, false, nil
	}
	lifetime := time.Duration(r.MaxSeconds) * time.Second
	if j.lifetime > 0 {
		lifetime = j.lifetime
	}
	v := WorkerRecord{Request: r, ProjectID: j.config.ProjectID, Region: j.config.Region, ImageID: j.config.ImageID, FlavorID: j.config.FlavorID, ReservationID: "reserved-" + r.RequestID, ReservedMilliSU: r.CeilingMilliSU, Deadline: time.Now().Add(lifetime), SupervisorApproved: j.supervisor}
	j.records[r.RequestID] = v
	return v, true, nil
}
func (j *memoryJournal) Save(_ context.Context, r WorkerRecord) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.saveCount++
	if j.saveDelay > 0 {
		time.Sleep(j.saveDelay)
	}
	j.records[r.Request.RequestID] = r
	if j.expireAfterPort && r.PortID != "" && !r.ServerAttempted {
		j.expireAfterPort = false
		if remaining := time.Until(r.Deadline); remaining > 0 {
			time.Sleep(remaining + 10*time.Millisecond)
		}
	}
	return nil
}
func (j *memoryJournal) Lookup(_ context.Context, id string) (WorkerRecord, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	r, ok := j.records[id]
	if !ok {
		return r, errors.New("unknown record")
	}
	return r, nil
}

type apiFixture struct {
	paginated      bool
	missingList    bool
	storage        bool
	mu             sync.Mutex
	url            string
	postCount      int
	deletes        int
	port           bool
	server         bool
	lostServer     bool
	foreign        bool
	wrongProject   bool
	badHash        bool
	quotaFail      bool
	redirect       bool
	portDelay      time.Duration
	portPayload    map[string]any
	serverPayload  map[string]any
	lostPort       bool
	outage         bool
	duplicate      bool
	deleteFail     bool
	deletePortFail bool
	serverBlock    chan struct{}
	created        chan struct{}
}

func (f *apiFixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	write := func(v any) { json.NewEncoder(w).Encode(v) }
	if r.URL.Path == "/v3/auth/tokens" {
		if f.redirect {
			http.Redirect(w, r, f.url+"/leaked", 302)
			return
		}
		project := "project"
		if f.wrongProject {
			project = "foreign"
		}
		w.Header().Set("X-Subject-Token", "private-token-never-output")
		catalog := []any{}
		for _, service := range []string{"compute", "network", "image"} {
			catalog = append(catalog, map[string]any{"type": service, "endpoints": []any{map[string]any{"interface": "public", "region": "region", "url": f.url + "/" + service}}})
		}
		write(map[string]any{"token": map[string]any{"expires_at": time.Now().Add(time.Hour), "project": map[string]string{"id": project}, "catalog": catalog}})
		return
	}
	if r.Header.Get("X-Auth-Token") != "private-token-never-output" {
		w.WriteHeader(401)
		return
	}
	if f.outage {
		w.WriteHeader(503)
		return
	}
	switch r.URL.Path {
	case "/compute/flavors/flavor":
		write(map[string]any{"flavor": map[string]any{"id": "flavor", "vcpus": 2, "ram": 2048, "disk": 20}})
	case "/image/v2/images/image":
		hash := strings.Repeat("a", 64)
		if f.badHash {
			hash = strings.Repeat("b", 64)
		}
		write(map[string]any{"id": "image", "status": "active", "os_hash_algo": "sha256", "os_hash_value": hash})
	case "/network/v2.0/networks/network":
		write(map[string]any{"network": map[string]any{"id": "network", "status": "ACTIVE", "project_id": "project"}})
	case "/network/v2.0/security-groups/group":
		write(map[string]any{"security_group": map[string]any{"id": "group", "project_id": "project"}})
	case "/compute/os-quota-sets/project/detail":
		limit := 100000
		if f.quotaFail {
			limit = 0
		}
		q := map[string]any{}
		for _, name := range []string{"cores", "ram", "instances"} {
			q[name] = map[string]any{"limit": limit, "in_use": 0, "reserved": 0}
		}
		write(map[string]any{"quota_set": q})
	case "/network/v2.0/ports":
		if r.Method == "GET" {
			ps := []any{}
			if f.port {
				p := f.portData()
				ps = append(ps, p)
				if f.duplicate {
					p2 := f.portData()
					p2["id"] = "duplicate"
					ps = append(ps, p2)
				}
			}
			if f.missingList {
				write(map[string]any{})
				return
			}
			if f.paginated {
				write(map[string]any{"ports": ps, "ports_links": []any{map[string]string{"rel": "next", "href": "https://foreign.invalid/steal"}}})
				return
			}
			write(map[string]any{"ports": ps})
			return
		}
		var body struct{ Port map[string]any }
		json.NewDecoder(r.Body).Decode(&body)
		f.portPayload = body.Port
		f.postCount++
		f.port = true
		if f.portDelay > 0 {
			time.Sleep(f.portDelay)
		}
		if f.lostPort {
			w.WriteHeader(503)
			return
		}
		write(map[string]any{"port": map[string]string{"id": "port"}})
	case "/compute/servers":
		if r.Method != "POST" {
			w.WriteHeader(405)
			return
		}
		var body struct{ Server map[string]any }
		json.NewDecoder(r.Body).Decode(&body)
		f.serverPayload = body.Server
		f.postCount++
		f.server = true
		if f.created != nil {
			close(f.created)
			f.created = nil
		}
		if f.serverBlock != nil {
			<-f.serverBlock
		}
		if f.lostServer {
			w.WriteHeader(503)
			write(map[string]string{"error": "sensitive upstream body must never be returned"})
			return
		}
		write(map[string]any{"server": map[string]string{"id": "server"}})
	case "/compute/servers/detail":
		ss := []any{}
		if f.server {
			ss = append(ss, f.serverData())
		}
		write(map[string]any{"servers": ss})
	case "/compute/servers/server":
		if !f.server {
			w.WriteHeader(404)
			return
		}
		if r.Method == "DELETE" {
			if f.deleteFail {
				w.WriteHeader(503)
				return
			}
			f.deletes++
			f.server = false
			w.WriteHeader(204)
			return
		}
		write(map[string]any{"server": f.serverData()})
	case "/network/v2.0/ports/port":
		if !f.port {
			w.WriteHeader(404)
			return
		}
		if r.Method == "DELETE" {
			if f.deletePortFail {
				w.WriteHeader(503)
				return
			}
			if f.deleteFail {
				w.WriteHeader(503)
				return
			}
			f.deletes++
			f.port = false
			w.WriteHeader(204)
			return
		}
		write(map[string]any{"port": f.portData()})
	default:
		w.WriteHeader(404)
	}
}
func (f *apiFixture) portData() map[string]any {
	p := map[string]any{}
	for k, v := range f.portPayload {
		p[k] = v
	}
	p["id"] = "port"
	if f.server {
		p["device_id"] = "server"
	}
	if f.foreign {
		p["description"] = "foreign"
	}
	return p
}
func (f *apiFixture) serverData() map[string]any {
	volumes := []any{}
	if f.storage {
		volumes = append(volumes, map[string]string{"id": "retained-storage"})
	}
	m := map[string]any{}
	for k, v := range f.serverPayload["metadata"].(map[string]any) {
		m[k] = v
	}
	if f.foreign {
		m["outfitter_owner"] = "foreign"
	}
	return map[string]any{"id": "server", "name": f.serverPayload["name"], "tenant_id": "project", "status": "ACTIVE", "metadata": m, "image": map[string]string{"id": "image"}, "flavor": map[string]string{"id": "flavor"}, "os-extended-volumes:volumes_attached": volumes}
}
func setupCloud(t *testing.T) (*Client, *apiFixture, *memoryJournal) {
	t.Helper()
	f := &apiFixture{}
	server := httptest.NewTLSServer(f)
	t.Cleanup(server.Close)
	f.url = server.URL
	config := Config{AuthURL: f.url + "/v3", ProjectID: "project", Region: "region", ImageID: "image", ImageHash: "sha256:" + strings.Repeat("a", 64), FlavorID: "flavor", NetworkID: "network", SecurityGroupID: "group"}
	journal := &memoryJournal{config: config, records: map[string]WorkerRecord{}, supervisor: true}
	client, e := New(config, server.Client().Transport, journal)
	if e != nil {
		t.Fatal(e)
	}
	return client, f, journal
}
func auth(t *testing.T, c *Client) {
	t.Helper()
	if e := c.Authenticate(context.Background(), Credentials{"credential-id", "secret"}); e != nil {
		t.Fatal(e)
	}
}
func TestPreflightUsesOnlyReadsAfterAuthentication(t *testing.T) {
	c, f, _ := setupCloud(t)
	auth(t, c)
	r, e := c.Preflight(context.Background())
	if e != nil || !r.ResourceChecksPassed || r.ProvisioningEnabled || f.postCount != 0 || f.deletes != 0 || len(r.Unknowns) < 4 {
		t.Fatal(r, e)
	}
}
func TestWrongProjectAndRedirectRefused(t *testing.T) {
	for _, mode := range []string{"project", "redirect"} {
		t.Run(mode, func(t *testing.T) {
			c, f, _ := setupCloud(t)
			if mode == "project" {
				f.wrongProject = true
			} else {
				f.redirect = true
			}
			if e := c.Authenticate(context.Background(), Credentials{"id", "secret"}); e == nil {
				t.Fatal("unsafe auth accepted")
			}
		})
	}
}
func TestHashAndQuotaFailuresRemainClosed(t *testing.T) {
	for _, mode := range []string{"hash", "quota"} {
		t.Run(mode, func(t *testing.T) {
			c, f, _ := setupCloud(t)
			auth(t, c)
			if mode == "hash" {
				f.badHash = true
			} else {
				f.quotaFail = true
			}
			r, e := c.Preflight(context.Background())
			if e != nil || r.ResourceChecksPassed {
				t.Fatal(r, e)
			}
		})
	}
}
func TestWorkerLifecycleMockAndIdempotency(t *testing.T) {
	c, f, _ := setupCloud(t)
	auth(t, c)
	request := WorkerRequest{"request", "owner", 60, 100}
	r, e := c.CreateWorker(context.Background(), request)
	if e != nil || r.ServerID != "server" || r.PortID != "port" || f.postCount != 2 {
		t.Fatal(r, e)
	}
	again, e := c.CreateWorker(context.Background(), request)
	if e != nil || again.ServerID != r.ServerID || f.postCount != 2 {
		t.Fatal(again, e)
	}
	status, e := c.InspectWorker(context.Background(), r)
	if e != nil || status != "ACTIVE" {
		t.Fatal(status, e)
	}
	r, e = c.TeardownWorker(context.Background(), r)
	if e != nil || !r.CleanupVerified || !r.UnresolvedLiability || r.AccountingStatus != "reserved_unsettled" || f.deletes != 2 {
		t.Fatal(r, e)
	}
	r, e = c.TeardownWorker(context.Background(), r)
	if e != nil || !r.CleanupVerified || f.deletes != 2 {
		t.Fatal(r, e)
	}
}
func TestMissingBudgetOrSupervisorNeverCreates(t *testing.T) {
	for _, mode := range []string{"journal", "budget", "supervisor"} {
		t.Run(mode, func(t *testing.T) {
			c, f, j := setupCloud(t)
			auth(t, c)
			switch mode {
			case "journal":
				c.journal = nil
			case "budget":
				j.denied = true
			case "supervisor":
				j.supervisor = false
			}
			if _, e := c.CreateWorker(context.Background(), WorkerRequest{"request", "owner", 60, 100}); e == nil {
				t.Fatal("ungated create")
			}
			if f.postCount != 0 {
				t.Fatal("resource created without permit")
			}
		})
	}
}
func TestLostCreateResponseKeepsLiabilityAndNeverRetries(t *testing.T) {
	c, f, _ := setupCloud(t)
	auth(t, c)
	f.lostServer = true
	request := WorkerRequest{"request", "owner", 60, 100}
	r, e := c.CreateWorker(context.Background(), request)
	if e == nil || r.Status != "unknown" || !r.UnresolvedLiability || strings.Contains(e.Error(), "sensitive") {
		t.Fatal(r, e)
	}
	again, e := c.CreateWorker(context.Background(), request)
	if e != nil || again.Status != "unknown" || f.postCount != 2 {
		t.Fatal(again, e)
	}
	r, e = c.TeardownWorker(context.Background(), r)
	if e == nil || r.CleanupVerified || !r.UnresolvedLiability {
		t.Fatal(r, e)
	}
}
func TestForeignWorkerAndUnrecordedIDNotDeleted(t *testing.T) {
	for _, mode := range []string{"foreign", "unrecorded"} {
		t.Run(mode, func(t *testing.T) {
			c, f, _ := setupCloud(t)
			auth(t, c)
			r, e := c.CreateWorker(context.Background(), WorkerRequest{"request", "owner", 60, 100})
			if e != nil {
				t.Fatal(e)
			}
			if mode == "foreign" {
				f.foreign = true
			} else {
				r.ServerID = "other"
			}
			r, e = c.TeardownWorker(context.Background(), r)
			if e == nil || f.deletes != 0 || r.CleanupVerified {
				t.Fatal(r, e)
			}
		})
	}
}
func TestHTTPAndCredentialsInURLRefused(t *testing.T) {
	c, _, _ := setupCloud(t)
	for _, bad := range []string{"http://provider/v3", "https://user:secret@provider/v3"} {
		config := c.config
		config.AuthURL = bad
		if _, e := New(config, nil, nil); e == nil {
			t.Fatal("unsafe URL admitted")
		}
	}
}

func TestExpiredReservationNeverBeginsPortCreation(t *testing.T) {
	c, f, j := setupCloud(t)
	auth(t, c)
	j.lifetime = 200 * time.Millisecond
	j.saveDelay = 250 * time.Millisecond
	record, e := c.CreateWorker(context.Background(), WorkerRequest{"request", "owner", 1, 100})
	if e == nil || f.postCount != 0 || record.Status != "unknown" || !record.UnresolvedLiability {
		t.Fatal(record, e, f.postCount)
	}
}
func TestExpiredReservationAfterRecordedPortNeverBeginsServer(t *testing.T) {
	c, f, j := setupCloud(t)
	auth(t, c)
	j.lifetime = time.Second
	j.expireAfterPort = true
	record, e := c.CreateWorker(context.Background(), WorkerRequest{"request", "owner", 1, 100})
	if e == nil || f.postCount != 1 || record.PortID != "port" || record.ServerID != "" || !record.UnresolvedLiability {
		t.Fatal(record, e, f.postCount)
	}
	persisted, _ := j.Lookup(context.Background(), "request")
	if persisted.PortID != "port" {
		t.Fatal("known port ID lost")
	}
}
func TestDelayedPortResponseIsDeadlineBoundedAndCannotCreateServer(t *testing.T) {
	c, f, j := setupCloud(t)
	auth(t, c)
	j.lifetime = 200 * time.Millisecond
	f.portDelay = 400 * time.Millisecond
	record, e := c.CreateWorker(context.Background(), WorkerRequest{"request", "owner", 1, 100})
	if e == nil || record.Status != "unknown" || !record.UnresolvedLiability {
		t.Fatal(record, e)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.postCount != 1 || f.server {
		t.Fatal("server created after delayed port", f.postCount)
	}
}
