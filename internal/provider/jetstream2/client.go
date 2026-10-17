// Package jetstream2 implements a strict OpenStack API seam, with live creation gated off by default.
package jetstream2

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

type Config struct {
	AuthURL         string `json:"auth_url"`
	ProjectID       string `json:"project_id"`
	Region          string `json:"region"`
	ImageID         string `json:"image_id"`
	ImageHash       string `json:"image_hash"`
	FlavorID        string `json:"flavor_id"`
	NetworkID       string `json:"network_id"`
	SecurityGroupID string `json:"security_group_id"`
}
type Credentials struct{ ID, Secret string }
type Client struct {
	config    Config
	http      *http.Client
	token     string
	endpoints map[string]string
	journal   Journal
}

var ref = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,100}$`)

func (c Config) Validate() error {
	if e := httpsURL(c.AuthURL); e != nil {
		return e
	}
	for _, id := range []string{c.ProjectID, c.Region, c.ImageID, c.FlavorID, c.NetworkID, c.SecurityGroupID} {
		if !ref.MatchString(id) {
			return errors.New("invalid pinned project/region/resource identifier")
		}
	}
	parts := strings.Split(c.ImageHash, ":")
	if len(parts) != 2 || (parts[0] != "sha256" && parts[0] != "sha512") || !regexp.MustCompile(`^[a-f0-9]+$`).MatchString(parts[1]) || (parts[0] == "sha256" && len(parts[1]) != 64) || (parts[0] == "sha512" && len(parts[1]) != 128) {
		return errors.New("image_hash must pin a sha256 or sha512 checksum")
	}
	return nil
}
func httpsURL(raw string) error {
	u, e := url.Parse(raw)
	if e != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("provider endpoint must be an HTTPS URL without credentials, query or fragment")
	}
	return nil
}
func New(config Config, transport http.RoundTripper, journal Journal) (*Client, error) {
	if e := config.Validate(); e != nil {
		return nil, e
	}
	if transport == nil {
		transport = http.DefaultTransport
	}
	return &Client{config: config, http: &http.Client{Transport: transport, Timeout: 20 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("provider redirect refused") }}, endpoints: map[string]string{}, journal: journal}, nil
}

type apiError struct{ Status int }

func (e apiError) Error() string { return fmt.Sprintf("provider API returned HTTP %d", e.Status) }
func (c *Client) call(ctx context.Context, method, endpoint string, body any, out any) (http.Header, error) {
	if e := httpsURL(endpoint); e != nil {
		return nil, e
	}
	var payload []byte
	var e error
	if body != nil {
		payload, e = json.Marshal(body)
		if e != nil {
			return nil, e
		}
	}
	req, e := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(payload))
	if e != nil {
		return nil, errors.New("invalid provider request")
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("X-Auth-Token", c.token)
	}
	res, e := c.http.Do(req)
	if e != nil {
		return nil, errors.New("provider connection failed or timed out")
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return nil, apiError{res.StatusCode}
	}
	if out != nil {
		data, e := io.ReadAll(io.LimitReader(res.Body, (2<<20)+1))
		if e != nil || len(data) > 2<<20 {
			return nil, errors.New("provider response unavailable or oversized")
		}
		if e = json.Unmarshal(data, out); e != nil {
			return nil, errors.New("provider response was not valid JSON")
		}
	}
	return res.Header, nil
}
func (c *Client) Authenticate(ctx context.Context, creds Credentials) error {
	if creds.ID == "" || creds.Secret == "" {
		return errors.New("allocation-specific application credential environment is required")
	}
	var response struct {
		Token struct {
			ExpiresAt time.Time `json:"expires_at"`
			Project   struct{ ID string }
			Catalog   []struct {
				Type      string
				Endpoints []struct {
					Interface string
					Region    string
					URL       string
				}
			}
		}
	}
	body := map[string]any{"auth": map[string]any{"identity": map[string]any{"methods": []string{"application_credential"}, "application_credential": map[string]string{"id": creds.ID, "secret": creds.Secret}}}}
	headers, e := c.call(ctx, "POST", strings.TrimRight(c.config.AuthURL, "/")+"/auth/tokens", body, &response)
	if e != nil {
		return e
	}
	if response.Token.Project.ID != c.config.ProjectID || !response.Token.ExpiresAt.After(time.Now().Add(time.Minute)) {
		return errors.New("token project mismatch or expiring token")
	}
	token := headers.Get("X-Subject-Token")
	if token == "" {
		return errors.New("identity response lacked token")
	}
	endpoints := map[string]string{}
	for _, service := range response.Token.Catalog {
		if service.Type != "compute" && service.Type != "network" && service.Type != "image" {
			continue
		}
		for _, ep := range service.Endpoints {
			if ep.Interface == "public" && ep.Region == c.config.Region {
				if e = httpsURL(ep.URL); e != nil {
					return e
				}
				if endpoints[service.Type] != "" {
					return errors.New("ambiguous service endpoint for selected region")
				}
				endpoints[service.Type] = strings.TrimRight(ep.URL, "/")
			}
		}
	}
	for _, name := range []string{"compute", "network", "image"} {
		if endpoints[name] == "" {
			return errors.New("selected region lacks required public service endpoint")
		}
	}
	c.token = token
	c.endpoints = endpoints
	return nil
}
func (c *Client) endpoint(service, path string) (string, error) {
	base := c.endpoints[service]
	if c.token == "" || base == "" {
		return "", errors.New("authenticate selected project before provider access")
	}
	if service == "image" && strings.HasSuffix(base, "/v2") && strings.HasPrefix(path, "/v2/") {
		path = strings.TrimPrefix(path, "/v2")
	}
	if service == "network" && strings.HasSuffix(base, "/v2.0") && strings.HasPrefix(path, "/v2.0/") {
		path = strings.TrimPrefix(path, "/v2.0")
	}
	return base + path, nil
}
func (c *Client) get(ctx context.Context, service, path string, out any) error {
	endpoint, e := c.endpoint(service, path)
	if e != nil {
		return e
	}
	_, e = c.call(ctx, "GET", endpoint, nil, out)
	return e
}

type Check struct {
	Name   string `json:"name"`
	Passed bool   `json:"passed"`
}
type Preflight struct {
	ProjectID            string   `json:"project_id"`
	Region               string   `json:"region"`
	Checks               []Check  `json:"checks"`
	VCPUs                int      `json:"vcpus"`
	RAMMB                int      `json:"ram_mb"`
	DiskGB               int      `json:"ephemeral_disk_gb"`
	ResourceChecksPassed bool     `json:"resource_checks_passed"`
	ProvisioningEnabled  bool     `json:"provisioning_enabled"`
	Unknowns             []string `json:"unknowns"`
}

func (c *Client) Preflight(ctx context.Context) (Preflight, error) {
	result := Preflight{ProjectID: c.config.ProjectID, Region: c.config.Region, Checks: []Check{}, Unknowns: []string{"allocation balance and resource entitlement unverified", "available host capacity unverified", "native SU charges and dated flavor rate unverified", "durable native-unit reservation integration not accepted", "independent expiry supervisor and live cleanup not accepted"}}
	var flavor struct {
		Flavor struct {
			ID    string
			VCPUs int `json:"vcpus"`
			RAM   int
			Disk  int
		}
	}
	if e := c.get(ctx, "compute", "/flavors/"+c.config.FlavorID, &flavor); e != nil {
		return result, e
	}
	result.VCPUs = flavor.Flavor.VCPUs
	result.RAMMB = flavor.Flavor.RAM
	result.DiskGB = flavor.Flavor.Disk
	result.Checks = append(result.Checks, Check{"pinned finite flavor", flavor.Flavor.ID == c.config.FlavorID && result.VCPUs > 0 && result.VCPUs <= 16 && result.RAMMB > 0 && result.RAMMB <= 65536 && result.DiskGB >= 0 && result.DiskGB <= 200})
	var image struct {
		ID        string
		Status    string
		HashAlgo  string `json:"os_hash_algo"`
		HashValue string `json:"os_hash_value"`
	}
	if e := c.get(ctx, "image", "/v2/images/"+c.config.ImageID, &image); e != nil {
		return result, e
	}
	result.Checks = append(result.Checks, Check{"active pinned image checksum", image.ID == c.config.ImageID && image.Status == "active" && image.HashAlgo+":"+image.HashValue == c.config.ImageHash})
	var network struct {
		Network struct {
			ID        string
			Status    string
			ProjectID string `json:"project_id"`
			Shared    bool
		}
	}
	if e := c.get(ctx, "network", "/v2.0/networks/"+c.config.NetworkID, &network); e != nil {
		return result, e
	}
	result.Checks = append(result.Checks, Check{"selected accessible active network", network.Network.ID == c.config.NetworkID && network.Network.Status == "ACTIVE" && (network.Network.ProjectID == c.config.ProjectID || network.Network.Shared)})
	var group struct {
		SecurityGroup struct {
			ID        string
			ProjectID string `json:"project_id"`
		} `json:"security_group"`
	}
	if e := c.get(ctx, "network", "/v2.0/security-groups/"+c.config.SecurityGroupID, &group); e != nil {
		return result, e
	}
	result.Checks = append(result.Checks, Check{"selected project security group", group.SecurityGroup.ID == c.config.SecurityGroupID && group.SecurityGroup.ProjectID == c.config.ProjectID})
	var quota struct {
		QuotaSet map[string]struct {
			Limit    *int
			InUse    *int `json:"in_use"`
			Reserved *int
		} `json:"quota_set"`
	}
	if e := c.get(ctx, "compute", "/os-quota-sets/"+c.config.ProjectID+"/detail", &quota); e != nil {
		return result, e
	}
	for name, needed := range map[string]int{"instances": 1, "cores": result.VCPUs, "ram": result.RAMMB} {
		q, ok := quota.QuotaSet[name]
		result.Checks = append(result.Checks, Check{"free " + name + " quota", ok && needed > 0 && q.Limit != nil && q.InUse != nil && q.Reserved != nil && *q.InUse >= 0 && *q.Reserved >= 0 && (*q.Limit == -1 || *q.Limit-*q.InUse-*q.Reserved >= needed)})
	}
	result.ResourceChecksPassed = true
	for _, check := range result.Checks {
		if !check.Passed {
			result.ResourceChecksPassed = false
		}
	}
	return result, nil
}
