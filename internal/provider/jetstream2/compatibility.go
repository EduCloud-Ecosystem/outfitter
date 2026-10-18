package jetstream2

// Pure metadata assessment only. This file has no transport, store or clock.
import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/netip"
	"sort"
	"strconv"
	"strings"
	"time"
)

const compatibilityBytes = 65536
const compatibilityRules = 128

// Observed preserves missing, explicit null and a typed value independently.
// Its zero value means missing. Factories avoid contradictory typed states.
type Observed[T any] struct {
	present, null bool
	value         T
}

func CompatibilityValue[T any](v T) Observed[T] { return Observed[T]{present: true, value: v} }
func CompatibilityNull[T any]() Observed[T]     { return Observed[T]{present: true, null: true} }
func (v Observed[T]) IsZero() bool              { return !v.present }
func (v Observed[T]) MarshalJSON() ([]byte, error) {
	if !v.present || v.null {
		return []byte("null"), nil
	}
	return json.Marshal(v.value)
}
func (v *Observed[T]) UnmarshalJSON(raw []byte) error {
	*v = Observed[T]{present: true}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		v.null = true
		return nil
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	return d.Decode(&v.value)
}

// CompatibilityProtocol accepts only JSON strings or integral numbers. Semantic
// support is checked separately; an unsupported observed protocol is UNKNOWN.
type CompatibilityProtocol string

func (p *CompatibilityProtocol) UnmarshalJSON(raw []byte) error {
	if len(raw) > 0 && raw[0] == '"' {
		var s string
		if json.Unmarshal(raw, &s) != nil {
			return errors.New("INVALID_PROTOCOL")
		}
		*p = CompatibilityProtocol(s)
		return nil
	}
	var n int64
	if json.Unmarshal(raw, &n) != nil {
		return errors.New("INVALID_PROTOCOL")
	}
	*p = CompatibilityProtocol(strconv.FormatInt(n, 10))
	return nil
}

type CompatibilityRule struct {
	Direction string                `json:"direction"`
	Ethertype string                `json:"ethertype"`
	Protocol  CompatibilityProtocol `json:"protocol"`
	PortMin   *int64                `json:"port_min"`
	PortMax   *int64                `json:"port_max"`
	CIDR      string                `json:"cidr"`
}
type CompatibilityProfile struct {
	Version                    int                 `json:"version"`
	ConfigDigest               string              `json:"config_digest"`
	PolicyDigest               string              `json:"policy_digest"`
	NotBefore                  time.Time           `json:"not_before"`
	ExpiresAt                  time.Time           `json:"expires_at"`
	MaxAgeSeconds              int64               `json:"max_age_seconds"`
	VCPUs                      int64               `json:"vcpus"`
	RAMMiB                     int64               `json:"ram_mib"`
	RootGiB                    int64               `json:"root_gib"`
	EphemeralGiB               int64               `json:"ephemeral_gib"`
	Architecture               string              `json:"architecture"`
	Firmware                   string              `json:"firmware"`
	DiskFormat                 string              `json:"disk_format"`
	ContainerFormat            string              `json:"container_format"`
	Stateful                   bool                `json:"stateful"`
	AllowUnrestrictedSelectors bool                `json:"allow_unrestricted_selectors"`
	AllowBroadEgress           bool                `json:"allow_broad_egress"`
	Rules                      []CompatibilityRule `json:"rules"`
}
type CompatibilityObservedRule struct {
	ID                 Observed[string]                `json:"id,omitzero"`
	Direction          Observed[string]                `json:"direction,omitzero"`
	Ethertype          Observed[string]                `json:"ethertype,omitzero"`
	Protocol           Observed[CompatibilityProtocol] `json:"protocol,omitzero"`
	PortMin            Observed[int64]                 `json:"port_min,omitzero"`
	PortMax            Observed[int64]                 `json:"port_max,omitzero"`
	CIDR               Observed[string]                `json:"cidr,omitzero"`
	RemoteGroup        Observed[string]                `json:"remote_group,omitzero"`
	RemoteAddressGroup Observed[string]                `json:"remote_address_group,omitzero"`
}
type CompatibilitySnapshot struct {
	Version         int                                   `json:"version"`
	ConfigDigest    string                                `json:"config_digest"`
	PolicyDigest    string                                `json:"policy_digest"`
	ProfileDigest   string                                `json:"profile_digest"`
	ObservedAt      time.Time                             `json:"observed_at"`
	ProjectID       Observed[string]                      `json:"project_id,omitzero"`
	Region          Observed[string]                      `json:"region,omitzero"`
	NetworkID       Observed[string]                      `json:"network_id,omitzero"`
	ImageID         Observed[string]                      `json:"image_id,omitzero"`
	ImageHash       Observed[string]                      `json:"image_hash,omitzero"`
	FlavorID        Observed[string]                      `json:"flavor_id,omitzero"`
	GroupID         Observed[string]                      `json:"group_id,omitzero"`
	GroupProjectID  Observed[string]                      `json:"group_project_id,omitzero"`
	VCPUs           Observed[int64]                       `json:"vcpus,omitzero"`
	RAMMiB          Observed[int64]                       `json:"ram_mib,omitzero"`
	RootGiB         Observed[int64]                       `json:"root_gib,omitzero"`
	EphemeralGiB    Observed[int64]                       `json:"ephemeral_gib,omitzero"`
	DiskMode        Observed[string]                      `json:"disk_mode,omitzero"`
	MinRAM          Observed[int64]                       `json:"min_ram,omitzero"`
	MinDisk         Observed[int64]                       `json:"min_disk,omitzero"`
	ImageBytes      Observed[int64]                       `json:"image_bytes,omitzero"`
	Architecture    Observed[string]                      `json:"architecture,omitzero"`
	Firmware        Observed[string]                      `json:"firmware,omitzero"`
	DiskFormat      Observed[string]                      `json:"disk_format,omitzero"`
	ContainerFormat Observed[string]                      `json:"container_format,omitzero"`
	Stateful        Observed[bool]                        `json:"stateful,omitzero"`
	RulesComplete   Observed[bool]                        `json:"rules_complete,omitzero"`
	Rules           Observed[[]CompatibilityObservedRule] `json:"rules,omitzero"`
}
type CompatibilityCheck struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	Reason string `json:"reason"`
}
type CompatibilityReport struct {
	Version             int                  `json:"version"`
	EvidenceScope       string               `json:"evidence_scope"`
	ProvisioningEnabled bool                 `json:"provisioning_enabled"`
	LiveReadiness       string               `json:"live_readiness"`
	BootVerified        bool                 `json:"boot_verified"`
	MetadataMatch       bool                 `json:"metadata_match"`
	ConfigDigest        string               `json:"config_digest,omitempty"`
	PolicyDigest        string               `json:"policy_digest,omitempty"`
	ProfileDigest       string               `json:"profile_digest,omitempty"`
	SnapshotDigest      string               `json:"snapshot_digest,omitempty"`
	Checks              []CompatibilityCheck `json:"checks"`
}

func closedCompatibilityReport() CompatibilityReport {
	return CompatibilityReport{Version: 1, EvidenceScope: "synthetic_offline", LiveReadiness: "BLOCKED", Checks: []CompatibilityCheck{}}
}
func compatibilityTime(t time.Time) bool { return !t.IsZero() && t.Year() >= 2000 && t.Year() <= 2100 }
func compatibilityHash(s string) bool {
	if len(s) != 64 {
		return false
	}
	_, e := hex.DecodeString(s)
	return e == nil && strings.ToLower(s) == s
}
func compatibilityDigest(kind string, v any) (string, error) {
	b, e := json.Marshal(v)
	if e != nil || len(b) > compatibilityBytes {
		return "", errors.New("INVALID_OR_OVERSIZED_INPUT")
	}
	h := sha256.New()
	h.Write([]byte("outfitter-metadata-compatibility-v1\x00" + kind + "\x00"))
	h.Write(b)
	return hex.EncodeToString(h.Sum(nil)), nil
}

// CompatibilityBindings binds all existing configuration and policy fields.
func CompatibilityBindings(c Config, p Policy) (string, string, error) {
	if c.Validate() != nil || p.validate() != nil || p.Config != c || !compatibilityTime(p.ApprovalExpiresAt) {
		return "", "", errors.New("INVALID_CONFIG_OR_POLICY")
	}
	p.ApprovalExpiresAt = p.ApprovalExpiresAt.UTC()
	ch, e := compatibilityDigest("config", c)
	if e != nil {
		return "", "", e
	}
	ph, e := compatibilityDigest("policy", p)
	return ch, ph, e
}
func protocol(p CompatibilityProtocol) (CompatibilityProtocol, bool) {
	switch p {
	case "any", "0":
		return "any", true
	case "tcp", "6":
		return "tcp", true
	case "udp", "17":
		return "udp", true
	case "icmp", "1":
		return "icmp", true
	case "ipv6-icmp", "icmpv6", "58":
		return "ipv6-icmp", true
	}
	return p, false
}
func compatibilityRule(r CompatibilityRule) (CompatibilityRule, error) {
	if r.Direction != "ingress" && r.Direction != "egress" {
		return r, errors.New("INVALID_RULE")
	}
	if r.Ethertype != "IPv4" && r.Ethertype != "IPv6" {
		return r, errors.New("INVALID_RULE")
	}
	var ok bool
	r.Protocol, ok = protocol(r.Protocol)
	if !ok {
		return r, errors.New("UNSUPPORTED_PROTOCOL")
	}
	prefix, e := netip.ParsePrefix(r.CIDR)
	if e != nil || prefix.Addr().Is4In6() || prefix != prefix.Masked() || prefix.String() != r.CIDR || (prefix.Addr().Is4() != (r.Ethertype == "IPv4")) {
		return r, errors.New("INVALID_CIDR")
	}
	if r.Protocol == "icmp" && r.Ethertype != "IPv4" || r.Protocol == "ipv6-icmp" && r.Ethertype != "IPv6" {
		return r, errors.New("INVALID_RULE")
	}
	switch r.Protocol {
	case "tcp", "udp":
		if r.PortMin == nil && r.PortMax == nil {
			break
		}
		if r.PortMin == nil || r.PortMax == nil || *r.PortMin < 1 || *r.PortMax > 65535 || *r.PortMin > *r.PortMax {
			return r, errors.New("INVALID_PORTS")
		}
		if *r.PortMin == 1 && *r.PortMax == 65535 {
			r.PortMin = nil
			r.PortMax = nil
		}
	case "icmp", "ipv6-icmp":
		if r.PortMin == nil && r.PortMax != nil || r.PortMin != nil && (*r.PortMin < 0 || *r.PortMin > 255) || r.PortMax != nil && (*r.PortMax < 0 || *r.PortMax > 255) {
			return r, errors.New("INVALID_PORTS")
		}
	default:
		if r.PortMin != nil || r.PortMax != nil {
			return r, errors.New("INVALID_PORTS")
		}
	}
	return r, nil
}
func ruleKey(r CompatibilityRule) string { b, _ := json.Marshal(r); return string(b) }
func compatibilityProfile(p CompatibilityProfile) (CompatibilityProfile, error) {
	if p.Version != 1 || !compatibilityHash(p.ConfigDigest) || !compatibilityHash(p.PolicyDigest) || !compatibilityTime(p.NotBefore) || !compatibilityTime(p.ExpiresAt) || !p.NotBefore.Before(p.ExpiresAt) || p.MaxAgeSeconds < 1 || p.MaxAgeSeconds > 3600 || p.VCPUs < 1 || p.VCPUs > 16 || p.RAMMiB < 1 || p.RAMMiB > 65536 || p.RootGiB < 1 || p.RootGiB > 200 || p.EphemeralGiB != 0 || (p.Architecture != "x86_64" && p.Architecture != "aarch64") || (p.Firmware != "bios" && p.Firmware != "uefi") || (p.DiskFormat != "raw" && p.DiskFormat != "qcow2") || p.ContainerFormat != "bare" || p.Rules == nil || len(p.Rules) > compatibilityRules {
		return p, errors.New("INVALID_PROFILE")
	}
	p.NotBefore = p.NotBefore.UTC()
	p.ExpiresAt = p.ExpiresAt.UTC()
	p.Rules = append([]CompatibilityRule{}, p.Rules...)
	seen := map[string]bool{}
	for i, r := range p.Rules {
		n, e := compatibilityRule(r)
		if e != nil {
			return p, errors.New("INVALID_PROFILE_RULE")
		}
		prefix, _ := netip.ParsePrefix(n.CIDR)
		if prefix.Bits() == 0 && !p.AllowUnrestrictedSelectors || n.Direction == "egress" && (prefix.Bits() == 0 || n.Protocol == "any" || n.PortMin == nil && n.PortMax == nil) && !p.AllowBroadEgress {
			return p, errors.New("UNREVIEWED_BROAD_RULE")
		}
		k := ruleKey(n)
		if seen[k] {
			return p, errors.New("DUPLICATE_PROFILE_RULE")
		}
		seen[k] = true
		p.Rules[i] = n
	}
	sort.Slice(p.Rules, func(i, j int) bool { return ruleKey(p.Rules[i]) < ruleKey(p.Rules[j]) })
	return p, nil
}
func CompatibilityProfileDigest(p CompatibilityProfile) (string, error) {
	p, e := compatibilityProfile(p)
	if e != nil {
		return "", e
	}
	return compatibilityDigest("profile", p)
}
func observation[T any](o Observed[T]) string {
	if !o.present {
		return "MISSING"
	}
	if o.null {
		return "NULL"
	}
	return ""
}
func observedRule(r CompatibilityObservedRule) (CompatibilityRule, string, string) {
	var n CompatibilityRule
	if !r.ID.present || !r.Direction.present || !r.Ethertype.present || !r.Protocol.present || !r.PortMin.present || !r.PortMax.present || !r.CIDR.present || !r.RemoteGroup.present || !r.RemoteAddressGroup.present {
		return n, "UNKNOWN", "MISSING_RULE_FIELD"
	}
	if r.ID.null || r.Direction.null || r.Ethertype.null {
		return n, "UNKNOWN", "NULL_RULE_IDENTITY"
	}
	if !ref.MatchString(r.ID.value) {
		return n, "MISMATCH", "INVALID_RULE_ID"
	}
	if (!r.RemoteGroup.null && !r.RemoteAddressGroup.null) || (!r.CIDR.null && (!r.RemoteGroup.null || !r.RemoteAddressGroup.null)) {
		return n, "MISMATCH", "CONFLICTING_SELECTORS"
	}
	for _, v := range []Observed[string]{r.RemoteGroup, r.RemoteAddressGroup} {
		if !v.null {
			if v.value == "" {
				return n, "MISMATCH", "INVALID_SELECTOR"
			}
			return n, "UNKNOWN", "DYNAMIC_SELECTOR"
		}
	}
	n = CompatibilityRule{Direction: r.Direction.value, Ethertype: r.Ethertype.value, CIDR: r.CIDR.value, Protocol: r.Protocol.value}
	if r.Protocol.null {
		n.Protocol = "any"
	}
	if !r.PortMin.null {
		v := r.PortMin.value
		n.PortMin = &v
	}
	if !r.PortMax.null {
		v := r.PortMax.value
		n.PortMax = &v
	}
	if r.CIDR.null {
		switch n.Ethertype {
		case "IPv4":
			n.CIDR = "0.0.0.0/0"
		case "IPv6":
			n.CIDR = "::/0"
		}
	}
	if _, ok := protocol(n.Protocol); !ok {
		return n, "UNKNOWN", "UNSUPPORTED_PROTOCOL"
	}
	n, e := compatibilityRule(n)
	if e != nil {
		return n, "MISMATCH", "INVALID_RULE"
	}
	return n, "MATCH", "MATCH"
}
func compatibilitySnapshot(s CompatibilitySnapshot) (CompatibilitySnapshot, error) {
	if s.Version != 1 || !compatibilityHash(s.ConfigDigest) || !compatibilityHash(s.PolicyDigest) || !compatibilityHash(s.ProfileDigest) || !compatibilityTime(s.ObservedAt) || len(s.Rules.value) > compatibilityRules {
		return s, errors.New("INVALID_SNAPSHOT")
	}
	s.ObservedAt = s.ObservedAt.UTC()
	if s.Rules.present && !s.Rules.null {
		if s.Rules.value == nil {
			return s, errors.New("INVALID_RULE_ARRAY")
		}
		s.Rules.value = append([]CompatibilityObservedRule{}, s.Rules.value...)
		for i, r := range s.Rules.value {
			n, status, _ := observedRule(r)
			if status == "MATCH" {
				// Preserve absence/null states; normalize only present supported values.
				if !r.Protocol.null {
					r.Protocol = CompatibilityValue(n.Protocol)
				}
				if !r.CIDR.null {
					r.CIDR = CompatibilityValue(n.CIDR)
				}
				if n.PortMin == nil {
					r.PortMin = CompatibilityNull[int64]()
					r.PortMax = CompatibilityNull[int64]()
				}
				s.Rules.value[i] = r
			}
		}
		sort.Slice(s.Rules.value, func(i, j int) bool {
			a, b := s.Rules.value[i], s.Rules.value[j]
			ai, _ := json.Marshal(a.ID)
			bi, _ := json.Marshal(b.ID)
			a.ID, b.ID = Observed[string]{}, Observed[string]{}
			ab, _ := json.Marshal(a)
			bb, _ := json.Marshal(b)
			if string(ab) == string(bb) {
				return string(ai) < string(bi)
			}
			return string(ab) < string(bb)
		})
	}
	return s, nil
}
func (r *CompatibilityReport) add(id, status, reason string) {
	r.Checks = append(r.Checks, CompatibilityCheck{id, status, reason})
	if status != "MATCH" {
		r.MetadataMatch = false
	}
}
func (r *CompatibilityReport) condition(id string, ok bool, reason string) {
	if ok {
		r.add(id, "MATCH", "MATCH")
	} else {
		r.add(id, "MISMATCH", reason)
	}
}
func compareCompatibility[T comparable](r *CompatibilityReport, id string, o Observed[T], expected T) {
	if why := observation(o); why != "" {
		r.add(id, "UNKNOWN", why)
		return
	}
	r.condition(id, o.value == expected, "DIFFERENT")
}
func compareDeclaration(r *CompatibilityReport, id string, o Observed[string], expected string, allowed ...string) {
	if why := observation(o); why != "" {
		r.add(id, "UNKNOWN", why)
		return
	}
	for _, v := range allowed {
		if o.value == v {
			r.condition(id, o.value == expected, "DIFFERENT")
			return
		}
	}
	r.add(id, "UNKNOWN", "UNSUPPORTED_DECLARATION")
}
func compareBound(r *CompatibilityReport, id string, o Observed[int64], limit int64) {
	if why := observation(o); why != "" {
		r.add(id, "UNKNOWN", why)
		return
	}
	r.condition(id, o.value >= 0 && o.value <= limit, "OUT_OF_BOUNDS")
}

func EvaluateCompatibility(p CompatibilityProfile, s CompatibilitySnapshot, c Config, policy Policy, now time.Time) (CompatibilityReport, error) {
	r := closedCompatibilityReport()
	if !compatibilityTime(now) {
		return r, errors.New("INVALID_EVALUATION_TIME")
	}
	ch, ph, e := CompatibilityBindings(c, policy)
	if e != nil {
		return r, e
	}
	p, e = compatibilityProfile(p)
	if e != nil {
		return r, e
	}
	pd, e := compatibilityDigest("profile", p)
	if e != nil {
		return r, e
	}
	s, e = compatibilitySnapshot(s)
	if e != nil {
		return r, e
	}
	sd, e := compatibilityDigest("snapshot", s)
	if e != nil {
		return r, e
	}
	r.ConfigDigest = ch
	r.PolicyDigest = ph
	r.ProfileDigest = pd
	r.SnapshotDigest = sd
	r.MetadataMatch = true
	r.condition("profile.config", p.ConfigDigest == ch, "BINDING_MISMATCH")
	r.condition("profile.policy", p.PolicyDigest == ph, "BINDING_MISMATCH")
	r.condition("snapshot.config", s.ConfigDigest == ch, "BINDING_MISMATCH")
	r.condition("snapshot.policy", s.PolicyDigest == ph, "BINDING_MISMATCH")
	r.condition("snapshot.profile", s.ProfileDigest == pd, "BINDING_MISMATCH")
	r.condition("profile.validity", !now.Before(p.NotBefore) && now.Before(p.ExpiresAt), "OUTSIDE_VALIDITY")
	r.condition("profile.permit_window", !p.ExpiresAt.After(policy.ApprovalExpiresAt), "PERMIT_WINDOW_MISMATCH")
	r.condition("snapshot.freshness", !s.ObservedAt.Before(p.NotBefore) && !s.ObservedAt.After(now) && now.Sub(s.ObservedAt) <= time.Duration(p.MaxAgeSeconds)*time.Second, "INVALID_FRESHNESS")
	for _, v := range []struct {
		id   string
		o    Observed[string]
		want string
	}{{"identity.project", s.ProjectID, c.ProjectID}, {"identity.region", s.Region, c.Region}, {"identity.network", s.NetworkID, c.NetworkID}, {"identity.image", s.ImageID, c.ImageID}, {"identity.flavor", s.FlavorID, c.FlavorID}, {"identity.group", s.GroupID, c.SecurityGroupID}, {"identity.group_project", s.GroupProjectID, c.ProjectID}, {"image.checksum", s.ImageHash, c.ImageHash}} {
		compareCompatibility(&r, v.id, v.o, v.want)
	}
	compareCompatibility(&r, "flavor.vcpus", s.VCPUs, p.VCPUs)
	compareCompatibility(&r, "flavor.ram", s.RAMMiB, p.RAMMiB)
	compareCompatibility(&r, "flavor.root", s.RootGiB, p.RootGiB)
	compareCompatibility(&r, "flavor.ephemeral", s.EphemeralGiB, p.EphemeralGiB)
	compareDeclaration(&r, "flavor.disk_mode", s.DiskMode, "ordinary_image", "ordinary_image")
	compareBound(&r, "image.min_ram", s.MinRAM, p.RAMMiB)
	compareBound(&r, "image.min_disk", s.MinDisk, p.RootGiB)
	compareBound(&r, "image.size", s.ImageBytes, p.RootGiB*(1<<30))
	compareDeclaration(&r, "image.architecture", s.Architecture, p.Architecture, "x86_64", "aarch64")
	compareDeclaration(&r, "image.firmware", s.Firmware, p.Firmware, "bios", "uefi")
	compareDeclaration(&r, "image.disk_format", s.DiskFormat, p.DiskFormat, "raw", "qcow2")
	compareDeclaration(&r, "image.container_format", s.ContainerFormat, p.ContainerFormat, "bare")
	compareCompatibility(&r, "security.stateful", s.Stateful, p.Stateful)
	status, reason := compareRules(p, s)
	r.add("security.rules", status, reason)
	return r, nil
}
func compareRules(p CompatibilityProfile, s CompatibilitySnapshot) (string, string) {
	if why := observation(s.RulesComplete); why != "" {
		return "UNKNOWN", why
	}
	if !s.RulesComplete.value {
		return "UNKNOWN", "INCOMPLETE_RULES"
	}
	if why := observation(s.Rules); why != "" {
		return "UNKNOWN", why
	}
	ids := map[string]bool{}
	semantic := map[string]bool{}
	// Fixed precedence makes diagnostic output independent of source order.
	status, reason := "MATCH", "MATCH"
	for _, r := range s.Rules.value {
		if observation(r.ID) == "" && ref.MatchString(r.ID.value) {
			if ids[r.ID.value] {
				return "MISMATCH", "DUPLICATE_RULE"
			}
			ids[r.ID.value] = true
		}
		n, st, why := observedRule(r)
		if st != "MATCH" {
			if status == "MATCH" || st == "MISMATCH" && status == "UNKNOWN" || st == status && why < reason {
				status, reason = st, why
			}
			continue
		}
		k := ruleKey(n)
		if semantic[k] {
			return "MISMATCH", "DUPLICATE_RULE"
		}
		semantic[k] = true
	}
	if status != "MATCH" {
		return status, reason
	}
	if len(semantic) != len(p.Rules) {
		return "MISMATCH", "RULE_SET_MISMATCH"
	}
	for _, r := range p.Rules {
		if !semantic[ruleKey(r)] {
			return "MISMATCH", "RULE_SET_MISMATCH"
		}
	}
	return "MATCH", "MATCH"
}
