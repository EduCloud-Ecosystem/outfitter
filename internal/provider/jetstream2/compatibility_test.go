package jetstream2

import (
	"bytes"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func compatibilityFixture(t *testing.T) (CompatibilityProfile, CompatibilitySnapshot, Config, Policy, time.Time) {
	t.Helper()
	now := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	c := Config{AuthURL: "https://fixture.invalid/v3", ProjectID: "project", Region: "region", ImageID: "image", ImageHash: "sha256:" + strings.Repeat("a", 64), FlavorID: "flavor", NetworkID: "network", SecurityGroupID: "group"}
	policy := Policy{Config: c, BudgetMilliSU: 1000, RateMilliSUPerHour: 1000, RateCheckedAt: "2026-10-17", RateSource: "https://docs.jetstream-cloud.org/general/vmsizes/", SupervisorID: "fixture-supervisor", ApprovalExpiresAt: now.Add(time.Hour), AllowSupervisedException: true}
	ch, ph, e := CompatibilityBindings(c, policy)
	if e != nil {
		t.Fatal(e)
	}
	port := int64(443)
	p := CompatibilityProfile{Version: 1, ConfigDigest: ch, PolicyDigest: ph, NotBefore: now.Add(-10 * time.Minute), ExpiresAt: now.Add(10 * time.Minute), MaxAgeSeconds: 60, VCPUs: 1, RAMMiB: 3072, RootGiB: 20, Architecture: "x86_64", Firmware: "uefi", DiskFormat: "qcow2", ContainerFormat: "bare", Stateful: true, Rules: []CompatibilityRule{{Direction: "egress", Ethertype: "IPv4", Protocol: "tcp", PortMin: &port, PortMax: &port, CIDR: "192.0.2.0/24"}}}
	pd, e := CompatibilityProfileDigest(p)
	if e != nil {
		t.Fatal(e)
	}
	r := CompatibilityObservedRule{ID: CompatibilityValue("rule"), Direction: CompatibilityValue("egress"), Ethertype: CompatibilityValue("IPv4"), Protocol: CompatibilityValue(CompatibilityProtocol("tcp")), PortMin: CompatibilityValue(port), PortMax: CompatibilityValue(port), CIDR: CompatibilityValue("192.0.2.0/24"), RemoteGroup: CompatibilityNull[string](), RemoteAddressGroup: CompatibilityNull[string]()}
	s := CompatibilitySnapshot{Version: 1, ConfigDigest: ch, PolicyDigest: ph, ProfileDigest: pd, ObservedAt: now, ProjectID: CompatibilityValue(c.ProjectID), Region: CompatibilityValue(c.Region), NetworkID: CompatibilityValue(c.NetworkID), ImageID: CompatibilityValue(c.ImageID), ImageHash: CompatibilityValue(c.ImageHash), FlavorID: CompatibilityValue(c.FlavorID), GroupID: CompatibilityValue(c.SecurityGroupID), GroupProjectID: CompatibilityValue(c.ProjectID), VCPUs: CompatibilityValue(p.VCPUs), RAMMiB: CompatibilityValue(p.RAMMiB), RootGiB: CompatibilityValue(p.RootGiB), EphemeralGiB: CompatibilityValue(int64(0)), DiskMode: CompatibilityValue("ordinary_image"), MinRAM: CompatibilityValue(int64(0)), MinDisk: CompatibilityValue(int64(0)), ImageBytes: CompatibilityValue(int64(1024)), Architecture: CompatibilityValue(p.Architecture), Firmware: CompatibilityValue(p.Firmware), DiskFormat: CompatibilityValue(p.DiskFormat), ContainerFormat: CompatibilityValue(p.ContainerFormat), Stateful: CompatibilityValue(true), RulesComplete: CompatibilityValue(true), Rules: CompatibilityValue([]CompatibilityObservedRule{r})}
	return p, s, c, policy, now
}
func compatibilityCheck(t *testing.T, r CompatibilityReport, id string) CompatibilityCheck {
	t.Helper()
	for _, c := range r.Checks {
		if c.ID == id {
			return c
		}
	}
	t.Fatal("missing check", id)
	return CompatibilityCheck{}
}
func compatibilityClosed(t *testing.T, r CompatibilityReport) {
	t.Helper()
	if r.EvidenceScope != "synthetic_offline" || r.LiveReadiness != "BLOCKED" || r.ProvisioningEnabled || r.BootVerified {
		t.Fatal("gates opened", r)
	}
}
func compatibilityRebind(t *testing.T, p *CompatibilityProfile, s *CompatibilitySnapshot, c Config, policy Policy) {
	t.Helper()
	var e error
	p.ConfigDigest, p.PolicyDigest, e = CompatibilityBindings(c, policy)
	if e != nil {
		t.Fatal(e)
	}
	s.ConfigDigest = p.ConfigDigest
	s.PolicyDigest = p.PolicyDigest
	s.ProfileDigest, e = CompatibilityProfileDigest(*p)
	if e != nil {
		t.Fatal(e)
	}
}
func TestCompatibilityMatchAndCanonicalInputs(t *testing.T) {
	p, s, c, policy, now := compatibilityFixture(t)
	p.Rules = append(p.Rules, CompatibilityRule{Direction: "ingress", Ethertype: "IPv6", Protocol: "ipv6-icmp", CIDR: "2001:db8::/64"})
	second := s.Rules.value[0]
	second.ID = CompatibilityValue("second")
	second.Direction = CompatibilityValue("ingress")
	second.Ethertype = CompatibilityValue("IPv6")
	second.Protocol = CompatibilityValue(CompatibilityProtocol("ipv6-icmp"))
	second.CIDR = CompatibilityValue("2001:db8::/64")
	second.PortMin = CompatibilityNull[int64]()
	second.PortMax = CompatibilityNull[int64]()
	s.Rules.value = append(s.Rules.value, second)
	compatibilityRebind(t, &p, &s, c, policy)
	beforeP, _ := json.Marshal(p)
	beforeS, _ := json.Marshal(s)
	a, e := EvaluateCompatibility(p, s, c, policy, now)
	if e != nil || !a.MetadataMatch {
		t.Fatal(a, e)
	}
	compatibilityClosed(t, a)
	afterP, _ := json.Marshal(p)
	afterS, _ := json.Marshal(s)
	if !bytes.Equal(beforeP, afterP) || !bytes.Equal(beforeS, afterS) {
		t.Fatal("mutated input")
	}
	p.Rules[0], p.Rules[1] = p.Rules[1], p.Rules[0]
	p.Rules[0].Protocol = "58"
	s.Rules.value[0], s.Rules.value[1] = s.Rules.value[1], s.Rules.value[0]
	s.Rules.value[0].Protocol = CompatibilityValue(CompatibilityProtocol("icmpv6"))
	p.NotBefore = p.NotBefore.In(time.FixedZone("fixture", 3600))
	s.ObservedAt = s.ObservedAt.In(time.FixedZone("fixture", 3600))
	policy.ApprovalExpiresAt = policy.ApprovalExpiresAt.In(time.FixedZone("fixture", 3600))
	b, e := EvaluateCompatibility(p, s, c, policy, now)
	if e != nil || !reflect.DeepEqual(a, b) {
		t.Fatal("canonical difference", a, b, e)
	}
	rawP, _ := json.Marshal(p)
	rawS, _ := json.Marshal(s)
	dp, e := DecodeCompatibilityProfile(rawP)
	if e != nil {
		t.Fatal(e)
	}
	ds, e := DecodeCompatibilitySnapshot(rawS)
	if e != nil {
		t.Fatal(e)
	}
	d, e := EvaluateCompatibility(dp, ds, c, policy, now)
	if e != nil || !reflect.DeepEqual(a, d) {
		t.Fatal("decode difference", e)
	}
	if len(a.Checks) != 30 {
		t.Fatal("check contract changed", len(a.Checks))
	}
}
func TestCompatibilityBindings(t *testing.T) {
	for _, field := range []string{"AuthURL", "ProjectID", "Region", "ImageID", "ImageHash", "FlavorID", "NetworkID", "SecurityGroupID"} {
		t.Run("config/"+field, func(t *testing.T) {
			p, s, c, policy, now := compatibilityFixture(t)
			v := reflect.ValueOf(&c).Elem().FieldByName(field)
			next := "changed"
			if field == "AuthURL" {
				next = "https://other.invalid/v3"
			}
			if field == "ImageHash" {
				next = "sha256:" + strings.Repeat("b", 64)
			}
			v.SetString(next)
			policy.Config = c
			r, e := EvaluateCompatibility(p, s, c, policy, now)
			if e != nil || r.MetadataMatch || compatibilityCheck(t, r, "profile.config").Status != "MISMATCH" {
				t.Fatal(r, e)
			}
			compatibilityClosed(t, r)
		})
	}
	for _, field := range []string{"BudgetMilliSU", "RateMilliSUPerHour", "RateCheckedAt", "RateSource", "SupervisorID", "ApprovalExpiresAt", "AllowSupervisedException", "Rescue"} {
		t.Run("policy/"+field, func(t *testing.T) {
			p, s, c, policy, now := compatibilityFixture(t)
			switch field {
			case "BudgetMilliSU":
				policy.BudgetMilliSU++
			case "RateMilliSUPerHour":
				policy.RateMilliSUPerHour++
			case "RateCheckedAt":
				policy.RateCheckedAt = "2026-10-16"
			case "RateSource":
				policy.RateSource = "https://other.invalid/rates"
			case "SupervisorID":
				policy.SupervisorID = "changed"
			case "ApprovalExpiresAt":
				policy.ApprovalExpiresAt = policy.ApprovalExpiresAt.Add(time.Minute)
			case "AllowSupervisedException":
				policy.AllowSupervisedException = false
			case "Rescue":
				policy.Rescue = &RescuePolicy{}
			}
			r, e := EvaluateCompatibility(p, s, c, policy, now)
			if r.MetadataMatch || e == nil && compatibilityCheck(t, r, "profile.policy").Status != "MISMATCH" {
				t.Fatal(r, e)
			}
			compatibilityClosed(t, r)
		})
	}
	for _, field := range []string{"ConfigDigest", "PolicyDigest", "ProfileDigest"} {
		t.Run("snapshot/"+field, func(t *testing.T) {
			p, s, c, policy, now := compatibilityFixture(t)
			reflect.ValueOf(&s).Elem().FieldByName(field).SetString(strings.Repeat("f", 64))
			r, e := EvaluateCompatibility(p, s, c, policy, now)
			if e != nil || r.MetadataMatch {
				t.Fatal(r, e)
			}
		})
	}
	t.Run("profile-content", func(t *testing.T) {
		p, s, c, policy, now := compatibilityFixture(t)
		p.MaxAgeSeconds++
		r, e := EvaluateCompatibility(p, s, c, policy, now)
		if e != nil || compatibilityCheck(t, r, "snapshot.profile").Status != "MISMATCH" {
			t.Fatal(r, e)
		}
	})
	for _, field := range []string{"ProjectID", "Region", "NetworkID", "ImageID", "ImageHash", "FlavorID", "GroupID", "GroupProjectID"} {
		t.Run("mixed/"+field, func(t *testing.T) {
			p, s, c, policy, now := compatibilityFixture(t)
			reflect.ValueOf(&s).Elem().FieldByName(field).Set(reflect.ValueOf(CompatibilityValue("foreign")))
			r, e := EvaluateCompatibility(p, s, c, policy, now)
			if e != nil || r.MetadataMatch {
				t.Fatal(r, e)
			}
		})
	}
}
func TestCompatibilityResourceObservations(t *testing.T) {
	for _, field := range []string{"VCPUs", "RAMMiB", "RootGiB", "EphemeralGiB", "MinRAM", "MinDisk", "ImageBytes"} {
		for _, mode := range []string{"missing", "null", "negative", "overflow"} {
			t.Run(field+"/"+mode, func(t *testing.T) {
				p, s, c, policy, now := compatibilityFixture(t)
				var v Observed[int64]
				switch mode {
				case "null":
					v = CompatibilityNull[int64]()
				case "negative":
					v = CompatibilityValue(int64(-1))
				case "overflow":
					v = CompatibilityValue(int64(math.MaxInt64))
				}
				reflect.ValueOf(&s).Elem().FieldByName(field).Set(reflect.ValueOf(v))
				r, e := EvaluateCompatibility(p, s, c, policy, now)
				if e != nil || r.MetadataMatch {
					t.Fatal(r, e)
				}
				compatibilityClosed(t, r)
			})
		}
	}
	for _, mode := range []string{"zero", "equal", "above"} {
		t.Run("minima/"+mode, func(t *testing.T) {
			p, s, c, policy, now := compatibilityFixture(t)
			v1, v2, v3 := int64(0), int64(0), int64(0)
			if mode != "zero" {
				v1 = p.RAMMiB
				v2 = p.RootGiB
				v3 = p.RootGiB * (1 << 30)
			}
			if mode == "above" {
				v1++
				v2++
				v3++
			}
			s.MinRAM = CompatibilityValue(v1)
			s.MinDisk = CompatibilityValue(v2)
			s.ImageBytes = CompatibilityValue(v3)
			r, e := EvaluateCompatibility(p, s, c, policy, now)
			if e != nil || r.MetadataMatch != (mode != "above") {
				t.Fatal(r, e)
			}
		})
	}
	for _, mode := range []string{"volume", "zero-root", "extra-disk"} {
		t.Run(mode, func(t *testing.T) {
			p, s, c, policy, now := compatibilityFixture(t)
			switch mode {
			case "volume":
				s.DiskMode = CompatibilityValue("volume")
			case "zero-root":
				s.RootGiB = CompatibilityValue(int64(0))
			case "extra-disk":
				s.EphemeralGiB = CompatibilityValue(int64(1))
			}
			r, e := EvaluateCompatibility(p, s, c, policy, now)
			if e != nil || r.MetadataMatch {
				t.Fatal(r, e)
			}
		})
	}
	for _, field := range []string{"Architecture", "Firmware", "DiskFormat", "ContainerFormat", "DiskMode"} {
		for _, mode := range []string{"missing", "null", "unsupported", "mismatch"} {
			t.Run(field+"/"+mode, func(t *testing.T) {
				p, s, c, policy, now := compatibilityFixture(t)
				var v Observed[string]
				switch mode {
				case "null":
					v = CompatibilityNull[string]()
				case "unsupported":
					v = CompatibilityValue("unknown")
				case "mismatch":
					next := map[string]string{"Architecture": "aarch64", "Firmware": "bios", "DiskFormat": "raw", "ContainerFormat": "docker", "DiskMode": "volume"}[field]
					v = CompatibilityValue(next)
				}
				reflect.ValueOf(&s).Elem().FieldByName(field).Set(reflect.ValueOf(v))
				r, e := EvaluateCompatibility(p, s, c, policy, now)
				if e != nil || r.MetadataMatch {
					t.Fatal(r, e)
				}
			})
		}
	}
}
func TestCompatibilityRuleControls(t *testing.T) {
	for _, mode := range []string{"missing-list", "null-list", "incomplete", "missing-complete", "empty-list", "extra", "missing-rule", "duplicate", "duplicate-semantic", "family", "direction", "wildcard", "missing-cidr", "null-cidr", "dynamic-group", "dynamic-address", "missing-selector", "null-direction", "missing-id", "invalid-id", "unknown-protocol", "missing-protocol", "null-protocol", "bad-range", "missing-bound", "null-bound", "host-bits", "invalid-cidr", "wrong-family-cidr", "stateful"} {
		t.Run(mode, func(t *testing.T) {
			p, s, c, policy, now := compatibilityFixture(t)
			r := &s.Rules.value[0]
			switch mode {
			case "missing-list":
				s.Rules = Observed[[]CompatibilityObservedRule]{}
			case "null-list":
				s.Rules = CompatibilityNull[[]CompatibilityObservedRule]()
			case "incomplete":
				s.RulesComplete = CompatibilityValue(false)
			case "missing-complete":
				s.RulesComplete = Observed[bool]{}
			case "empty-list", "missing-rule":
				s.Rules = CompatibilityValue([]CompatibilityObservedRule{})
			case "extra":
				next := *r
				next.ID = CompatibilityValue("extra")
				next.CIDR = CompatibilityValue("198.51.100.0/24")
				s.Rules.value = append(s.Rules.value, next)
			case "duplicate":
				s.Rules.value = append(s.Rules.value, *r)
			case "duplicate-semantic":
				next := *r
				next.ID = CompatibilityValue("other-id")
				s.Rules.value = append(s.Rules.value, next)
			case "family":
				r.Ethertype = CompatibilityValue("IPv6")
			case "direction":
				r.Direction = CompatibilityValue("ingress")
			case "wildcard":
				r.CIDR = CompatibilityValue("0.0.0.0/0")
			case "missing-cidr":
				r.CIDR = Observed[string]{}
			case "null-cidr":
				r.CIDR = CompatibilityNull[string]()
			case "dynamic-group":
				r.RemoteGroup = CompatibilityValue("dynamic")
			case "dynamic-address":
				r.RemoteAddressGroup = CompatibilityValue("dynamic")
			case "missing-selector":
				r.RemoteAddressGroup = Observed[string]{}
			case "null-direction":
				r.Direction = CompatibilityNull[string]()
			case "missing-id":
				r.ID = Observed[string]{}
			case "invalid-id":
				r.ID = CompatibilityValue("bad id")
			case "unknown-protocol":
				r.Protocol = CompatibilityValue(CompatibilityProtocol("sctp"))
			case "missing-protocol":
				r.Protocol = Observed[CompatibilityProtocol]{}
			case "null-protocol":
				r.Protocol = CompatibilityNull[CompatibilityProtocol]()
			case "bad-range":
				r.PortMin = CompatibilityValue(int64(444))
			case "missing-bound":
				r.PortMax = Observed[int64]{}
			case "null-bound":
				r.PortMax = CompatibilityNull[int64]()
			case "host-bits":
				r.CIDR = CompatibilityValue("192.0.2.1/24")
			case "invalid-cidr":
				r.CIDR = CompatibilityValue("wrong")
			case "wrong-family-cidr":
				r.CIDR = CompatibilityValue("2001:db8::/64")
			case "stateful":
				s.Stateful = CompatibilityValue(false)
			}
			result, e := EvaluateCompatibility(p, s, c, policy, now)
			if e != nil || result.MetadataMatch {
				t.Fatal(result, e)
			}
			compatibilityClosed(t, result)
		})
	}
	t.Run("complete-empty", func(t *testing.T) {
		p, s, c, policy, now := compatibilityFixture(t)
		p.Rules = []CompatibilityRule{}
		s.Rules = CompatibilityValue([]CompatibilityObservedRule{})
		compatibilityRebind(t, &p, &s, c, policy)
		r, e := EvaluateCompatibility(p, s, c, policy, now)
		if e != nil || !r.MetadataMatch {
			t.Fatal(r, e)
		}
	})
	for _, mode := range []string{"icmp-zero", "all-ports", "wildcard-reviewed"} {
		t.Run(mode, func(t *testing.T) {
			p, s, c, policy, now := compatibilityFixture(t)
			r := &s.Rules.value[0]
			switch mode {
			case "icmp-zero":
				zero := int64(0)
				p.Rules[0].Protocol = "icmp"
				p.Rules[0].PortMin = &zero
				p.Rules[0].PortMax = nil
				r.Protocol = CompatibilityValue(CompatibilityProtocol("1"))
				r.PortMin = CompatibilityValue(zero)
				r.PortMax = CompatibilityNull[int64]()
			case "all-ports":
				low, high := int64(1), int64(65535)
				p.AllowBroadEgress = true
				p.Rules[0].PortMin = &low
				p.Rules[0].PortMax = &high
				r.PortMin = CompatibilityNull[int64]()
				r.PortMax = CompatibilityNull[int64]()
			case "wildcard-reviewed":
				p.AllowBroadEgress = true
				p.AllowUnrestrictedSelectors = true
				p.Rules[0].Protocol = "any"
				p.Rules[0].PortMin = nil
				p.Rules[0].PortMax = nil
				p.Rules[0].CIDR = "0.0.0.0/0"
				r.Protocol = CompatibilityNull[CompatibilityProtocol]()
				r.CIDR = CompatibilityNull[string]()
				r.PortMin = CompatibilityNull[int64]()
				r.PortMax = CompatibilityNull[int64]()
			}
			compatibilityRebind(t, &p, &s, c, policy)
			result, e := EvaluateCompatibility(p, s, c, policy, now)
			if e != nil || !result.MetadataMatch {
				t.Fatal(result, e)
			}
		})
	}
}
func TestCompatibilityTimeBoundaries(t *testing.T) {
	for _, mode := range []string{"start", "before-start", "at-expiry", "after-expiry", "max-age", "stale", "future", "before-profile", "zero-now", "invalid-now", "permit-expiry"} {
		t.Run(mode, func(t *testing.T) {
			p, s, c, policy, now := compatibilityFixture(t)
			want := false
			wantErr := false
			switch mode {
			case "start":
				now = p.NotBefore
				s.ObservedAt = now
				want = true
			case "before-start":
				now = p.NotBefore.Add(-time.Nanosecond)
			case "at-expiry":
				now = p.ExpiresAt
			case "after-expiry":
				now = p.ExpiresAt.Add(time.Nanosecond)
			case "max-age":
				s.ObservedAt = now.Add(-60 * time.Second)
				want = true
			case "stale":
				s.ObservedAt = now.Add(-60*time.Second - time.Nanosecond)
			case "future":
				s.ObservedAt = now.Add(time.Nanosecond)
			case "before-profile":
				s.ObservedAt = p.NotBefore.Add(-time.Nanosecond)
			case "zero-now":
				now = time.Time{}
				wantErr = true
			case "invalid-now":
				now = time.Date(2101, 1, 1, 0, 0, 0, 0, time.UTC)
				wantErr = true
			case "permit-expiry":
				policy.ApprovalExpiresAt = p.ExpiresAt.Add(-time.Second)
				compatibilityRebind(t, &p, &s, c, policy)
			}
			r, e := EvaluateCompatibility(p, s, c, policy, now)
			if (e != nil) != wantErr || r.MetadataMatch != want {
				t.Fatal(r, e)
			}
			compatibilityClosed(t, r)
		})
	}
}

func TestCompatibilityStrictDecoders(t *testing.T) {
	p, s, _, _, _ := compatibilityFixture(t)
	pr, _ := json.Marshal(p)
	sr, _ := json.Marshal(s)
	for _, target := range []string{"profile", "snapshot"} {
		for _, mode := range []string{"duplicate", "nested-duplicate", "unknown", "wrong-case", "trailing", "oversize", "depth", "null-root", "wrong-type", "overflow", "float", "version", "missing-envelope", "malformed"} {
			t.Run(target+"/"+mode, func(t *testing.T) {
				raw := pr
				if target == "snapshot" {
					raw = sr
				}
				raw = append([]byte{}, raw...)
				switch mode {
				case "duplicate":
					raw = []byte(`{"version":1,` + string(raw[1:]))
				case "nested-duplicate":
					raw = bytes.Replace(raw, []byte(`"direction":"egress"`), []byte(`"direction":"egress","direction":"ingress"`), 1)
				case "unknown":
					raw = []byte(`{"secret_sentinel":"DO_NOT_ECHO",` + string(raw[1:]))
				case "wrong-case":
					raw = bytes.Replace(raw, []byte(`"version"`), []byte(`"Version"`), 1)
				case "trailing":
					raw = append(raw, []byte(` {}`)...)
				case "oversize":
					raw = append(raw, bytes.Repeat([]byte(" "), compatibilityBytes)...)
				case "depth":
					raw = []byte(`{"extra":` + strings.Repeat("[", 20) + `0` + strings.Repeat("]", 20) + `}`)
				case "null-root":
					raw = []byte("null")
				case "wrong-type":
					raw = bytes.Replace(raw, []byte(`"vcpus":1`), []byte(`"vcpus":"DO_NOT_ECHO"`), 1)
				case "overflow":
					raw = bytes.Replace(raw, []byte(`"vcpus":1`), []byte(`"vcpus":9223372036854775808`), 1)
				case "float":
					raw = bytes.Replace(raw, []byte(`"vcpus":1`), []byte(`"vcpus":1.5`), 1)
				case "version":
					raw = bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":2`), 1)
				case "missing-envelope":
					raw = bytes.Replace(raw, []byte(`"version":1,`), nil, 1)
				case "malformed":
					raw = []byte(`{"version":`)
				}
				var e error
				if target == "profile" {
					v, err := DecodeCompatibilityProfile(raw)
					e = err
					if err != nil && !reflect.DeepEqual(v, CompatibilityProfile{}) {
						t.Fatal("partial profile escaped rejection")
					}
				} else {
					v, err := DecodeCompatibilitySnapshot(raw)
					e = err
					if err != nil && !reflect.DeepEqual(v, CompatibilitySnapshot{}) {
						t.Fatal("partial snapshot escaped rejection")
					}
				}
				if e == nil || strings.Contains(e.Error(), "DO_NOT_ECHO") || strings.Contains(e.Error(), "secret_sentinel") {
					t.Fatal("unsafe decoder result", e)
				}
			})
		}
	}
	t.Run("explicit-booleans", func(t *testing.T) {
		for _, name := range []string{"allow_broad_egress", "allow_unrestricted_selectors", "stateful"} {
			var m map[string]any
			json.Unmarshal(pr, &m)
			delete(m, name)
			raw, _ := json.Marshal(m)
			if _, e := DecodeCompatibilityProfile(raw); e == nil {
				t.Fatal("missing profile field", name)
			}
		}
	})
	t.Run("observation-presence", func(t *testing.T) {
		var m map[string]any
		json.Unmarshal(sr, &m)
		delete(m, "min_ram")
		m["min_disk"] = nil
		m["image_bytes"] = 0
		raw, _ := json.Marshal(m)
		v, e := DecodeCompatibilitySnapshot(raw)
		if e != nil || v.MinRAM.present || !v.MinDisk.present || !v.MinDisk.null || !v.ImageBytes.present || v.ImageBytes.null || v.ImageBytes.value != 0 {
			t.Fatal(v, e)
		}
	})
	t.Run("numeric-protocol", func(t *testing.T) {
		raw := bytes.Replace(sr, []byte(`"protocol":"tcp"`), []byte(`"protocol":6`), 1)
		v, e := DecodeCompatibilitySnapshot(raw)
		if e != nil || v.Rules.value[0].Protocol.value != "tcp" {
			t.Fatal(v, e)
		}
	})
	t.Run("protocol-boolean", func(t *testing.T) {
		raw := bytes.Replace(sr, []byte(`"protocol":"tcp"`), []byte(`"protocol":true`), 1)
		if _, e := DecodeCompatibilitySnapshot(raw); e == nil {
			t.Fatal("boolean protocol")
		}
	})
	t.Run("byte-boundary", func(t *testing.T) {
		raw := append(append([]byte{}, sr...), bytes.Repeat([]byte(" "), compatibilityBytes-len(sr))...)
		if _, e := DecodeCompatibilitySnapshot(raw); e != nil {
			t.Fatal(e)
		}
		if _, e := DecodeCompatibilitySnapshot(append(raw, ' ')); e == nil {
			t.Fatal("oversize accepted")
		}
	})
}
func TestCompatibilityMalformedProfilesAndBounds(t *testing.T) {
	for _, mode := range []string{"version", "hash", "window", "time", "age-zero", "age-large", "cpu-zero", "cpu-large", "ram-large", "root-zero", "root-large", "extra-disk", "architecture", "firmware", "disk-format", "container-format", "nil-rules", "many-rules", "duplicate-rules", "wildcard-unreviewed", "broad-unreviewed", "bad-ports", "bad-cidr"} {
		t.Run(mode, func(t *testing.T) {
			p, s, c, policy, now := compatibilityFixture(t)
			switch mode {
			case "version":
				p.Version = 2
			case "hash":
				p.ConfigDigest = "bad"
			case "window":
				p.ExpiresAt = p.NotBefore
			case "time":
				p.NotBefore = time.Time{}
			case "age-zero":
				p.MaxAgeSeconds = 0
			case "age-large":
				p.MaxAgeSeconds = 3601
			case "cpu-zero":
				p.VCPUs = 0
			case "cpu-large":
				p.VCPUs = 17
			case "ram-large":
				p.RAMMiB = 65537
			case "root-zero":
				p.RootGiB = 0
			case "root-large":
				p.RootGiB = 201
			case "extra-disk":
				p.EphemeralGiB = 1
			case "architecture":
				p.Architecture = "amd64"
			case "firmware":
				p.Firmware = "unknown"
			case "disk-format":
				p.DiskFormat = "iso"
			case "container-format":
				p.ContainerFormat = "ova"
			case "nil-rules":
				p.Rules = nil
			case "many-rules":
				p.Rules = make([]CompatibilityRule, 129)
			case "duplicate-rules":
				p.Rules = append(p.Rules, p.Rules[0])
			case "wildcard-unreviewed":
				p.Rules[0].CIDR = "0.0.0.0/0"
			case "broad-unreviewed":
				p.Rules[0].PortMin = nil
				p.Rules[0].PortMax = nil
			case "bad-ports":
				zero := int64(0)
				p.Rules[0].PortMin = &zero
			case "bad-cidr":
				p.Rules[0].CIDR = "192.0.2.1/24"
			}
			r, e := EvaluateCompatibility(p, s, c, policy, now)
			if e == nil || r.MetadataMatch {
				t.Fatal(r, e)
			}
			compatibilityClosed(t, r)
		})
	}
	t.Run("snapshot-rule-count", func(t *testing.T) {
		p, s, c, policy, now := compatibilityFixture(t)
		s.Rules.value = make([]CompatibilityObservedRule, 129)
		r, e := EvaluateCompatibility(p, s, c, policy, now)
		if e == nil {
			t.Fatal(r)
		}
		raw, _ := json.Marshal(s)
		if _, e := DecodeCompatibilitySnapshot(raw); e == nil {
			t.Fatal("decoder accepted excess rules")
		}
	})
	t.Run("typed-byte-limit", func(t *testing.T) {
		p, s, c, policy, now := compatibilityFixture(t)
		s.Architecture = CompatibilityValue(strings.Repeat("x", compatibilityBytes))
		r, e := EvaluateCompatibility(p, s, c, policy, now)
		if e == nil {
			t.Fatal(r)
		}
		compatibilityClosed(t, r)
	})
	t.Run("secret-sanitized", func(t *testing.T) {
		p, s, c, policy, now := compatibilityFixture(t)
		s.Architecture = CompatibilityValue("PRIVATE_SENTINEL")
		s.Rules.value[0].CIDR = CompatibilityValue("PRIVATE_SENTINEL")
		r, e := EvaluateCompatibility(p, s, c, policy, now)
		b, _ := json.Marshal(r)
		if e != nil || strings.Contains(string(b), "PRIVATE_SENTINEL") {
			t.Fatal("input escaped", e)
		}
	})
}
func TestCompatibilityNoIOOrAccountingEffects(t *testing.T) {
	allowed := map[string]bool{"bytes": true, "crypto/sha256": true, "encoding/hex": true, "encoding/json": true, "errors": true, "net/netip": true, "sort": true, "strconv": true, "strings": true, "time": true, "io": true, "reflect": true}
	for _, name := range []string{"compatibility.go", "compatibility_decode.go"} {
		f, e := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		if e != nil {
			t.Fatal(e)
		}
		for _, i := range f.Imports {
			if !allowed[strings.Trim(i.Path.Value, `"`)] {
				t.Fatal("I/O import", i.Path.Value)
			}
		}
		ast.Inspect(f, func(n ast.Node) bool {
			if x, ok := n.(*ast.SelectorExpr); ok {
				if pkg, ok := x.X.(*ast.Ident); ok && pkg.Name == "time" && (x.Sel.Name == "Now" || x.Sel.Name == "Sleep" || x.Sel.Name == "After") {
					t.Fatal("implicit clock", x.Sel.Name)
				}
			}
			if x, ok := n.(*ast.Ident); ok && (x.Name == "Client" || x.Name == "DurableJournal" || x.Name == "Credentials") {
				t.Fatal("effect authority", x.Name)
			}
			return true
		})
	}
	c, f, j, _ := durableCloud(t)
	p, s, _, _, now := compatibilityFixture(t)
	compatibilityRebind(t, &p, &s, c.config, j.policy)
	before, e := os.ReadFile(filepath.Join(j.Store.Dir, "ledger.json"))
	if e != nil {
		t.Fatal(e)
	}
	f.mu.Lock()
	posts, deletes := f.postCount, f.deletes
	f.mu.Unlock()
	r, e := EvaluateCompatibility(p, s, c.config, j.policy, now)
	if e != nil || !r.MetadataMatch {
		t.Fatal(r, e)
	}
	after, e := os.ReadFile(filepath.Join(j.Store.Dir, "ledger.json"))
	if e != nil || !bytes.Equal(before, after) {
		t.Fatal("accounting changed", e)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.postCount != posts || f.deletes != deletes {
		t.Fatal("provider effects")
	}
}
