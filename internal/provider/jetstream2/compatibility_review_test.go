package jetstream2

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestCompatibilityReviewProtocolZeroIsNotAny(t *testing.T) {
	p, s, c, policy, now := compatibilityFixture(t)
	p.Rules[0].Protocol = "any"
	p.Rules[0].PortMin = nil
	p.Rules[0].PortMax = nil
	p.AllowBroadEgress = true
	s.Rules.value[0].Protocol = CompatibilityValue(CompatibilityProtocol("0"))
	s.Rules.value[0].PortMin = CompatibilityNull[int64]()
	s.Rules.value[0].PortMax = CompatibilityNull[int64]()
	compatibilityRebind(t, &p, &s, c, policy)
	report, err := EvaluateCompatibility(p, s, c, policy, now)
	if err != nil {
		t.Fatal(err)
	}
	if report.MetadataMatch {
		t.Fatal("protocol 0 incorrectly matched any")
	}
}
func TestCompatibilityReviewNullRuleObjectRejected(t *testing.T) {
	_, s, _, _, _ := compatibilityFixture(t)
	raw, _ := json.Marshal(s)
	var m map[string]json.RawMessage
	json.Unmarshal(raw, &m)
	m["rules"] = json.RawMessage(`[null]`)
	raw, _ = json.Marshal(m)
	value, err := DecodeCompatibilitySnapshot(raw)
	if err == nil {
		out, _ := json.Marshal(value)
		t.Fatalf("null rule object accepted and rewritten: %s", out)
	}
}
func TestCompatibilityReviewUTCYearBounds(t *testing.T) {
	for _, stamp := range []string{"2000-01-01T00:30:00+01:00", "2100-12-31T23:30:00-01:00"} {
		t.Run(stamp, func(t *testing.T) {
			outside, err := time.Parse(time.RFC3339, stamp)
			if err != nil {
				t.Fatal(err)
			}
			for _, field := range []string{"now", "not_before", "expires_at", "observed_at", "approval"} {
				t.Run(field, func(t *testing.T) {
					p, s, c, policy, now := compatibilityFixture(t)
					switch field {
					case "now":
						now = outside
					case "not_before":
						p.NotBefore = outside
					case "expires_at":
						p.ExpiresAt = outside
					case "observed_at":
						s.ObservedAt = outside
					case "approval":
						policy.ApprovalExpiresAt = outside
					}
					report, err := EvaluateCompatibility(p, s, c, policy, now)
					compatibilityClosed(t, report)
					if err == nil || report.MetadataMatch {
						t.Fatal("out-of-range UTC instant accepted")
					}
				})
			}
		})
	}
	for _, stamp := range []string{"1999-12-31T23:30:00-01:00", "2101-01-01T00:30:00+01:00"} {
		inside, err := time.Parse(time.RFC3339, stamp)
		if err != nil {
			t.Fatal(err)
		}
		if !compatibilityTime(inside) {
			t.Fatal("in-range UTC instant rejected")
		}
	}
}
func TestCompatibilityReviewProtocolAliasMissingFieldDigest(t *testing.T) {
	p, s, c, policy, now := compatibilityFixture(t)
	s.Rules.value[0].RemoteGroup = Observed[string]{}
	a, err := EvaluateCompatibility(p, s, c, policy, now)
	if err != nil {
		t.Fatal(err)
	}
	s.Rules.value[0].Protocol = CompatibilityValue(CompatibilityProtocol("6"))
	b, err := EvaluateCompatibility(p, s, c, policy, now)
	if err != nil {
		t.Fatal(err)
	}
	if a.SnapshotDigest != b.SnapshotDigest {
		t.Fatalf("same supported alias different digest on incomplete rule: %s vs %s", a.SnapshotDigest, b.SnapshotDigest)
	}
}
func TestCompatibilityReviewRejectInvalidUTF8(t *testing.T) {
	_, s, _, _, _ := compatibilityFixture(t)
	raw, _ := json.Marshal(s)
	raw = bytes.Replace(raw, []byte(`"architecture":"x86_64"`), append([]byte(`"architecture":"`), 0xff, '"'), 1)
	if _, err := DecodeCompatibilitySnapshot(raw); err == nil {
		t.Fatal("invalid UTF8 accepted")
	}
}

func TestCompatibilityReviewProtocolWireStates(t *testing.T) {
	for _, wire := range []string{`0`, `"0"`, `null`, `"any"`} {
		t.Run(wire, func(t *testing.T) {
			p, s, c, policy, now := compatibilityFixture(t)
			p.Rules[0].Protocol = "any"
			p.Rules[0].PortMin = nil
			p.Rules[0].PortMax = nil
			p.AllowBroadEgress = true
			s.Rules.value[0].Protocol = CompatibilityValue(CompatibilityProtocol("any"))
			s.Rules.value[0].PortMin = CompatibilityNull[int64]()
			s.Rules.value[0].PortMax = CompatibilityNull[int64]()
			compatibilityRebind(t, &p, &s, c, policy)
			raw, _ := json.Marshal(s)
			raw = bytes.Replace(raw, []byte(`"protocol":"any"`), []byte(`"protocol":`+wire), 1)
			decoded, err := DecodeCompatibilitySnapshot(raw)
			if err != nil {
				t.Fatal(err)
			}
			report, err := EvaluateCompatibility(p, decoded, c, policy, now)
			if err != nil {
				t.Fatal(err)
			}
			check := compatibilityCheck(t, report, "security.rules")
			if wire == `0` || wire == `"0"` {
				if report.MetadataMatch || check.Status != "UNKNOWN" || check.Reason != "UNSUPPORTED_PROTOCOL" {
					t.Fatal(check)
				}
				p.Rules[0].Protocol = "0"
				if _, err := CompatibilityProfileDigest(p); err == nil {
					t.Fatal("profile accepted protocol zero")
				}
			} else if !report.MetadataMatch {
				t.Fatal(check)
			}
			compatibilityClosed(t, report)
		})
	}
}
func TestCompatibilityReviewRuleContainerStates(t *testing.T) {
	for _, wire := range []string{`null`, `[]`, `[{}]`, `[null]`, `[{},null]`, `[1]`, `["rule"]`} {
		t.Run(wire, func(t *testing.T) {
			p, s, c, policy, now := compatibilityFixture(t)
			raw, _ := json.Marshal(s)
			var m map[string]json.RawMessage
			json.Unmarshal(raw, &m)
			m["rules"] = json.RawMessage(wire)
			raw, _ = json.Marshal(m)
			decoded, err := DecodeCompatibilitySnapshot(raw)
			malformed := wire == `[null]` || wire == `[{},null]` || wire == `[1]` || wire == `["rule"]`
			if malformed {
				if err == nil || !reflect.DeepEqual(decoded, CompatibilitySnapshot{}) {
					t.Fatal("malformed rule object survived", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			report, err := EvaluateCompatibility(p, decoded, c, policy, now)
			if err != nil {
				t.Fatal(err)
			}
			want := "UNKNOWN"
			if wire == `[]` {
				want = "MISMATCH"
			}
			if compatibilityCheck(t, report, "security.rules").Status != want {
				t.Fatal(report)
			}
		})
	}
}
func TestCompatibilityReviewTypedInvalidUTF8(t *testing.T) {
	bad := string([]byte{0xff})
	for _, field := range []string{"snapshot_string", "snapshot_protocol", "snapshot_selector", "profile_protocol", "config", "policy"} {
		t.Run(field, func(t *testing.T) {
			p, s, c, policy, now := compatibilityFixture(t)
			switch field {
			case "snapshot_string":
				s.Architecture = CompatibilityValue(bad)
			case "snapshot_protocol":
				s.Rules.value[0].Protocol = CompatibilityValue(CompatibilityProtocol(bad))
			case "snapshot_selector":
				s.Rules.value[0].RemoteGroup = CompatibilityValue(bad)
			case "profile_protocol":
				p.Rules[0].Protocol = CompatibilityProtocol(bad)
			case "config":
				c.Region = bad
				policy.Config = c
			case "policy":
				policy.SupervisorID = bad
			}
			report, err := EvaluateCompatibility(p, s, c, policy, now)
			if err == nil || report.MetadataMatch {
				t.Fatal("invalid typed UTF8 survived")
			}
			compatibilityClosed(t, report)
		})
	}
	p, _, _, _, _ := compatibilityFixture(t)
	raw, _ := json.Marshal(p)
	raw = bytes.Replace(raw, []byte(`"architecture":"x86_64"`), append([]byte(`"architecture":"`), 0xff, '"'), 1)
	if _, err := DecodeCompatibilityProfile(raw); err == nil {
		t.Fatal("invalid profile UTF8 survived")
	}
}
