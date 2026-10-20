package assessmentexec

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/EduCloud-Ecosystem/outfitter/internal/owneddata"
)

// The complete pre-change decoder is copied independently, including its
// ClosedJSON call. The lexical reference below removes only that final call;
// neither reference invokes CheckIntegerDepthJSON or the optimized decoder.
var legacyMetadataIntegerToken = regexp.MustCompile(`^(0|[1-9][0-9]{0,15})$`)

func legacyMetadataDecodeClosed(raw []byte, out any, limit int) error {
	if len(raw) == 0 || len(raw) > limit {
		return ErrInvalid
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	depth := 0
	for {
		t, e := d.Token()
		if e == io.EOF {
			break
		}
		if e != nil {
			return ErrInvalid
		}
		switch v := t.(type) {
		case json.Delim:
			if v == '{' || v == '[' {
				depth++
				if depth > 24 {
					return ErrInvalid
				}
			} else {
				depth--
			}
		case json.Number:
			if !legacyMetadataIntegerToken.MatchString(string(v)) {
				return ErrInvalid
			}
			n, e := strconv.ParseInt(string(v), 10, 64)
			if e != nil || !owneddata.Uint(n) {
				return ErrInvalid
			}
		}
	}
	if depth != 0 || owneddata.ClosedJSON(raw, out, limit) != nil {
		return ErrInvalid
	}
	return nil
}

func legacyMetadataTokenScan(raw []byte, limit int) error {
	if len(raw) == 0 || len(raw) > limit {
		return ErrInvalid
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	depth := 0
	for {
		t, e := d.Token()
		if e == io.EOF {
			break
		}
		if e != nil {
			return ErrInvalid
		}
		switch v := t.(type) {
		case json.Delim:
			if v == '{' || v == '[' {
				depth++
				if depth > 24 {
					return ErrInvalid
				}
			} else {
				depth--
			}
		case json.Number:
			if !legacyMetadataIntegerToken.MatchString(string(v)) {
				return ErrInvalid
			}
			n, e := strconv.ParseInt(string(v), 10, 64)
			if e != nil || !owneddata.Uint(n) {
				return ErrInvalid
			}
		}
	}
	if depth != 0 {
		return ErrInvalid
	}
	return nil
}

func metadataDecodePair(t *testing.T, raw []byte, limit int, makeOut func() any) bool {
	t.Helper()
	before := append([]byte(nil), raw...)
	want, got := makeOut(), makeOut()
	we := legacyMetadataDecodeClosed(raw, want, limit)
	ge := DecodeClosed(raw, got, limit)
	if we != ge || !reflect.DeepEqual(want, got) {
		t.Fatal("full decoder acceptance, error or partial output differs")
	}
	if !bytes.Equal(before, raw) {
		t.Fatal("decoder changed raw bytes")
	}
	return ge == nil
}
func TestMetadataIntegerDepthLexerMatchesIndependentLegacy(t *testing.T) {
	vectors := []string{"", "0", "9007199254740991", "9007199254740992", "9223372036854775808", "-0", "-1", "1.0", "1e0", "00", "null", "true", "false", `"0"`, `{}`, `[]`, `{"a":0}`, `{"a":0,"a":1}`, `{"a":0,"\u0061":1}`, "0 1", "{} {}", "[0,1]", "[0,-0]", "["}
	for _, depth := range []int{24, 25, 64, 65} {
		vectors = append(vectors, strings.Repeat("[", depth)+"0"+strings.Repeat("]", depth))
	}
	for _, v := range vectors {
		raw := []byte(v)
		for _, limit := range []int{len(raw) - 1, len(raw), len(raw) + 1} {
			if legacyMetadataTokenScan(raw, limit) != CheckIntegerDepthJSON(raw, limit) {
				t.Fatal("lexical extraction changed old token semantics")
			}
			metadataDecodePair(t, raw, limit, func() any { var v any; return &v })
		}
	}
	// Multiple values and duplicate names are intentionally lexical-only
	// successes; closure is still provided by the independent full decoder.
	for _, raw := range [][]byte{[]byte("0 1"), []byte(`{"a":0,"a":1}`)} {
		if CheckIntegerDepthJSON(raw, len(raw)) != nil || metadataDecodePair(t, raw, len(raw), func() any { var v any; return &v }) {
			t.Fatal("lexical helper was promoted to full closure")
		}
	}
	deep := []byte(strings.Repeat("[", 25) + "0" + strings.Repeat("]", 25))
	var whole any
	if owneddata.ClosedJSON(deep, &whole, len(deep)) != nil || CheckIntegerDepthJSON(deep, len(deep)) == nil {
		t.Fatal("whole-data depth64 versus payload depth24 distinction lost")
	}
}

func TestMetadataLiteralPublicDecodersMatchIndependentLegacy(t *testing.T) {
	s := executionFixture(t)
	binding, e := json.Marshal(s.Binding())
	if e != nil {
		t.Fatal("binding fixture failed")
	}
	slot, e := json.Marshal(s.Slots[0])
	if e != nil {
		t.Fatal("slot fixture failed")
	}
	control := []byte(fmt.Sprintf(`{"schema":"outfitter.assessment-execution-control/v1","operation":"status","binding":%s,"expected_generation":null,"admitting_owner_epoch":null,"writer_epoch":null,"manifest":null,"declared_upload_ceiling_bytes":null,"expires_at":null,"content_length":null,"slot":null,"handoff_nonce":null,"reason":null}`, binding))
	status := []byte(fmt.Sprintf(`{"schema":"outfitter.assessment-execution-status/v1","operation":"status","code":"OK","binding":%s,"generation":1,"state":"preparing","deny_latched":false,"reason":"none","expires_at":"2026-10-20T02:00:00Z","declared_upload_ceiling_bytes":5000000000,"sealed_length_bytes":null,"writer_epoch":"77777777777777777777777777777777","watch_epoch":null,"admitting_owner_epoch":"88888888888888888888888888888888","clock_epoch":"99999999999999999999999999999999","deadline_tick_ms":7200000,"writer_started":false,"upload_complete":false,"content_close_confirmed":false,"content_close_uncertain":false,"execution_deny_latched":false,"liveness":"unknown","observations":[],"observations_saturated":false,"attempt":null,"trust":{"assurance":"synthetic","upload_authorized":false,"execution_authorized":false,"cleanup_trusted":false,"actual_resource_accepted":false}}`, binding))
	result := []byte(fmt.Sprintf(`{"schema":"outfitter.assessment-execution-result/v1","operation":"execute","code":"NOT_ADMITTED","disposition":"not_admitted","binding":%s,"slot":%s,"handoff_nonce":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","binding_digest":"%s","scope_generation":1,"attempt":null,"compute":{"reserved_cpu_seconds":0,"charged_cpu_seconds":0,"provenance":"not_admitted"},"accounting_unresolved":false,"normalized_receipts":null,"normalized_receipts_sha256":null,"trust":{"assurance":"synthetic","upload_authorized":false,"execution_authorized":false,"cleanup_trusted":false,"actual_resource_accepted":false}}`, binding, slot, AttemptBindingDigest(s.Binding(), s.Slots[0], strings.Repeat("a", 32))))
	errorReply := []byte(`{"schema":"outfitter.assessment-execution-error/v1","code":"ASSESSMENT_UNRESOLVED"}`)
	cases := []struct {
		raw []byte
		out func() any
	}{
		{control, func() any { return &Control{} }}, {status, func() any { return &Status{} }}, {result, func() any { return &Result{} }}, {errorReply, func() any { return &Error{} }},
	}
	for _, c := range cases {
		if !metadataDecodePair(t, c.raw, len(c.raw), c.out) {
			t.Fatal("literal closed public shape refused")
		}
		metadataDecodePair(t, c.raw, len(c.raw)-1, c.out)
		for _, mutation := range []func([]byte) []byte{
			func(b []byte) []byte { return append(append([]byte(nil), b...), []byte(" {}")...) },
			func(b []byte) []byte { return bytes.Replace(b, []byte(`"schema":`), []byte(`"Schema":`), 1) },
			func(b []byte) []byte { return append([]byte(`{"unknown":0,`), b[1:]...) },
			func(b []byte) []byte {
				return bytes.Replace(b, []byte(`{"schema":`), []byte(`{"schema":null,"\u0073chema":`), 1)
			},
			func(b []byte) []byte {
				return bytes.Replace(b, []byte(`{"schema":`), []byte(`{"schema":null,"schema":`), 1)
			},
			func(b []byte) []byte {
				return bytes.Replace(b, []byte(`"schema":"outfitter`), append([]byte(`"schema":"`), 0xff), 1)
			},
		} {
			bad := mutation(c.raw)
			if bytes.Equal(bad, c.raw) || metadataDecodePair(t, bad, len(bad), c.out) {
				t.Fatal("literal closed-shape negative missed or was accepted")
			}
		}
	}
	for _, replacement := range []string{"null", "true", "-0", "-1", "1.0", "1e0", "9007199254740992", "9223372036854775808"} {
		bad := bytes.Replace(status, []byte(`"generation":1`), []byte(`"generation":`+replacement), 1)
		if bytes.Equal(status, bad) || metadataDecodePair(t, bad, len(bad), func() any { return &Status{} }) {
			t.Fatal("raw numeric coercion was accepted")
		}
	}
	missing := bytes.Replace(status, []byte(`,"attempt":null`), nil, 1)
	if bytes.Equal(missing, status) || metadataDecodePair(t, missing, len(missing), func() any { return &Status{} }) {
		t.Fatal("missing nullable field was accepted")
	}
}
