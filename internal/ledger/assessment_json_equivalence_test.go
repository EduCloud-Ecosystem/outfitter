package ledger

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"

	x "github.com/EduCloud-Ecosystem/outfitter/internal/assessmentexec"
	"github.com/EduCloud-Ecosystem/outfitter/internal/owneddata"
)

// Frozen pre-change two-pass read and complete payload decoder. Neither
// reference uses the extracted lexer; all partial Data and error-order paths
// remain observable, including mutation by the old duplicate ClosedJSON pass.
var metadataLegacyPayloadIntegerToken = regexp.MustCompile(`^(0|[1-9][0-9]{0,15})$`)

func metadataLegacyPayloadDecodeClosed(raw []byte, out any, limit int) error {
	if len(raw) == 0 || len(raw) > limit {
		return x.ErrInvalid
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
			return x.ErrInvalid
		}
		switch v := t.(type) {
		case json.Delim:
			if v == '{' || v == '[' {
				depth++
				if depth > 24 {
					return x.ErrInvalid
				}
			} else {
				depth--
			}
		case json.Number:
			if !metadataLegacyPayloadIntegerToken.MatchString(string(v)) {
				return x.ErrInvalid
			}
			n, e := strconv.ParseInt(string(v), 10, 64)
			if e != nil || !owneddata.Uint(n) {
				return x.ErrInvalid
			}
		}
	}
	if depth != 0 || owneddata.ClosedJSON(raw, out, limit) != nil {
		return x.ErrInvalid
	}
	return nil
}

func metadataLegacyStoreRead(s *Store) (Data, error) {
	var d Data
	p := filepath.Join(s.Dir, "ledger.json")
	st, e := os.Lstat(p)
	if e != nil {
		return d, e
	}
	if !st.Mode().IsRegular() || st.Size() > 64<<20 {
		return d, errors.New("invalid ledger file")
	}
	f, e := os.OpenFile(p, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if e != nil {
		return d, e
	}
	defer f.Close()
	opened, e := f.Stat()
	if e != nil || !opened.Mode().IsRegular() || !os.SameFile(st, opened) {
		return d, owneddata.ErrUnresolved
	}
	raw, e := io.ReadAll(io.LimitReader(f, maxLedgerBytes+1))
	if e != nil || len(raw) > maxLedgerBytes {
		return d, errors.New("invalid ledger file")
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if e = dec.Decode(&d); e != nil {
		return d, fmt.Errorf("invalid ledger: %w", e)
	}
	if dec.Decode(new(any)) != io.EOF {
		return d, errors.New("trailing ledger data")
	}
	if d.Version == 6 || d.Version == 7 {
		if e := owneddata.ClosedJSON(raw, &d, maxLedgerBytes); e != nil {
			return d, e
		}
	}
	if d.Version == 7 {
		var envelope struct {
			AssessmentExecution json.RawMessage `json:"assessment_execution"`
		}
		if json.Unmarshal(raw, &envelope) != nil || metadataLegacyPayloadDecodeClosed(envelope.AssessmentExecution, d.AssessmentExecution, maxLedgerBytes) != nil {
			return d, x.ErrInvalid
		}
	}
	if e = validate(d); e != nil {
		return d, e
	}
	if d.Version >= 2 && d.Authority.StatePath != s.Dir {
		return d, errors.New("ledger belongs to a different state path; use held restore")
	}
	if d.Version == 6 {
		if e := s.checkOwnedIdentity(d.OwnedDataIdentity); e != nil {
			return d, e
		}
	}
	if d.Version == 7 {
		if e := s.checkOwnedIdentity(executionIdentity(d)); e != nil {
			return d, e
		}
	}
	return d, nil
}

func metadataReadErrorClass(e error) string {
	if e == nil {
		return "none"
	}
	if errors.Is(e, x.ErrInvalid) {
		return "execution_invalid"
	}
	if errors.Is(e, owneddata.ErrInvalid) {
		return "closed_invalid"
	}
	if errors.Is(e, owneddata.ErrUnresolved) {
		return "identity_unresolved"
	}
	var typeError *json.UnmarshalTypeError
	if errors.As(e, &typeError) {
		return "typed_json"
	}
	var syntaxError *json.SyntaxError
	if errors.As(e, &syntaxError) {
		return "syntax_json"
	}
	return "other"
}
func metadataCompareReads(t *testing.T, s *Store, raw []byte, class string) {
	t.Helper()
	path := filepath.Join(s.Dir, "ledger.json")
	if e := os.WriteFile(path, raw, 0600); e != nil {
		t.Fatal("fixture write failed")
	}
	for i := 0; i < 3; i++ {
		want, we := metadataLegacyStoreRead(s)
		got, ge := s.read()
		if metadataReadErrorClass(we) != class || metadataReadErrorClass(ge) != class || !reflect.DeepEqual(want, got) {
			t.Fatal("Store acceptance, error category/order or partial Data differs")
		}
		// Internal comparison only: never print path-bearing OS/parser errors.
		if (we == nil) != (ge == nil) || we != nil && (reflect.TypeOf(we) != reflect.TypeOf(ge) || we.Error() != ge.Error()) {
			t.Fatal("Store exact error differs from independent baseline")
		}
		after, e := os.ReadFile(path)
		if e != nil || !bytes.Equal(raw, after) {
			t.Fatal("Store read changed fixture bytes")
		}
	}
}
func TestMetadataStoreReadMatchesIndependentTwoPass(t *testing.T) {
	s, _ := freshExecutionAuthority(t)
	fixture, e := os.ReadFile("../assessmentexec/testdata/fresh-scope.literal.json")
	if e != nil {
		t.Fatal("scope literal unavailable")
	}
	var row x.ExecutionScope
	if json.Unmarshal(fixture, &row) != nil || row.Validate() != nil || !initialScope(row) {
		t.Fatal("scope literal is not an initial metadata-only scope")
	}
	// Only the ordinary initial-scope admission is used. No resource, attempt,
	// positive completion, data payload or opaque owner is manufactured.
	if e = s.Update(func(d *Data) error { d.AssessmentExecution.Scopes[row.ScopeID] = row; return nil }); e != nil {
		t.Fatal("initial metadata scope admission failed")
	}
	path := filepath.Join(s.Dir, "ledger.json")
	raw, e := os.ReadFile(path)
	if e != nil || len(raw) > 65536 {
		t.Fatal("small fixture read failed")
	}
	metadataCompareReads(t, s, raw, "none")
	metadataCompareReads(t, s, append(append([]byte(" "), raw...), '\n'), "none")
	mutations := []struct{ from, to, class string }{
		{`"generation":1`, `"generation":1,"generation":1`, "closed_invalid"},
		{`"generation":1`, `"generation":1,"\u0067eneration":1`, "closed_invalid"},
		{`"generation":1`, `"Generation":1`, "closed_invalid"},
		{`"generation":1`, `"generation":1,"unknown_metadata":0`, "other"},
		{`"generation":1,`, ``, "closed_invalid"},
		{`"generation":1`, `"generation":null`, "closed_invalid"},
		{`"generation":1`, `"generation":true`, "typed_json"},
		{`"generation":1`, `"generation":1.0`, "typed_json"},
		{`"generation":1`, `"generation":1e0`, "typed_json"},
		{`"generation":1`, `"generation":9223372036854775808`, "typed_json"},
		{`"tick_high_water_ms":0`, `"tick_high_water_ms":-0`, "execution_invalid"},
		{`"tick_high_water_ms":0`, `"tick_high_water_ms":-1`, "execution_invalid"},
		{`"tick_high_water_ms":0`, `"tick_high_water_ms":9007199254740992`, "execution_invalid"},
		{`"tick_high_water_ms":0`, `"tick_high_water_ms":9007199254740991`, "none"},
		{`"lease_close_uncertain":false`, `"lease_close_uncertain":null`, "closed_invalid"},
		{`"assessment_execution":{`, `"assessment_execution":null,"discarded_authority":{`, "other"},
		{`"state":"preparing"`, `"state":"unknown-state"`, "execution_invalid"},
	}
	for _, m := range mutations {
		bad := bytes.Replace(raw, []byte(m.from), []byte(m.to), 1)
		if bytes.Equal(raw, bad) {
			t.Fatal("targeted Store mutation missed")
		}
		metadataCompareReads(t, s, bad, m.class)
	}
	// Missing/null authority is checked separately with the actual whole-Data
	// encoder shape; omission is allowed by Data's tag but semantic v7 refuses.
	var data Data
	if json.Unmarshal(raw, &data) != nil {
		t.Fatal("fixture decode failed")
	}
	data.AssessmentExecution = nil
	missing, e := json.Marshal(data)
	if e != nil || bytes.Equal(raw, missing) {
		t.Fatal("missing-authority mutation failed")
	}
	metadataCompareReads(t, s, missing, "execution_invalid")
	null := bytes.Replace(missing, []byte(`{`), []byte(`{"assessment_execution":null,`), 1)
	metadataCompareReads(t, s, null, "closed_invalid")
	invalidUTF8 := bytes.Replace(raw, []byte(`"assurance":"synthetic"`), append([]byte(`"assurance":"`), append([]byte{0xff}, []byte(`"`)...)...), 1)
	if bytes.Equal(invalidUTF8, raw) {
		t.Fatal("UTF8 mutation missed")
	}
	metadataCompareReads(t, s, invalidUTF8, "closed_invalid")
	metadataCompareReads(t, s, append(append([]byte(nil), raw...), []byte(" {}")...), "other")
	trimmed := bytes.TrimSpace(raw)
	metadataCompareReads(t, s, trimmed[:len(trimmed)-1], "other")
	// Current state-path and original opened authority identity checks stay
	// after semantic validation. These negative reads cannot repair/adopt data.
	if json.Unmarshal(raw, &data) != nil {
		t.Fatal("fixture restore failed")
	}
	data.Authority.StatePath = filepath.Join(s.Dir, "different")
	wrongPath, _ := json.Marshal(data)
	metadataCompareReads(t, s, wrongPath, "other")
	if json.Unmarshal(raw, &data) != nil {
		t.Fatal("fixture restore failed")
	}
	if data.AssessmentExecution.Identity.Directory == data.AssessmentExecution.Identity.JournalLease {
		t.Fatal("real directory and lease identities unexpectedly equal")
	}
	data.AssessmentExecution.Identity.Directory = data.AssessmentExecution.Identity.JournalLease
	wrongIdentity, _ := json.Marshal(data)
	metadataCompareReads(t, s, wrongIdentity, "identity_unresolved")
	metadataCompareReads(t, s, raw, "none")
	// Store's 64MiB preflight is unchanged source, not exercised by a huge
	// allocation here. Tiny exact-limit boundaries are covered by codec units.
}
