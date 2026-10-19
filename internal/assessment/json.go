// Package assessment projects existing attempt receipts into a bounded,
// explicitly unverified assessment wire contract. It never dispatches work.
package assessment

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"regexp"
	"strconv"
	"unicode/utf8"
)

var jsonKey = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
var jsonInteger = regexp.MustCompile(`^-?(0|[1-9][0-9]*)$`)

// strictJSON rejects duplicates before Go's ordinary decoder can discard them.
// Integer-only numbers make the shared Python/Go canonical representation exact.
func strictJSON(raw []byte, out any, limit int) error {
	if len(raw) == 0 || len(raw) > limit || !utf8.Valid(raw) || !json.Valid(raw) {
		return errors.New("INVALID_JSON")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if jsonTokens(d, 0) != nil {
		return errors.New("INVALID_JSON")
	}
	if _, e := d.Token(); e != io.EOF {
		return errors.New("INVALID_JSON")
	}
	d = json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(out) != nil {
		return errors.New("INVALID_JSON")
	}
	return nil
}

// decodeClosed also requires every emitted field and preserves explicit null.
// json.Unmarshal alone treats null booleans/integers as their zero values.
func decodeClosed(raw []byte, out any, limit int) error {
	if strictJSON(raw, out, limit) != nil {
		return errors.New("INVALID_RECEIPT_JSON")
	}
	typed, e := json.Marshal(out)
	if e != nil {
		return errors.New("INVALID_RECEIPT_JSON")
	}
	decode := func(data []byte) any {
		var value any
		d := json.NewDecoder(bytes.NewReader(data))
		d.UseNumber()
		_ = d.Decode(&value)
		return value
	}
	if !sameJSONShape(decode(raw), decode(typed)) {
		return errors.New("INVALID_RECEIPT_JSON")
	}
	return nil
}

func sameJSONShape(a, b any) bool {
	if reflect.TypeOf(a) != reflect.TypeOf(b) {
		return false
	}
	switch x := a.(type) {
	case map[string]any:
		y := b.(map[string]any)
		if len(x) != len(y) {
			return false
		}
		for key, value := range x {
			other, ok := y[key]
			if !ok || !sameJSONShape(value, other) {
				return false
			}
		}
	case []any:
		y := b.([]any)
		if len(x) != len(y) {
			return false
		}
		for i := range x {
			if !sameJSONShape(x[i], y[i]) {
				return false
			}
		}
	}
	return true
}

// Canonical emits recursively sorted JSON keys, integer tokens and no newline.
// Callers validate closed vocabularies before invoking this function.
func Canonical(value any) ([]byte, error) {
	raw, e := json.Marshal(value)
	if e != nil {
		return nil, errors.New("CANONICAL_ENCODING_FAILED")
	}
	var tree any
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if d.Decode(&tree) != nil {
		return nil, errors.New("CANONICAL_ENCODING_FAILED")
	}
	return json.Marshal(tree)
}

func hashCanonical(value any) (string, error) {
	raw, e := Canonical(value)
	if e != nil {
		return "", e
	}
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:]), nil
}

func jsonTokens(d *json.Decoder, depth int) error {
	if depth > 12 {
		return errors.New("JSON_DEPTH")
	}
	t, e := d.Token()
	if e != nil {
		return e
	}
	if n, ok := t.(json.Number); ok {
		if !jsonInteger.MatchString(string(n)) {
			return errors.New("INTEGER_REQUIRED")
		}
		value, e := strconv.ParseInt(string(n), 10, 64)
		if e != nil || value < -9007199254740991 || value > 9007199254740991 {
			return errors.New("INTEGER_RANGE")
		}
	}
	delim, ok := t.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]bool{}
		for d.More() {
			key, e := d.Token()
			if e != nil {
				return e
			}
			name, ok := key.(string)
			if !ok || !jsonKey.MatchString(name) || seen[name] {
				return errors.New("JSON_KEY")
			}
			seen[name] = true
			if e := jsonTokens(d, depth+1); e != nil {
				return e
			}
		}
	case '[':
		for d.More() {
			if e := jsonTokens(d, depth+1); e != nil {
				return e
			}
		}
	default:
		return errors.New("JSON_DELIMITER")
	}
	_, e = d.Token()
	return e
}

func objectFields(raw []byte, required []string, nullable map[string]bool) (map[string]json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || fields == nil {
		return nil, errors.New("OBJECT_REQUIRED")
	}
	for _, key := range required {
		value, exists := fields[key]
		if !exists || bytes.Equal(bytes.TrimSpace(value), []byte("null")) && !nullable[key] {
			return nil, errors.New("FIELD_REQUIRED")
		}
	}
	return fields, nil
}
