package jetstream2

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"time"
)

// Decoders accept only the closed Outfitter fixture envelope, not raw provider
// responses. Provider capture and transport are deliberately absent.
func DecodeCompatibilityProfile(raw []byte) (CompatibilityProfile, error) {
	var p CompatibilityProfile
	if compatibilityJSON(raw, &p) != nil || requiredCompatibilityFields(raw, reflect.TypeOf(p)) != nil {
		return CompatibilityProfile{}, errors.New("INVALID_PROFILE_JSON")
	}
	n, e := compatibilityProfile(p)
	if e != nil {
		return CompatibilityProfile{}, e
	}
	return n, nil
}
func DecodeCompatibilitySnapshot(raw []byte) (CompatibilitySnapshot, error) {
	var s CompatibilitySnapshot
	if compatibilityJSON(raw, &s) != nil {
		return CompatibilitySnapshot{}, errors.New("INVALID_SNAPSHOT_JSON")
	}
	n, e := compatibilitySnapshot(s)
	if e != nil {
		return CompatibilitySnapshot{}, e
	}
	return n, nil
}
func compatibilityJSON(raw []byte, out any) error {
	if len(raw) == 0 || len(raw) > compatibilityBytes || !json.Valid(raw) {
		return errors.New("INVALID_JSON")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if compatibilityTokens(d, 0) != nil {
		return errors.New("INVALID_JSON")
	}
	if _, e := d.Token(); e != io.EOF {
		return errors.New("TRAILING_JSON")
	}
	d = json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(out) != nil {
		return errors.New("INVALID_JSON")
	}
	return nil
}
func compatibilityTokens(d *json.Decoder, depth int) error {
	if depth > 16 {
		return errors.New("JSON_DEPTH")
	}
	t, e := d.Token()
	if e != nil {
		return e
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
			s, ok := key.(string)
			if !ok || seen[s] || strings.ToLower(s) != s {
				return errors.New("JSON_KEY")
			}
			seen[s] = true
			if e = compatibilityTokens(d, depth+1); e != nil {
				return e
			}
		}
	case '[':
		for d.More() {
			if e := compatibilityTokens(d, depth+1); e != nil {
				return e
			}
		}
	default:
		return errors.New("JSON_DELIMITER")
	}
	_, e = d.Token()
	return e
}

// Profile fields are all required. Observation fields on snapshots intentionally
// use a different contract: omission is UNKNOWN, while explicit null is retained.
func requiredCompatibilityFields(raw []byte, t reflect.Type) error {
	raw = bytes.TrimSpace(raw)
	if t.Kind() == reflect.Pointer {
		if bytes.Equal(raw, []byte("null")) {
			return nil
		}
		return requiredCompatibilityFields(raw, t.Elem())
	}
	if bytes.Equal(raw, []byte("null")) {
		return errors.New("NULL_PROFILE_FIELD")
	}
	if t == reflect.TypeOf(time.Time{}) {
		return nil
	}
	switch t.Kind() {
	case reflect.Struct:
		var fields map[string]json.RawMessage
		if json.Unmarshal(raw, &fields) != nil {
			return errors.New("INVALID_PROFILE_OBJECT")
		}
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			name := strings.Split(f.Tag.Get("json"), ",")[0]
			b, ok := fields[name]
			if !ok {
				return errors.New("MISSING_PROFILE_FIELD")
			}
			if e := requiredCompatibilityFields(b, f.Type); e != nil {
				return e
			}
		}
	case reflect.Slice:
		var fields []json.RawMessage
		if json.Unmarshal(raw, &fields) != nil {
			return errors.New("INVALID_PROFILE_ARRAY")
		}
		for _, b := range fields {
			if e := requiredCompatibilityFields(b, t.Elem()); e != nil {
				return e
			}
		}
	}
	return nil
}
