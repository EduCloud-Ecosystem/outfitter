package owneddata

import (
	"bytes"
	"encoding/json"
	"io"
	"reflect"
	"unicode/utf8"
)

// ClosedJSON checks serialized internal metadata without interpreting any raw
// legacy/native payload. Typed numbers keep their existing decoder semantics;
// duplicate/missing/aliased keys and null-for-scalar coercions are refused.
func ClosedJSON(raw []byte, out any, limit int) error {
	if len(raw) == 0 || len(raw) > limit || !utf8.Valid(raw) || !json.Valid(raw) {
		return ErrInvalid
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if uniqueTokens(d, 0) != nil {
		return ErrInvalid
	}
	if _, e := d.Token(); e != io.EOF {
		return ErrInvalid
	}
	d = json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(out) != nil {
		return ErrInvalid
	}
	normal, e := json.Marshal(out)
	if e != nil {
		return ErrInvalid
	}
	decode := func(b []byte) any {
		var x any
		d := json.NewDecoder(bytes.NewReader(b))
		d.UseNumber()
		_ = d.Decode(&x)
		return x
	}
	if !sameShape(decode(raw), decode(normal)) {
		return ErrInvalid
	}
	return nil
}
func sameShape(a, b any) bool {
	if reflect.TypeOf(a) != reflect.TypeOf(b) {
		return false
	}
	switch x := a.(type) {
	case map[string]any:
		y := b.(map[string]any)
		if len(x) != len(y) {
			return false
		}
		for k, v := range x {
			w, ok := y[k]
			if !ok || !sameShape(v, w) {
				return false
			}
		}
	case []any:
		y := b.([]any)
		if len(x) != len(y) {
			return false
		}
		for i, v := range x {
			if !sameShape(v, y[i]) {
				return false
			}
		}
	}
	return true
}
func uniqueTokens(d *json.Decoder, depth int) error {
	if depth > 64 {
		return ErrInvalid
	}
	token, e := d.Token()
	if e != nil {
		return ErrInvalid
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]bool{}
		for d.More() {
			key, e := d.Token()
			if e != nil {
				return ErrInvalid
			}
			s, ok := key.(string)
			if !ok || seen[s] {
				return ErrInvalid
			}
			seen[s] = true
			if uniqueTokens(d, depth+1) != nil {
				return ErrInvalid
			}
		}
	case '[':
		for d.More() {
			if uniqueTokens(d, depth+1) != nil {
				return ErrInvalid
			}
		}
	default:
		return ErrInvalid
	}
	_, e = d.Token()
	return e
}
