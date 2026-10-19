package assessment

import (
	"encoding/json"
	"errors"
	"io"
)

// Serve is a pure bounded stdin/stdout transform. No state, ledger, provider,
// credentials, filesystem paths or process dispatch are accepted by this API.
func Serve(in io.Reader, out io.Writer) error {
	raw, e := io.ReadAll(io.LimitReader(in, MaxRequestBytes+1))
	if e != nil || len(raw) > MaxRequestBytes {
		return errors.New("INVALID_ASSESSMENT_REQUEST")
	}
	var fields map[string]json.RawMessage
	if strictJSON(raw, &fields, MaxRequestBytes) != nil || fields == nil {
		return errors.New("INVALID_ASSESSMENT_REQUEST")
	}
	var operation string
	if json.Unmarshal(fields["operation"], &operation) != nil {
		return errors.New("INVALID_ASSESSMENT_REQUEST")
	}
	keys := []string{"operation", "manifest", "receipts"}
	if operation == "project_legacy" {
		keys = []string{"operation", "manifest", "binding", "cleanup_receipt_id", "source_receipt"}
	} else if operation != "validate" {
		return errors.New("INVALID_ASSESSMENT_OPERATION")
	}
	if len(fields) != len(keys) {
		return errors.New("INVALID_ASSESSMENT_REQUEST")
	}
	if _, e := objectFields(raw, keys, nil); e != nil {
		return errors.New("INVALID_ASSESSMENT_REQUEST")
	}
	m, e := DecodeManifest(fields["manifest"])
	if e != nil {
		return e
	}
	var result Result
	if operation == "validate" {
		var s ReceiptSet
		s, e = DecodeReceipts(fields["receipts"])
		if e == nil {
			result, e = Validate(m, s)
		}
	} else {
		var binding Binding
		var cleanupID string
		if decodeClosed(fields["binding"], &binding, MaxReceiptBytes) != nil || json.Unmarshal(fields["cleanup_receipt_id"], &cleanupID) != nil {
			return errors.New("INVALID_ASSESSMENT_BINDING")
		}
		var source LegacyReceipt
		source, e = DecodeLegacy(fields["source_receipt"])
		if e == nil {
			result, e = ProjectLegacy(m, binding, cleanupID, source)
		}
	}
	if e != nil {
		return e
	}
	raw, e = Canonical(result)
	if e != nil || len(raw) > MaxOutputBytes {
		return errors.New("RECEIPT_OUTPUT_LIMIT")
	}
	if n, e := out.Write(raw); e != nil || n != len(raw) {
		return errors.New("RECEIPT_OUTPUT_FAILED")
	}
	return nil
}
