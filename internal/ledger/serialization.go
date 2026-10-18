package ledger

import (
	"encoding/json"
	"errors"

	"github.com/EduCloud-Ecosystem/outfitter/internal/provider"
)

const diagnosticPersistenceError = "execution diagnostics could not be persisted; authority accounting remains intact"

// A plan retains original strings and references, never the encoded blobs. Output
// preparation/publication each encodes only one diagnostic at a time.
type plannedOutput struct {
	stdout, stderr string
	digest         string
	bytes          int64
}
type preparedLedgerWrite struct {
	compact      Data
	outputs      map[string]plannedOutput
	maximumBytes int
}

func encodeOutput(stdout, stderr string) ([]byte, error) {
	data, e := json.Marshal(struct {
		Stdout string `json:"stdout"`
		Stderr string `json:"stderr"`
	}{stdout, stderr})
	if e != nil {
		return nil, e
	}
	if len(data) > (6<<20)+512 {
		return nil, errors.New("diagnostic encoding exceeds bound")
	}
	return data, nil
}
func planOutput(stdout, stderr string) (plannedOutput, error) {
	data, e := encodeOutput(stdout, stderr)
	if e != nil {
		return plannedOutput{}, e
	}
	return plannedOutput{stdout: stdout, stderr: stderr, digest: hash(data), bytes: int64(len(data))}, nil
}
func diagnosticFailure(r provider.Record) provider.Record {
	r.OutputSHA256 = ""
	r.OutputBytes = 0
	r.OutputError = diagnosticPersistenceError
	return r
}
func marshalRecord(r provider.Record, limit int) ([]byte, error) {
	encoded, e := json.Marshal(r)
	if e != nil {
		return nil, e
	}
	if len(encoded) > limit {
		return nil, errors.New("authority record exceeds reserved metadata capacity")
	}
	return encoded, nil
}
func marshalCompact(d Data, recordLimit, ledgerLimit int) ([]byte, error) {
	if e := validate(d); e != nil {
		return nil, e
	}
	for _, r := range d.Runs {
		if _, e := marshalRecord(r, recordLimit); e != nil {
			return nil, e
		}
	}
	encoded, e := json.Marshal(d)
	if e != nil {
		return nil, e
	}
	if len(encoded) > ledgerLimit {
		return nil, errors.New("authority store exceeds reserved metadata capacity")
	}
	return encoded, nil
}

// Limits are private test seams. Store.write always supplies the unchanged
// production constants. Preparation is pure: no paths, sidecars or temp files.
func prepareLedgerWrite(d Data, statePath string, recordLimit, ledgerLimit int) (preparedLedgerWrite, error) {
	var plan preparedLedgerWrite
	if e := validate(d); e != nil {
		return plan, e
	}
	if d.Version >= 2 && d.Authority.StatePath != statePath {
		return plan, errors.New("authority path mismatch")
	}
	success, failure, largest := d, d, d
	success.Runs = make(map[string]provider.Record, len(d.Runs))
	failure.Runs = make(map[string]provider.Record, len(d.Runs))
	largest.Runs = make(map[string]provider.Record, len(d.Runs))
	outputs := make(map[string]plannedOutput)
	for id, r := range d.Runs {
		okRecord, failedRecord := r, r
		okRecord.Stdout, okRecord.Stderr = "", ""
		failedRecord.Stdout, failedRecord.Stderr = "", ""
		if r.Stdout != "" || r.Stderr != "" {
			failedRecord = diagnosticFailure(failedRecord)
			output, e := planOutput(r.Stdout, r.Stderr)
			if e != nil {
				// Diagnostic preparation failure cannot block conservative accounting.
				okRecord = failedRecord
			} else {
				outputs[id] = output
				okRecord.OutputSHA256, okRecord.OutputBytes, okRecord.OutputError = output.digest, output.bytes, ""
			}
		}
		okBytes, e := marshalRecord(okRecord, recordLimit)
		if e != nil {
			return plan, e
		}
		failedBytes, e := marshalRecord(failedRecord, recordLimit)
		if e != nil {
			return plan, e
		}
		success.Runs[id], failure.Runs[id] = okRecord, failedRecord
		largest.Runs[id] = okRecord
		if len(failedBytes) > len(okBytes) {
			largest.Runs[id] = failedRecord
		}
	}
	// Only output reference/error fields differ. Their constraints are per-record;
	// all identity and aggregate budget inputs remain the validated originals.
	// Checking both forms therefore covers every mixed persistence outcome.
	if e := validate(success); e != nil {
		return plan, e
	}
	if e := validate(failure); e != nil {
		return plan, e
	}
	// Map keys, object syntax and all native/authority fields are invariant. Taking
	// the longer encoded record independently yields the maximum mixed JSON size.
	maximum, e := marshalCompact(largest, recordLimit, ledgerLimit)
	if e != nil {
		return plan, e
	}
	return preparedLedgerWrite{compact: success, outputs: outputs, maximumBytes: len(maximum)}, nil
}

func (s *Store) persistPlannedOutput(output plannedOutput) error {
	data, e := encodeOutput(output.stdout, output.stderr)
	if e != nil {
		return e
	}
	if int64(len(data)) != output.bytes || hash(data) != output.digest {
		return errors.New("diagnostic bytes differ from prepared authority plan")
	}
	return s.saveOutput(data, output.digest)
}
