package provider

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path"
	"strings"
	"unicode/utf8"
)

// ConsumerBinding is provenance within the existing reservation, not a second
// authority. Hashes attest bytes; they do not authenticate an instructor or Git.
type ConsumerBinding struct {
	Consumer             string        `json:"consumer"`
	JobID                string        `json:"job_id"`
	Attempt              int           `json:"attempt"`
	SubmissionCommit     string        `json:"submission_commit"`
	PolicyCommit         string        `json:"policy_commit"`
	PolicyManifestSHA256 string        `json:"policy_manifest_sha256"`
	InputFiles           []ContentFile `json:"input_files"`
	RecipeFiles          []ContentFile `json:"recipe_files"`
}
type ContentFile struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

func ConsumerRequestID(consumer, project, job string) (string, error) {
	if consumer != "cairn-synthetic-v1" || !ValidID(project) || !ValidID(job) {
		return "", errors.New("unsupported consumer or invalid project/job key")
	}
	raw, _ := json.Marshal([]string{"consumer-key-v1", consumer, project, job, "1"})
	h := sha256.Sum256(raw)
	return "consumer-" + hex.EncodeToString(h[:]), nil
}
func contentHex(s string, bytes int) bool {
	b, e := hex.DecodeString(s)
	return e == nil && len(b) == bytes && s == strings.ToLower(s)
}
func (r Request) validateConsumer() error {
	b := r.Consumer
	if b == nil {
		return nil
	}
	id, e := ConsumerRequestID(b.Consumer, r.ProjectID, b.JobID)
	if e != nil || id != r.RequestID || b.Attempt != 1 {
		return errors.New("invalid fixed consumer attempt identity")
	}
	for _, commit := range []string{b.SubmissionCommit, b.PolicyCommit} {
		if !contentHex(commit, 20) && !contentHex(commit, 32) {
			return errors.New("full lowercase submission and policy commits required")
		}
	}
	if !contentHex(b.PolicyManifestSHA256, 32) {
		return errors.New("policy manifest SHA-256 required")
	}
	if r.CPUs > 1 || r.MemoryMB > 256 || r.PIDsLimit > 64 || r.TimeoutSeconds > 30 || r.MaxOutputBytes > 65536 {
		return errors.New("limits exceed synthetic consumer profile")
	}
	if b.InputFiles == nil || b.RecipeFiles == nil || len(b.InputFiles)+len(b.RecipeFiles) > 128 {
		return errors.New("explicit bounded input and recipe manifests required")
	}
	var total int64
	for _, files := range [][]ContentFile{b.InputFiles, b.RecipeFiles} {
		previous := ""
		for _, f := range files {
			if f.Path <= previous || len(f.Path) > 256 || !utf8.ValidString(f.Path) || f.Path == "." || f.Path == ".." || path.IsAbs(f.Path) || path.Clean(f.Path) != f.Path || strings.HasPrefix(f.Path, "../") || strings.ContainsAny(f.Path, "\\\x00\r\n") || f.Size < 0 || f.Size > 16<<20 || !contentHex(f.SHA256, 32) {
				return errors.New("invalid, unsorted or duplicate content manifest entry")
			}
			total += f.Size
			previous = f.Path
		}
	}
	if total > 16<<20 {
		return errors.New("consumer content exceeds 16 MiB")
	}
	return nil
}
