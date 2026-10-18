package broker

import (
	"crypto/sha256"
	"encoding/hex"
	"github.com/EduCloud-Ecosystem/outfitter/internal/provider"
	"os"
	"path/filepath"
	"testing"
)

func TestCopiedSnapshotVerificationUsesLogicalPathsAndExactBytes(t *testing.T) {
	root := t.TempDir()
	os.Mkdir(filepath.Join(root, "a"), 0700)
	files := []provider.ContentFile{}
	// Global lexical order differs from WalkDir's directory traversal order.
	for _, name := range []string{"a-file", "a/file"} {
		data := []byte(name)
		h := sha256.Sum256(data)
		os.WriteFile(filepath.Join(root, name), data, 0600)
		files = append(files, provider.ContentFile{Path: name, Size: int64(len(data)), SHA256: hex.EncodeToString(h[:])})
	}
	if e := verifyContent(root, files); e != nil {
		t.Fatal(e)
	}
	os.WriteFile(filepath.Join(root, "a-file"), []byte("b-file"), 0600)
	if e := verifyContent(root, files); e == nil {
		t.Fatal("same-size replaced snapshot accepted")
	}
}
