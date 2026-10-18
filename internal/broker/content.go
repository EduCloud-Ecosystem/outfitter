package broker

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"

	"github.com/EduCloud-Ecosystem/outfitter/internal/provider"
)

// Verify the private copied bytes, not the original paths, before any creation.
func verifyContent(root string, expected []provider.ContentFile) error {
	entries := 0
	files := make(map[string]provider.ContentFile, len(expected))
	for _, f := range expected {
		files[f.Path] = f
	}
	err := filepath.WalkDir(root, func(p string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		entries++
		if entries > 1024 {
			return errors.New("snapshot entry bound exceeded")
		}
		if entry.IsDir() {
			return nil
		}
		rel, e := filepath.Rel(root, p)
		if e != nil {
			return e
		}
		f, ok := files[filepath.ToSlash(rel)]
		if !ok {
			return errors.New("unexpected snapshot file")
		}
		info, e := entry.Info()
		if e != nil || !info.Mode().IsRegular() || info.Size() != f.Size {
			return errors.New("snapshot size/type mismatch")
		}
		fd, e := syscall.Open(p, syscall.O_RDONLY|syscall.O_NOFOLLOW, 0)
		if e != nil {
			return e
		}
		file := os.NewFile(uintptr(fd), p)
		h := sha256.New()
		n, e := io.Copy(h, io.LimitReader(file, f.Size+1))
		file.Close()
		if e != nil || n != f.Size || hex.EncodeToString(h.Sum(nil)) != f.SHA256 {
			return errors.New("snapshot digest mismatch")
		}
		delete(files, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		return err
	}
	if len(files) != 0 {
		return errors.New("missing snapshot file")
	}
	return nil
}
