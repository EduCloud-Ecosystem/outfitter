// Package qualification exists only in disposable source exports. It does not
// ship with Outfitter. Hooks observe or fail named production I/O boundaries.
package qualification

import "os"

var Hook func(stage, path string) error
var LowerRecordLimit int

func Gate(stage, path string) error {
	if Hook != nil {
		return Hook(stage, path)
	}
	return nil
}

func Sync(f *os.File) error {
	if err := Gate("sync", f.Name()); err != nil {
		return err
	}
	return f.Sync()
}

func Rename(old, new string) error {
	if err := Gate("rename", old); err != nil {
		return err
	}
	return os.Rename(old, new)
}

func RecordLimit(actual int) int {
	if LowerRecordLimit > 0 && LowerRecordLimit < actual {
		return LowerRecordLimit
	}
	return actual
}
