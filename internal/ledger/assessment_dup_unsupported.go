//go:build !((darwin || linux) && (amd64 || arm64))

package ledger

import (
	"errors"
	"os"
)

func assessmentDupCloseOnExec(*os.File) (*os.File, error) {
	return nil, errors.New("assessment atomic duplicate unsupported")
}
