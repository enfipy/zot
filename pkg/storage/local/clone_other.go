//go:build !linux

package local

import (
	"io"
	"os"
)

// cloneFile copies in's bytes to out: only Linux's FICLONE is used to share blocks.
func cloneFile(out, in *os.File) error {
	_, err := io.Copy(out, in)

	return err
}
