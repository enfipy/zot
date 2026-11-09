//go:build linux

package local

import (
	"errors"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

// cloneFile makes out share in's blocks with FICLONE, and copies in's bytes when the filesystem
// cannot clone, or in and out lie on different filesystems.
func cloneFile(out, in *os.File) error {
	err := unix.IoctlFileClone(int(out.Fd()), int(in.Fd()))
	if err == nil || !cloneUnsupported(err) {
		return err
	}

	_, err = io.Copy(out, in)

	return err
}

func cloneUnsupported(err error) bool {
	return errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.EXDEV) ||
		errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOTTY) || errors.Is(err, unix.ENOSYS)
}
