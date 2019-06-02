// +build darwin

package zseries

import (
	"errors"
	"golang.org/x/sys/unix"
)

func Fadvise(fd int, offset, length int64) error {
	// no-op
	return errors.New("Unsupported")
}

func Fallocate(fd int, mode uint32, offset, length int64) error {
	// no-op
	return errors.New("Unsupported")
}

func Ftruncate(fd int, length int64) error {
	return unix.Ftruncate(fd, length)
}
