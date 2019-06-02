// +build !darwin,!dragonfly,!freebsd,!linux,!netbsd,!openbsd

package zseries

import "errors"

func Fadvise(fd int, offset, length int64) error {
	// no-op
	return errors.New("Unsupported")
}

func Fallocate(fd int, mode uint32, offset, length int64) error {
	// no-op
	return errors.New("Unsupported")
}

func Ftruncate(fd int, length int64) error {
	// no-op
	return errors.New("Unsupported")
}
