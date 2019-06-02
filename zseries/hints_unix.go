// +build !darwin dragonfly freebsd linux netbsd openbsd

package zseries

import "golang.org/x/sys/unix"

func Fadvise(fd int, offset, length int64) error {
	if err := unix.Fadvise(fd, offset, length, unix.FADV_WILLNEED); err != nil {
		return err
	}
	return unix.Fadvise(fd, offset, length, unix.FADV_SEQUENTIAL)
}

func Fallocate(fd int, mode uint32, offset, length int64) error {
	return unix.Fallocate(fd, mode, offset, length)
}

func Ftruncate(fd int, length int64) error {
	return unix.Ftruncate(fd, length)
}
