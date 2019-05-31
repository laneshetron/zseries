package mmap

import (
	"os"
	"syscall"
	"unsafe"
)

// he protec
func Mprotect_ro(f *os.File, b []byte) error {
	return syscall.Mprotect(b, syscall.PROT_READ)
}

// but he also attac
func Mprotect_rw(f *os.File, b []byte) error {
	return syscall.Mprotect(b, syscall.PROT_READ|syscall.PROT_WRITE)
}

func Mmap(f *os.File, size int) ([]byte, error) {
	b, err := syscall.Mmap(int(f.Fd()), 0, size, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	// Advise the kernel of random access
	if err := madvise(b, syscall.MADV_RANDOM); err != nil {
		return b, err
	}
	return b, nil
}

// Copied from https://github.com/boltdb/bolt/blob/master/bolt_unix.go (who copied it from stdlib?)
func Madvise(b []byte, advice int) (err error) {
	_, _, e1 := syscall.Syscall(syscall.SYS_MADVISE, uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)), uintptr(advice))
	if e1 != 0 {
		err = e1
	}
	return
}
