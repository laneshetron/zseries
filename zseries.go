package main

import (
	"C"
	"os"
	"path"
	"strconv"
	"syscall"
	"time"
	"unsafe"
)

type handler struct {
	log     *os.File
	index   *os.File
	logData []byte
	logSize int
	written int
	offset  int
}

type ZFiles map[string]*handler

type ZSeries struct {
	Handlers ZFiles
}

// he protec
func mprotect_ro(f *os.File, b []byte) error {
	return syscall.Mprotect(b, syscall.PROT_READ)
}

// but he also attac
func mprotect_rw(f *os.File, b []byte) error {
	return syscall.Mprotect(b, syscall.PROT_READ|syscall.PROT_WRITE)
}

func mmap(f *os.File, size int) ([]byte, error) {
	b, err := syscall.Mmap(int(f.Fd()), 0, size, syscall.PROT_READ, syscall.MAP_SHARED|syscall.MAP_POPULATE)
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
func madvise(b []byte, advice int) (err error) {
	_, _, e1 := syscall.Syscall(syscall.SYS_MADVISE, uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)), uintptr(advice))
	if e1 != 0 {
		err = e1
	}
	return
}

func (z *ZSeries) getPath(key string) string {
	return path.Join(BASE_DIR, key, strconv.FormatInt(time.UnixNano(), 10))
}

func (z *ZSeries) initTopic(key string) error {
	if _, ok := z.Handlers[key]; !ok {
		err := os.MkdirAll(path.Join(BASE_DIR, key), os.ModePerm)
		if err != nil {
			return err
		}

		path := z.getPath(key)
		z.Handlers[key] = &handler{}
		z.Handlers[key].log, err = os.OpenFile(path+".log", os.O_RDWR|os.O_CREATE, os.ModePerm)
		if err != nil {
			return err
		}
		z.Handlers[key].index, err = os.OpenFile(path+".index", os.O_RDWR|os.O_CREATE, os.ModePerm)
		if err != nil {
			return err
		}

		// The integer math here should truncate any misalignment
		size := os.Getpagesize() * (FILE_SIZE / os.Getpagesize())
		z.Handlers[key].logData, err = mmap(z.Handlers[key].log, size)
		if err != nil {
			return err
		}
	}
	return nil
}

func (z *ZSeries) Write(key string, data []byte) int {
	err := z.initTopic(key)
	if err != nil {
		// TODO handle error
		return 0
	}
	h := z.Handlers[key]
	if len(data)+h.written > h.logSize {
		// close handlers & reopen
	}
}

const (
	BASE_DIR  = "_zseries"
	FILE_SIZE = 1048576
)

var z ZSeries

func init() {
	z = ZSeries{}
}

//export Write
func Write(key string, data []byte) int {
	return z.Write(key, data)
}

func main() {}
