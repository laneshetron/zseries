package main

import (
	"C"
	"bufio"
	"fmt"
	"os"
	"path"
	"strconv"
	"syscall"
	"time"
	"unsafe"

	"github.com/DataDog/zstd"
)

type handler struct {
	log     *os.File
	index   *os.File
	buffer  *bufio.Writer
	zWriter *zstd.Writer
	logData []byte
	logSize int
	written int
	offset  int
}

type ZFiles map[string]*handler

type ZSeries struct {
	Handlers ZFiles
}

func (h *handler) Write(p []byte) (int, error) {
	i, err := h.log.Write(p)
	// Write index offsets
	h.index.WriteString(fmt.Sprintf("%i,%i\n", h.offset, h.written))

	h.written += i
	h.offset += 1
	return i, err
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
func madvise(b []byte, advice int) (err error) {
	_, _, e1 := syscall.Syscall(syscall.SYS_MADVISE, uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)), uintptr(advice))
	if e1 != 0 {
		err = e1
	}
	return
}

func (z *ZSeries) getPath(key string) string {
	return path.Join(BASE_DIR, key, strconv.FormatInt(time.Now().UnixNano(), 10))
}

func (z *ZSeries) initTopic(key string) error {
	if _, ok := z.Handlers[key]; !ok {
		err := os.MkdirAll(path.Join(BASE_DIR, key), os.ModePerm)
		if err != nil {
			return err
		}

		path := z.getPath(key)
		z.Handlers[key] = &handler{}
		h := z.Handlers[key]
		h.log, err = os.OpenFile(path+".log", os.O_RDWR|os.O_CREATE, os.ModePerm)
		if err != nil {
			return err
		}
		h.index, err = os.OpenFile(path+".index", os.O_RDWR|os.O_CREATE, os.ModePerm)
		if err != nil {
			return err
		}

		// The integer math here should truncate any misalignment
		h.logSize = os.Getpagesize() * (FILE_SIZE / os.Getpagesize())
		h.zWriter = zstd.NewWriterLevel(h, 1)
		h.buffer = bufio.NewWriterSize(h.zWriter, SEGMENT_SIZE)
		if err != nil {
			return err
		}
	}
	return nil
}

func (z *ZSeries) rollLog(key string, size int) error {
	if h, ok := z.Handlers[key]; ok {
		if size+h.buffer.Buffered()+h.written > h.logSize {
			// close handlers & reopen
			h.buffer.Flush()
			h.zWriter.Close()
			h.log.Sync()
			h.index.Sync()
			h.log.Close()
			h.index.Close()
			delete(z.Handlers, key)
		}
	}
	return z.initTopic(key)
}

func (z *ZSeries) Write(key string, data []byte) (int, error) {
	err := z.rollLog(key, len(data))
	if err != nil {
		// TODO handle error
		return 0, err
	}
	h := z.Handlers[key]
	if len(data) > h.buffer.Available() && len(data) <= h.buffer.Size() {
		err = h.buffer.Flush()
		if err != nil {
			return 0, err
		}
	}
	return h.buffer.Write(data)
}

const (
	BASE_DIR     = "_zseries"
	FILE_SIZE    = 10485760
	SEGMENT_SIZE = 102400
)

var z ZSeries

func init() {
	z = ZSeries{}
}

//export Write
func Write(key string, data []byte) int {
	i, _ := z.Write(key, data)
	return i
}

func main() {}
