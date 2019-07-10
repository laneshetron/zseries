package zseries

import (
	"bufio"
	"fmt"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/DataDog/zstd"
)

const (
	BASE_DIR     = "_zseries"
	FILE_SIZE    = 10485760
	SEGMENT_SIZE = 102400
)

var wg sync.WaitGroup

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

func NewZSeries() *ZSeries {
	z := &ZSeries{}
	z.Handlers = make(ZFiles)
	return z
}

func (h *handler) Write(p []byte) (int, error) {
	i, err := h.log.Write(p)
	// Write index offsets
	h.index.WriteString(fmt.Sprintf("%d,%d\n", h.offset, h.written))

	h.written += i
	h.offset += 1
	return i, err
}

type AsyncWriter struct {
	handler *handler
}

func (w *AsyncWriter) Write(p []byte) (int, error) {
	zWriter := zstd.NewWriterLevel(w.handler, 1)

	wg.Add(1)
	go func(buffer *zstd.Writer, data []byte) {
		defer wg.Done()
		buffer.Write(data)
		buffer.Close()
	}(zWriter, p)
	return len(p), nil
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
		h.log, err = os.OpenFile(path+".log", os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return err
		}
		h.index, err = os.OpenFile(path+".index", os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return err
		}

		// The integer math here should truncate any misalignment
		h.logSize = os.Getpagesize() * (FILE_SIZE / os.Getpagesize())
		asyncWriter := &AsyncWriter{handler: h}
		h.buffer = bufio.NewWriterSize(asyncWriter, SEGMENT_SIZE)
		if err != nil {
			return err
		}

		// Advise the kernel of future writes
		Fadvise(int(h.log.Fd()), 0, int64(h.logSize))
		err = Fallocate(int(h.log.Fd()), 0, 0, int64(h.logSize))
		if err != nil {
			// If Fallocate fails it's likely unsupported on this filesystem
			// ergo, fallback to Ftruncate
			Ftruncate(int(h.log.Fd()), int64(h.logSize))
		}
		h.log.Seek(0, 0)
	}
	return nil
}

func (z *ZSeries) rollLog(key string, size int) error {
	if h, ok := z.Handlers[key]; ok {
		if size+h.buffer.Buffered()+h.written > h.logSize {
			wg.Wait()
			// close handlers & reopen
			h.buffer.Flush()
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
