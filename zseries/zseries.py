import atexit
import os
import os.path
import time
import zstd

BASE_DIR = '_zseries'
BLOCK_SIZE = 512
FILE_SIZE = 1048576

class ZSeries:
    msg_buffers = {}
    handlers = {}
    index_handlers = {}
    offsets = {}
    written = {}

    def __init__(self):
        atexit.register(self.close)

    def _get_path(self, key):
        return os.path.join(BASE_DIR, key, str(int(time.time() * 1000000)))

    def _init_handler(self, key):
        if not os.path.exists(os.path.join(BASE_DIR, key)):
            os.makedirs(os.path.join(BASE_DIR, key))

        if key not in self.handlers:
            path = self._get_path(key)
            self.handlers[key] = open(path + '.log', 'ab+')
            self.index_handlers[key] = open(path + '.index', 'ab+')
            self.msg_buffers[key] = bytearray()
            self.written[key] = 0
            self.offsets[key] = 0

    def _write_handler(self, key):
        self._init_handler(key)

        if len(self.msg_buffers[key]) > 0:
            compressed = zstd.compress(bytes(self.msg_buffers[key]), 1)
            if self.written[key] + len(compressed) <= FILE_SIZE:
                self.handlers[key].write(compressed)
            else:
                self.handlers[key].close()
                self.index_handlers[key].close()
                self.written[key] = 0
                path = self._get_path(key)
                self.handlers[key] = open(path + '.log', 'ab+')
                self.index_handlers[key] = open(path + '.index', 'ab+')
                self.handlers[key].write(compressed)
            self.index_handlers[key].write(','.join([str(self.offsets[key]), str(self.written[key])]).encode() + '\n'.encode())
            self.offsets[key] += 1
            self.written[key] += len(compressed)
        self.msg_buffers[key] = bytearray()

    def write(self, key, data):
        self._init_handler(key)

        msg = data.encode() + "\n".encode()
        if len(msg) + len(self.msg_buffers[key]) > BLOCK_SIZE * 200:
            self._write_handler(key)

            self.msg_buffers[key] += msg
        elif len(msg) + len(self.msg_buffers[key]) == BLOCK_SIZE * 200:
            self.msg_buffers[key] += msg
            self._write_handler(key)
        else:
            self.msg_buffers[key] += msg

    def close(self):
        for key in self.handlers:
            self._write_handler(key)
            self.handlers[key].close()
