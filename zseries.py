'''Python ZSeries Bindings'''
import atexit
from ctypes import *

class GoSlice(Structure):
    _fields_ = [("data", POINTER(c_void_p)),
                ("len", c_longlong), ("cap", c_longlong)]

class GoString(Structure):
    _fields_ = [("p", c_char_p), ("n", c_longlong)]

lib = cdll.LoadLibrary("./zseries.so")
lib.Write.argtypes = [GoString, GoSlice]

def Write(key, data):
    key = key.encode('utf-8')
    key = GoString(key, len(key))
    p1 = c_char_p(data)
    data = GoSlice(cast(p1, POINTER(c_void_p)), len(data), len(data))
    return lib.Write(key, data)

def Close():
    lib.Close()

atexit.register(Close)
