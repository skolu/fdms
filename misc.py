import ctypes
import ctypes.util

libc = ctypes.CDLL(ctypes.util.find_library('c'))

libc.malloc.argtypes = [ctypes.c_int]
libc.malloc.restype = ctypes.c_void_p
libc.free.argtypes = [ctypes.c_void_p]
libc.free.restype = None

b=libc.malloc(100)
buffer=ctypes.string_at(b, 100)
libc.free(b)
#pb=ctypes.cast(b, ctypes.POINTER(ctypes.c_byte))
#buffer = bytes((pb[i] for i in range(100)))
print(buffer)

