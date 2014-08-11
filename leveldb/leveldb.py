import ctypes
from .bindings import ldb, ffi

class LevelDbError(Exception):
    pass


def _check_error(error):
    if bool(error):
        message = ctypes.string_at(error)
        ldb.leveldb_free(ctypes.cast(error, ctypes.c_void_p))
        raise LevelDbError(message)


class DbOptions:
    def __init__(self):
        self.create_if_missing = True
        self.error_if_exists = True
        self.paranoid_checks = False
        self.write_buffer_size = (4 * 1024 * 1024)
        self.max_open_files = 1000


class ReadOptions:
    def __init__(self):
        pass


class WriteOptions:
    pass


class DB:
    def __init__(self):
        self.db_ref = None
        self.db_options = DbOptions()
        self.read_options = None
        self.write_options = None
        pass

    def open(self, path: str, options: DbOptions=None):
        opts = ldb.leveldb_options_create()
        if options is None:
            options = self.db_options
        ldb.leveldb_options_set_create_if_missing(opts, options.create_if_missing)
        ldb.leveldb_options_set_error_if_exists(opts, options.error_if_exists)
        ldb.leveldb_options_set_paranoid_checks(opts, options.paranoid_checks)
        ldb.leveldb_options_set_write_buffer_size(opts, options.write_buffer_size)
        ldb.leveldb_options_set_max_open_files(opts, options.max_open_files)

        errptr = ffi.new('char **', ffi.NULL)
        self.db_ref = ldb.leveldb_open(opts, path.encode(), errptr)
        #error = ctypes.POINTER(ctypes.c_char)()
        #self.db_ref = ldb.leveldb_open(opts, path.encode(), ctypes.byref(error))
        ldb.leveldb_options_destroy(opts)
        if errptr[0] != ffi.NULL:
            str = ffi.string(errptr[0])
            print(str)
        #_check_error(error)

    def close(self):
        if self.db_ref is not None:
            ldb.leveldb_close(self.db_ref)
            self.db_ref = None

    def put(self, key: bytes, value: bytes, options: WriteOptions=None):
        if self.db_ref is None:
            raise LevelDbError('not open')
        error = ctypes.POINTER(ctypes.c_char)()
        ldb.leveldb_put(self.db_ref, self.write_options, key, len(key), value, len(value), ctypes.byref(error))
        _check_error(error)

    def delete(self, key: bytes, options: WriteOptions=None):
        if self.db_ref is None:
            raise LevelDbError('not open')
        error = ctypes.POINTER(ctypes.c_char)()
        ldb.leveldb_delete(self.db_ref, self.write_options, key, len(key), ctypes.byref(error))
        _check_error(error)

    def get(self, key: bytes, options: WriteOptions=None) -> bytes:
        if self.db_ref is None:
            raise LevelDbError('not open')
        error = ctypes.POINTER(ctypes.c_char)()
        value_len = ctypes.c_size_t(0)
        value_p = ldb.leveldb_get(self.db_ref, self.read_options, key, len(key), ctypes.byref(value_len),
                                   ctypes.byref(error))
        _check_error(error)
        if bool(value_p):
            value = ctypes.string_at(value_p, value_len.value)
            ldb.leveldb_free(ctypes.cast(value_p, ctypes.c_void_p))
            return value
        else:
            return None

