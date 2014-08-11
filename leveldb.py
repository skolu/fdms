import ctypes
import ctypes.util

_ldb = ctypes.CDLL(ctypes.util.find_library('leveldb'))

''' DB operations
extern leveldb_t* leveldb_open(const leveldb_options_t* options, const char* name, char** errptr);
extern void leveldb_close(leveldb_t* db);
extern void leveldb_put(leveldb_t* db, const leveldb_writeoptions_t* options, const char* key, size_t keylen,
                        const char* val, size_t vallen, char** errptr);
extern void leveldb_delete(leveldb_t* db, const leveldb_writeoptions_t* options,
                           const char* key, size_t keylen, char** errptr);
extern void leveldb_write(leveldb_t* db, const leveldb_writeoptions_t* options,
                          leveldb_writebatch_t* batch, char** errptr);
extern char* leveldb_get(leveldb_t* db, const leveldb_readoptions_t* options,
                         const char* key, size_t keylen, size_t* vallen, char** errptr);
extern leveldb_iterator_t* leveldb_create_iterator(leveldb_t* db, const leveldb_readoptions_t* options);
extern const leveldb_snapshot_t* leveldb_create_snapshot(leveldb_t* db);
extern void leveldb_release_snapshot(leveldb_t* db, const leveldb_snapshot_t* snapshot);


'''
_ldb.leveldb_open.leveldb_readoptions_create = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p]
_ldb.leveldb_open.restype = ctypes.c_void_p
_ldb.leveldb_close.argtypes = [ctypes.c_void_p]
_ldb.leveldb_close.restype = None
_ldb.leveldb_put.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_size_t,
                             ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p]
_ldb.leveldb_put.restype = None
_ldb.leveldb_delete.argtypes = [ctypes.c_void_p, ctypes.c_void_p,
                                ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p]
_ldb.leveldb_delete.restype = None
_ldb.leveldb_write.argtypes = [ctypes.c_void_p, ctypes.c_void_p,
                               ctypes.c_void_p, ctypes.c_void_p]
_ldb.leveldb_write.restype = None
_ldb.leveldb_get.argtypes = [ctypes.c_void_p, ctypes.c_void_p,
                             ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p, ctypes.c_void_p]
_ldb.leveldb_get.restype = ctypes.POINTER(ctypes.c_char)


''' Read options
extern leveldb_readoptions_t* leveldb_readoptions_create();
extern void leveldb_readoptions_destroy(leveldb_readoptions_t*);
extern void leveldb_readoptions_set_verify_checksums(leveldb_readoptions_t*, unsigned char);
extern void leveldb_readoptions_set_fill_cache(leveldb_readoptions_t*, unsigned char);
extern void leveldb_readoptions_set_snapshot(leveldb_readoptions_t*, const leveldb_snapshot_t*);
'''
_ldb.leveldb_readoptions_create.leveldb_readoptions_create = []
_ldb.leveldb_readoptions_create.restype = ctypes.c_void_p
_ldb.leveldb_readoptions_destroy.argtypes = [ctypes.c_void_p]
_ldb.leveldb_readoptions_destroy.restype = None

''' Write options
extern leveldb_writeoptions_t* leveldb_writeoptions_create();
extern void leveldb_writeoptions_destroy(leveldb_writeoptions_t*);
extern void leveldb_writeoptions_set_sync(leveldb_writeoptions_t*, unsigned char);
'''
_ldb.leveldb_writeoptions_create.leveldb_readoptions_create = []
_ldb.leveldb_writeoptions_create.restype = ctypes.c_void_p
_ldb.leveldb_writeoptions_destroy.argtypes = [ctypes.c_void_p]
_ldb.leveldb_writeoptions_destroy.restype = None

''' Options
extern leveldb_options_t* leveldb_options_create();
extern void leveldb_options_destroy(leveldb_options_t*);
extern void leveldb_options_set_comparator(leveldb_options_t*, leveldb_comparator_t*);
extern void leveldb_options_set_filter_policy(leveldb_options_t*, leveldb_filterpolicy_t*);
extern void leveldb_options_set_create_if_missing(leveldb_options_t*, unsigned char);
extern void leveldb_options_set_error_if_exists(leveldb_options_t*, unsigned char);
extern void leveldb_options_set_paranoid_checks(leveldb_options_t*, unsigned char);
extern void leveldb_options_set_env(leveldb_options_t*, leveldb_env_t*);
extern void leveldb_options_set_info_log(leveldb_options_t*, leveldb_logger_t*);
extern void leveldb_options_set_write_buffer_size(leveldb_options_t*, size_t);
extern void leveldb_options_set_max_open_files(leveldb_options_t*, int);
extern void leveldb_options_set_cache(leveldb_options_t*, leveldb_cache_t*);
extern void leveldb_options_set_block_size(leveldb_options_t*, size_t);
extern void leveldb_options_set_block_restart_interval(leveldb_options_t*, int);
'''

_ldb.leveldb_options_create.argtypes = []
_ldb.leveldb_options_create.restype = ctypes.c_void_p

_ldb.leveldb_options_set_create_if_missing.argtypes = [ctypes.c_void_p, ctypes.c_ubyte]
_ldb.leveldb_options_set_create_if_missing.restype = None
_ldb.leveldb_options_set_error_if_exists.argtypes = [ctypes.c_void_p, ctypes.c_ubyte]
_ldb.leveldb_options_set_error_if_exists.restype = None
_ldb.leveldb_options_set_paranoid_checks.argtypes = [ctypes.c_void_p, ctypes.c_ubyte]
_ldb.leveldb_options_set_paranoid_checks.restype = None
_ldb.leveldb_options_set_write_buffer_size.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
_ldb.leveldb_options_set_write_buffer_size.restype = None
_ldb.leveldb_options_set_max_open_files.argtypes = [ctypes.c_void_p, ctypes.c_int]
_ldb.leveldb_options_set_max_open_files.restype = None

_ldb.leveldb_options_destroy.argtypes = [ctypes.c_void_p]
_ldb.leveldb_options_destroy.restype = None


''' Comparator
extern leveldb_comparator_t* leveldb_comparator_create(void* state, void (*destructor)(void*),
                             int (*compare)(void*, const char* a, size_t alen, const char* b, size_t blen),
                             const char* (*name)(void*));
extern void leveldb_comparator_destroy(leveldb_comparator_t*);
'''
CMP_DESTROY_FUNC = ctypes.CFUNCTYPE(ctypes.c_void_p)
CMP_COMPARE_FUNC = ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p,
                                    ctypes.c_char_p, ctypes.c_size_t,
                                    ctypes.c_char_p, ctypes.c_size_t)
CMP_NAME_FUNC = ctypes.CFUNCTYPE(ctypes.c_char_p, ctypes.c_void_p)

''' Utility

extern void leveldb_free(void* ptr);
extern int leveldb_major_version();
extern int leveldb_minor_version();
'''

_ldb.leveldb_free.argtypes = [ctypes.c_void_p]
_ldb.leveldb_free.restype = None

_ldb.leveldb_major_version.argtypes = []
_ldb.leveldb_major_version.restype = ctypes.c_int

_ldb.leveldb_minor_version.argtypes = []
_ldb.leveldb_minor_version.restype = ctypes.c_int


class LevelDbError(Exception):
    pass


def _check_error(error):
    if bool(error):
        message = ctypes.string_at(error)
        _ldb.leveldb_free(ctypes.cast(error, ctypes.c_void_p))
        raise LevelDbError(message)


class DbOptions:
    def __init__(self):
        self.create_if_missing = True
        self.error_if_exists = False
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
        self.read_options = None
        self.write_options = None
        pass

    def open(self, path: str, options: DbOptions=None):
        opts = _ldb.leveldb_options_create()
        if options is not None:
            _ldb.leveldb_options_set_create_if_missing(opts, options.create_if_missing)
            _ldb.leveldb_options_set_error_if_exists(opts, options.error_if_exists)
            _ldb.leveldb_options_set_paranoid_checks(opts, options.paranoid_checks)
            _ldb.leveldb_options_set_write_buffer_size(opts, options.write_buffer_size)
            _ldb.leveldb_options_set_max_open_files(opts, options.max_open_files)

        error = ctypes.POINTER(ctypes.c_char)()
        self.db_ref = _ldb.leveldb_open(opts, path.encode(), ctypes.byref(error))
        _ldb.leveldb_options_destroy(opts)
        _check_error(error)

    def close(self):
        if self.db_ref is not None:
            _ldb.leveldb_close(self.db_ref)
            self.db_ref = None

    def put(self, key: bytes, value: bytes, options: WriteOptions=None):
        if self.db_ref is None:
            raise LevelDbError('not open')
        error = ctypes.POINTER(ctypes.c_char)()
        _ldb.leveldb_put(self.db_ref, self.write_options, key, len(key), value, len(value), ctypes.byref(error))
        _check_error(error)

    def delete(self, key: bytes, options: WriteOptions=None):
        if self.db_ref is None:
            raise LevelDbError('not open')
        error = ctypes.POINTER(ctypes.c_char)()
        _ldb.leveldb_delete(self.db_ref, self.write_options, key, len(key), ctypes.byref(error))
        _check_error(error)

    def get(self, key: bytes, options: WriteOptions=None) -> bytes:
        if self.db_ref is None:
            raise LevelDbError('not open')
        error = ctypes.POINTER(ctypes.c_char)()
        value_len = ctypes.c_size_t(0)
        value_p = _ldb.leveldb_get(self.db_ref, self.read_options, key, len(key), ctypes.byref(value_len),
                                   ctypes.byref(error))
        _check_error(error)
        if bool(value_p):
            value = ctypes.string_at(value_p, value_len.value)
            _ldb.leveldb_free(ctypes.cast(value_p, ctypes.c_void_p))
            return value
        else:
            return None

