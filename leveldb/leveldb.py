from ._ffi import ldb, ffi


class LevelDbError(Exception):
    pass


class DbOptions:
    def __init__(self):
        self.use_bloom_filter = False
        self.compression = True
        self.create_if_missing = True
        self.error_if_exists = False
        self.paranoid_checks = False
        self.write_buffer_size = 0
        self.cache_size = 0
        self.max_open_files = 0


class ReadOptions:
    def __init__(self):
        self._options_ptr = ldb.leveldb_readoptions_create()
        self._verify_checksums = False
        self._fill_cache = False

        self.verify_checksums = False
        self.fill_cache = True

    def __del__(self):
        ldb.leveldb_readoptions_destroy(self._options_ptr)

    def get_verify_checksums(self) -> bool:
        return self._verify_checksums

    def set_verify_checksums(self, value: bool):
        self._verify_checksums = value
        ldb.leveldb_readoptions_set_verify_checksums(self._options_ptr, value)

    verify_checksums = property(fget=get_verify_checksums, fset=set_verify_checksums)

    def get_fill_cache(self) -> bool:
        return self._fill_cache

    def set_fill_cache(self, value: bool):
        self._fill_cache = value
        ldb.leveldb_readoptions_set_fill_cache(self._options_ptr, value)

    fill_cache = property(fget=get_fill_cache, fset=set_fill_cache)

    options_ptr = property(fget=lambda self: self._options_ptr)


class WriteOptions:
    def __init__(self):
        self._options = ldb.leveldb_writeoptions_create()
        self._sync = False

        self.sync = False

    def __del__(self):
        ldb.leveldb_writeoptions_destroy(self._options)

    def get_sync(self) -> bool:
        return self._sync

    def set_sync(self, value: bool):
        self._sync = value
        ldb.leveldb_writeoptions_set_sync(self._options, value)

    sync = property(fget=get_sync, fset=set_sync)

    options_ptr = property(fget=lambda self: self._options)


class Iterator:
    def __init__(self, iter_ptr):
        self._iter_ptr = iter_ptr
        self._size_ptr = ffi.new('size_t *')
        self._error_ptr = ffi.new('char **')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __iter__(self):
        return self

    def __next__(self) -> (bytes, bytes):
        if self.valid():
            key, value = self.key(), self.value()
            self.next()
            return key, value
        else:
            raise StopIteration()

    def close(self):
        if self._iter_ptr is not None:
            ldb.leveldb_iter_destroy(self._iter_ptr)
            self._iter_ptr = None

    def valid(self) -> bool:
        assert self._iter_ptr is not None
        valid = ldb.leveldb_iter_valid(self._iter_ptr)
        return bool(valid)

    def next(self):
        assert self._iter_ptr is not None
        ldb.leveldb_iter_next(self._iter_ptr)

    def prev(self):
        assert self._iter_ptr is not None
        ldb.leveldb_iter_prev(self._iter_ptr)
        return self.valid()

    def seek_to_first(self):
        assert self._iter_ptr is not None
        ldb.leveldb_iter_seek_to_first(self._iter_ptr)

    def seek_to_last(self):
        assert self._iter_ptr is not None
        ldb.leveldb_iter_seek_to_last(self._iter_ptr)

    def key(self) -> bytes:
        assert self._iter_ptr is not None
        void_p = ldb.leveldb_iter_key(self._iter_ptr, self._size_ptr)
        if void_p != ffi.NULL:
            b = ffi.string(void_p, self._size_ptr[0])
            return b
        else:
            return None

    def value(self) -> bytes:
        assert self._iter_ptr is not None
        void_p = ldb.leveldb_iter_value(self._iter_ptr, self._size_ptr)
        if void_p != ffi.NULL:
            b = ffi.string(void_p, self._size_ptr[0])
            return b
        else:
            return None

    def seek(self, key: bytes):
        assert self._iter_ptr is not None
        ldb.leveldb_iter_seek(self._iter_ptr, key, len(key))

    def get_error(self) -> str:
        assert self._iter_ptr is not None
        self._error_ptr[0] = ffi.NULL
        ldb.leveldb_iter_get_error(self._iter_ptr, self._error_ptr)
        if self._error_ptr[0] != ffi.NULL:
            message = ffi.string(self._error_ptr[0])
            ldb.leveldb_free(self._error_ptr[0])
            self._error_ptr[0] = ffi.NULL
            return message.decode()
        else:
            return None


class Batch:
    def __init__(self):
        self._batch_ptr = ldb.leveldb_writebatch_create()

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        if self._batch_ptr is not None:
            ldb.leveldb_writebatch_destroy(self._batch_ptr)
            self._batch_ptr = None

    def clear(self):
        assert self._batch_ptr is not None
        ldb.leveldb_writebatch_clear(self._batch_ptr)

    def put(self, key: bytes, value: bytes):
        assert self._batch_ptr is not None
        assert key is not None
        ldb.leveldb_writebatch_put(self._batch_ptr, key, len(key), value, len(value) if value is not None else 0)

    def delete(self, key: bytes):
        assert self._batch_ptr is not None
        assert key is not None
        ldb.leveldb_writebatch_delete(self._batch_ptr, key, len(key))

    batch_ptr = property(fget=lambda self: self._batch_ptr)


class DB:
    def __init__(self):
        self._db_ptr = None
        self._options_ptr = None
        self._filter_ptr = None
        self._cache_ptr = None
        self._error_ptr = ffi.new('char **', ffi.NULL)
        self._size_ptr = ffi.new('size_t *')
        self._db_options = DbOptions()
        self._read_options = ReadOptions()
        self._write_options = WriteOptions()

    def __del__(self):
        self.close()
        del self._read_options
        del self._write_options

    def check_error(self):
        if self._error_ptr[0] != ffi.NULL:
            message = ffi.string(self._error_ptr[0])
            ''':type : bytes'''
            ldb.leveldb_free(self._error_ptr[0])
            self._error_ptr[0] = ffi.NULL
            raise LevelDbError(message.decode())

    @property
    def major_version(self) -> int:
        return ldb.leveldb_major_version()

    @property
    def minor_version(self) -> int:
        return ldb.leveldb_minor_version()

    def _create_options(self, options: DbOptions=None):
        if options is None:
            options = self._db_options

        self._options_ptr = ldb.leveldb_options_create()
        ldb.leveldb_options_set_create_if_missing(self._options_ptr, options.create_if_missing)
        ldb.leveldb_options_set_error_if_exists(self._options_ptr, options.error_if_exists)
        ldb.leveldb_options_set_paranoid_checks(self._options_ptr, options.paranoid_checks)
        if options.write_buffer_size > 0:
            ldb.leveldb_options_set_write_buffer_size(self._options_ptr, options.write_buffer_size)
        if options.max_open_files > 0:
            ldb.leveldb_options_set_max_open_files(self._options_ptr, options.max_open_files)
        ldb.leveldb_options_set_compression(self._options_ptr, 1 if options.compression else 0)
        if options.use_bloom_filter:
            self._filter_ptr = ldb.leveldb_filterpolicy_create_bloom(8)
            ldb.leveldb_options_set_filter_policy(self._options_ptr, self._filter_ptr)

        if options.cache_size > 0:
            self._cache_ptr = ldb.leveldb_cache_create_lru(options.cache_size)
            ldb.leveldb_options_set_cache(self._options_ptr, self._cache_ptr)

    def open(self, path: str, options: DbOptions=None):
        self.close()

        self._create_options(options)
        self._error_ptr[0] = ffi.NULL
        self._db_ptr = ldb.leveldb_open(self._options_ptr, path.encode(), self._error_ptr)
        self.check_error()

    def destroy(self, path: str, options: DbOptions=None):
        self.close()

        self._create_options(options)
        self._error_ptr[0] = ffi.NULL
        ldb.leveldb_destroy_db(self._options_ptr, path.encode(), self._error_ptr)
        self.check_error()

    def repair(self, path: str, options: DbOptions=None):
        self.close()

        self._create_options(options)
        self._error_ptr[0] = ffi.NULL
        ldb.leveldb_repair_db(self._options_ptr, path.encode(), self._error_ptr)
        self.check_error()

    def close(self):
        if self._db_ptr is not None:
            ldb.leveldb_close(self._db_ptr)
            self._db_ptr = None

        if self._filter_ptr is not None:
            ldb.leveldb_filterpolicy_destroy(self._filter_ptr)
            self._filter_ptr = None

        if self._cache_ptr is not None:
            ldb.leveldb_cache_destroy(self._cache_ptr)
            self._cache_ptr = None

        if self._options_ptr is not None:
            ldb.leveldb_options_destroy(self._options_ptr)
            self._options_ptr = None

    def put(self, key: bytes, value: bytes, options: WriteOptions=None):
        assert self._db_ptr is not None

        if options is None:
            options = self._write_options
        self._error_ptr[0] = ffi.NULL
        ldb.leveldb_put(self._db_ptr, options.options_ptr, key, len(key),
                        value, len(value) if value is not None else 0, self._error_ptr)
        self.check_error()

    def delete(self, key: bytes, options: WriteOptions=None):
        assert self._db_ptr is not None

        if options is None:
            options = self._write_options
        self._error_ptr[0] = ffi.NULL
        ldb.leveldb_delete(self._db_ptr, options.options_ptr, key, len(key), self._error_ptr)
        self.check_error()

    def get(self, key: bytes, options: ReadOptions=None) -> bytes:
        assert self._db_ptr is not None

        if options is None:
            options = self._read_options
        self._error_ptr[0] = ffi.NULL
        value_p = ldb.leveldb_get(self._db_ptr, options.options_ptr, key, len(key), self._size_ptr, self._error_ptr)
        self.check_error()
        if value_p != ffi.NULL:
            value = ffi.string(value_p, self._size_ptr[0])
            ldb.leveldb_free(value_p)
            return value
        else:
            return None

    def iterator(self, options: ReadOptions=None) -> Iterator:
        assert self._db_ptr is not None

        if options is None:
            options = self._read_options

        iter_ptr = ldb.leveldb_create_iterator(self._db_ptr, options.options_ptr)
        return Iterator(iter_ptr)

    def write(self, batch: Batch, options: WriteOptions=None):
        assert self._db_ptr is not None

        if options is None:
            options = self._write_options
        self._error_ptr[0] = ffi.NULL
        ldb.leveldb_write(self._db_ptr, options.options_ptr, batch.batch_ptr, self._error_ptr)
        self.check_error()
