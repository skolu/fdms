from .fdms_model import *
from enum import Enum
import leveldb
import json


class FdmsEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ClosedBatch):
            return {}


class IndexPrefix(Enum):
    Sequence = b'0'
    Authorization = b'1'
    OpenBatch = b'2'
    ClosedBatch = b'3'
    BatchRecord = b'4'



class LevelDbStorage(FdmsStorage):
    def __init__(self):
        super().__init__()
        self._db = None
        ''':type: leveldb.DB'''

    def save(self):
        pass

    def _next_sequence(self, entity: IndexPrefix) -> int:
        key = IndexPrefix.Sequence.value + entity.value
        seq = self._db.get(key)
        result = 0
        if seq is not None:
            result = int.from_bytes(bytes=seq, byteorder='big', signed=False)
        result += 1
        self._db.put(key, result.to_bytes(4, byteorder='big', signed=False))
        return result

    def last_closed_batch(self, merchant_number: str, device_id: str) -> ClosedBatch:
        key_prefix = b'\x00'.join((IndexPrefix.ClosedBatch.value, merchant_number.encode(), device_id.encode()))
        iterator = self._db.iterator()
        iterator.seek(key_prefix)
        key = iterator.key()
        if key.startswith(key_prefix):
            while iterator.next():
                key = iterator.key()
                if not key.startswith(key_prefix):
                    break
            iterator.prev()

        else:
            return None
