from .fdms_model import *
from enum import Enum
import leveldb
import json

Authorization.JsonFields = ['id', 'merchant_number', 'authorization_code', 'is_credit', 'is_captured', 'card_hash',
                            'date:dt', 'amount']
OpenBatch.JsonFields = ['id', 'merchant_number', 'device_id', 'batch_no', 'date_open']
ClosedBatch.JsonFields = OpenBatch.JsonFields + \
                         ['date_closed', 'credit_count', 'debit_count', 'credit_amount', 'debit_amount']
BatchRecord.JsonFields = ['id', 'batch_id', 'auth_id', 'item_no', 'revision_no', 'txn_code', 'is_credit', 'amount']

ClassMap = {clazz.__name__: clazz for clazz in (Authorization, OpenBatch, ClosedBatch, BatchRecord)}


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj.__class__, 'JsonFields'):
            fields = obj.__class__.JsonFields
            d = {'__class__': obj.__class__.__name__}
            for fld in fields:
                assert isinstance(fld, str)
                pos = fld.find(':')
                if pos > 0:
                    fld = fld[0:pos]
                if hasattr(obj, fld):
                    d[fld] = getattr(obj, fld)
            return d
        elif isinstance(obj, datetime.datetime):
            return obj.timestamp()
        else:
            return obj.__dict__


class CustomDecoder(json.JSONDecoder):
    def __init__(self):
        super().__init__(object_hook=self.fdms_object_hook)

    @staticmethod
    def fdms_object_hook(d):
        obj = None
        if '__class__' in d:
            class_name = d['__class__']
            if class_name in ClassMap:
                obj = ClassMap[class_name]()
        if obj is None:
            return d
        if not hasattr(obj.__class__, 'JsonFields'):
            return d

        for fld in obj.__class__.JsonFields:
            assert isinstance(fld, str)
            pos = fld.find(':')
            type = None
            if pos > 0:
                type = fld[pos + 1:]
                fld = fld[0:pos]
            if fld in d:
                value = d[fld]
                if type is not None:
                    if type == 'dt' and isinstance(value, float):
                        value = datetime.datetime.fromtimestamp(value)
                if hasattr(obj, fld):
                    setattr(obj, fld, value)

        return obj


class IndexPrefix(Enum):
    Sequence = b'0'
    Authorization = b'1'
    OpenBatch = b'2'
    ClosedBatch = b'3'
    BatchRecord = b'4'
    AuthorizationCode = b'5'


_db = leveldb.DB()
_db.open('level.db')


class LevelDbStorage(FdmsStorage):
    def __init__(self):
        super().__init__()
        self._db = _db

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def save(self):
        pass

    @staticmethod
    def _id_to_bytes(id: int) -> bytes:
        return id.to_bytes(4, byteorder='big', signed=False)

    @staticmethod
    def _bytes_to_id(seq: bytes) -> int:
        return int.from_bytes(bytes=seq, byteorder='big', signed=False)

    def _next_sequence(self, entity: IndexPrefix) -> int:
        key = b'\x00'.join((IndexPrefix.Sequence.value, entity.value))
        seq = self._db.get(key)
        result = 0
        if seq is not None:
            result = self._bytes_to_id(seq)
        result += 1
        self._db.put(key, self._id_to_bytes(result))
        return result

    def last_closed_batch(self, merchant_number: str, device_id: str) -> ClosedBatch:
        key_prefix = b'\x00'.join((IndexPrefix.ClosedBatch.value, merchant_number.encode(), device_id.encode(), b'\xff'))
        with self._db.iterator() as iterator:
            iterator.seek(key_prefix)
            iterator.prev()
            if iterator.valid():
                key = iterator.key()
                if key.startswith(key_prefix):
                    value = iterator.value()
                    obj = json.loads(value.decode(), cls=CustomDecoder)
                    assert isinstance(obj, ClosedBatch)
                    return obj
        return None

    def get_open_batch(self, merchant_number, device_id) -> OpenBatch:
        key = b'\x00'.join((IndexPrefix.OpenBatch.value, merchant_number.encode(), device_id.encode()))
        value = self._db.get(key)
        if value is not None:
            obj = json.loads(value.decode(), cls=CustomDecoder)
            assert isinstance(obj, OpenBatch)
            return obj
        return None

    def create_batch(self, merchant_number, device_id, batch_no) -> OpenBatch:
        batch = OpenBatch()
        batch.id = self._next_sequence(IndexPrefix.OpenBatch)
        batch.merchant_number = merchant_number
        batch.device_id = device_id
        batch.batch_no = batch_no
        key = b'\x00'.join((IndexPrefix.OpenBatch.value, merchant_number.encode(), device_id.encode()))
        value = json.dumps(batch, cls=CustomEncoder)
        self._db.put(key, value.encode())

    def get_batch_record(self, batch_id: int, item_no: str) -> BatchRecord:
        key = b'\x00'.join((IndexPrefix.BatchRecord.value, self._id_to_bytes(batch_id), item_no.encode()))
        value = self._db.get(key)
        if value is not None:
            obj = json.loads(value.decode(), cls=CustomDecoder)
            assert isinstance(obj, BatchRecord)
            return obj
        return None

    def query_batch_items(self, batch_id: int) -> list:
        key_prefix = b'\x00'.join((IndexPrefix.BatchRecord.value, self._id_to_bytes(batch_id)))
        result = []
        with self._db.iterator() as iterator:
            iterator.seek(key_prefix)
            while iterator.valid():
                key = iterator.key()
                if key.startswith(key_prefix):
                    value = iterator.value()
                    obj = json.loads(value.decode(), cls=CustomDecoder)
                    assert isinstance(obj, BatchRecord)
                    result.append(obj)
                    iterator.next()
                else:
                    break

        return result

    def put_batch_record(self, batch_record: BatchRecord):
        if batch_record.id is None:
            batch_record.id = self._next_sequence(IndexPrefix.BatchRecord)
        key = b'\x00'.join((IndexPrefix.BatchRecord.value, self._id_to_bytes(batch_record.batch_id),
                            self._id_to_bytes(batch_record.id)))
        value = json.dumps(batch_record, cls=CustomEncoder)
        self._db.put(key, value.encode())

    def query_authorization(self, merchant_number, authorization_code) -> list:
        index_key = b'\x00'.join((IndexPrefix.AuthorizationCode.value, merchant_number.encode(),
                                  authorization_code.encode(), b''))
        result = []
        with self._db.iterator() as iterator:
            iterator.seek(index_key)
            while iterator.valid():
                key = iterator.key()
                if not key.startswith(index_key):
                    break
                id = key[len(index_key):]
                auth_key = b'\x00'.join((IndexPrefix.Authorization.value, id))
                value = self._db.get(auth_key)
                obj = json.loads(value.decode(), cls=CustomDecoder)
                assert isinstance(obj, Authorization)
                result.append(obj)
        return result

    def get_authorization(self, rec_id) -> Authorization:
        key = b'\x00'.join((IndexPrefix.Authorization.value, self._id_to_bytes(rec_id)))
        value = self._db.get(key)
        if value is not None:
            obj = json.loads(value.decode(), cls=CustomDecoder)
            assert isinstance(obj, Authorization)
            return obj
        return None

    def put_authorization(self, authorization: Authorization):
        if authorization.id is None:
            authorization.id = self._next_sequence(IndexPrefix.Authorization)
        key = b'\x00'.join((IndexPrefix.Authorization.value, self._id_to_bytes(authorization.id)))
        value = json.dumps(authorization, cls=CustomEncoder)
        self._db.put(key, value.encode())
        index_key = b'\x00'.join((IndexPrefix.AuthorizationCode.value, authorization.merchant_number.encode(),
                                  authorization.authorization_code.encode(), self._id_to_bytes(authorization.id)))
        self._db.put(index_key, b'')

    def close_batch(self, batch: OpenBatch, credit: (int, float), debit: (int, float)) -> ClosedBatch:
        closed_batch = ClosedBatch()
        closed_batch.from_batch(batch)
        closed_batch.credit_count = credit[0]
        closed_batch.credit_amount = credit[1]
        closed_batch.debit_count = debit[0]
        closed_batch.debit_amount = debit[1]
        key = b'\x00'.join((IndexPrefix.ClosedBatch.value, closed_batch.merchant_number.encode(),
                            closed_batch.device_id.encode(), self._id_to_bytes(closed_batch.id)))
        value = json.dumps(closed_batch, cls=CustomEncoder)
        self._db.put(key, value.encode())
        key = b'\x00'.join((IndexPrefix.OpenBatch.value, batch.merchant_number.encode(), batch.device_id.encode()))
        self._db.delete(key)





