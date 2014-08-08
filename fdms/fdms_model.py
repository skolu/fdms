import datetime
import hashlib

class Authorization:
    def __init__(self):
        self.id = None
        self.merchant_number = ''
        self.authorization_code = ''
        self.is_credit = True
        self.is_captured = True
        self.card_hash = ''
        self.date = datetime.datetime.now()
        self.amount = 0.0

    def __repr__(self):
        return "<%s(authorization_code='%s', date='%s', amount='%.2f')>" % \
               (self.__class__.__name__, self.authorization_code, self.date, self.amount)


class OpenBatch:
    def __init__(self, merchant_number='', device_id='', batch_no=''):
        self.id = None
        self.merchant_number = merchant_number
        self.device_id = device_id
        self.batch_no = batch_no
        self.date_open = datetime.datetime.now()

    def __repr__(self):
        return "<%s(merchant_number='%s', device_id='%s', batch_no='%s' date_open='%s')>" % \
               (self.__class__.__name__, self.merchant_number, self.device_id, self.batch_no, self.date_open)


class ClosedBatch(OpenBatch):
    def __init__(self):
        super().__init__()
        self.date_closed = datetime.datetime.now()
        self.credit_count = 0
        self.debit_count = 0
        self.credit_amount = 0.0
        self.debit_amount = 0.0

    def from_batch(self, batch: OpenBatch):
        self.id = batch.id
        self.merchant_number = batch.merchant_number
        self.device_id = batch.device_id
        self.batch_no = batch.batch_no
        self.date_open = batch.date_open

    def __repr__(self):
        return "<%s(merchant_number='%s', device_id='%s', batch_no='%s' " \
               "open='%s', closed='%s', count='%d/%d', amount='%.2f/%.2f')>" % \
               (self.__class__.__name__, self.merchant_number, self.device_id, self.batch_no, self.date_open,
                self.date_closed, self.credit_count, self.debit_count, self.credit_amount, self.debit_amount)


class BatchRecord:
    def __init__(self):
        self.id = None
        self.batch_id = 0
        self.auth_id = 0
        self.item_no = ''
        self.revision_no = ''
        self.txn_code = ''
        self.is_credit = True
        self.amount = 0.0

    def __repr__(self):
        return "<%s(batch_id='%d', item_no='%s', revision_no='%s' txn_code='%s', amount='%.2f')>" % \
               (self.__class__.__name__, self.batch_id, self.item_no, self.revision_no, self.txn_code, self.amount)


class FdmsStorage:
    def __init__(self):
        pass

    def save(self):
        raise NotImplementedError('%s.save()' % self.__class__.__name__)

    def last_closed_batch(self, merchant_number, device_id) -> ClosedBatch:
        raise NotImplementedError('%s.last_closed_batch()' % self.__class__.__name__)

    def get_open_batch(self, merchant_number, device_id) -> OpenBatch:
        raise NotImplementedError('%s.get_open_batch()' % self.__class__.__name__)

    def create_batch(self, merchant_number, device_id, batch_no) -> OpenBatch:
        raise NotImplementedError('%s.create_batch()' % self.__class__.__name__)

    def get_batch_record(self, batch_id: int, item_no: str) -> BatchRecord:
        raise NotImplementedError('%s.get_batch_record()' % self.__class__.__name__)

    def query_batch_items(self, batch_id: int) -> list:
        raise NotImplementedError('%s.query_batch_items()' % self.__class__.__name__)

    def put_batch_record(self, batch_record: BatchRecord):
        raise NotImplementedError('%s.put_batch_record()' % self.__class__.__name__)

    def query_authorization(self, merchant_number, authorization_code) -> list:
        raise NotImplementedError('%s.query_authorization()' % self.__class__.__name__)

    def get_authorization(self, rec_id) -> Authorization:
        raise NotImplementedError('%s.get_authorization()' % self.__class__.__name__)

    def put_authorization(self, authorization: Authorization):
        raise NotImplementedError('%s.put_authorization()' % self.__class__.__name__)

    def close_batch(self, batch: OpenBatch, credit: (int, float), debit: (int, float)=(0, 0.0)) -> ClosedBatch:
        raise NotImplementedError('%s.close_batch()' % self.__class__.__name__)


def card_info_md5(card_number: str, card_expiration: str):
    h = hashlib.md5()
    h.update(card_number.encode())
    h.update(':'.encode())
    h.update(card_expiration.encode())
    return h.hexdigest().upper()

def extract_card_info(track: str) -> (str, str):
    if track is None:
        raise ValueError('Invalid track')
    if track[0] == '%':
        return extract_track1(track)
    else:
        return extract_track2(track)

def extract_track2(track: str) -> (str, str):
    p1 = 0
    if track[p1] == ';':
        p1 += 1
    p2 = track.index('=', p1)
    p3 = p2 + 1
    p4 = p3 + 4
    return track[p1:p2], track[p3:p4]


def extract_track1(track: str):
    p1 = 0
    if track[0] == '%':
        p1 = 2
    p2 = track.index('^', p1)
    p3 = track.index('^', p2 + 1)
    p3 += 1
    p4 = p3 + 4
    return track[p1:p2], track[p3:p4]



