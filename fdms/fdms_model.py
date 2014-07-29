import datetime


class Authorization:
    def __init__(self):
        self.id = None
        self.number = ''
        self.is_credit = True
        self.card_hash = ''
        self.date = datetime.datetime.now()
        self.amount = 0.0

    def __repr__(self):
        return "<Authorization(number='%s', date='%s', amount='%.2f')>" % \
               (self.number, self.date, self.amount)


class OpenBatch:
    def __init__(self, merchant_number='', device_id='', batch_number=''):
        self.id = None
        self.merchant_number = merchant_number
        self.device_id = device_id
        self.batch_number = batch_number
        self.date_open = datetime.datetime.now()

    def __repr__(self):
        return "<%s(merchant_number='%s', device_id='%s', batch_number='%s' open='%s')>" % \
               (self.__class__.__name__, self.merchant_number, self.device_id, self.batch_number, self.date_open)


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
        self.batch_number = batch.batch_number
        self.date_open = batch.date_open

    def __repr__(self):
        return "<%s(merchant_number='%s', device_id='%s', batch_number='%s' " \
               "open='%s', closed='%s', count='%d/%d', amount='%.2f/%.2f')>" % \
               (self.__class__.__name__, self.merchant_number, self.device_id, self.batch_number, self.date_open,
                self.date_closed, self.credit_count, self.debit_count, self.credit_amount, self.debit_amount)


class BatchRecord:
    def __init__(self):
        self.id = None
        self.batch_id = 0
        self.auth_id = 0
        self.item_no = ''
        self.revision_no = ''
        self.txn_code = ''
        self.amount = 0.0

    def __repr__(self):
        return "<BatchRecord(batch_id='%d', item_no='%s', revision_no='%s' txn_code='%s', amount='%.2f')>" % \
               (self.batch_id, self.item_no, self.revision_no, self.txn_code, self.amount)


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

    def put_batch_record(self, batch_record: BatchRecord):
        raise NotImplementedError('%s.put_batch_record()' % self.__class__.__name__)
