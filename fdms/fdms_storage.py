from sqlalchemy import MetaData, Table, Column, Index, Integer, String, Boolean, DateTime, Float, desc
from sqlalchemy import create_engine
from sqlalchemy.orm import mapper, sessionmaker, Session

from .fdms_model import *

fdms_metadata = MetaData()

authorization_table = Table('Authorization', fdms_metadata,
                            Column('Id', Integer, primary_key=True, nullable=False),
                            Column('MerchantNumber', String(32), nullable=False),
                            Column('AuthorizationCode', String(8), nullable=False),
                            Column('CardHash', String(32), nullable=False),
                            Column('IsCredit', Boolean, nullable=False, default=True),
                            Column('IsCaptured', Boolean, nullable=False, default=True),
                            Column('Date', DateTime, nullable=False),
                            Column('Amount', Float, nullable=False, default=0.0),
                            Index('Authorization_Number_Idx', 'MerchantNumber', 'AuthorizationCode', unique=False),
                            sqlite_autoincrement=True)

mapper(Authorization, authorization_table, properties={
    'id': authorization_table.columns.Id,
    'merchant_number': authorization_table.columns.MerchantNumber,
    'authorization_code': authorization_table.columns.AuthorizationCode,
    'card_hash': authorization_table.columns.CardHash,
    'is_credit': authorization_table.columns.IsCredit,
    'is_captured': authorization_table.columns.IsCaptured,
    'date': authorization_table.columns.Date,
    'amount': authorization_table.columns.Amount,
})

open_batch_table = Table('OpenBatch', fdms_metadata,
                         Column('Id', Integer, primary_key=True, nullable=False),
                         Column('MerchantNumber', String(32), nullable=False),
                         Column('DeviceId', String(4), nullable=False),
                         Column('BatchNumber', String(8), nullable=False),
                         Column('DateOpen', DateTime, nullable=False),
                         Index('OpenBatch_Number_Idx', 'MerchantNumber', 'DeviceId', unique=True),
                         sqlite_autoincrement=True)

mapper(OpenBatch, open_batch_table, properties={
    'id': open_batch_table.columns.Id,
    'merchant_number': open_batch_table.columns.MerchantNumber,
    'device_id': open_batch_table.columns.DeviceId,
    'batch_no': open_batch_table.columns.BatchNumber,
    'date_open': open_batch_table.columns.DateOpen
})

closed_batch_table = Table('ClosedBatch', fdms_metadata,
                           Column('Id', Integer, primary_key=True, nullable=False),
                           Column('MerchantNumber', String(32), nullable=False),
                           Column('DeviceId', String(4), nullable=False),
                           Column('BatchNumber', String(1), nullable=False),
                           Column('DateOpen', DateTime, nullable=False),
                           Column('DateClosed', DateTime, nullable=False),
                           Column('CreditCount', Integer, nullable=False),
                           Column('DebitCount', Integer, nullable=False),
                           Column('CreditAmount', Float, nullable=False),
                           Column('DebitAmount', Float, nullable=False),
                           Index('ClosedBatch_Number_Idx', 'MerchantNumber', 'DeviceId', 'DateClosed'),
)

mapper(ClosedBatch, closed_batch_table, properties={
    'id': closed_batch_table.columns.Id,
    'merchant_number': closed_batch_table.columns.MerchantNumber,
    'device_id': closed_batch_table.columns.DeviceId,
    'batch_no': closed_batch_table.columns.BatchNumber,
    'date_open': closed_batch_table.columns.DateOpen,
    'date_closed': closed_batch_table.columns.DateClosed,
    'credit_count': closed_batch_table.columns.CreditCount,
    'debit_count': closed_batch_table.columns.DebitCount,
    'credit_amount': closed_batch_table.columns.CreditAmount,
    'debit_amount': closed_batch_table.columns.DebitAmount
})


batch_record_table = Table('BatchRecord', fdms_metadata,
                           Column('Id', Integer, primary_key=True, nullable=False),
                           Column('BatchId', Integer, nullable=False),
                           Column('AuthId', Integer, nullable=False),
                           Column('ItemNumber', String(3), nullable=False),
                           Column('RevNumber', String(1), nullable=False),
                           Column('TxnCode', String(1), nullable=False),
                           Column('IsCredit', Boolean, nullable=False),
                           Column('Amount', Float, nullable=False),
                           Index('BatchRecord_BatchId_Idx', 'BatchId', 'ItemNumber', unique=True),
                           Index('BatchRecord_AuthId_Idx', 'AuthId', unique=True),
                           sqlite_autoincrement=True)

mapper(BatchRecord, batch_record_table, properties={
    'id': batch_record_table.columns.Id,
    'batch_id': batch_record_table.columns.BatchId,
    'auth_id': batch_record_table.columns.AuthId,
    'item_no': batch_record_table.columns.ItemNumber,
    'revision_no': batch_record_table.columns.RevNumber,
    'txn_code': batch_record_table.columns.TxnCode,
    'is_credit': batch_record_table.columns.IsCredit,
    'amount': batch_record_table.columns.Amount
})


engine = create_engine('sqlite:///:memory:', echo=True)

fdms_metadata.create_all(engine)

_SqlSession = sessionmaker(bind=engine)


class SqlFdmsStorage(FdmsStorage):
    def __init__(self):
        super().__init__()
        self.session = None
        ''':type: Session'''

    def __enter__(self):
        self.session = _SqlSession()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()
        self.session = None

    def save(self):
        self.session.commit()

    def last_closed_batch(self, merchant_number: str, device_id: str) -> ClosedBatch:
        query = self.session.query(ClosedBatch). \
            filter(ClosedBatch.merchant_number == merchant_number, ClosedBatch.device_id == device_id). \
            order_by(ClosedBatch.date_closed.desc())
        return query.first()

    def get_open_batch(self, merchant_number, device_id) -> OpenBatch:
        query = self.session.query(OpenBatch). \
            filter(OpenBatch.merchant_number == merchant_number, OpenBatch.device_id == device_id)
        return query.first()

    def create_batch(self, merchant_number, device_id, batch_no):
        batch = OpenBatch(merchant_number=merchant_number, device_id=device_id, batch_no=batch_no)
        self.session.add(batch)
        self.session.flush()
        return batch

    def get_batch_record(self, batch_id, item_no) -> BatchRecord:
        query = self.session.query(BatchRecord). \
            filter(BatchRecord.batch_id == batch_id, BatchRecord.item_no == item_no)
        return query.first()

    def put_batch_record(self, batch_record):
        self.session.add(batch_record)
        self.session.flush()

    def get_authorization(self, rec_id) -> Authorization:
        return self.session.query(Authorization).get(rec_id)

    def put_authorization(self, authorization: Authorization):
        self.session.add(authorization)
        self.session.flush()

    def query_authorization(self, merchant_number: str, authorization_code: str) -> list:
        ''':rtype: list of Authorization'''
        query = self.session.query(Authorization). \
            filter(Authorization.merchant_number == merchant_number,
                   Authorization.authorization_code == authorization_code)
        return query.all()

    def query_batch_items(self, batch_id: int) -> list:
        query = self.session.query(BatchRecord). \
            filter(BatchRecord.batch_id == batch_id)
        return query.all()

    def close_batch(self, batch: OpenBatch, credit: (int, float), debit: (int, float)) -> ClosedBatch:
        closed_batch = ClosedBatch()
        closed_batch.from_batch(batch)
        closed_batch.credit_count = credit[0]
        closed_batch.credit_amount = credit[1]
        closed_batch.debit_count = debit[0]
        closed_batch.debit_amount = debit[1]
        self.session.add(closed_batch)
        self.session.delete(batch)
        self.session.flush()
