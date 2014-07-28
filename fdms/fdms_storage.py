from sqlalchemy import Table, MetaData, Column, Index, Integer, String, Boolean, DateTime, Float
from sqlalchemy.orm import mapper

from .fdms_model import *

fdms_metadata = MetaData()

authorization_table = Table('Authorization', fdms_metadata,
                            Column('Id', Integer, primary_key=True, nullable=False),
                            Column('Number', String(8), nullable=False),
                            Column('CardHash', String(32), nullable=False),
                            Column('IsCredit', Boolean, nullable=False, default=True),
                            Column('IsSwiped', Boolean, nullable=False, default=True),
                            Column('Date', DateTime, nullable=False),
                            Column('Amount', Float, nullable=False, default=0.0),
                            Index('Authorization_Number_Idx', 'Number', unique=False),
                            sqlite_autoincrement=True)

mapper(Authorization, authorization_table, properties={
    'id': authorization_table.columns.Id,
    'number': authorization_table.columns.Number,
    'card_hash': authorization_table.columns.CardHash,
    'is_credit': authorization_table.columns.IsCredit,
    'is_swiped': authorization_table.columns.IsSwiped,
    'date': authorization_table.columns.Date,
    'amount': authorization_table.columns.Amount,
})

open_batch_table = Table('OpenBatch', fdms_metadata,
                         Column('Id', Integer, primary_key=True, nullable=False),
                         Column('MerchantNumber', String(32), nullable=False),
                         Column('DeviceId', String(8), nullable=False),
                         Column('BatchNumber', String(8), nullable=False),
                         Column('DateOpen', DateTime, nullable=False),
                         Index('OpenBatch_Number_Idx', 'MerchantNumber', 'DeviceId', unique=True),
                         sqlite_autoincrement=True)

mapper(OpenBatch, open_batch_table, properties={
    'id': open_batch_table.columns.Id,
    'merchant_number': open_batch_table.columns.MerchantNumber,
    'device_id': open_batch_table.columns.DeviceId,
    'batch_number': open_batch_table.columns.BatchNumber,
    'date_open': open_batch_table.columns.DateOpen
})

closed_batch_table = Table('ClosedBatch', fdms_metadata,
                           Column('Id', Integer, primary_key=True, nullable=False),
                           Column('MerchantNumber', String(32), nullable=False),
                           Column('DeviceId', String(8), nullable=False),
                           Column('BatchNumber', String(8), nullable=False),
                           Column('DateOpen', DateTime, nullable=False),
                           Column('DateClosed', DateTime, nullable=False),
                           Column('CreditCount', DateTime, nullable=False),
                           Column('DebitCount', DateTime, nullable=False),
                           Column('CreditAmount', Float, nullable=False),
                           Column('DebitAmount', Float, nullable=False),
                           Index('OpenBatch_Number_Idx', 'MerchantNumber', 'DeviceId', 'DateClosed', unique=True),
)

mapper(ClosedBatch, closed_batch_table, properties={
    'id': open_batch_table.columns.Id,
    'merchant_number': open_batch_table.columns.MerchantNumber,
    'device_id': open_batch_table.columns.DeviceId,
    'batch_number': open_batch_table.columns.BatchNumber,
    'date_open': open_batch_table.columns.DateOpen,
    'date_closed': open_batch_table.columns.DateClosed,
    'credit_count': open_batch_table.columns.CreditCount,
    'debit_count': open_batch_table.columns.DebitCount,
    'credit_amount': open_batch_table.columns.CreditAmount,
    'debit_amount': open_batch_table.columns.DebitAmount
})


batch_record_table = Table('BatchRecord', fdms_metadata,
                           Column('Id', Integer, primary_key=True, nullable=False),
                           Column('BatchId', Integer, nullable=False),
                           Column('AuthId', Integer, nullable=False),
                           Column('ItemNumber', Integer, nullable=False),
                           Column('RevNumber', Integer, nullable=False),
                           Column('TxnCode', String(1), nullable=False),
                           Column('Amount', Float, nullable=False),
                           Index('BatchRecord_BatchId_Idx', 'BatchId', unique=False),
                           Index('BatchRecord_AuthId_Idx', 'AuthId', unique=True),
                           sqlite_autoincrement=True)

mapper(BatchRecord, batch_record_table, properties={
    'id': batch_record_table.columns.Id,
    'batch_id': batch_record_table.columns.BatchId,
    'auth_id': batch_record_table.columns.AuthId,
    'item_number': batch_record_table.columns.ItemNumber,
    'revision_number': batch_record_table.columns.RevNumber,
    'txn_code': batch_record_table.columns.TxnCode,
    'amount': batch_record_table.columns.Amount
})


class SqlFdmsStorage(FdmsStorage):
    def __init__(self):
        super().__init__()

    def last_closed_batch(self, merchant_number, device_id):
