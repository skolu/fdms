from sqlalchemy import Table, MetaData, Column, Index, Integer, String, Boolean, DateTime, Float
from sqlalchemy.orm import mapper
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from enum import Enum


class MerchantNumber:
    def __init__(self, number=None):
        self.id = None
        self.number = number

    def __repr__(self):
        return "<MerchantNumber(number='%s')>" % (self.number,)


class MerchantHistory:
    def __init__(self, id=None):
        self.id = id
        self.merchant_id = 0
        self.history = ''




engine = create_engine('sqlite:///:memory:', echo=True)

metadata = MetaData()

merchant_number_table = Table('MerchantNumber', metadata,
                              Column('Id', Integer, primary_key=True, nullable=False),
                              Column('Number', String(32), nullable=False),
                              Index('MerchantNumber_Idx', 'Number', unique=True),
                              sqlite_autoincrement=True)

mapper(MerchantNumber, merchant_number_table, properties={
    'id': merchant_number_table.columns.Id,
    'number': merchant_number_table.columns.Number
})

merchant_history_table = Table('MerchantHistory', metadata,
                               Column('Id', Integer, primary_key=True, nullable=False),
                               Column('MerchantId', Integer, nullable=True),
                               Column('History', String),
                               Index('MerchantId_Idx', 'MerchantId', unique=False),
                               sqlite_autoincrement=True)

metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

mn = MerchantNumber(number='123456799765')
session.add(mn)
session.flush()
mh = MerchantHistory(id=mn.id)
mh.history = 'dsadasdasdas'
session.commit()
query = session.query(MerchantNumber).filter(MerchantNumber.number.like('123%'))
r = query.all()
print(r)
session.close()