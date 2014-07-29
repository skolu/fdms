from enum import Enum
from .fdms_model import *
from .fdms_storage import SqlFdmsStorage

class FdmsTxnCode(Enum):
    Close = '0'
    Sale = '1'
    Return = '2'
    TicketOnly = '3'
    AuthOnly = '4'
    VoidSale = '5'
    VoidReturn = '6'
    VoidTicketOnly = '7'
    DepositInquiry = '9'
    RevisionInquiry = 'I'
    NegativeResponse = 'N'

MONETARY_TRANSACTIONS = {FdmsTxnCode.Sale, FdmsTxnCode.Return, FdmsTxnCode.TicketOnly, FdmsTxnCode.AuthOnly,
                         FdmsTxnCode.VoidSale, FdmsTxnCode.VoidReturn, FdmsTxnCode.VoidTicketOnly}

TRANSACTION_VOID = {FdmsTxnCode.VoidSale, FdmsTxnCode.VoidReturn, FdmsTxnCode.VoidTicketOnly}

class FdmsTransaction:
    def __init__(self):
        pass

    def parse(self, data: bytes):
        raise NotImplementedError('%s.parse' % self.__class__.__name__)


class FdmsResponse:
    def __init__(self):
        self.action_code = '0'
        self.response_code = '0'
        self.batch_number = '0'
        self.item_number = '000'
        self.revision_number = '0'
        self.response_text = ''

    def body(self) -> bytes:
        raise NotImplementedError('%s.body' % self.__class__.__name__)

    def response(self) -> bytes:
        raise NotImplementedError('%s.response' % self.__class__.__name__)

    def set_positive(self):
        self.response_code = '0'

    def set_negative(self):
        self.response_code = '1'

    def set_item_number(self, number: int):
        n = '%.3d' % number
        if len(n) != 3:
            raise ValueError('Item Number: %d' % number)
        self.item_number = n

    def set_batch_number(self, number: int):
        n = str(number)
        if len(n) != 1:
            raise ValueError('Batch number: %d' % number)

        self.batch_number = n

    def set_revision(self, number: int):
        n = str(number)
        if len(n) != 1:
            raise ValueError('Revision: %d' % number)
        self.revision_number = n


class FdmsHeader:
    def __init__(self):
        self.protocol_type = ''
        self.terminal_id = ''
        self.merchant_number = ''
        self.device_id = ''
        self.wcc = ''
        self.txn_type = ''
        self.txn_code = FdmsTxnCode.Close

    def create_txn(self) -> FdmsTransaction:
        if self.txn_code in MONETARY_TRANSACTIONS:
            if self.wcc in ['@', 'B']:
                return KeyedMonetaryTransaction()
            else:
                return SwipedMonetaryTransaction()
        elif self.txn_code == FdmsTxnCode.Close:
            raise ValueError('Transaction is not supported')
        elif self.txn_code == FdmsTxnCode.DepositInquiry:
            return DepositInquiryTransaction()

        raise ValueError('Transaction is not supported')


class DepositInquiryTransaction(FdmsTransaction):
    pass


class MonetaryTransaction(FdmsTransaction):
    def __init__(self):
        super().__init__()
        self.total_amount = 0.0
        self.invoice_no = ''
        self.batch_no = ''
        self.item_no = ''
        self.revision_no = ''


class SwipedMonetaryTransaction(MonetaryTransaction):
    def __init__(self):
        super().__init__()
        self.track_data = ''


class KeyedMonetaryTransaction(MonetaryTransaction):
    def __init__(self):
        super().__init__()
        self.account_no = ''
        self.cv_presence = ''
        self.cvv = ''
        self.exp_date = ''


class DepositInquiryResponse(FdmsResponse):
    def __init__(self):
        super().__init__()
        self.batch_id_number = ''


class CreditResponse(FdmsResponse):
    def __init__(self):
        super().__init__()
        self.avc_rs_code = ''
        self.cvv_rs_code = ''
        self.transaction_id = ''

Storage = SqlFdmsStorage
''':type: FdmsStorage'''

INV_BATCH_SEQ = 'INV BATCH SEQ'

def process_txn(header: FdmsHeader, txn: FdmsTransaction) -> FdmsResponse:
    if isinstance(txn, DepositInquiryTransaction):
        ''':type: txn: DepositInquiryTransaction'''
        response = DepositInquiryResponse()
        try:
            with Storage() as storage:
                last_batch = storage.last_closed_batch(header.merchant_number, header.device_id)
                response.set_positive()
                response.set_revision(0)
                if last_batch is None:
                    last_batch = ClosedBatch()
                    last_batch.id = 0
                    last_batch.merchant_number = header.merchant_number
                    last_batch.device_id = header.device_id
                    last_batch.batch_number = '0'

                response.set_item_number(last_batch.credit_count + last_batch.debit_count)
                response.set_batch_number(last_batch.batch_number)
                response.response_text = 'DEP %8.2f' % (last_batch.credit_amount + last_batch.debit_amount,)
                response.batch_id_number = '%d' % last_batch.id
        except Exception as e:
            response.set_negative()
            response.set_revision(0)
            response.response_text = 'UNKNOWN ERROR %s' % e

        return response

    elif isinstance(txn, KeyedMonetaryTransaction):
        ''':type: txn: KeyedMonetaryTransaction'''
        response = CreditResponse()
        response.batch_number = txn.batch_no
        response.revision_number = txn.revision_no
        response.set_negative()
        response.response_text = INV_BATCH_SEQ
        try:
            txn_code = FdmsTxnCode(header.txn_code)
            if txn_code in TRANSACTION_VOID:
                if txn.revision_no == '0':
                    return response
            elif txn_code == FdmsTxnCode.TicketOnly:
                if txn.revision_no != '0':
                    return response

            with Storage() as storage:
                batch = storage.get_open_batch(header.merchant_number, header.device_id)
                if batch is None:
                    last_batch = storage.last_closed_batch(header.merchant_number, header.device_id)
                    if last_batch is not None:
                        if last_batch.batch_number == txn.batch_no:
                            return response

                    batch = storage.create_batch(header.merchant_number, header.device_id, txn.batch_no)
                else:
                    if batch.batch_number != txn.batch_no:
                        return response

                record = storage.get_batch_record(batch.id, txn.item_no)
                if record is None:
                    if txn.revision_no != '0':
                        return response
                else:
                    if int(txn.revision_no) - int(record.revision_number) != 1:
                        return response

                if txn_code in TRANSACTION_VOID:
                    if record is None:
                        return response
                    cur_code = FdmsTxnCode(record.txn_code)
                    if not ((txn_code == FdmsTxnCode.VoidSale and cur_code == FdmsTxnCode.Sale) or
                            (txn_code == FdmsTxnCode.VoidReturn and cur_code == FdmsTxnCode.Return) or
                            (txn_code == FdmsTxnCode.VoidTicketOnly and cur_code == FdmsTxnCode.TicketOnly)):
                        return response

                    record.txn_code = header.txn_code
                    record.revision_number = txn.revision_no
                    storage.put_batch_record(record)
                elif txn_code in {FdmsTxnCode.Sale, FdmsTxnCode.Return}:
                    #Query batch record
                    #Authorize card or Query authorization
                    #Prepare batch record
                    pass
                elif txn_code == FdmsTxnCode.TicketOnly:
                    # Query authorization
                    # Create batch record
                    pass





                storage.save()

                response.set_positive()


        except Exception as e:
            response.set_negative()
            response.set_revision(0)
            response.response_text = 'UNKNOWN ERROR %s' % e

        return response



    return None