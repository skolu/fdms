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

class FdmsActionCode(Enum):
    RegularResponse = '0'
    HostResponse = '1'
    RevisionInquiry = '2'
    PartialApproval = '3'

class FdmsTransaction:
    def __init__(self):
        pass

    def parse(self, data: bytes):
        raise NotImplementedError('%s.parse' % self.__class__.__name__)


class FdmsResponse:
    def __init__(self):
        self.action_code = FdmsActionCode.RegularResponse
        self.response_code = '0'
        self.batch_no = '0'
        self.item_no = '000'
        self.revision_no = '0'

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
        self.item_no = n

    def set_batch_number(self, number: int):
        n = str(number)
        if len(n) != 1:
            raise ValueError('Batch number: %d' % number)

        self.batch_no = n

    def set_revision(self, number: int):
        n = str(number)
        if len(n) != 1:
            raise ValueError('Revision: %d' % number)
        self.revision_no = n

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
        self.format_code = ''
        self.transaction_id = ''
        self.card_type = ''
        self.pin_block = ''
        self.smid_block = ''
        self.authorization_code = ''
        self.partial_indicator = ''


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


class BatchCloseTransaction(FdmsTransaction):
    def __init__(self):
        super().__init__()
        self.batch_no = ''
        self.item_no = '000'
        self.credit_batch_amount = 0.0
        self.debit_batch_count = 0
        self.debit_batch_amount = 0.0
        self.offline_items = 0


class FdmsTextResponse(FdmsResponse):
    def __init__(self):
        super().__init__()
        self.response_text = ''


class BatchResponse(FdmsTextResponse):
    def __init__(self):
        super().__init__()
        self.batch_id_number = ''
        self.response_text2 = ''


class CreditResponse(FdmsTextResponse):
    def __init__(self):
        super().__init__()
        self.avc_rs_code = ''
        self.cvv_rs_code = ''
        self.transaction_id = ''


Storage = SqlFdmsStorage

INV_BATCH_SEQ = 'INV BATCH SEQ'
INVLD_BATCH_SEQ = 'INVLD BATCH SEQ'
INV_TRAN_CODE = 'INV TRAN CODE'
INV_AUTH_CODE = 'INV AUTH CODE'
UNMATCHED_VOID = 'UNMATCHED VOID'
INVALID_PIN = 'INVALID PIN'

def process_txn(header: FdmsHeader, txn: FdmsTransaction) -> FdmsResponse:
    response = FdmsTextResponse()
    try:
        if isinstance(txn, DepositInquiryTransaction):
            response = BatchResponse()
            process_deposit_inquiry(header, txn, response)
        elif isinstance(txn, MonetaryTransaction):
            response = CreditResponse()
            response.item_no = txn.item_no
            response.batch_no = txn.batch_no
            response.revision_no = txn.revision_no
            process_monetary_transaction(header, txn, response)
        elif isinstance(txn, BatchCloseTransaction):
            return process_batch_close(header, txn)
        else:
            response.set_negative()
            response.response_text = INV_TRAN_CODE
    except ValueError as ve:
        response.set_negative()
        response.response_text = ve
    except Exception as e:
        response.set_negative()
        response.response_text = 'ERROR %s' % e
    return response

def process_batch_close(header: FdmsHeader, body: BatchCloseTransaction) -> FdmsResponse:
    with Storage() as storage:
        batch = storage.get_open_batch(header.merchant_number, header.device_id)
        if batch is None:
            response = BatchResponse()
            response.set_negative()
            response.response_text = INVLD_BATCH_SEQ
            return response





def process_deposit_inquiry(header: FdmsHeader, body: DepositInquiryTransaction, response: BatchResponse):
    with Storage() as storage:
        last_batch = storage.last_closed_batch(header.merchant_number, header.device_id)
        if last_batch is None:
            last_batch = ClosedBatch()
            last_batch.id = 0
            last_batch.merchant_number = header.merchant_number
            last_batch.device_id = header.device_id
            last_batch.batch_no = '0'

        response.set_positive()
        response.set_revision(0)
        response.set_item_number(last_batch.credit_count + last_batch.debit_count)
        response.set_batch_number(last_batch.batch_no)
        response.response_text = 'DEP %8.2f' % (last_batch.credit_amount + last_batch.debit_amount,)
        response.batch_id_number = '%d' % last_batch.id


def process_monetary_transaction(header: FdmsHeader, body: MonetaryTransaction, response: CreditResponse):

    def calc_card_hash() -> str:
        if isinstance(body, KeyedMonetaryTransaction):
            return card_info_md5(body.account_no, body.exp_date)
        elif isinstance(body, SwipedMonetaryTransaction):
            card_no, card_exp = extract_card_info(body.track_data)
            return card_info_md5(card_no, card_exp)

    def authorize(capture: bool) -> Authorization:
        auth = Authorization()
        auth.is_captured = capture
        auth.merchant_number = header.merchant_number
        auth.card_hash = calc_card_hash()
        if body.card_type == 'D':
            auth.is_credit = False
            auth.is_captured = True
            if body.pin_block is None or body.smid_block is None:
                raise ValueError(INVALID_PIN)
            if len(body.pin_block) == 0 or len(body.smid_block) == 0:
                raise ValueError(INVALID_PIN)
        else:
            auth.is_credit = True
        auth.amount = body.total_amount
        storage.put_authorization(auth)
        auth.authorization_code = str(auth.id).rjust(6, '0')
        return auth

    response.set_negative()
    response.response_text = INV_BATCH_SEQ
    txn_code = FdmsTxnCode(header.txn_code)
    if txn_code == FdmsTxnCode.AuthOnly:
        if body.item_no != '000':
            raise ValueError(INV_BATCH_SEQ)
        if body.batch_no != '0':
            raise ValueError(INV_BATCH_SEQ)
    else:
        no = int(body.item_no)
        if no < 1 or no > 999:
            raise ValueError(INV_BATCH_SEQ)

    if txn_code in TRANSACTION_VOID:
        if body.revision_no == '0':
            raise ValueError(INV_BATCH_SEQ)
    elif txn_code == FdmsTxnCode.TicketOnly:
        if body.revision_no != '0':
            raise ValueError(INV_BATCH_SEQ)
    elif txn_code == FdmsTxnCode.AuthOnly:
        if body.revision_no != '0':
            raise ValueError(INV_BATCH_SEQ)

    if body.card_type == 'D':
        if body.revision_no != '0':
            raise ValueError(INV_BATCH_SEQ)
        if txn_code != FdmsTxnCode.Sale:
            raise ValueError(INV_TRAN_CODE)
        if not isinstance(body, SwipedMonetaryTransaction):
            raise ValueError(INV_TRAN_CODE)

    with Storage() as storage:
        if txn_code == FdmsTxnCode.AuthOnly:
            authorization = authorize(False)
            response.response_text = 'APPROVED %s' % authorization.authorization_code
        else:
            batch = storage.get_open_batch(header.merchant_number, header.device_id)
            if batch is None:
                last_batch = storage.last_closed_batch(header.merchant_number, header.device_id)
                if last_batch is not None:
                    if last_batch.batch_no == body.batch_no:
                        raise ValueError(INV_BATCH_SEQ)

                batch = storage.create_batch(header.merchant_number, header.device_id, body.batch_no)
            else:
                if batch.batch_no != body.batch_no:
                    raise ValueError(INV_BATCH_SEQ)

            record = storage.get_batch_record(batch.id, body.item_no)
            if record is None:
                if body.revision_no != '0':
                    raise ValueError(INV_BATCH_SEQ)
            else:
                if int(body.revision_no) - int(record.revision_no) != 1:
                    raise ValueError(INV_BATCH_SEQ)

            if txn_code in TRANSACTION_VOID:
                if record is None:
                    raise ValueError(UNMATCHED_VOID)
                cur_code = FdmsTxnCode(record.txn_code)
                if not ((txn_code == FdmsTxnCode.VoidSale and cur_code == FdmsTxnCode.Sale) or
                            (txn_code == FdmsTxnCode.VoidReturn and cur_code == FdmsTxnCode.Return) or
                            (txn_code == FdmsTxnCode.VoidTicketOnly and cur_code == FdmsTxnCode.TicketOnly)):
                    raise ValueError(UNMATCHED_VOID)

                record.txn_code = header.txn_code
                record.revision_no = body.revision_no
                storage.put_batch_record(record)
                response.response_text = ''

            elif txn_code in {FdmsTxnCode.Sale, FdmsTxnCode.Return}:
                authorization = None
                ''':type: Authorization'''

                if record is None:
                    authorization = authorize(True)
                    record = BatchRecord()
                    record.batch_id = batch.id
                    record.auth_id = authorization.id
                    record.item_no = body.item_no
                    record.txn_code = header.txn_code
                    record.is_credit = authorization.is_credit
                    record.amount = body.total_amount
                    storage.put_batch_record(record)
                    response.transaction_id = str(record.id).zfill(10)
                    response.response_text = '%s %s' % ('AUTH/TKT' if txn_code == FdmsTxnCode.Sale else 'RETURN',
                                                        authorization.authorization_code)
                else:
                    if header.txn_code != record.txn_code:
                        raise ValueError(INV_TRAN_CODE)
                    if len(body.authorization_code) == 0:
                        raise ValueError(INV_AUTH_CODE)
                    authorization = storage.get_authorization(record.auth_id)
                    if authorization is None:
                        raise ValueError(INV_AUTH_CODE)
                    if authorization.card_hash != calc_card_hash():
                        raise ValueError(INV_AUTH_CODE)
                    if authorization.authorization_code != body.authorization_code:
                        raise ValueError(INV_AUTH_CODE)
                    response.response_text = ''

                record.revision_no = body.revision_no
                record.amount = body.total_amount
                storage.put_authorization(authorization)
                storage.put_batch_record(record)
            elif txn_code == FdmsTxnCode.TicketOnly:
                if len(body.authorization_code) == 0:
                    raise ValueError(INV_AUTH_CODE)
                if record is not None:
                    raise ValueError(INV_TRAN_CODE)

                authorization = None
                for auth in storage.query_authorization(header.merchant_number, body.authorization_code):
                    if not auth.is_captured:
                        authorization = auth
                        break
                if authorization is None:
                    raise ValueError(INV_AUTH_CODE)

                authorization.is_captured = True
                record = BatchRecord()
                record.batch_id = batch.id
                record.auth_id = authorization.id
                record.item_no = body.item_no
                record.txn_code = header.txn_code
                record.is_credit = authorization.is_credit
                record.revision_no = body.revision_no
                record.amount = body.total_amount
                storage.put_authorization(authorization)
                storage.put_batch_record(record)
                response.response_text = '%s %s' % ('TKT CODE', authorization.authorization_code)
            else:
                raise ValueError(INV_TRAN_CODE)

        storage.save()
    response.set_positive()
