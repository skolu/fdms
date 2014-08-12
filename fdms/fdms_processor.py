from enum import Enum
from .fdms_model import *
from .sqlite_storage import SqlFdmsStorage
from .leveldb_storage import LevelDbStorage
from . import LOG_NAME
import logging
import math


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


MONETARY_TRANSACTIONS = {FdmsTxnCode.Sale, FdmsTxnCode.Return, FdmsTxnCode.TicketOnly,
                         FdmsTxnCode.AuthOnly, FdmsTxnCode.VoidSale, FdmsTxnCode.VoidReturn,
                         FdmsTxnCode.VoidTicketOnly}

TRANSACTION_VOID = {FdmsTxnCode.VoidSale, FdmsTxnCode.VoidReturn, FdmsTxnCode.VoidTicketOnly}


class BatchCloseState(Enum):
    ReadyToClose = 1
    HostSpecificPollTransaction = 2
    RevisionInquiry = 3
    HostSpecificPollRevision = 4
    Closed = 5


class FdmsActionCode(Enum):
    RegularResponse = '0'
    HostSpecificPoll = '1'
    RevisionInquiry = '2'
    PartialApproval = '3'


class FdmsTransactionType(Enum):
    Online = '0'
    OfflinePiggyBack = '1'
    OfflineCloseBatch = '2'
    RevisedPiggyBack = '3'
    RevisedCloseBatch = '4'
    SpecificPollRevised = '5'
    SpecificPollTransaction = '6'


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
        self.txn_code = FdmsTxnCode.Close.value

    def create_txn(self) -> FdmsTransaction:
        txn_code = FdmsTxnCode(self.txn_code)
        if txn_code in MONETARY_TRANSACTIONS:
            if self.wcc in ['@', 'B']:
                return KeyedMonetaryTransaction()
            else:
                return SwipedMonetaryTransaction()
        elif txn_code == FdmsTxnCode.Close:
            return BatchCloseTransaction()
        elif txn_code == FdmsTxnCode.RevisionInquiry:
            return RevisionInquiryTransaction()
        elif txn_code == FdmsTxnCode.DepositInquiry:
            return DepositInquiryTransaction()
        elif txn_code == FdmsTxnCode.NegativeResponse:
            return NegativeResponseTransaction()

        raise ValueError('Transaction is not supported')


class DepositInquiryTransaction(FdmsTransaction):
    pass


class NegativeResponseTransaction(FdmsTransaction):
    pass


class RevisionInquiryTransaction(FdmsTransaction):
    def __init__(self):
        super().__init__()
        self.item_no = ''
        self.revisions = list()
        ''':type: list of [str]'''


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
        self.batch_items = dict()
        """:type : dict of [str, BatchRecord]"""
        self.poll_items = set()
        ''':type: set of [str]'''
        self.last_item_no = '001'
        self.state = BatchCloseState.ReadyToClose

    def set_last_item_no(self, number):
        if number >= 1000:
            number = 999
        self.last_item_no = '%.3d' % number


class FdmsTextResponse(FdmsResponse):
    def __init__(self):
        super().__init__()
        self.response_text = ''


class BatchResponse(FdmsTextResponse):
    def __init__(self):
        super().__init__()
        self.batch_id_number = ''
        self.response_text2 = ''

    def set_batch_id_number(self, number: int):
        self.batch_id_number = '%.6d' % number


class CreditResponse(FdmsTextResponse):
    def __init__(self):
        super().__init__()
        self.avc_rs_code = ''
        self.cvv_rs_code = ''
        self.transaction_id = ''
        self.balance_amount = None
        ''':type : float'''
        self.approved_amount = 0.0
        self.requested_amount = 0.0


class SpecificPollResponse(FdmsResponse):
    def __init__(self):
        super().__init__()
        self.request_type = FdmsTransactionType.SpecificPollTransaction.value


#Storage = SqlFdmsStorage
Storage = LevelDbStorage

INV_BATCH_SEQ = 'INV BATCH SEQ'
INVLD_BATCH_SEQ = 'INVLD BATCH SEQ'
INV_TRAN_CODE = 'INV TRAN CODE'
INV_AUTH_CODE = 'INV AUTH CODE'
UNMATCHED_VOID = 'UNMATCHED VOID'
INVALID_PIN = 'INVALID PIN'
CLOSE_UNAVAIL = 'CLOSE UNAVAIL'


def process_add_on_txn(main: (FdmsHeader, FdmsTransaction), add_on: (FdmsHeader, FdmsTransaction)):
    if isinstance(main[1], BatchCloseTransaction) and isinstance(add_on[1], RevisionInquiryTransaction):
        close_txn = main[1]
        ''':type: BatchCloseTransaction'''
        revision_txn = add_on[1]
        ''':type: RevisionInquiryTransaction'''
        start_item_no = int(revision_txn.item_no)
        for i in range(len(revision_txn.revisions)):
            item_no = '%.3d' % (start_item_no + i)
            revision = revision_txn.revisions[i]
            if len(revision) == 1:
                if '0' <= revision <= '9':
                    item = close_txn.batch_items[item_no]
                    if item is not None:
                        if item.revision_no < revision:
                            close_txn.poll_items.add(item_no)

        start_item_no += len(revision_txn.revisions)
        close_txn.set_last_item_no(start_item_no)
    else:
        raise ValueError(INV_TRAN_CODE)


def process_txn(transaction: (FdmsHeader, FdmsTransaction)) -> FdmsResponse:
    header, body = transaction
    response = FdmsTextResponse()
    try:
        if isinstance(body, DepositInquiryTransaction):
            response = BatchResponse()
            process_deposit_inquiry(header, response)
        elif isinstance(body, MonetaryTransaction):
            response = CreditResponse()
            response.item_no = body.item_no
            response.batch_no = body.batch_no
            response.revision_no = body.revision_no
            process_monetary_transaction(header, body, response)
        elif isinstance(body, BatchCloseTransaction):
            response = BatchResponse()
            response.batch_no = body.batch_no
            response.item_no = body.item_no
            return process_batch_close(header, body)
        else:
            response.set_negative()
            response.response_text = INV_TRAN_CODE
    except ValueError as ve:
        logging.getLogger(LOG_NAME).debug('Txn Process Error: %s', str(ve))
        response.set_negative()
        response.response_text = ve.args[0]
    except Exception as e:
        logging.getLogger(LOG_NAME).debug('Txn Process Error: %s', str(e))
        response.set_negative()
        response.response_text = 'ERROR'
    return response


def process_batch_close(header: FdmsHeader, body: BatchCloseTransaction) -> FdmsResponse:
    def get_specific_poll_response() -> SpecificPollResponse:
        if len(body.poll_items) == 0:
            return None
        rs = SpecificPollResponse()
        rs.set_positive()
        rs.item_no = body.poll_items.pop()
        rs.batch_no = body.batch_no
        rs.action_code = FdmsActionCode.HostSpecificPoll
        rs.request_type = FdmsTransactionType.SpecificPollTransaction.value \
            if body.state == BatchCloseState.HostSpecificPollTransaction \
            else FdmsTransactionType.SpecificPollRevised.value

        return rs

    def get_revision_inquiry_response() -> FdmsResponse:
        if body.last_item_no >= body.item_no:
            return None
        rs = FdmsResponse()
        rs.set_positive()
        rs.item_no = body.last_item_no
        rs.batch_no = body.batch_no
        rs.action_code = FdmsActionCode.RevisionInquiry
        body.last_item_no = body.item_no

        return rs

    if body.state in {BatchCloseState.HostSpecificPollTransaction, BatchCloseState.HostSpecificPollRevision}:
        response = get_specific_poll_response()
        if response is not None:
            return response
    elif body.state == BatchCloseState.RevisionInquiry:
        response = get_revision_inquiry_response()
        if response is not None:
            return response

    with Storage() as storage:
        batch = storage.get_open_batch(header.merchant_number, header.device_id)
        if batch is None:
            raise ValueError(INVLD_BATCH_SEQ)

        body.batch_items.clear()
        for item in storage.query_batch_items(batch.id):
            assert isinstance(item, BatchRecord)
            body.batch_items[item.item_no] = item

        max_item_no = 0
        credit_count = 0
        debit_count = 0
        credit_amount = 0.0
        debit_amount = 0.0
        for item in body.batch_items.values():
            assert isinstance(item, BatchRecord)
            txn_code = FdmsTxnCode(item.txn_code)
            amount = 0.0
            if txn_code not in TRANSACTION_VOID:
                amount = item.amount
                if txn_code == FdmsTxnCode.Return:
                    amount = -amount
            if item.is_credit:
                credit_count += 1
                credit_amount += amount
                item_number = int(item.item_no)
                if item_number > max_item_no:
                    max_item_no = item_number
            else:
                debit_count += 1
                debit_amount += amount

        max_item_no += 1
        rq_max_item_no = int(body.item_no)

        if body.state == BatchCloseState.ReadyToClose:
            body.state = BatchCloseState.HostSpecificPollTransaction
            body.poll_items.clear()
            for i in range(1, rq_max_item_no):
                i_no = '%.3d' % i
                if i_no not in body.batch_items:
                    body.poll_items.add(i_no)

            response = get_specific_poll_response()
            if response is not None:
                logging.getLogger(LOG_NAME).debug('Batch close: Host specific poll transaction is required')
                return response

        if math.fabs(body.credit_batch_amount - credit_amount) > 0.01:
            if body.state == BatchCloseState.HostSpecificPollTransaction:
                body.state = BatchCloseState.RevisionInquiry
                body.last_item_no = '001'

                body.poll_items.clear()
                response = get_revision_inquiry_response()
                if response is not None:
                    logging.getLogger(LOG_NAME).debug('Batch close: revision inquiry is required')
                    return response
            elif body.state == BatchCloseState.RevisionInquiry:
                body.state = BatchCloseState.HostSpecificPollRevision
                response = get_specific_poll_response()
                if response is not None:
                    logging.getLogger(LOG_NAME).debug('Batch close: Host specific poll revision is required')
                    return response

        body.state = BatchCloseState.Closed
        storage.close_batch(batch, (credit_count, credit_amount), (debit_count, debit_amount))
        storage.save()

        response = BatchResponse()
        response.set_positive()
        response.batch_no = body.batch_no
        response.set_batch_id_number(batch.id)
        response.set_item_number(credit_count + debit_count)
        ok = max_item_no == rq_max_item_no and math.fabs(body.credit_batch_amount - credit_amount) < 0.01
        response.response_text = '%s %8.2f' % ('CLOSE' if ok else 'FORCE', credit_amount)
        if debit_count > 0:
            ok = debit_count == body.debit_batch_count and math.fabs(body.debit_batch_amount - debit_amount) < 0.01
            response.response_text2 = '%s %8.2f' % ('CLOSE' if ok else 'FORCE', debit_amount)

        body.state = BatchCloseState.Closed
        return response

    raise ValueError(CLOSE_UNAVAIL)


def process_deposit_inquiry(header: FdmsHeader, response: BatchResponse):
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
                    if txn_code == FdmsTxnCode.Sale:
                        response.approved_amount = record.amount
                        response.requested_amount = record.amount
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
                for a in storage.query_authorization(header.merchant_number, body.authorization_code):
                    if not a.is_captured:
                        authorization = a
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
