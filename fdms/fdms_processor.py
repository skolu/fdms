from enum import Enum


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




def process_txn(header: FdmsHeader, txn: FdmsTransaction) -> FdmsResponse:
    if isinstance(txn, DepositInquiryTransaction):
        return DepositInquiryResponse()

    return None