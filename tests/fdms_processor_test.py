import unittest
import fdms.fdms_processor as processor

class DepositInquiryTest(unittest.TestCase):

    def setUp(self):
        self.header = processor.FdmsHeader()
        self.header.protocol_type = '1'
        self.header.terminal_id = 'POSHOME1.'
        self.header.merchant_number = '1234567890'
        self.header.device_id = '0239'

    def test_auth_only(self):
        self.header.wcc = '@'
        self.header.txn_type = '0'
        self.header.txn_code = '4'

        body = processor.KeyedMonetaryTransaction()
        body.total_amount = 10.0
        body.invoice_no = 'Invoice'
        body.item_no = '000'
        body.batch_no = '0'
        body.revision_no = '0'
        body.account_no = '377481701087006'
        body.exp_date = '0314'

        response = processor.process_txn(self.header, body)
        self.assertIsInstance(response, processor.CreditResponse)

    def test_deposit_inquiry(self):
        self.header.wcc = '@'
        self.header.txn_type = '0'
        self.header.txn_code = '9'

        body = processor.DepositInquiryTransaction()

        response = processor.process_txn(self.header, body)
        self.assertIsInstance(response, processor.DepositInquiryResponse)
        self.assertEqual(response.response_code, '0')

if __name__ == '__main__':
    unittest.main()
