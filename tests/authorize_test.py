import unittest
import fdms.fdms_processor as processor
from fdms_test_case import FdmsTestCase

class AuthorizeTest(FdmsTestCase):

    def test_auth_capture_void(self):
        self.header.wcc = '@'
        self.header.txn_type = '0'
        self.header.txn_code = processor.FdmsTxnCode.AuthOnly.value

        body = processor.KeyedMonetaryTransaction()
        body.batch_no = '0'
        body.revision_no = '0'
        body.invoice_no = 'Invoice'
        body.total_amount = 10.0
        body.item_no = '000'
        body.account_no = '377481701087006'
        body.exp_date = '0314'

        response = processor.process_txn((self.header, body))
        self.assertIsInstance(response, processor.CreditResponse)
        self.assertEqual(response.response_code, '0')
        self.assertTrue(response.response_text.startswith('APPROVED'))
        auth_code = response.response_text[len('APPROVED')+1:]
        auth_code = auth_code.strip(' ')

        self.header.txn_code = processor.FdmsTxnCode.TicketOnly.value

        body.item_no = '001'
        body.authorization_code = auth_code
        response = processor.process_txn((self.header, body))
        self.assertIsInstance(response, processor.CreditResponse)
        self.assertEqual(response.response_code, '0')


        self.header.txn_code = processor.FdmsTxnCode.VoidTicketOnly.value
        body.revision_no = '1'
        response = processor.process_txn((self.header, body))
        self.assertIsInstance(response, processor.CreditResponse)
        self.assertEqual(response.response_code, '0')

    def test_swiped_sale(self):
        self.header.wcc = '@'
        self.header.txn_type = '0'
        self.header.txn_code = processor.FdmsTxnCode.Sale.value

        body = processor.SwipedMonetaryTransaction()
        body.batch_no = '0'
        body.item_no = '002'
        body.total_amount = 20.0
        body.invoice_no = 'SwipedSale'
        body.revision_no = '0'
        body.track_data = '%B4393410316009875^KOLUPAEV/ SERGEY^17061211000000762000000?'

        response = processor.process_txn((self.header, body))
        self.assertIsInstance(response, processor.CreditResponse)
        self.assertEqual(response.response_code, '0')
        self.assertTrue(response.response_text.startswith('AUTH/TKT'))
        self.assertEqual(response.batch_no, body.batch_no)
        self.assertEqual(response.item_no, body.item_no)
        self.assertEqual(response.batch_no, body.batch_no)

    def test_swiped_return(self):
        self.header.wcc = '@'
        self.header.txn_type = '0'
        self.header.txn_code = processor.FdmsTxnCode.Return.value

        body = processor.SwipedMonetaryTransaction()
        body.batch_no = '0'
        body.item_no = '003'
        body.total_amount = 10.0
        body.invoice_no = 'SwipedReturn'
        body.revision_no = '0'
        body.track_data = ';4393410316009875=170612110000762?'

        response = processor.process_txn((self.header, body))
        self.assertIsInstance(response, processor.CreditResponse)
        self.assertEqual(response.response_code, '0')
        self.assertTrue(response.response_text.startswith('RETURN'))

    def test_debit_sale(self):
        self.header.wcc = '@'
        self.header.txn_type = '0'
        self.header.txn_code = processor.FdmsTxnCode.Sale.value

        body = processor.SwipedMonetaryTransaction()
        body.batch_no = '0'
        body.item_no = '999'
        body.total_amount = 10.0
        body.invoice_no = 'DebitSale'
        body.revision_no = '0'
        body.track_data = ';4393410316009875=170612110000762?'
        body.card_type = 'D'
        body.pin_block = '<PIN BLOCK>'
        body.smid_block = '<SMID BLOCK>'

        response = processor.process_txn((self.header, body))
        self.assertIsInstance(response, processor.CreditResponse)
        self.assertEqual(response.response_code, '0')
        self.assertTrue(response.response_text.startswith('AUTH/TKT'))
        self.assertEqual(response.batch_no, body.batch_no)
        self.assertEqual(response.item_no, body.item_no)
        self.assertEqual(response.batch_no, body.batch_no)


if __name__ == '__main__':
    unittest.main()
