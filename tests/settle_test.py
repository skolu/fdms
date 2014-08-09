import unittest
import fdms.fdms_processor as processor
from fdms_test_case import FdmsTestCase
from authorize_test import AuthorizeTest


class SettleTest(FdmsTestCase):
    def test_settle(self):
        self.header.wcc = '@'
        self.header.txn_type = '0'
        self.header.txn_code = processor.FdmsTxnCode.Close.value

        body = processor.BatchCloseTransaction()
        body.batch_no = '0'
        body.item_no = '005'
        body.credit_batch_amount = 10.0
        body.debit_batch_amount = 10.0
        body.debit_batch_count = 1

        rs = processor.process_txn((self.header, body))
        self.assertTrue(isinstance(rs, processor.SpecificPollResponse))
        self.assertEqual(rs.item_no, '004')

        body.item_no = '004'
        rs = processor.process_txn((self.header, body))
        self.assertTrue(isinstance(rs, processor.BatchResponse))



def suite():
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.makeSuite(AuthorizeTest))
    test_suite.addTest(unittest.makeSuite(SettleTest))
    return test_suite

if __name__ == "__main__":
    unittest.main()