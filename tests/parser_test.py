import unittest
import fdms.fdms_protocol as protocol
import fdms.fdms_processor as processor

DEPOSIT_INQUIRY_REQUEST = b'\x02*1PIM1.4266962000000048#0239\x1c@09\x1c\x1c\x1c\x1c\x1c\x1c0\x03\x1a'
AUTHORIZE_SWIPE_REQUEST = b'\x02*1PIM1.4266962000000048#0239\x1cC01\x1c6011202300201767=14111011000058900000' \
                          b'\x1c10.00\x1cMK71BB7M\x1c22222\x1c6\x1c\x1c\x1c\x1c\x1c\x1c\x1c\x1c\x1c\x1c\x1c\x1c' \
                          b'\x1c\x1c\x1c\x1c\x1c\x1c\x1c\x1c\x1c\x03L'

AUTHORIZE_KEYED_REQUEST = b'\x02*1PIM1.4266962000000048#0239\x1cB01\x1c4111111111111111\x1f1\x1f123 \x1c1214' \
                          b'\x1c10.00\x1cMK71BCEA\x1c22222\x1c6\x1c\x1c\x1c\x1c\x1c95630\x1c\x1c\x1c\x1c\x1c\x1c' \
                          b'\x1c\x1c\x1c\x1c\x1c\x1c\x1c\x1c\x1c\x1c\x03\x08'

BATCH_SETTLE_REQUEST = b'\x02*2PIM1.4266962000000048#0239\x1c@00\x1c11.00\x1c000\x1c002\x1c2.00\x1c10020\x1c0\x03\x13'

class FdmsParserTest(unittest.TestCase):
    def test_deposit_inquiry_parse(self):
        pos, header = protocol.parse_header(DEPOSIT_INQUIRY_REQUEST)
        self.assertEqual(header.merchant_number, '4266962000000048')
        self.assertEqual(header.device_id, '0239')
        self.assertEqual(header.txn_type, '0')
        self.assertEqual(header.txn_code, processor.FdmsTxnCode.DepositInquiry.value)
        body = header.create_txn()
        self.assertTrue(isinstance(body, processor.DepositInquiryTransaction))
        body.parse(DEPOSIT_INQUIRY_REQUEST[pos:-2])

    def test_authorize_swipe_parse(self):
        pos, header = protocol.parse_header(AUTHORIZE_SWIPE_REQUEST)
        self.assertEqual(header.merchant_number, '4266962000000048')
        self.assertEqual(header.device_id, '0239')
        self.assertEqual(header.txn_type, '0')
        self.assertEqual(header.txn_code, processor.FdmsTxnCode.Sale.value)
        body = header.create_txn()
        self.assertTrue(isinstance(body, processor.SwipedMonetaryTransaction))
        body.parse(AUTHORIZE_SWIPE_REQUEST[pos:-2])
        assert isinstance(body, processor.SwipedMonetaryTransaction)
        self.assertTrue(body.track_data.startswith('601120'))
        self.assertAlmostEquals(body.total_amount, 10.0)
        self.assertEqual(body.invoice_no, 'MK71BB7M')
        self.assertEqual(body.batch_no, '2')
        self.assertEqual(body.item_no, '222')
        self.assertEqual(body.revision_no, '2')
        self.assertEqual(body.format_code, '6')

    def test_authorize_keyed_parse(self):
        pos, header = protocol.parse_header(AUTHORIZE_KEYED_REQUEST)
        self.assertEqual(header.merchant_number, '4266962000000048')
        self.assertEqual(header.device_id, '0239')
        self.assertEqual(header.txn_type, '0')
        self.assertEqual(header.txn_code, processor.FdmsTxnCode.Sale.value)
        body = header.create_txn()
        self.assertTrue(isinstance(body, processor.KeyedMonetaryTransaction))
        body.parse(AUTHORIZE_KEYED_REQUEST[pos:-2])
        assert isinstance(body, processor.KeyedMonetaryTransaction)
        self.assertTrue(body.account_no, '4111111111111111')
        self.assertEqual(body.exp_date, '1214')
        self.assertAlmostEquals(body.total_amount, 10.0)
        self.assertEqual(body.invoice_no, 'MK71BCEA')
        self.assertEqual(body.batch_no, '2')
        self.assertEqual(body.item_no, '222')
        self.assertEqual(body.revision_no, '2')
        self.assertEqual(body.format_code, '6')
        self.assertEqual(body.cv_presence, '1')
        self.assertEqual(body.cvv, '123 ')

    def test_close_batch_parse(self):
        pos, header = protocol.parse_header(BATCH_SETTLE_REQUEST)
        self.assertEqual(header.txn_code, processor.FdmsTxnCode.Close.value)
        body = header.create_txn()
        self.assertTrue(isinstance(body, processor.BatchCloseTransaction))
        body.parse(BATCH_SETTLE_REQUEST[pos:-2])
        assert isinstance(body, processor.BatchCloseTransaction)
        self.assertAlmostEquals(body.credit_batch_amount, 11.0)
        self.assertEqual(body.offline_items, 0)
        self.assertEqual(body.offline_items, 0)
        self.assertEqual(body.debit_batch_count, 2)
        self.assertAlmostEquals(body.debit_batch_amount, 2.0)
        self.assertEqual(body.batch_no, '1')
        self.assertEqual(body.item_no, '002')



    def test_buffer_chop(self):
        buffer = b'0 1 2 3 4 5 6 7 8 9'
        digits = list(protocol.buf_chop(buffer, protocol.sep_gen(b' '[0], buffer)))
        self.assertEqual(len(digits), 10)
        for i in range(len(digits)):
            self.assertEqual(digits[i], str(i))

if __name__ == '__main__':
    unittest.main()
