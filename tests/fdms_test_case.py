import unittest
import fdms.fdms_processor as processor

class FdmsTestCase(unittest.TestCase):

    def setUp(self):
        self.header = processor.FdmsHeader()
        self.header.protocol_type = '1'
        self.header.terminal_id = 'POSHOME1.'
        self.header.merchant_number = '1234567890'
        self.header.device_id = '0239'

