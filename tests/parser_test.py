import unittest
import fdms.fdms_protocol as protocol

class FdmsParserTest(unittest.TestCase):
    def test_auth_parse(self):
        body = b'\x1c4111111111111111\x1f0\x1f\x1c1214\x1c1.00\x1cMK711CAP\x1c10010\x1c6\x1c\x1c\x1c\x1c\x1c95630' \
               b'\x1c\x1c\x1c\x1c\x1c\x1c\x1c\x1c\x1c\x1c\x1c\x1c\x1c\x1c\x1c\x1c\x03L'

        fs_pos = list(protocol.sep_gen(protocol.FS, body, 0, 5))
        self.assertEqual(len(fs_pos), 5)

if __name__ == '__main__':
    unittest.main()
