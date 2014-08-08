import unittest
import fdms.fdms_protocol as protocol

class FdmsParserTest(unittest.TestCase):
    def test_auth_parse(self):
        body = b'\x1c4111111111111111\x1f0\x1f\x1c1214\x1c1.00\x1cMK711CAP\x1c10010\x1c6\x1c\x1c\x1c\x1c\x1c95630' \
               b'\x1c\x1c\x1c\x1c\x1c\x1c\x1c\x1c\x1c\x1c\x1c\x1c\x1c\x1c\x1c\x1c\x03L'

        fs_pos = list(protocol.sep_gen(protocol.FS, body, 0, 5))
        self.assertEqual(len(fs_pos), 5)

    def test_buffer_chop(self):
        buffer = b'0 1 2 3 4 5 6 7 8 9'
        digits = list(protocol.buf_chop(buffer, protocol.sep_gen(b' '[0], buffer)))
        self.assertEqual(len(digits), 10)
        for i in range(len(digits)):
            self.assertEqual(digits[i], str(i))

if __name__ == '__main__':
    unittest.main()
