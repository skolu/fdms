import asyncio
import functools
import logging
from .fdms_processor import *
from . import LOG_NAME

STX = 2
ETX = 3
EOT = 4
ENQ = 5
ACK = 6
NAK = 21
FS = 28
US = 31
SEP = 35

@asyncio.coroutine
def read_fdms_packet(reader: asyncio.StreamReader) -> bytes:
    buffer = bytearray()
    ba = yield from reader.read(1)
    b = ba[0]
    if b > 0x7f:
        b &= 0x7f

    buffer.append(b)

    if b == STX:
        got_etx = False
        while True:
            ba = yield from reader.read(1)
            b = ba[0]
            if b > 0x7f:
                b &= 0x7f
            buffer.append(b)
            if got_etx:
                break
            else:
                got_etx = (b == ETX)

    return buffer

@asyncio.coroutine
def fdms_session(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    online = None
    ''':type: (FdmsHeader, FdmsTransaction)'''
    offline = []

    writer.write(bytes((ENQ,)))
    yield from writer.drain()

    while True:

        # Get Request
        attempt = 0
        while True:
            try:
                if attempt > 4:
                    return

                request = yield from asyncio.wait_for(read_fdms_packet(reader), timeout=15.0)
                if len(request) == 0:
                    return

                control_byte = request[0]
                if control_byte == STX:
                    lrs = functools.reduce(lambda x, y: x ^ int(y), request[2:-1], int(request[1]))
                    if lrs != request[-1]:
                        raise ValueError('LRS sum')

                    pos, header = parse_header(request)
                    txn = header.create_txn()
                    txn.parse(request[pos:])
                    if header.txn_type == '0':
                        if online is not None:
                            return
                        online = (header, txn)
                    else:
                        offline.append((header, txn))

                    # Respond with ACK
                    attempt = 0
                    writer.write(bytes((ACK,)))

                elif control_byte == EOT:
                    break

            # Close session
            except asyncio.TimeoutError as e:
                return

            # Respond with NAK
            except Exception as e:
                logging.getLogger(LOG_NAME).debug('Request error: %s', e)
                attempt += 1
                writer.write(bytes((NAK,)))


            yield from writer.drain()

        if online is None:
            return

        # Process Transactions & Send Response
        for txn in offline:
            rs = process_txn(txn[0], txn[1])
        offline.clear()

        rs = process_txn(online[0], online[1])

        # Send Response
        rs_bytes = rs.response()
        attempt = 0
        while True:
            if attempt >= 4:
                return

            writer.write(rs_bytes)
            yield from writer.drain()

            control_byte = 0
            try:
                while True:
                    rs_head = yield from asyncio.wait_for(reader.read(1), timeout=4.0)
                    if len(rs_head) == 0:
                        return
                    control_byte = rs_head[0] & 0x7f
                    if control_byte == ACK:
                        break
                    elif control_byte == NAK:
                        break
            # Close session
            except asyncio.TimeoutError as e:
                return

            if control_byte == ACK:
                break
            else:
                attempt += 1

        if online[0].wcc in {'B', 'C'}:
            # Send ENQ
            writer.write(bytes((ENQ,)))
            yield from writer.drain()
            continue
        else:
            break

    writer.write(bytes((EOT,)))
    yield from writer.drain()
    if writer.can_write_eof():
        writer.write_eof()


def sep_gen(sep, buffer, offset = 0, count = -1):
    for i in range(offset, len(buffer)):
        if count == 0:
            break
        if buffer[i] == sep:
            yield i
            if count > 0:
                count -= 1


def monetary_parse(self: MonetaryTransaction, data: bytes):
    fs_pos = list(sep_gen(FS, data, 0, 3))
    if len(fs_pos) != 3:
        raise ValueError('Monetary: parse')
    if fs_pos[2] - (fs_pos[1] + 1) != 5:
        raise ValueError('Monetary: parse')

    self.total_amount = float(data[0:fs_pos[0]].decode())
    self.invoice_no = data[fs_pos[0]+1:fs_pos[1]].decode()
    self.batch_no = data[fs_pos[1]+1:fs_pos[1]+2].decode()
    self.item_no = data[fs_pos[1]+2:fs_pos[1]+5].decode()
    self.revision_no = data[fs_pos[1]+5:fs_pos[2]].decode()

    pos = fs_pos[2]+1
    fs_pos = list(sep_gen(FS, data, pos))
    self.format_code = data[pos:fs_pos[0]].decode()
    aux_data = bytes()
    if self.format_code == '6': #Retail
        if len(fs_pos) < 15:
            raise ValueError('Monetary: Retail: parse')
        self.transaction_id = data[fs_pos[11]+1:fs_pos[12]].decode()
        aux_data = data[fs_pos[14]+1:fs_pos[15]]
    elif self.format_code == '2': #Restaurant
        self.transaction_id = data[fs_pos[4]+1:fs_pos[5]].decode()
        aux_data = data[fs_pos[6]+1:fs_pos[7]]
    elif self.format_code == '4': #Hotel
        self.transaction_id = data[fs_pos[7]+1:fs_pos[8]].decode()
        aux_data = data[fs_pos[12]+1:fs_pos[13]]

    if len(aux_data) > 0:
        us_pos = list(sep_gen(US, aux_data))
        if len(us_pos) < 7:
            raise ValueError('Monetary: parse')
        self.pin_block = aux_data[0:us_pos[0]].decode()
        self.card_type = aux_data[us_pos[0]+1:us_pos[1]].decode()
        self.authorization_code = aux_data[us_pos[4]+1:us_pos[5]].decode()
        self.smid_block = aux_data[us_pos[5]+1:us_pos[6]].decode()
        if len(us_pos) > 7:
            self.partial_indicator = aux_data[us_pos[6]+1:us_pos[7]].decode()

MonetaryTransaction.parse = monetary_parse


def swiped_parse(self: SwipedMonetaryTransaction, data: bytes):
    fs_pos = data.index(FS, 0, 77)
    self.track_data = data[0:fs_pos].decode()

    MonetaryTransaction.parse(self, data[fs_pos + 1:])

SwipedMonetaryTransaction.parse = swiped_parse


def keyed_parse(self: KeyedMonetaryTransaction, data: bytes):
    fs_pos = list(sep_gen(FS, data, 0, 2))
    if len(fs_pos) != 2:
        raise ValueError('Keyed: parse')

    field1 = data[0:fs_pos[0]]
    e_pos = list(sep_gen(US, field1, 0))
    s_pos = [i+1 for i in e_pos]
    e_pos.append(len(field1))
    s_pos.insert(0, 0)
    fields = [field1[s:e].decode() for s, e in zip(s_pos, e_pos)]
    if len(fields) > 2:
        self.cvv = fields[2]
    if len(fields) > 1:
        self.cv_presence = fields[1]
    if len(fields) > 0:
        self.account_no = fields[0]
    MonetaryTransaction.parse(self, data[fs_pos[1]+1:])

KeyedMonetaryTransaction.parse = keyed_parse


def deposit_inquiry_parse(self: DepositInquiryTransaction, data: bytes):
    pass

DepositInquiryTransaction.parse = deposit_inquiry_parse


def batch_close_parse(self: BatchCloseTransaction, data: bytes):
    fs_pos = list(sep_gen(FS, data, 0))
    if fs_pos[0] > 0:
        self.credit_batch_amount = float(data[0:fs_pos[0]].decode())
    if fs_pos[1] - fs_pos[0] == 3:
        self.offline_items = int(data[fs_pos[0]+1:fs_pos[1]].decode())
    if fs_pos[2] - fs_pos[1] == 3:
        self.debit_batch_count = int(data[fs_pos[1]+1:fs_pos[2]].decode())
    if fs_pos[3] - fs_pos[2] > 0:
        self.debit_batch_amount = float(data[fs_pos[1]+1:fs_pos[2]].decode())

    self.batch_no = data[fs_pos[2]+1:fs_pos[2]+2].decode()
    self.item_no = int(data[fs_pos[2]+2:fs_pos[2]+5].decode())

BatchCloseTransaction.parse = batch_close_parse


def response(self: FdmsResponse) -> bytes:
    ba = bytearray()
    ba.append(STX)
    ba.append(self.action_code.value.encode()[0])
    ba.append(self.response_code.encode()[0])
    ba.append(self.batch_no.encode()[0])
    ba.extend(self.item_no.encode()[0:4])
    ba.append('0'.encode()[0])
    ba.extend(self.body())
    ba.append(ETX)
    ba.append(functools.reduce(lambda x, y: x ^ y, ba[2:], ba[1]))
    return ba

FdmsResponse.response = response


def text_response_body(self: FdmsTextResponse) -> bytes:
    if len(self.response_text) < 16:
        self.response_text = self.response_text.ljust(16, ' ')
    if len(self.response_text) > 16:
        self.response_text = self.response_text[0:16]

    ba = bytes([FS])
    ba += self.response_text.encode()
    return ba

FdmsTextResponse.body = text_response_body


def deposit_response_body(self: BatchResponse) -> bytes:
    ba = FdmsTextResponse.body(self)
    ba += bytes([FS])
    ba += self.batch_id_number.encode()
    if self.response_text2 is not None:
        if len(self.response_text2) > 0:
            if len(self.response_text2) < 16:
                self.response_text2 = self.response_text2.ljust(16, ' ')
            if len(self.response_text2) > 16:
                self.response_text2 = self.response_text2[0:16]
            ba += bytes([FS])
            ba += self.response_text2.encode()

    return ba

BatchResponse.body = deposit_response_body


def credit_response_body(self: CreditResponse) -> bytes:
    ba = FdmsTextResponse.body(self)
    if len(self.avc_rs_code) > 0:
        ba += self.avc_rs_code.encode()
    else:
        ba += '0'.encode()
    if len(self.cvv_rs_code) > 0:
        ba += self.cvv_rs_code.encode()
    ba += bytes([FS])
    ba += bytes([FS])
    if len(self.transaction_id) > 0:
        tid = self.transaction_id.encode()
        if len(tid) > 15:
            tid = tid[0:15]
        ba += tid
    ba += bytes([FS])

    return ba

CreditResponse.body = credit_response_body


def parse_header(data: bytes) -> (int, FdmsHeader):
    pos = 0
    if data[pos] == STX:
        pos += 1

    p_ch = data[pos:pos+1].decode()
    if p_ch != '*':
        raise ValueError('Protocol flag * expected')

    pos += 1
    p_type = data[pos:pos+1].decode()
    if not p_type in ['1', '2', '3']:
        raise ValueError('Protocol type is invalid')

    pos += 1
    term_id = data[pos:pos+6].decode()

    pos += 5
    p_sep = data.index(SEP, pos, pos + 20)
    merch_num = data[pos:p_sep].decode()

    pos = p_sep + 1
    p_sep = data.index(FS, pos, pos+5)
    device_id = data[pos:p_sep].decode()

    pos = p_sep + 1
    wcc = data[pos:pos+1].decode()

    pos += 1
    txn_type = data[pos:pos+1].decode()

    pos += 1
    txn_code = data[pos:pos+1].decode()

    pos += 1
    p_sep = data[pos]
    if p_sep != FS:
        raise ValueError('Invalid transaction header')

    pos += 1

    header = FdmsHeader()
    header.protocol_type = p_type
    header.terminal_id = term_id
    header.merchant_number = merch_num
    header.device_id = device_id
    header.wcc = wcc
    header.txn_type = txn_type
    header.txn_code = txn_code

    return pos, header


