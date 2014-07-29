import asyncio
import functools
from .fdms_processor import *

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
    state = 0
    while True:
        ba = yield from reader.read(1)
        b = ba[0]
        if b > 0x7f:
            b &= 0x7f
        if state == 0:
            if b == STX:
                state = 1
            else:
                state = 3
        elif state == 1:
            if b == ETX:
                state = 2
        else:
            state = 3
        buffer.append(b)
        if state > 2:
            break

    return buffer

@asyncio.coroutine
def fdms_session(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    #Send EQN
    writer.write(bytes((ENQ,)))
    yield from writer.drain()


    online = None
    offline = []
    attempt = 0

    # Get Request
    while True:
        try:
            if attempt > 4:
                return
            request = yield from asyncio.wait_for(read_fdms_packet(reader), timeout=15.0)
            got_eot = False
            while len(request) > 0:
                if request[0] == STX:
                    try:
                        if request[-2] == ETX:
                            lrs = functools.reduce(lambda x, y: x ^ int(y), request[2:-1], int(request[1]))
                            if lrs != request[-1]:
                                raise ValueError('LRS sum')

                            pos, header = parse_header(request)
                            txn = header.create_txn()
                            txn.parse(request[pos:])
                            if online is None:
                                online = (header, txn)
                            else:
                                offline.append((header, txn))
                            # Send ACK
                            attempt = 0
                            writer.write(bytes((ACK,)))
                        else:
                            raise ValueError('Invalid packet')
                    except (ValueError, IndexError) as e:
                        attempt += 1
                        writer.write(bytes((NAK,)))

                    yield from writer.drain()
                    break
                elif request[0] == EOT:
                    got_eot = True
                    break
                else:
                    request = request[1:]

            if got_eot:
                break

        except asyncio.TimeoutError as e:
            # Close session
            return

    # Process Transactions & Send Response
    for txn in offline:
        rs = process_txn(txn[0], txn[1])
    rs = process_txn(online[0], online[1])

    rs_bytes = rs.response()
    attempt = 0
    while True:
        writer.write(rs_bytes)
        yield from writer.drain()

        request = yield from asyncio.wait_for(read_fdms_packet(reader), timeout=4.0)
        if request[0] == ACK:
            break
        elif request[0] == NAK:
            attempt += 1
        else:
            return
        if attempt >= 4:
            return

        # Sent EOT
    writer.write(bytes((EOT,)))
    yield from writer.drain()

    if writer.can_write_eof():
        writer.write_eof()


def sep_gen(sep, buffer, offset, count = -1):
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
    if fs_pos[2] - fs_pos[1] != 5:
        raise ValueError('Monetary: parse')

    self.total_amount = float(data[0:fs_pos[0]].decode())
    self.invoice_no = data[fs_pos[0]+1:fs_pos[1]].decode()
    self.batch_no = data[fs_pos[1]+1:fs_pos[1]+2].decode()
    self.item_no = data[fs_pos[1]+2:fs_pos[1]+5].decode()
    self.revision_no = data[fs_pos[1]+5:fs_pos[2]].decode()

MonetaryTransaction.parse = monetary_parse


def swiped_parse(self: SwipedMonetaryTransaction, data: bytes):
    fs_pos = data.index(FS, 0, 0+77)
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
    fields = [field1[s:e] for s, e in zip(s_pos, e_pos)]
    if len(fields) > 2:
        self.cvv = fields[2]
    if len(fields) > 1:
        self.cv_presence = fields[1]
    if len(fields) > 0:
        self.account_no = fields[0]
    MonetaryTransaction.parse(self, data[fs_pos + 1:])

KeyedMonetaryTransaction.parse = keyed_parse


def deposit_inquiry_parse(self: DepositInquiryTransaction, data: bytes):
    pass

DepositInquiryTransaction.parse = deposit_inquiry_parse


def response(self: FdmsResponse) -> bytes:
    ba = bytearray()
    ba.append(STX)
    ba.append(self.action_code.encode()[0])
    ba.append(self.response_code.encode()[0])
    ba.append(self.batch_number.encode()[0])
    ba.extend(self.item_number.encode()[0:4])
    ba.append('0'.encode()[0])
    ba.append(FS)
    if len(self.response_text) < 16:
        self.response_text = self.response_text.ljust(16, ' ')
    if len(self.response_text) > 16:
        self.response_text = self.response_text[0:16]
    ba.extend(self.response_text.encode())
    ba.append(FS)
    ba.extend(self.body())
    ba.append(ETX)
    ba.append(functools.reduce(lambda x, y: x ^ y, ba[2:], ba[1]))
    return ba

FdmsResponse.response = response


def deposit_response_body(self: DepositInquiryResponse) -> bytes:
    return self.batch_id_number.encode()

DepositInquiryResponse.body = deposit_response_body


def credit_response_body(self: CreditResponse) -> bytes:
    rs = bytearray()
    if len(self.avc_rs_code) > 0:
        rs.append(self.avc_rs_code.encode()[0])
    else:
        rs.extend('0'.encode())
    if len(self.cvv_rs_code) > 0:
        rs.append(self.cvv_rs_code.encode()[0])
    rs.append(FS)
    rs.append(FS)
    if len(self.transaction_id) > 0:
        tid = self.transaction_id.encode()
        if len(tid) > 15:
            tid = tid[0:15]
        rs.extend(tid)
    rs.append(FS)

    return rs

CreditResponse.body = credit_response_body


def parse_header(data: bytes) -> (int, FdmsHeader):
    pos = 0
    p_b = data[0]
    if p_b != STX:
        raise ValueError('<STX> expected')

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

    header = FdmsHeader()
    header.protocol_type = p_type
    header.terminal_id = term_id
    header.merchant_number = merch_num
    header.device_id = device_id
    header.wcc = wcc
    header.txn_type = txn_type
    header.txn_code = FdmsTxnCode(txn_code)

    return pos, header


