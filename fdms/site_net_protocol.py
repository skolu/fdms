import struct
from .fdms_protocol import *
from . import LOG_NAME
import logging

@asyncio.coroutine
def read_site_net_packet(reader: asyncio.StreamReader) -> (str, bytes):
    h = yield from reader.read(4)
    packet_length = h[0] * 256 + h[1]
    packet_type = h[2:4].decode()
    packet = yield from reader.read(packet_length)
    return packet_type, packet

@asyncio.coroutine
def site_net_session(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, connect_task: asyncio.Task):
    client_info = SiteNetClientInfo()
    frame_type, data = yield from read_site_net_packet(reader)
    if frame_type == '01':
        info_record = data.decode()
        fields = info_record.split(',')
        client_info.customer_id = fields[0]
        client_info.merchant_no = fields[1]
        client_info.message_format = fields[2]
        client_info.transaction_type = fields[3]
        if len(fields) > 4:
            client_info.driver_version = fields[4]

        client_info.fdms_reader, client_info.fdms_writer = yield from connect_task
    else:
        err_rs = '201 SERVER PROTOCOL ERROR'.encode()
        writer.write(struct.pack('!H', len(err_rs)))
        writer.write('02'.encode())
        writer.write(err_rs)
        yield from writer.drain()
        return

    outer_task = None
    inner_task = None
    while True:
        if outer_task is None:
            outer_task = asyncio.Task(read_site_net_packet(reader))
        if inner_task is None:
            inner_task = asyncio.Task(read_fdms_packet(client_info.fdms_reader))
        tasks = (outer_task, inner_task)
        yield from asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

        if outer_task.done():
            try:
                frame_type, request = outer_task.result()
                if frame_type == '22':
                    logging.getLogger(LOG_NAME).debug('SiteNET to FDMS: %s', request)
                    client_info.fdms_writer.write(request)
                    yield from client_info.fdms_writer.drain()
            except asyncio.CancelledError:
                pass
            except Exception as e:
                break
            outer_task = None

        if inner_task.done():
            try:
                response = inner_task.result()
                logging.getLogger(LOG_NAME).debug('FDMS to SiteNET: %s', response)
                writer.write(struct.pack('!H', len(response)))
                writer.write('22'.encode())
                writer.write(response)
                yield from writer.drain()
            except asyncio.CancelledError:
                pass
            except Exception:
                break
            inner_task = None

    if outer_task is not None:
        outer_task.cancel()
    if inner_task is not None:
        inner_task.cancel()

    if writer.can_write_eof():
        writer.write_eof()


class SiteNetClientInfo:

    def __init__(self):
        self.customer_id = ''
        self.merchant_no = ''
        self.message_format = ''
        self.transaction_type = ''
        self.driver_version = ''
        self.fdms_reader = None
        ''':type: asyncio.StreamReader'''
        self.fdms_writer = None
        ''':type: asyncio.StreamWriter'''
