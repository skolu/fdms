import fdms
import asyncio
import os
import ssl

UNIX_SOCKET_PATH = 'fdms.1'

loop = asyncio.get_event_loop()


def accept_fdms_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    asyncio.Task(fdms.fdms_session(reader, writer), loop=loop).add_done_callback(lambda fut: writer.close())


def accept_site_net_client(reader, writer):
    task = asyncio.Task(asyncio.open_unix_connection(path=UNIX_SOCKET_PATH))
    asyncio.Task(fdms.site_net_session(reader, writer, task)).add_done_callback(lambda fut: writer.close())



# Start FDMS
if os.path.exists(UNIX_SOCKET_PATH):
    os.remove(UNIX_SOCKET_PATH)

f = asyncio.start_unix_server(accept_fdms_client, path=UNIX_SOCKET_PATH)
loop.run_until_complete(f)

pem_path = os.path.dirname(__file__)
pem_path = os.path.join(pem_path, 'cert.pem')
context = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
context.load_cert_chain(pem_path)
f = asyncio.start_server(accept_site_net_client, port=8444, ssl=context)
loop.run_until_complete(f)

try:
    loop.run_forever()
finally:
    if os.path.exists(UNIX_SOCKET_PATH):
        os.remove(UNIX_SOCKET_PATH)


