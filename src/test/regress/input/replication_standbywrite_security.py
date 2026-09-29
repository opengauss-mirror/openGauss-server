#!/usr/bin/env python3
"""Helper for replication_standbywrite_security.source.

Binds the client socket to 127.0.0.2 and connects to 127.0.0.1 so the
server sees remote=127.0.0.2 / local=127.0.0.1, which is NOT considered a
node-internal connection. With the DB-04 fix in place, a matching remote
replication trust rule must be rejected by check_hba's remote-trust
prohibition.
"""
import socket
import struct
import sys


def parse_error_message(payload):
    """Parse a PostgreSQL ErrorResponse payload into a dict."""
    fields = {}
    i = 0
    while i < len(payload):
        fid = payload[i:i + 1]
        if fid == b'\x00':
            break
        end = payload.find(b'\x00', i + 1)
        if end == -1:
            break
        fields[fid] = payload[i + 1:end].decode('utf-8', errors='replace')
        i = end + 1
    return fields


def main():
    if len(sys.argv) < 2:
        print('USAGE: replication_standbywrite_security.py <post_port>')
        sys.exit(1)

    # In thread_pool mode replication must come through the HA port.
    # For the temporary single-node install the HA/Pooler port is
    # PostPortNumber + 1.
    port = int(sys.argv[1]) + 1
    user = 'test_standbywrite_sec'

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(('127.0.0.2', 0))
    try:
        sock.connect(('127.0.0.1', port))
    except ConnectionRefusedError:
        print('CONNECTION_REFUSED')
        return

    # Build a v3 startup message.
    startup = b''
    startup += b'user\x00' + user.encode() + b'\x00'
    startup += b'database\x00postgres\x00'
    startup += b'replication\x00standbywrite\x00'
    startup += b'client_encoding\x00UTF8\x00\x00'
    msg = struct.pack('!I', 8 + len(startup)) + struct.pack('!I', 196608) + startup
    sock.sendall(msg)

    # Read the first backend message.
    typ = sock.recv(1)
    if not typ:
        print('NO_RESPONSE')
        return
    length = struct.unpack('!I', sock.recv(4))[0]
    payload = sock.recv(length - 4)

    if typ == b'R':
        auth_method = struct.unpack('!I', payload[:4])[0]
        if auth_method == 0:
            print('TRUST_ACCEPTED')
        else:
            print('AUTH_REQUIRED:%d' % auth_method)
    elif typ == b'E':
        fields = parse_error_message(payload)
        msg = fields.get(b'M', '')
        if 'Forbid remote connection with trust method' in msg:
            print('REJECTED_BY_REMOTE_TRUST_POLICY')
        else:
            print('ERROR:%s' % msg)
    else:
        print('UNKNOWN_RESPONSE:%s' % typ.decode('latin1'))


if __name__ == '__main__':
    main()
