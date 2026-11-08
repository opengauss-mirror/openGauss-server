"""Synchronous text/vector Bind adapter for HybridRecall benchmarks.

Uses psycopg2's own libpq on an idle autocommit connection, one caller at a
time. This is not a general DB-API cursor: only two non-null input parameters
(text, vector), text results, and integer result columns are supported by
execute(). query(..., ints=False) is for byte-exact input validation.

Pre-encodes a finite, caller-supplied query set. Never caches result rows across
requests. Each PGresult is freed before returning, including server errors.
Do not interleave with asynchronous operations or an active psycopg2 call.
The current server's idle-session timer can overwrite an extended-query
statement timer. Require both idle timers to be explicitly disabled by the
caller; never silently weaken/change timeout settings here.
"""
import ctypes as c
import locale
import math
import re
import struct

import psycopg2
import psycopg2._psycopg


class BoundCursor:
    def __init__(self, connection, arguments, binary, statement_name='steady_abba'):
        if not connection.autocommit or connection.encoding.upper() != 'UTF8':
            raise ValueError('An idle UTF8 autocommit connection is required')
        if connection.get_transaction_status() != psycopg2.extensions.TRANSACTION_STATUS_IDLE:
            raise ValueError('Connection must be idle')
        with connection.cursor() as guard:
            guard.execute("SELECT name,setting FROM pg_settings WHERE name IN "
                          "('session_timeout','idle_in_transaction_session_timeout')")
            if any(value != '0' for _, value in guard.fetchall()):
                raise ValueError('Bind benchmark requires explicit zero idle timers; preserve statement_timeout')
        if not re.fullmatch(r'[a-z_][a-z_0-9]*', statement_name):
            raise ValueError('Unsafe prepared-statement name')
        self.conn = connection
        self.pointer = connection.pgconn_ptr
        self.statement = statement_name.encode('ascii')
        self.expected_sql = f'EXECUTE {statement_name}(%s,%s)'
        # dlsym through this module resolves the same libpq that owns PGconn.
        self.lib = c.CDLL(psycopg2._psycopg.__file__)
        signatures = {
            'PQexecPrepared': (c.c_void_p, [c.c_void_p, c.c_char_p, c.c_int,
                               c.POINTER(c.c_char_p), c.POINTER(c.c_int), c.POINTER(c.c_int), c.c_int]),
            'PQresultStatus': (c.c_int, [c.c_void_p]),
            'PQntuples': (c.c_int, [c.c_void_p]),
            'PQnfields': (c.c_int, [c.c_void_p]),
            'PQgetisnull': (c.c_int, [c.c_void_p, c.c_int, c.c_int]),
            'PQgetvalue': (c.c_char_p, [c.c_void_p, c.c_int, c.c_int]),
            'PQresultErrorMessage': (c.c_char_p, [c.c_void_p]),
            'PQerrorMessage': (c.c_char_p, [c.c_void_p]),
            'PQclear': (None, [c.c_void_p]),
            'PQlibVersion': (c.c_int, []),
        }
        for name, (result, args) in signatures.items():
            fn = getattr(self.lib, name)
            fn.restype, fn.argtypes = result, args
        if self.lib.PQlibVersion() != psycopg2.extensions.libpq_version():
            raise RuntimeError('libpq version mismatch')
        if binary and locale.setlocale(locale.LC_NUMERIC) not in ('C', 'POSIX'):
            raise ValueError('Binary benchmark encoding requires the C numeric locale')
        parse_float = self.lib.strtof
        parse_float.restype = c.c_float
        parse_float.argtypes = [c.c_char_p, c.POINTER(c.c_char_p)]
        self.params = {}
        for text, vector in arguments:
            if '\0' in text or '\0' in vector:
                raise ValueError('NUL in text input')
            if binary:
                if not vector.startswith('[') or not vector.endswith(']'):
                    raise ValueError('Expected the canonical benchmark vector string')
                values = []
                for token in vector[1:-1].split(','):
                    token_bytes = token.strip().encode('ascii')
                    end = c.c_char_p()
                    # Match vector_in's strtof, avoiding double rounding via
                    # Python float/float64 for decimal numbers near a midpoint.
                    value = parse_float(token_bytes, c.byref(end))
                    if not token_bytes or end.value != b'' or not math.isfinite(value):
                        raise ValueError('Invalid finite vector element')
                    values.append(value)
                if not values or len(values) > 32767:
                    raise ValueError('Invalid finite vector')
                payload = struct.pack('!HH', len(values), 0) + struct.pack('!' + str(len(values)) + 'f', *values)
            else:
                payload = vector.encode('utf8')
            text_bytes = text.encode('utf8')
            # ctypes arrays retain the byte objects, including embedded NULs
            # in a binary vector; libpq uses the explicit binary length.
            self.params[(text, vector)] = ((c.c_char_p * 2)(text_bytes, payload),
                (c.c_int * 2)(len(text_bytes), len(payload)), (c.c_int * 2)(0, int(binary)))
        self.rows = None

    def query(self, name, arguments, ints=True):
        if self.conn.closed:
            raise RuntimeError('Connection is closed')
        values, lengths, formats = self.params[arguments]
        result = self.lib.PQexecPrepared(self.pointer, name, 2, values, lengths, formats, 0)
        if not result:
            raise RuntimeError(self.lib.PQerrorMessage(self.pointer).decode('utf8', 'replace'))
        try:
            if self.lib.PQresultStatus(result) != 2:  # PGRES_TUPLES_OK
                raise RuntimeError(self.lib.PQresultErrorMessage(result).decode('utf8', 'replace'))
            columns = self.lib.PQnfields(result)
            rows = []
            for row in range(self.lib.PQntuples(result)):
                values = []
                for col in range(columns):
                    if self.lib.PQgetisnull(result, row, col):
                        values.append(None)
                    else:
                        value = self.lib.PQgetvalue(result, row, col)
                        values.append(int(value) if ints else value)
                rows.append(tuple(values))
            return rows
        finally:
            self.lib.PQclear(result)

    def execute(self, sql, arguments):
        if sql != self.expected_sql:
            raise ValueError('Unexpected statement for bound benchmark cursor')
        self.rows = None
        self.rows = self.query(self.statement, arguments)

    def fetchall(self):
        rows, self.rows = self.rows, None
        if rows is None:
            raise RuntimeError('No pending result')
        return rows
