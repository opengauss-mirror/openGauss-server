#!/usr/bin/env python3
"""Read-only live checks; creates only session-local prepared statements."""
import argparse
import json
import os
from pathlib import Path

import psycopg2

from libpq_bind import BoundCursor


def main(args):
    out = Path(args.out)
    if out.exists():
        raise FileExistsError(out)
    conn = psycopg2.connect(host='/tmp', port=args.port, dbname='postgres', user=os.environ.get('USER', 'qiumc'))
    conn.autocommit = True
    cur = conn.cursor()
    cases = [('中文 apostrophe\' quotes" \\ newline\n', '[1,0,-1,0.5]'),
             ('', '[-0,0,1e-30,3.4028234e38]'),
             ('rounding', '[1.000000059604644775390625000000000001,0,1,2]'),
             ('bad_dimension', '[1,2,3]')]
    checks = []
    try:
        cur.execute('SET default_transaction_read_only=on')
        cur.execute('SET session_timeout=0')
        cur.execute('SET idle_in_transaction_session_timeout=0')
        cur.execute("PREPARE bind_probe(text,vector) AS SELECT $1,encode(vector_send($2),'hex')")
        text = BoundCursor(conn, cases, False)
        binary = BoundCursor(conn, cases, True)
        for argv in cases:
            a = text.query(b'bind_probe', argv, False)
            b = binary.query(b'bind_probe', argv, False)
            cur.execute('EXECUTE bind_probe(%s,%s)', argv)
            sql = [tuple(v.encode('utf8') if v is not None else None for v in row) for row in cur.fetchall()]
            # A-compatible databases map empty text to NULL on both routes.
            assert sql == a == b, (argv, sql, a, b)
            checks.append('wire identity')
        for vector in ('[]', '[1,nope]', '[nan,0]', '[inf,0]', 'not_a_vector'):
            try:
                BoundCursor(conn, [('x', vector)], True)
                raise AssertionError('Malformed vector accepted')
            except ValueError:
                checks.append('invalid vector rejected')
        try:
            BoundCursor(conn, [('a\0b', '[1,2]')], True)
            raise AssertionError('NUL text accepted')
        except ValueError:
            checks.append('NUL text rejected')
        cur.execute('PREPARE bind_dimension(text,vector) AS SELECT vector_dims($2::vector(4))')
        try:
            binary.query(b'bind_dimension', cases[-1])
            raise AssertionError('Dimension error missing')
        except RuntimeError as error:
            assert 'dimension' in str(error).lower(), error
            checks.append('server error propagated')
        assert binary.query(b'bind_dimension', cases[0]) == [(4,)]
        checks.append('recovered after server error')
        cur.execute('PREPARE bind_cancel(text,vector) AS SELECT 1 FROM pg_sleep(1) '
                    'WHERE $1 IS NOT NULL AND vector_dims($2)>0')
        cur.execute('SET statement_timeout=1')
        try:
            binary.query(b'bind_cancel', cases[0])
            raise AssertionError('Timeout missing')
        except RuntimeError as error:
            assert 'timeout' in str(error).lower(), error
            checks.append('timeout propagated')
        cur.execute('SET statement_timeout=0')
        assert binary.query(b'bind_dimension', cases[0]) == [(4,)]
        checks.append('recovered after timeout')
        try:
            BoundCursor(conn, cases, True, statement_name='bad;name')
            raise AssertionError('Unsafe statement name accepted')
        except ValueError:
            checks.append('unsafe statement name rejected')
        cur.execute("SET session_timeout='10min'")
        try:
            BoundCursor(conn, cases, True)
            raise AssertionError('Unsafe idle timer configuration accepted')
        except ValueError:
            checks.append('idle timer conflict rejected')
    finally:
        cur.close()
        conn.close()
    try:
        binary.query(b'bind_dimension', cases[0])
        raise AssertionError('Closed connection used')
    except RuntimeError as error:
        assert str(error) == 'Connection is closed'
        checks.append('closed connection rejected')
    result = {'passed': True, 'checks': checks, 'count': len(checks), 'persistent_objects_created': False}
    out.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(result, ensure_ascii=False))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port', type=int, default=55440)
    parser.add_argument('--out', required=True)
    main(parser.parse_args())
