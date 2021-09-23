/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * 00_CreateSnapshotAPI.sql
 *    DB4AI.Snapshot test case.
 *
 *
 * -------------------------------------------------------------------------
 */

CREATE OR REPLACE FUNCTION _db4ai_test.test(
)
RETURNS VOID LANGUAGE plpgsql SECURITY INVOKER SET client_min_messages TO NOTICE
AS $$
BEGIN

    PERFORM _db4ai_test.assert_exception('db4ai.create_snapshot(null,null,null)',
        'i_name cannot be NULL or empty');
    PERFORM _db4ai_test.assert_exception('db4ai.create_snapshot(null, ''name@org'', null)',
        'i_name must not contain ''@'' characters');
    PERFORM _db4ai_test.assert_exception('db4ai.create_snapshot(null, ''name'', null)',
        'i_commands array malformed',
        'pass SQL commands as TEXT[] literal, e.g. ''{SELECT *, FROM public.t, DISTRIBUTE BY HASH(id)''');
    PERFORM _db4ai_test.assert_exception('db4ai.create_snapshot(null, ''name'',''{}'')',
        'i_commands array malformed',
        'pass SQL commands as TEXT[] literal, e.g. ''{SELECT *, FROM public.t, DISTRIBUTE BY HASH(id)''');
    PERFORM _db4ai_test.assert_exception('db4ai.create_snapshot(null, ''name'', ''{{a},{a}}'')',
        'i_commands array malformed',
        'pass SQL commands as TEXT[] literal, e.g. ''{SELECT *, FROM public.t, DISTRIBUTE BY HASH(id)''');
    PERFORM _db4ai_test.assert_exception('db4ai.create_snapshot(null, ''name'',''{1, null, 2}'')',
        'i_commands array contains NULL values');

    PERFORM _db4ai_test.assert_exception('db4ai.create_snapshot(null, ''name'', ''{ SELECT   a1  ,  SELECT  a2 }'')',
        'multiple SELECT clauses in i_commands: ''SELECT   a1'' ''SELECT  a2''');
    PERFORM _db4ai_test.assert_exception('db4ai.create_snapshot(null, ''name'', ''{ FROM  a1  ,  FROM a2 }'')',
        'multiple FROM clauses in i_commands: ''FROM  a1'' ''FROM a2''');
    PERFORM _db4ai_test.assert_exception('db4ai.create_snapshot(null, ''name'', ''{ DISTRIBUTE BY  F (x )  ,  DISTRIBUTE BY X(f) }'')',
        'multiple DISTRIBUTE BY clauses in i_commands: ''DISTRIBUTE BY  F (x )'' ''DISTRIBUTE BY X(f)''');

    PERFORM _db4ai_test.assert_exception('db4ai.create_snapshot(null, ''name'', ''{SeLeCT}'')',
        'unrecognized command in i_commands: ''SeLeCT''');
    PERFORM _db4ai_test.assert_exception('db4ai.create_snapshot(null, ''name'', ''{from  }'')',
        'unrecognized command in i_commands: ''from''');
    PERFORM _db4ai_test.assert_exception('db4ai.create_snapshot(null, ''name'', ''{  distribute by}'')',
        'unrecognized command in i_commands: ''distribute by''');
    PERFORM _db4ai_test.assert_exception('db4ai.create_snapshot(null, ''name'', ''{  distr ibute  by}'')',
        'unrecognized command in i_commands: ''distr ibute  by''');
    PERFORM _db4ai_test.assert_exception('db4ai.create_snapshot(null, ''name'', ''{ InSeRt  INTO x  }'')',
        'unrecognized command in i_commands: ''InSeRt  INTO x''');
    PERFORM _db4ai_test.assert_exception('db4ai.create_snapshot(null, ''name'', ''{DISTRIBUTE BY Pi }'')',
        'SELECT and FROM clauses are missing in i_commands');
    PERFORM _db4ai_test.assert_exception('db4ai.create_snapshot(null, ''name'', ''{SELECT 42, DISTRIBUTE BY Pi }'')',
        'FROM clause is missing in i_commands');

    PERFORM _db4ai_test.assert_exception('db4ai.create_snapshot(null, ''name'', ''{FROM 42}'', ''@'')',
        'illegal i_vers: ''@''');
    PERFORM _db4ai_test.assert_exception('db4ai.create_snapshot(null, ''name'', ''{FROM 42}'', ''@@6'')',
        'i_vers may contain only one single, leading ''@'' character',
        'specify snapshot version as [@]x.y.z or [@]label with optional, leading ''@''');
    PERFORM _db4ai_test.assert_exception('db4ai.create_snapshot(null, ''name'', ''{FROM 42}'', ''4@6'')',
        'i_vers may contain only one single, leading ''@'' character',
        'specify snapshot version as [@]x.y.z or [@]label with optional, leading ''@''');

    PERFORM _db4ai_test.assert_exception('db4ai.create_snapshot(null, '' _ _123456789012345678901234567890123456789012345678901234'', ''{FROM 42}'')',
        'snapshot name too long: '' _ _123456789012345678901234567890123456789012345678901234@1.0.0''');
    PERFORM _db4ai_test.assert_exception('db4ai.create_snapshot(null, '' 12345678901234567890123456789012345678901234567890 '', ''{FROM 42}'', '' 123456789 '')',
        'snapshot name too long: '' 12345678901234567890123456789012345678901234567890 @ 123456789 ''');

    PERFORM _db4ai_test.assert_equal(
        'db4ai.create_snapshot(null, '' test'', ''{Select 1 a, FROM dual}'')',
        '(''_db4ai_test''::NAME,'' test@1.0.0''::NAME)');
    PERFORM _db4ai_test.assert_equal(
        'SELECT (schema, name) FROM '
            'db4ai.create_snapshot(''_db4ai test'', '' test '', ''{Select 1 \"x <> y\", FROM dual}'')',
        '(''_db4ai test''::NAME,'' test @1.0.0''::NAME)');

    PERFORM _db4ai_test.assert_exception('db4ai.create_snapshot(null, ''test '', ''{Select 1 a, FROM dual, DIStRIBUTE BY HAsH}'')',
        'cannot match DISTRIBUTE BY clause',
        'currently only DISTRIBUTE BY REPLICATION and DISTRIBUTE BY HASH(column_name [, ...]) supported');
    PERFORM _db4ai_test.assert_exception('db4ai.create_snapshot(null, ''test '', ''{Select 1 a, FROM dual, DISTRIBUTE BY HaSH()}'')',
        'cannot match DISTRIBUTE BY clause',
        'currently only DISTRIBUTE BY REPLICATION and DISTRIBUTE BY HASH(column_name [, ...]) supported');
    PERFORM _db4ai_test.assert_exception('db4ai.create_snapshot(null, ''test '', ''{Select 1 a, FROM dual, DISTRIBUTE BY RELICATioN()}'')',
        'cannot match DISTRIBUTE BY clause',
        'currently only DISTRIBUTE BY REPLICATION and DISTRIBUTE BY HASH(column_name [, ...]) supported');

    PERFORM _db4ai_test.assert_exception(
        'db4ai.create_snapshot(null, ''test '', ''{Select 1 a, FROM dual," DISTRIBUTE BY HASH(\"B\"\"b\")"}'')',
        'unable to map field "B"b" to backing table');
    PERFORM _db4ai_test.assert_exception(
        'db4ai.create_snapshot(null, ''test '', ARRAY[''Select 1 a'','' FROM dual'','' DISTRIBUTE BY HASH("B""b") ''])',
        'unable to map field "B"b" to backing table');
    PERFORM _db4ai_test.assert_exception(
        'db4ai.create_snapshot(null, ''test '', ARRAY[''Select 1 a'','' FROM dual'','' DISTRIBUTE BY HASH("b ) ''])',
        'unterminated quoted identifier ''b '' at or near: ''DISTRIBUTE BY HASH("b )''');

    PERFORM db4ai.create_snapshot(null, 'test ', '{Select 1 a, FROM dual,   DISTRIBUTE BY  RePLICATION } ');
    PERFORM _db4ai_test.assert_exception('db4ai.create_snapshot(''_db4ai test'', '' test '', ''{Select 1 a, FROM dual}'')',
        'snapshot "_db4ai test"." test @1.0.0" already exists');
    PERFORM db4ai.create_snapshot(null, 'test', '{Select 1 a, FROM dual}', ' zzz');
    PERFORM db4ai.create_snapshot(null, 'test', '{Select 1 a, FROM dual}', 'zzz ');
    PERFORM db4ai.create_snapshot(null, 'test', '{Select 1 a, FROM dual}', '@ zzz ');
    PERFORM _db4ai_test.assert_exception('db4ai.create_snapshot(null, ''test'', ''{Select 1 a, FROM dual}'', '' zzz '')',
        'snapshot _db4ai_test."test@ zzz " already exists');

    PERFORM _db4ai_test.assert_exception('db4ai.create_snapshot_internal(null, null, null, null, null, null)',
        'direct call to db4ai.create_snapshot_internal(bigint,name,name,text[],text,name) is not allowed',
        'call public interface db4ai.create_snapshot instead');

    SET db4ai_snapshot_mode = 8;
    PERFORM _db4ai_test.assert_exception('db4ai.create_snapshot(null, ''test'', ''{Select 1 a, FROM dual}'')',
        'invalid snapshot mode: ''8''');

END;
$$;
