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
 * 05_PurgeSnapshotAPI.sql
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

    PERFORM _db4ai_test.assert_exception('db4ai.purge_snapshot(null,null)',
        'i_name cannot be NULL or empty');
    PERFORM _db4ai_test.assert_exception('db4ai.purge_snapshot(null,''name'')',
        'i_name must contain exactly one ''@'' character',
        'reference a snapshot using the format: snapshot_name@version');
    PERFORM _db4ai_test.assert_exception('db4ai.purge_snapshot(null,''name@org@com'')',
        'i_name must contain exactly one ''@'' character',
        'reference a snapshot using the format: snapshot_name@version');
    PERFORM _db4ai_test.assert_exception('db4ai.purge_snapshot(''_db4ai test'',''name@1.0.0'')',
        'snapshot "_db4ai test"."name@1.0.0" does not exist');

    PERFORM db4ai.create_snapshot('_db4ai test', 'test', '{SELECT 1 \"x <> y\", FROM DUAL}');
    PERFORM db4ai.prepare_snapshot('_db4ai test', 'test@1.0.0', '{DELETE}');
    PERFORM _db4ai_test.assert_exception('db4ai.purge_snapshot(''_db4ai test'',''test@1.0.0'')',
        'cannot purge root snapshot ''"_db4ai test"."test@1.0.0"'' having dependent snapshots',
        'purge all dependent snapshots first');

    PERFORM _db4ai_test.assert_equal(
        'SELECT (schema, name) FROM '
            'db4ai.purge_snapshot(''_db4ai test'', ''test@1.1.0'')',
        '(''_db4ai test''::NAME,''test@1.1.0''::NAME)');

    PERFORM _db4ai_test.assert_exception('db4ai.purge_snapshot_internal(null, null)',
        'direct call to db4ai.purge_snapshot_internal(name,name) is not allowed',
        'call public interface db4ai.purge_snapshot instead');

    SET db4ai_snapshot_mode = 8;
    PERFORM _db4ai_test.assert_exception('db4ai.purge_snapshot(null, null)',
        'invalid snapshot mode: ''8''');

END;
$$;
