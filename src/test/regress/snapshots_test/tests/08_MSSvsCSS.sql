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
 * 08_MSSvsCSS.sql
 *    DB4AI.Snapshot test case.
 *
 *
 * -------------------------------------------------------------------------
 */

-- create synthetic tests data set
CALL _db4ai_test.create_testset(null,'test_data');

CREATE TYPE _db4ai_test.snapshot_name AS ("schema" NAME, "name" NAME); -- openGauss BUG: array type not created during initdb

CREATE OR REPLACE FUNCTION _db4ai_test.test(
)
RETURNS VOID LANGUAGE plpgsql SECURITY INVOKER SET client_min_messages TO NOTICE
AS $func$
DECLARE
    s NAME := '_db4ai test';
    css _db4ai_test.snapshot_name[];
    mss _db4ai_test.snapshot_name[];
BEGIN

-- In principle, this test case is redundant, since MSS CSS equivalence is tested transitively in the RAW table tests.
-- Testing MSS and CSS against RAW tables also gives a clue which of CSS or MSS is broken, in case they differ.
-- Still we apply direct testing for completeness.

    SET db4ai_snapshot_mode = CSS;
    FOR i IN 1 .. 2 LOOP

        mss[1] := db4ai.create_snapshot(s, 's' || i, ARRAY[
            $$SELECT *$$, $$FROM _db4ai_test.test_data$$, $$DISTRIBUTE BY HASH(pk)$$
        ]);
        mss[2] := db4ai.prepare_snapshot(s, 's' || i || '@1.0.0', ARRAY[
            $$ADD "x <> x" INT DEFAULT 2$$, $$DROP a2$$, $$DELETE$$, $$WHERE pk = 3$$
        ]);
        mss[2] := db4ai.prepare_snapshot(s, 's' || i || '@2.0.0', ARRAY[
            $$DROP "x <> x"$$
        ]);

        PERFORM db4ai.purge_snapshot(s, 's' || i || '@2.0.0');

        mss[3] := db4ai.prepare_snapshot(s, 's' || i || '@3.0.0', ARRAY[
            $$UPDATE$$, $$SET a1 = a4$$, $$WHERE a3 % 8 = 0$$
        ]);
        mss[4] := db4ai.prepare_snapshot(s, 's' || i || '@3.0.1', ARRAY[
            $$DELETE$$, $$USING _db4ai_test.test_data o$$, $$WHERE o.pk = snapshot.pk AND o.a1 < 100$$
        ]);
        mss := mss || array_agg((schema, name)::_db4ai_test.snapshot_name) FROM db4ai.sample_snapshot(s, 's' || i ||'@3.1.0', '{_train, _test}','{.8,.2}');

        PERFORM _db4ai_test.assert_equal('FALSE',
            'SELECT archived OR published FROM db4ai.snapshot WHERE schema = ''' || s || ''' AND name = ''s' || i || '_train@3.1.0''');

        PERFORM db4ai.archive_snapshot(s, 's' || i ||'_train@3.1.0');
        PERFORM _db4ai_test.assert_equal('TRUE',
            'SELECT archived AND NOT published FROM db4ai.snapshot WHERE schema = ''' || s || ''' AND name = ''s' || i || '_train@3.1.0''');

        PERFORM db4ai.publish_snapshot(s, 's' || i ||'_test@3.1.0');
        PERFORM _db4ai_test.assert_equal('TRUE',
            'SELECT NOT archived AND published FROM db4ai.snapshot WHERE schema = ''' || s || ''' AND name = ''s' || i || '_test@3.1.0''');

        IF i = 1 THEN
            SET db4ai_snapshot_mode = MSS;
            css := mss;
            mss := NULL;
        END IF;
    END LOOP;

    FOR i IN 1 .. array_length(css, 1) -2 LOOP
        PERFORM _db4ai_test.assert_rel_equal(
            quote_ident(css[i].schema) || '.' || quote_ident(css[i].name),
            quote_ident(mss[i].schema) || '.' || quote_ident(mss[i].name));
    END LOOP;

END;
$func$;
