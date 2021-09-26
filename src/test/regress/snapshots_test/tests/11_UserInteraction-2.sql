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
 * 11_UserInteraction-2.sql
 *    DB4AI.Snapshot test case.
 *
 *
 * -------------------------------------------------------------------------
 */

-- create synthetic tests data set
CALL _db4ai_test.create_testset(null,'test_data');

CREATE TYPE _db4ai_test.snapshot_name AS ("schema" NAME, "name" NAME); -- openGauss BUG: array type not created during initdb

:TEST_BREAK_POINT

CREATE OR REPLACE FUNCTION _db4ai_test.fail() RETURNS CHAR AS 'BEGIN RAISE EXCEPTION ''fail''; END;' LANGUAGE plpgsql;

SET db4ai.message_level = DEFAULT;

-- direct calls for all APIs
-- test different call stack as in previous test functions
-- test different escape syntax as EXECUTE, which is invoking literals
-- test different CURRENT_SCHEMA settings, affecting the call stack

SET CURRENT_SCHEMA = db4ai;

DECLARE
    res db4ai.snapshot_name;
BEGIN
    CALL db4ai.create_snapshot(null, 's0', '{DISTRIBUTE BY HASH(pk),'
        '"SELECT a1, a3, a6, a8, a2, pk, a1 \"x <<>> y\", a7, a5",'
        'FROM _db4ai_test.test_data}') INTO STRICT res;
    IF res <> ('_db4ai_test', 's0@1.0.0')::db4ai.snapshot_name THEN
        _db4ai_test.fail();
    END IF;
END;
SELECT CASE WHEN ('_db4ai_test', ' s1 @ a ')::db4ai.snapshot_name =
    db4ai.create_snapshot(null, ' s1 ', '{DISTRIBUTE BY HASH(pk),'
            '"SELECT a1, a3, a6, a8, a2, pk, a1 \"x <<>> y\", a7, a5",'
            'FROM _db4ai_test.test_data}', ' a ')
    THEN 'success' ELSE _db4ai_test.fail() END AS RESULT;
SELECT CASE WHEN ('_db4ai test', 's2@1.0.0')::db4ai.snapshot_name =
    (schema, name) THEN 'success' ELSE _db4ai_test.fail() END AS RESULT
    FROM db4ai.create_snapshot('_db4ai test', 's2', '{DISTRIBUTE BY HASH(pk),'
            '"SELECT a1, a3, a6, a8, a2, pk, a1 \"x <<>> y\", a7, a5",'
            'FROM _db4ai_test.test_data}');


DECLARE
    res db4ai.snapshot_name;
BEGIN
    CALL db4ai.prepare_snapshot(null, 's0@1.0.0', '{ADD \"r+s~\" INT}') INTO STRICT res;
    IF res <> ('_db4ai_test', 's0@2.0.0')::db4ai.snapshot_name THEN
        _db4ai_test.fail();
    END IF;
END;
SELECT CASE WHEN ('_db4ai_test', ' s1 @ b ')::db4ai.snapshot_name =
    db4ai.prepare_snapshot(null, ' s1 @ a ', '{ADD \"r+s~\" INT}', ' b ')
    THEN 'success' ELSE _db4ai_test.fail() END AS RESULT;
SELECT CASE WHEN ('_db4ai test', 's2@2.0.0')::db4ai.snapshot_name =
    (schema, name) THEN 'success' ELSE _db4ai_test.fail() END AS RESULT
    FROM db4ai.prepare_snapshot('_db4ai test', 's2@1.0.0', '{ADD \"r+s~\" INT}');


DECLARE
    res _db4ai_test.snapshot_name[];
BEGIN
    SELECT array_agg((schema, name)::_db4ai_test.snapshot_name) FROM db4ai.sample_snapshot(null, 's0@2.0.0', '{_x,_y}', '{.1,.2}') INTO STRICT res;
    IF res <> ARRAY[('_db4ai_test', 's0_x@2.0.0'), ('_db4ai_test', 's0_y@2.0.0')]::_db4ai_test.snapshot_name[] THEN
        _db4ai_test.fail();
    END IF;
END;
SELECT CASE WHEN ARRAY[('_db4ai_test', ' s1  x @ b '), ('_db4ai_test', ' s1  y @ b ')]::_db4ai_test.snapshot_name[] =
    (SELECT array_agg((schema, name)::_db4ai_test.snapshot_name) FROM db4ai.sample_snapshot(null, ' s1 @ b ', '{" x ", " y "}', '{.1, .2}'))
    THEN 'success' ELSE _db4ai_test.fail() END AS RESULT;
SELECT CASE WHEN ARRAY[('_db4ai test', 's2_x@2.0.0'), ('_db4ai test', 's2_y@2.0.0')]::_db4ai_test.snapshot_name[] =
    array_agg((schema, name)::_db4ai_test.snapshot_name) THEN 'success' ELSE _db4ai_test.fail() END AS RESULT
    FROM db4ai.sample_snapshot('_db4ai test', 's2@2.0.0', '{_x, _y}', '{.1, .2}');


DECLARE
    res db4ai.snapshot_name;
BEGIN
    CALL db4ai.archive_snapshot(null, 's0_x@2.0.0') INTO STRICT res;
    IF res <> ('_db4ai_test', 's0_x@2.0.0')::db4ai.snapshot_name THEN
        _db4ai_test.fail();
    END IF;
END;
SELECT CASE WHEN ('_db4ai_test', ' s1  x @ b ')::db4ai.snapshot_name =
    db4ai.archive_snapshot(null, ' s1  x @ b ')
    THEN 'success' ELSE _db4ai_test.fail() END AS RESULT;
SELECT CASE WHEN ('_db4ai test', 's2_x@2.0.0')::db4ai.snapshot_name =
    archive_snapshot THEN 'success' ELSE _db4ai_test.fail() END AS RESULT
    FROM db4ai.archive_snapshot('_db4ai test', 's2_x@2.0.0');


DECLARE
    res db4ai.snapshot_name;
BEGIN
    CALL db4ai.publish_snapshot(null, 's0_y@2.0.0') INTO STRICT res;
    IF res <> ('_db4ai_test', 's0_y@2.0.0')::db4ai.snapshot_name THEN
        _db4ai_test.fail();
    END IF;
END;
SELECT CASE WHEN ('_db4ai_test', ' s1  y @ b ')::db4ai.snapshot_name =
    db4ai.publish_snapshot(null, ' s1  y @ b ')
    THEN 'success' ELSE _db4ai_test.fail() END AS RESULT;
SELECT CASE WHEN ('_db4ai test', 's2_y@2.0.0')::db4ai.snapshot_name =
    archive_snapshot THEN 'success' ELSE _db4ai_test.fail() END AS RESULT
    FROM db4ai.archive_snapshot('_db4ai test', 's2_y@2.0.0');


DECLARE
    res db4ai.snapshot_name;
BEGIN
    CALL db4ai.purge_snapshot(null, 's0_y@2.0.0') INTO STRICT res;
    IF res <> ('_db4ai_test', 's0_y@2.0.0')::db4ai.snapshot_name THEN
        _db4ai_test.fail();
    END IF;
END;
SELECT CASE WHEN ('_db4ai_test', ' s1  y @ b ')::db4ai.snapshot_name =
    db4ai.purge_snapshot(null, ' s1  y @ b ')
    THEN 'success' ELSE _db4ai_test.fail() END AS RESULT;
SELECT CASE WHEN ('_db4ai test', 's2_y@2.0.0')::db4ai.snapshot_name =
    purge_snapshot THEN 'success' ELSE _db4ai_test.fail() END AS RESULT
    FROM db4ai.purge_snapshot('_db4ai test', 's2_y@2.0.0');


-- dummy function for this test case
CREATE OR REPLACE FUNCTION _db4ai_test.test() RETURNS VOID AS 'BEGIN END;' LANGUAGE plpgsql;
