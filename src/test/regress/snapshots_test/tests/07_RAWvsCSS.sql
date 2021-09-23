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
 * 07_RAWvsCSS.sql
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
    rawtables _db4ai_test.snapshot_name[];
    snapshots _db4ai_test.snapshot_name[];
BEGIN

    -- side-by-side test of CSS and RAW tables
    SET db4ai_snapshot_mode = CSS;
    snapshots[1] := db4ai.create_snapshot('_db4ai test', 'm0', ARRAY[
        $$SELECT a1, a3, a6, a8, a2, pk, a1 b9, a7, a5$$, $$FROM _db4ai_test.test_data$$,
        $$DISTRIBUTE BY HASH(pk)$$
    ]);
    rawtables[1] := ('_db4ai_test','s0');
    CREATE TABLE _db4ai_test.s0 /* DISTRIBUTE BY HASH(pk) */ AS
        SELECT a1, a3, a6, a8, a2, pk, a1 b9, a7, a5 FROM _db4ai_test.test_data;

    snapshots[2] := db4ai.prepare_snapshot('_db4ai test', 'm0@1.0.0', ARRAY[
        $$UPDATE$$, $$SET a1 = 5$$, $$WHERE pk % 10 = 0$$,
        $$ADD " u+ " INTEGER$$, $$ADD "x <> y"INT DEFAULT 2$$, $$ADD t CHAR(10) DEFAULT ''$$,
        $$DROP a2$$, $$DROP COLUMN IF EXISTS b9$$, $$DROP COLUMN IF EXISTS b10$$,
        $$UPDATE$$, $$SET "x <> y" = 8$$, $$WHERE pk < 100$$,
        $$DROP " u+ "$$, $$DROP IF EXISTS " x+ "$$,
        $$DELETE$$, $$WHERE pk = 3$$
    ]);
    rawtables[2] := ('_db4ai_test','s1');
    CREATE TABLE _db4ai_test.s1 /* DISTRIBUTE BY HASH(pk) */ AS SELECT * FROM _db4ai_test.s0;
    UPDATE _db4ai_test.s1 SET a1 = 5 WHERE pk % 10 = 0;
    ALTER TABLE _db4ai_test.s1 ADD " u+ " INTEGER, ADD "x <> y"INT DEFAULT 2, ADD t CHAR(10) DEFAULT '',
        DROP a2, DROP COLUMN IF EXISTS b9, DROP COLUMN IF EXISTS b10;
    UPDATE _db4ai_test.s1 SET "x <> y" = 8 WHERE pk < 100;
    ALTER TABLE _db4ai_test.s1 DROP " u+ ", DROP IF EXISTS " x+ ";
    DELETE FROM _db4ai_test.s1 WHERE pk = 3;

    snapshots[3] := db4ai.prepare_snapshot('_db4ai test', 'm0@2.0.0', ARRAY[
        $$UPDATE$$, $$AS i$$, $$SET a5 = o.a2$$, $$FROM _db4ai_test.test_data o$$, $$WHERE i.pk = o.pk AND o.a3 % 8 = 0$$
    ]);
    rawtables[3] := ('_db4ai_test','s2');
    CREATE TABLE _db4ai_test.s2 /* DISTRIBUTE BY HASH(pk) */ AS SELECT * FROM _db4ai_test.s1;
    UPDATE _db4ai_test.s2 AS i SET a5 = o.a2 FROM _db4ai_test.test_data o WHERE i.pk = o.pk AND o.a3 % 8 = 0;

    snapshots[4] := db4ai.prepare_snapshot('_db4ai test', 'm0@2.0.1', ARRAY[
        $$DELETE$$, $$USING _db4ai_test.test_data o$$, $$WHERE snapshot.pk = o.pk AND o.A7 % 2 = 0$$
    ]);
    rawtables[4] := ('_db4ai_test','s3');
    CREATE TABLE _db4ai_test.s3 /* DISTRIBUTE BY HASH(pk) */ AS SELECT * FROM _db4ai_test.s2;
    DELETE FROM _db4ai_test.s3 AS snapshot USING _db4ai_test.test_data o WHERE snapshot.pk = o.pk AND o.A7 % 2 = 0;

    snapshots[5] := db4ai.prepare_snapshot('_db4ai test', 'm0@2.1.0', ARRAY[
        $$INSERT$$, $$SELECT a1, a3, a6, a8, a2, pk+1000 pk, a7, a5, a4$$, $$FROM _db4ai_test.test_data$$, $$WHERE pk % 10 = 4$$
    ]);
    rawtables[5] := ('_db4ai_test','s4');
    CREATE TABLE _db4ai_test.s4 /* DISTRIBUTE BY HASH(pk) */ AS SELECT * FROM _db4ai_test.s3;
    INSERT INTO _db4ai_test.s4 SELECT a1, a3, a6, a8, a2, pk+1000 pk, a7, a5, a4 FROM _db4ai_test.test_data WHERE pk % 10 = 4;

    snapshots[6] := db4ai.prepare_snapshot('_db4ai test', 'm0@1.0.0', ARRAY[
        $$UPDATE$$, $$SET a1 = 5$$, $$WHERE pk % 10 = 0$$,
        $$ADD " u+ " INTEGER$$, $$ADD "x <> y" INT DEFAULT 2$$, $$ADD t CHAR(10) DEFAULT ''$$,
        $$DROP a2$$, $$DROP COLUMN IF EXISTS b9$$, $$DROP COLUMN IF EXISTS b10$$,
        $$UPDATE$$, $$SET "x <> y" = 8$$, $$WHERE pk < 100$$,
        $$DROP " u+ "$$, $$DROP IF EXISTS " x+ "$$,
        $$DELETE$$, $$WHERE pk = 3$$,
        $$UPDATE$$, $$AS i$$, $$SET a5 = o.a2$$, $$FROM _db4ai_test.test_data o$$, $$WHERE i.pk = o.pk AND o.a3 % 8 = 0$$,
        $$DELETE$$, $$USING _db4ai_test.test_data o$$, $$WHERE snapshot.pk = o.pk AND o.A7 % 2 = 0$$,
        $$INSERT$$, $$SELECT a1, a3, a6, a8, a2, pk+1000 pk, a7, a5, a4$$, $$FROM _db4ai_test.test_data$$, $$WHERE pk % 10 = 4$$
    ], 'squash');
    rawtables[6] := ('_db4ai_test','s4');

    FOR i IN 1 .. array_length(snapshots, 1) LOOP
        PERFORM _db4ai_test.assert_rel_equal(
            quote_ident(rawtables[i].schema) || '.' || quote_ident(rawtables[i].name),
            quote_ident(snapshots[i].schema) || '.' || quote_ident(snapshots[i].name));
    END LOOP;

END;
$func$;
