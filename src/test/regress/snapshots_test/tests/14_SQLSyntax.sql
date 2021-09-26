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
 * 14_SQLSyntax.sql
 *    DB4AI.Snapshot test case.
 *
 *
 * -------------------------------------------------------------------------
 */

:TEST_BREAK_POINT

-- create synthetic tests data set
CALL _db4ai_test.create_testset(null,'test_data');

:TEST_BREAK_POINT

CREATE OR REPLACE FUNCTION _db4ai_test.fail() RETURNS CHAR AS 'BEGIN RAISE EXCEPTION ''fail''; END;' LANGUAGE plpgsql;

-- side-by-side test of MSS and RAW tables
SET db4ai_snapshot_mode = MSS; -- MSS is faster and applies DISTRIBUTE BY
CREATE SNAPSHOT "_db4ai test".m0 /* DISTRIBUTE BY HASH(pk) */ AS
    SELECT a1, a3, a6, a8, a2, pk, a1 b9, a7, a5 FROM _db4ai_test.test_data;

CREATE TABLE _db4ai_test.s0 /* DISTRIBUTE BY HASH(pk) */ AS
    SELECT a1, a3, a6, a8, a2, pk, a1 b9, a7, a5 FROM _db4ai_test.test_data;

SELECT _db4ai_test.assert_rel_equal(
    quote_ident('_db4ai test') || '.' || quote_ident('m0') || ' @ 1.0.0',
    quote_ident('_db4ai_test') || '.' || quote_ident('s0'));

CREATE SNAPSHOT "_db4ai test".m0 FROM @1.0.0 USING (
    UPDATE SNAPSHOT SET a1 = 5 WHERE pk % 10 = 0;
    ALTER SNAPSHOT ADD " u+ " INTEGER, ADD "x <> y"INT DEFAULT 2, ADD t CHAR(10) DEFAULT '',
        DROP a2, DROP COLUMN IF EXISTS b9, DROP COLUMN IF EXISTS b10;
    UPDATE SNAPSHOT SET "x <> y" = 8 WHERE pk < 100;
    ALTER SNAPSHOT DROP " u+ ", DROP IF EXISTS " x+ ";
    DELETE FROM SNAPSHOT WHERE pk = 3);

CREATE TABLE _db4ai_test.s1 /* DISTRIBUTE BY HASH(pk) */ AS SELECT * FROM _db4ai_test.s0;
    UPDATE _db4ai_test.s1 SET a1 = 5 WHERE pk % 10 = 0;
    ALTER TABLE _db4ai_test.s1 ADD " u+ " INTEGER, ADD "x <> y"INT DEFAULT 2, ADD t CHAR(10) DEFAULT '',
        DROP a2, DROP COLUMN IF EXISTS b9, DROP COLUMN IF EXISTS b10;
    UPDATE _db4ai_test.s1 SET "x <> y" = 8 WHERE pk < 100;
    ALTER TABLE _db4ai_test.s1 DROP " u+ ", DROP IF EXISTS " x+ ";
    DELETE FROM _db4ai_test.s1 WHERE pk = 3;

SELECT _db4ai_test.assert_rel_equal(
    quote_ident('_db4ai test') || '.' || quote_ident('m0') || '@2.0.0',
    quote_ident('_db4ai_test') || '.' || quote_ident('s1'));

CREATE SNAPSHOT "_db4ai test".m0 FROM @2.0.0 USING (
    UPDATE SNAPSHOT AS i SET a5 = o.a2 FROM _db4ai_test.test_data o WHERE i.pk = o.pk AND o.a3 % 8 = 0);

CREATE TABLE _db4ai_test.s2 /* DISTRIBUTE BY HASH(pk) */ AS SELECT * FROM _db4ai_test.s1;
    UPDATE _db4ai_test.s2 AS i SET a5 = o.a2 FROM _db4ai_test.test_data o WHERE i.pk = o.pk AND o.a3 % 8 = 0;

SELECT _db4ai_test.assert_rel_equal(
    quote_ident('_db4ai test') || '.' || quote_ident('m0') || '@2.0.1',
    quote_ident('_db4ai_test') || '.' || quote_ident('s2'));

CREATE SNAPSHOT "_db4ai test".m0 FROM @2.0.1 USING (
    DELETE FROM SNAPSHOT USING _db4ai_test.test_data o WHERE SNAPSHOT.pk = o.pk AND o.A7 % 2 = 0);

CREATE TABLE _db4ai_test.s3 /* DISTRIBUTE BY HASH(pk) */ AS SELECT * FROM _db4ai_test.s2;
    DELETE FROM _db4ai_test.s3 AS snapshot USING _db4ai_test.test_data o WHERE snapshot.pk = o.pk AND o.A7 % 2 = 0;

SELECT _db4ai_test.assert_rel_equal(
    quote_ident('_db4ai test') || '.' || quote_ident('m0') || '@2.1.0',
    quote_ident('_db4ai_test') || '.' || quote_ident('s3'));

CREATE SNAPSHOT "_db4ai test".m0 FROM @2.1.0 USING (
    INSERT INTO SNAPSHOT SELECT a1, a3, a6, a8, a2, pk+1000 pk, a7, a5, a4 FROM _db4ai_test.test_data WHERE pk % 10 = 4);

CREATE TABLE _db4ai_test.s4 /* DISTRIBUTE BY HASH(pk) */ AS SELECT * FROM _db4ai_test.s3;
    INSERT INTO _db4ai_test.s4 SELECT a1, a3, a6, a8, a2, pk+1000 pk, a7, a5, a4 FROM _db4ai_test.test_data WHERE pk % 10 = 4;

SELECT _db4ai_test.assert_rel_equal(
    quote_ident('_db4ai test') || '.' || quote_ident('m0') || '@2.2.0',
    quote_ident('_db4ai_test') || '.' || quote_ident('s4'));

-- separation of conflicting ALTER OPS
CREATE SNAPSHOT "_db4ai test".m0 @ alter_op_chaining FROM @1.0.0 USING (
    ALTER ADD column a int; ALTER DROP column a);

-- SHORT syntax variant, without reference to SNAPSHOT, all operations squashed into single revision
CREATE SNAPSHOT "_db4ai test".m0 @ squash FROM @1.0.0 USING (
    UPDATE SET a1 = 5 WHERE pk % 10 = 0;
    ALTER ADD " u+ " INTEGER, ADD "x <> y"INT DEFAULT 2, ADD t CHAR(10) DEFAULT '',
        DROP a2, DROP COLUMN IF EXISTS b9, DROP COLUMN IF EXISTS b10;
    UPDATE SET "x <> y" = 8 WHERE pk < 100;
    ALTER DROP " u+ ", DROP IF EXISTS " x+ ";
    DELETE WHERE pk = 3;
    UPDATE AS i SET a5 = o.a2 FROM _db4ai_test.test_data o WHERE i.pk = o.pk AND o.a3 % 8 = 0;
    DELETE USING _db4ai_test.test_data o WHERE snapshot.pk = o.pk AND o.A7 % 2 = 0;
    INSERT SELECT a1, a3, a6, a8, a2, pk+1000 pk, a7, a5, a4 FROM _db4ai_test.test_data WHERE pk % 10 = 4);

SELECT _db4ai_test.assert_rel_equal(
    quote_ident('_db4ai test') || '.' || quote_ident('m0') || '@squash',
    quote_ident('_db4ai_test') || '.' || quote_ident('s4'));

SAMPLE SNAPSHOT "_db4ai test".m0 @ 1.0.0 STRATIFY BY a1 AS _a AT RATIO .1 COMMENT IS '10%', AS _b AT RATIO .6 COMMENT IS '60%';
SELECT CASE WHEN (SELECT COUNT(*) FROM "_db4ai test".m0_b @ 1.0.0) BETWEEN
    (SELECT COUNT(*) FROM "_db4ai test".m0_a @ 1.0.0) AND (SELECT COUNT(*) FROM "_db4ai test".m0 @ 1.0.0) THEN
    'success' ELSE _db4ai_test.fail() END AS RESULT;

PUBLISH SNAPSHOT "_db4ai test".m0 @ squash;
ARCHIVE SNAPSHOT "_db4ai test".m0 @ 2.1.0;
PURGE SNAPSHOT "_db4ai test".m0 @ 2.2.0;

-- dummy function for this test case
CREATE OR REPLACE FUNCTION _db4ai_test.test() RETURNS VOID AS 'BEGIN END;' LANGUAGE plpgsql;
