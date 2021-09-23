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
 * XX_ScratchPad.sql
 *    DB4AI.Snapshot testing infrastructure.
 *
 * -------------------------------------------------------------------------
 */

-- This modules serves as a convenient way to run automated test for debugging

-- Declare dummy test case
CREATE OR REPLACE FUNCTION _db4ai_test.test() RETURNS VOID AS 'BEGIN END;' LANGUAGE plpgsql;

-- Scratch area starts here

SET db4ai_snapshot_mode = CSS;

CALL _db4ai_test.create_testset('_db4ai_test','test_data');

    call db4ai.create_snapshot(null, 'test', '{SELECT 1 a, FROM DUAL, DISTRIBUTE BY  RePLICATION}');
:TEST_BREAK_POINT
SET db4ai_snapshot_mode = MSS;
:TEST_BREAK_POINT

    SELECT array_agg(schema || ',' || quote_ident(name)) FROM db4ai.sample_snapshot(null, 'test@1.0.0', '{_1, " _ 2 "}', '{.1, .2}');

        '''{"(_db4ai_test,test_1@1.0.0)","(_db4ai_test,\"test _ 2 @1.0.0\")"}''');


CALL db4ai.create_snapshot('_db4ai_test', 'm0', '{DISTRIBUTE BY HASH(pk),'
        '"SELECT a1, a3, a6, a8, a2, pk, a1 b9, a7, a5", FROM _db4ai_test.test_data}');
CALL db4ai.sample_snapshot('_db4ai_test', 'm0@1.0.0', '{_sample}', '{0.2}', '{a3}');

COMMIT;
\q

CALL db4ai.prepare_snapshot('_db4ai_test', 'm0@1.0.0', '{'
        'UPDATE, SET a1 = 5, WHERE pk % 10 = 0,'
        'ADD \" u+ \" INTEGER, ADD \"x <> y\"INT DEFAULT 2, ADD t CHAR(10) DEFAULT '''','
        'DROP a2, DROP COLUMN IF EXISTS b9, DROP COLUMN IF EXISTS b10,'
        'UPDATE, SET \"x <> y\" = 8, WHERE pk < 100,'
        'DROP \" u+ \", DROP IF EXISTS \" x+ \",'
        'DELETE, WHERE pk = 3'
    '}');

    CREATE TABLE _db4ai_test.s1 DISTRIBUTE BY HASH(pk) AS SELECT * FROM _db4ai_test.s0;
    UPDATE _db4ai_test.s1 SET a1 = 5 WHERE pk % 10 = 0;
    ALTER TABLE _db4ai_test.s1 ADD " u+ " INTEGER, ADD "x <> y"INT DEFAULT 2, ADD t CHAR(10) DEFAULT '',
        DROP a2, DROP COLUMN IF EXISTS b9, DROP COLUMN IF EXISTS b10;
    UPDATE _db4ai_test.s1 SET "x <> y" = 8 WHERE pk < 100;
    ALTER TABLE _db4ai_test.s1 DROP " u+ ", DROP IF EXISTS " x+ ";
    DELETE FROM _db4ai_test.s1 WHERE pk = 3;

call db4ai.prepare_snapshot('_db4ai_test', 'm0@2.0.0', '{'
        'UPDATE, AS i, SET a5 = o.a2, FROM _db4ai_test.test_data o, WHERE i.pk = o.pk AND o.a3 % 8 = 0'
    '}');

select * from  "_db4ai_test"."m0@2.0.0"  order by pk limit 20;
select * from  _db4ai_test.s1 order by pk limit 20;

    CREATE TABLE _db4ai_test.s2 DISTRIBUTE BY HASH(pk) AS SELECT * FROM _db4ai_test.s1;
    UPDATE _db4ai_test.s2 AS i SET a5 = o.a2 FROM _db4ai_test.test_data o WHERE i.pk = o.pk AND o.a3 % 8 = 0;

select * from  "_db4ai_test"."m0@2.0.1"  order by pk limit 20;
select * from  _db4ai_test.s2 order by pk limit 20;

CREATE OR REPLACE RULE _UPDATE AS ON UPDATE TO db4ai.v3 DO INSTEAD UPDATE db4ai.t1 SET f14=new.a5 WHERE t1.xc_node_id=old.xc_node_id AND t1.ctid=old.ctid;
UPDATE db4ai.v3 AS i SET a5 = o.a2 FROM _db4ai_test.test_data o WHERE i.pk = o.pk AND o.a3 % 8 = 0


:TEST_BREAK_POINT
\q

SET db4ai_snapshot_mode = MSS;

call db4ai.create_snapshot('_db4ai_test', 'av',
--    '{"from (values (1,2,2),(2,4,2),(3,4,1),(5,6,2)) as t(\"\"\"a\"\"\",\"B \"\"b\",c)", distribute by hash(\"\"\"a\"\"\")}');
    '{"from (values (1,2,2),(2,4,2),(3,4,1),(5,6,2)) as t(\"a\"\"a\",\"B \"\"b\",c)", distribute by hash(\"a\"\"a\")}');

call db4ai.prepare_snapshot('_db4ai_test','av@1.0.0',
 '{insert, "(c,\"B \"\"b\",\"a\"\"a\") values (2,4,1)", delete, where c=2}', '2.0.0');

-- unable to map field "x" to backing table
-- call db4ai.prepare_snapshot('_db4ai_test','av@1.0.0',
-- '{"add \"\"\"C \"\"d\" int not null default 5", drop column x y, drop c}', '3.0.0');

-- unable to map field "if" to backing table
--call db4ai.prepare_snapshot('_db4ai_test','av@1.0.0',
-- '{"add \"\"\"C \"\"d\" int not null default 5", drop column if, drop c}', '3.0.0');

-- expected EXISTS keyword in DROP operation after 'if'
--call db4ai.prepare_snapshot('_db4ai_test','av@1.0.0',
-- '{"add \"\"\"C \"\"d\" int not null default 5", drop column if if2, drop c}', '3.0.0');

-- expected EXISTS keyword in DROP operation after 'if'
-- call db4ai.prepare_snapshot('_db4ai_test','av@1.0.0',
-- '{"add \"\"\"C \"\"d\" int not null default 5", drop if if, drop c}', '3.0.0');

--column "du"mmy" already exists in current snapshot
--call db4ai.prepare_snapshot('_db4ai_test','av@2.0.0',
-- '{add \"\"\"C \"\"d\" int not null default 5, drop column if exists xx, drop c, add \"du\"\"mmy\" int, add \"du\"\"mmy\"}', '3.0.0');

--column "f5" of relation "t3" does not exist
-- call db4ai.prepare_snapshot('_db4ai_test','av@2.0.0',
-- '{add \"\"\"C \"\"d\" int not null default 5, drop column if exists xx, drop c, add \"du\"\"mmy\" int, drop \"du\"\"mmy\"}', '3.0.0');

call db4ai.prepare_snapshot('_db4ai_test','av@2.0.0',
 '{add \"\"\"C \"\"d\" int not null default 5, drop column if exists xx, drop c, add \"du\"\"mmy\" int}', '3.0.0');

call db4ai.prepare_snapshot('_db4ai_test','av@3.0.0',
 '{drop \"du\"\"mmy\"}', '4.0.0');

:TEST_BREAK_POINT

call db4ai.prepare_snapshot('_db4ai_test','av@4.0.0',
 '{update, "set \"\"\"C \"\"d\"=4", "add \"E \"\"f\"\"\" int not null default 3",'
 ' update, "set \"E \"\"f\"\"\"=\"\"\"C \"\"d\"", "drop if exists \"\"\"C \"\"d\""}', '5.0.0');

-- call db4ai.prepare_snapshot('_db4ai_test','av@1.0.0',
    -- '{add x1 int, drop column if exists, update, set c=3, where c=2, insert, "(c,\"B \"b\",a) values (2,4,1)", delete, where c=2}', '2.0.0');

-- call db4ai.prepare_snapshot('_db4ai_test','av@1.0.0',
    -- '{update, set c=3, where c=2, insert, "(c,\"B \"b\",a) values (2,4,1)", delete, where c=2}', '2.0.0');
-- call db4ai.prepare_snapshot('_db4ai_test','av@1.0.0',
    -- '{update, set c=3, where c=2, insert, "(c,\"B \"b\",a) values (2,4,1)", delete, where c=2}', '2.0.0');
