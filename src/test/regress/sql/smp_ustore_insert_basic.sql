-- Init test environment.
-- 1. If there are not tables for tests, create 2 tables through the 'ustore' storage,
--    init and insert 100w data for tests.
-- 2. If there has been tables with the same name, truncate these tables, init and insert 100w data.

-- create schema for test
-- set smp_ustore_insert_basic schema to current schema, and set query_dop to 4 and set enable_force_smp to on
drop schema if exists smp_ustore_insert_basic CASCADE;
create schema smp_ustore_insert_basic;
set current_schema = smp_ustore_insert_basic;

-- create test table(t1, t2)
-- if there are three tables, drop it.
create or replace procedure create_table() as
declare
begin
    set query_dop = 4;
    set enable_force_smp = on;
    drop table if exists t1;
    drop table if exists t2;
    drop table if exists t3;
    create table t1
    (
        id int,
        b int,
        c int
    )
        with
            (storage_type = USTORE);

    create table t2
    (
        id int,
        b int,
        c int
    )
        with
            (storage_type = USTORE);
    create table t3
    (
        id int,
        b int,
        c int
    )
        with
            (storage_type = USTORE);
    -- init and insert 100w data for test
    insert into t1 values(generate_series(1, 10), generate_series(1, 10), generate_series(1, 10));
    insert into t2 values(generate_series(1, 10), generate_series(1, 10), generate_series(1, 10));
    insert into t3 values(generate_series(1, 10), generate_series(1, 10), generate_series(1, 10));
end;
/

-- 1. Basic function tests.
--  1.1 Set query_dop > 1, if executing "insert into select" SQL statement,
--      the result shows that "query plan" will go through "SMP insert"
--      and commit the current transaction.
--  1.2 Set query_dop > 1, if executing "insert into select" SQL statement,
--      the result shows that "query plan" will go through "SMP insert"
--      and succeed to roll back the current transaction.
--  1.3 Set query_dop == 1, if executing "insert into select" SQL statement,
--      the result shows that "query plan" will not go through "SMP insert"
--      and commit the current transaction.
--  1.4 Set query_dop == 1, if executing "insert into select" SQL statement,
--      the result shows that "query plan" will not go through "SMP insert"
--      and succeed to roll back the current transaction.

--  1.1 Set query_dop > 1, if executing "insert into select" SQL statement,
--      the result shows that "query plan" will go through "SMP insert"
--      and commit the current transaction.


call create_table();

set query_dop = 4;
set enable_force_smp = on;

analyze t1;
analyze t2;
analyze t3;

select count(*) from t1;
-- 10
select count(*) from t2;
-- 10
select count(*) from t3;
-- 10

-- 1) execution plan
explain (costs off) insert into t1 select * from t2;
insert into t1 select * from t2;
select count(*) from t1;
-- 20


explain (costs off) insert into t3 select * from (select t1.b, t2.c from t1, t2 where t1.b = t2.b);
insert into t3 select * from (select t1.b, t2.c from t1, t2 where t1.b = t2.b);
select count(*) from t3;
-- 30

-- within subquery
explain (costs off) with subquery as ( select t1.b, t2.c from t1, t2 where t1.b = t2.b) insert into t3 select * from subquery;
with subquery as ( select t1.b, t2.c from t1, t2 where t1.b = t2.b) insert into t3 select * from subquery;
select count(*) from t3;
-- 50

-- transaction
start transaction;
    insert into t1 select * from t2;
    select count(*) from t1;
    -- 30
    insert into t3 select * from (select t1.b, t2.c from t1, t2 where t1.b = t2.b);
    select count(*) from t3;
    -- 80
    -- within subquery
    with subquery as ( select t1.b, t2.c from t1, t2 where t1.b = t2.b) insert into t3 select * from subquery;
    select count(*) from t3;
    -- 110
commit;

select count(*) from t1;
-- 30
select count(*) from t2;
-- 10
select count(*) from t3;
-- 110

-- procedure
create or replace procedure test_dop_greater_1_multi_insert_commit() as
declare
    res int;
begin
    insert into t1 select * from t2;
    select count(*) from t1 into res;
    raise info 'result: %', res;
    -- 40
    insert into t3 select * from (select t1.b, t2.c from t1, t2 where t1.b = t2.b);
    select count(*) from t3 into res;
    raise info 'result: %', res;
    -- 150
    -- within subquery
    with subquery as ( select t1.b, t2.c from t1, t2 where t1.b = t2.b) insert into t3 select * from subquery;
    select count(*) from t3 into res;
    raise info 'result: %', res;
    -- 190
    commit;
end;
/

call test_dop_greater_1_multi_insert_commit();
-- 40
-- 150
-- 190

select count(*) from t1;
-- 40
select count(*) from t2;
-- 10
select count(*) from t3;
-- 190

create or replace procedure test_dop_greater_1_multi_commit() as
declare
    res int;
begin
    insert into t1 select * from t2;
    commit;
    select count(*) from t1 into res;
    raise info 'result: %', res;
    -- 50

    insert into t3 select * from (select t1.b, t2.c from t1, t2 where t1.b = t2.b);
    commit;
    select count(*) from t3 into res;
    raise info 'result: %', res;
    -- 240

    -- within subquery
    with subquery as ( select t1.b, t2.c from t1, t2 where t1.b = t2.b) insert into t3 select * from subquery;
    commit;
    select count(*) from t3 into res;
    raise info 'result: %', res;
    -- 290
end;
/

call test_dop_greater_1_multi_commit();
-- 50
-- 240
-- 290

-- 3 execute immediate scenery in stored procedure
CREATE OR REPLACE function test_exe_immediate_in_stored_procedure() return text
AS
    temp text;
BEGIN
    temp := 'insert into t1 select * from t2;';
    EXECUTE IMMEDIATE temp;
    commit;
    return temp;
    exception
        WHEN OTHERS THEN
        RAISE notice 'error msg';
        rollback;
        return null;
    return temp;
END;
/

call create_table();

set query_dop = 4;
set enable_force_smp = on;

analyze t1;
analyze t2;

select count(*) from t1;
-- 10
select count(*) from t2;
-- 10
select count(*) from t3;
-- 10

-- 1) execution result
call test_exe_immediate_in_stored_procedure();
select count(*) from t1;
-- 20

--  1.3 Set query_dop == 1, if executing "insert into select" SQL statement,
--      the result shows that "query plan" will not go through "SMP insert"
--      and commit the current transaction.

create or replace procedure test_dop_equal_1_commit() as
declare
    res int;
begin
    insert into t1 select * from t2;
    select count(*) from t1 into res;
    raise info 'result: %', res;
    -- 20
    commit;
end;
/

call create_table();

select count(*) from t1;
-- 10
select count(*) from t2;
-- 10
select count(*) from t3;
-- 10

set query_dop = 1;
set enable_force_smp = on;
analyze t1;
analyze t2;

-- 1) execution plan
explain (costs off) insert into t1 select * from t2;
-- 2) execution result
call test_dop_equal_1_commit();
-- 20
select count(*) from t1;
-- 20

--  1.4 Set query_dop == 1, if executing "insert into select" SQL statement,
--      the result shows that "query plan" will not go through "SMP insert"
--      and succeed to roll back the current transaction.
create or replace procedure test_dop_equal_1_rollback() as
declare
    res int;
begin
    insert into t1 select * from t2;
    select count(*) from t1 into res;
    raise info 'result: %', res;
    -- 20
    rollback;
end;
/

call create_table();

select count(*) from t1;
-- 10
select count(*) from t2;
-- 10
select count(*) from t3;
-- 10

set query_dop = 1;
set enable_force_smp = on;
analyze t1;
analyze t2;

call test_dop_equal_1_rollback();
-- 20
select count(*) from t1;
-- 10


-- 4. multi insert, multi rows
call create_table();

select count(*) from t1;
-- 10
select count(*) from t2;
-- 10
select count(*) from t3;
-- 10

set query_dop = 4;
set enable_force_smp = on;
analyze t1;
analyze t2;
analyze t3;

-- 1) execution plan
explain (costs off) insert into t1 select * from t2;
insert into t1 select * from t2;
select count(*) from t1;
-- 20


explain (costs off) insert into t3 select * from (select t1.b, t2.c from t1, t2 where t1.b = t2.b);
insert into t3 select * from (select t1.b, t2.c from t1, t2 where t1.b = t2.b);
select count(*) from t3;
-- 30

-- within subquery
explain (costs off) with subquery as ( select t1.b, t2.c from t1, t2 where t1.b = t2.b) insert into t3 select * from subquery;
with subquery as ( select t1.b, t2.c from t1, t2 where t1.b = t2.b) insert into t3 select * from subquery;
select count(*) from t3;
-- 50

select count(*) from t1;
-- 20
select count(*) from t2;
-- 10
select count(*) from t3;
-- 50

-- transaction
start transaction;
    insert into t1 select * from t2;
    select count(*) from t1;
    -- 30
    insert into t3 select * from (select t1.b, t2.c from t1, t2 where t1.b = t2.b);
    select count(*) from t3;
    -- 80
    -- within subquery
    with subquery as ( select t1.b, t2.c from t1, t2 where t1.b = t2.b) insert into t3 select * from subquery;
    select count(*) from t3;
    -- 110
commit;

-- procedure
create or replace procedure test_dop_greater_1_multi_row_insert() as
declare
    res int;
begin
    insert into t1 select * from t2;
    select count(*) from t1 into res;
    raise info 'result: %', res;
    -- 40

    insert into t3 select * from (select t1.b, t2.c from t1, t2 where t1.b = t2.b);
    select count(*) from t3 into res;
    raise info 'result: %', res;
    -- 150

    -- within subquery
    with subquery as ( select t1.b, t2.c from t1, t2 where t1.b = t2.b) insert into t3 select * from subquery;
    select count(*) from t3 into res;
    raise info 'result: %', res;
    -- 190
commit;
end;
/

call test_dop_greater_1_multi_row_insert();
-- 40
-- 150
-- 190

select count(*) from t1;
-- 40
select count(*) from t2;
-- 10
select count(*) from t3;
-- 190

-- recover all the environment variables
reset enable_force_smp;
reset query_dop;

set current_schema=public;
drop schema smp_ustore_insert_basic cascade;
