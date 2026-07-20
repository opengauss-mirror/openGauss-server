--  1.2 Set query_dop > 1, if executing "insert into select" SQL statement,
--      the result shows that "query plan" will go through "SMP insert"
--      and succeed to roll back the current transaction.

drop schema if exists smp_ustore_insert_tx CASCADE;
create schema smp_ustore_insert_tx;
set current_schema = smp_ustore_insert_tx;

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

create or replace procedure test_dop_greater_1_rollback() as
declare
    res int;
begin
    insert into t1 select * from t2;
    select count(*) from t1 into res;
    raise info 'result: %', res;
    -- 20
    rollback;
    select count(*) from t1 into res;
    raise info 'result: %', res;
    -- 10
end;
/

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

start transaction;
    insert into t1 select * from t2;
    select count(*) from t1;
    -- 20
rollback;
select count(*) from t1;
-- 10

call test_dop_greater_1_rollback();
-- 20
-- 10

-- sub transaction rollback test
CREATE OR REPLACE PROCEDURE TEST_COMMIT_INSERT_EXCEPTION_ROLLBACK01()
AS
declare
    res int;
BEGIN
    insert into t1 select * from t2;
    select count(*) from t1 into res;
    -- 30
    raise info 'result: %', res;
    savepoint my_savepoint;
    select count(*) from t1 into res;
    raise info 'result: %', res;
    -- 30
    insert into t1 select * from t2;
    select count(*) from t1 into res;
    raise info 'result: %', res;
    -- 40
    rollback to savepoint my_savepoint;
    select count(*) from t1 into res;
    raise info 'result: %', res;
    -- 30
END;
/

start transaction;
    insert into t1 select * from t2;
    select count(*) from t1;
    --20
    savepoint my_savepoint;
    select count(*) from t1;
    --20
    insert into t1 select * from t2;
    select count(*) from t1;
    --30
    rollback to savepoint my_savepoint;
    select count(*) from t1;
    --20
end;

call TEST_COMMIT_INSERT_EXCEPTION_ROLLBACK01();
-- 30
-- 30
-- 40
-- 30

CREATE OR REPLACE PROCEDURE TEST_COMMIT_INSERT_EXCEPTION_ROLLBACK02()
AS
declare
    res int;
BEGIN
    insert into t1 select * from t2;
    select count(*) from t1 into res;
    -- 40
    raise info 'result: %', res;
    savepoint my_savepoint;
    insert into t1 select * from t2;
    select count(*) from t1 into res;
    -- 50
    raise info 'result: %', res;
    rollback to  savepoint my_savepoint;
    select count(*) from t1 into res;
    -- 40
    raise info 'result: %', res;
    rollback;
    select count(*) from t1 into res;
    -- 30
    raise info 'result: %', res;
END;
/

start transaction;
    insert into t1 select * from t2;
    select count(*) from t1;
    -- 40
    savepoint my_savepoint;
    insert into t1 select * from t2;
    select count(*) from t1;
    -- 50
    rollback to  savepoint my_savepoint;
    select count(*) from t1;
    -- 40
rollback;
select count(*) from t1;
-- 30

call TEST_COMMIT_INSERT_EXCEPTION_ROLLBACK02();
-- 40
-- 50
-- 40
-- 30

CREATE OR REPLACE PROCEDURE TEST_COMMIT_INSERT_EXCEPTION_ROLLBACK03()
AS
declare
    res int;
BEGIN
    insert into t1 select * from t2;
    select count(*) from t1 into res;
    raise info 'result: %', res;
    -- 50
    savepoint my_savepoint;
    select count(*) from t1 into res;
    raise info 'result: %', res;
    -- 50
    insert into t1 select * from t2;
    select count(*) from t1 into res;
    raise info 'result: %', res;
    -- 60
    rollback to  savepoint my_savepoint;
    select count(*) from t1 into res;
    raise info 'result: %', res;
    -- 50
    commit;
    select count(*) from t1 into res;
    raise info 'result: %', res;
    -- 50
END;
/

START TRANSACTION;
    insert into t1 select * from t2;
    select count(*) from t1;
    -- 40
    savepoint my_savepoint;
    select count(*) from t1;
    -- 40
    insert into t1 select * from t2;
    select count(*) from t1;
    -- 50
    rollback to  savepoint my_savepoint;
    select count(*) from t1;
    -- 40
commit;
select count(*) from t1;
-- 40

call TEST_COMMIT_INSERT_EXCEPTION_ROLLBACK03();
-- 50
-- 50
-- 60
-- 50
-- 50

-- sub transaction release test

CREATE OR REPLACE PROCEDURE TEST_COMMIT_INSERT_EXCEPTION_RELEASE01()
AS
declare
    res int;
BEGIN
    insert into t1 select * from t2;
    select count(*) from t1 into res;
    -- 80
    raise info 'result: %', res;
    savepoint my_savepoint;
    select count(*) from t1 into res;
    -- 80
    raise info 'result: %', res;
    insert into t1 select * from t2;
    select count(*) from t1 into res;
    -- 90
    raise info 'result: %', res;
    release savepoint my_savepoint;
    select count(*) from t1 into res;
    -- 90
    raise info 'result: %', res;
END;
/

start transaction;
    insert into t1 select * from t2;
    select count(*) from t1;
    -- 60
    savepoint my_savepoint;
    select count(*) from t1;
    -- 60
    insert into t1 select * from t2;
    select count(*) from t1;
    -- 70
    release savepoint my_savepoint;
    select count(*) from t1;
    -- 70
end;

call TEST_COMMIT_INSERT_EXCEPTION_RELEASE01();
-- 80
-- 80
-- 90
-- 90

CREATE OR REPLACE PROCEDURE TEST_COMMIT_INSERT_EXCEPTION_RELEASE02()
AS
declare
    res int;
BEGIN
    insert into t1 select * from t2;
    select count(*) from t1 into res;
    raise info 'result: %', res;
    -- 100
    savepoint my_savepoint;
    select count(*) from t1 into res;
    raise info 'result: %', res;
    -- 100
    insert into t1 select * from t2;
    select count(*) from t1 into res;
    raise info 'result: %', res;
    -- 110
    release savepoint my_savepoint;
    select count(*) from t1 into res;
    raise info 'result: %', res;
    -- 110
    rollback;
    select count(*) from t1 into res;
    raise info 'result: %', res;
    -- 90
END;
/

start transaction;
    insert into t1 select * from t2;
    select count(*) from t1;
    -- 100
    savepoint my_savepoint;
    select count(*) from t1;
    -- 100
    insert into t1 select * from t2;
    select count(*) from t1;
    -- 110
    release savepoint my_savepoint;
    select count(*) from t1;
    -- 110
rollback;
select count(*) from t1;
-- 90

call TEST_COMMIT_INSERT_EXCEPTION_RELEASE02();
-- 100
-- 100
-- 110
-- 110
-- 90

CREATE OR REPLACE PROCEDURE TEST_COMMIT_INSERT_EXCEPTION_RELEASE03()
AS
declare
    res int;
BEGIN
    select count(*) from t1 into res;
    raise info 'result: %', res;
    -- 110
    insert into t1 select * from t2;
    select count(*) from t1 into res;
    -- 120
    raise info 'result: %', res;
    savepoint my_savepoint;
    select count(*) from t1 into res;
    raise info 'result: %', res;
    -- 120
    insert into t1 select * from t2;
    select count(*) from t1 into res;
    raise info 'result: %', res;
    -- 130
    release savepoint my_savepoint;
    select count(*) from t1 into res;
    raise info 'result: %', res;
    -- 130
    commit;
    select count(*) from t1 into res;
    raise info 'result: %', res;
    -- 130
END;
/

start transaction;
    insert into t1 select * from t2;
    select count(*) from t1;
    -- 100
    savepoint my_savepoint;
    insert into t1 select * from t2;
    select count(*) from t1;
    -- 110
    release savepoint my_savepoint;
    select count(*) from t1;
    -- 110
commit;
select count(*) from t1;
-- 110


call TEST_COMMIT_INSERT_EXCEPTION_RELEASE03();
-- 110
-- 120
-- 120
-- 130
-- 130
-- 130

start transaction;
set query_dop = 2;
set enable_force_smp = on;
insert into t2 values(1, 1);
    select count(*) from t2;
-- 11
savepoint s1;
insert into t2 values(1, 1);
    select count(*) from t2;
-- 12
insert into t1 select * from t2;
    select count(*) from t1;
-- 142
end;

start transaction;
set query_dop = 2;
set enable_force_smp = on;
insert into t2 values(generate_series(1, 20000), generate_series(1, 20000));
select count(*) from t2;
--20012
insert into t1 select * from t2;
select count(*) from t1;
--20154
end;

start transaction;
set query_dop=4;
set enable_force_smp = on;
select count(*) from t1;
--20154
\copy t1 to '~/undo_smp_copytest.csv' csv;
savepoint s1;
\copy t2 from '~/undo_smp_copytest.csv' csv;
select count(*) from t2;
--40166
end;

-- recover all the environment variables
reset enable_force_smp;
reset query_dop;

set current_schema=public;
drop schema smp_ustore_insert_tx cascade;
