-- ustore concurrency whitebox testing

-- testing the following functions returning TM_Result 3-7:
-- UHeapLockTuple
-- UHeapDelete
-- UHeapUpdate

-- TM_Result reference:
-- TM_Ok=0              no concurrency
-- TM_Invisible=1       should never happen
-- TM_SelfModified=2
-- TM_Updated=3         update happens before target statmenet
-- TM_Deleted=4         delete happens before target statement
-- TM_BeingModified=5   select for share/update happens before target statement
-- TM_SelfCreated=6     tuple is created/modified in the same statement
-- TM_SelfUpdated=7     tuple is created/modified in the same statement

------------------------------
-- UHeapLockTuple returning 3 (TM_Updated)
------------------------------
create table t_lock (c1 int) with (storage_type=USTORE);
insert into t_lock values (1);

-- update happens before select foe update
\parallel on 2
begin
    perform pg_sleep(1);
    perform c1 from t_lock for update;
end;
/
begin
    update t_lock set c1 = 2;
    perform pg_sleep(2);
end;
/
\parallel off

drop table t_lock;

------------------------------
-- UHeapLockTuple returning 4 (TM_Deleted)
------------------------------
create table t_lock (c1 int) with (storage_type=USTORE);
insert into t_lock values (1);

-- delete happens before select for update
\parallel on 2
begin
    perform pg_sleep(1);
    perform c1 from t_lock for update;
end;
/
begin
    delete from t_lock;
    perform pg_sleep(2);
end;
/
\parallel off

drop table t_lock;

------------------------------
-- UHeapLockTuple returning 5 (TM_BeingModified)
------------------------------
create table t_lock (c1 int) with (storage_type=USTORE);
insert into t_lock values (1);

-- select for share happens before select foe update
\parallel on 2
begin
    perform pg_sleep(1);
    perform c1 from t_lock for update;
end;
/
begin
    perform c1 from t_lock for share;
    perform pg_sleep(2);
end;
/
\parallel off

drop table t_lock;

------------------------------
-- prepare for below 2 cases
------------------------------
CREATE TABLE t1_lock (
    c1 INT
) with(storage_type=ustore);

INSERT INTO t1_lock VALUES (1);
INSERT INTO t1_lock VALUES (1);
SELECT * FROM t1_lock;

CREATE TABLE t2_lock (
    c1 INT PRIMARY KEY,
    c2 INT DEFAULT 1
) with(storage_type=ustore);

------------------------------
-- UHeapLockTuple returning 6 (TM_SelfCreated)
------------------------------
explain (costs off)
INSERT INTO t2_lock (c1)
    SELECT c1 FROM t1_lock
    ON DUPLICATE KEY UPDATE c2 = c2 + 1;

INSERT INTO t2_lock (c1)
    SELECT c1 FROM t1_lock
    ON DUPLICATE KEY UPDATE c2 = c2 + 1;
select * from t2_lock;
delete from t2_lock;

INSERT INTO t2_lock (c1)
    SELECT c1 FROM t1_lock
    ON DUPLICATE KEY UPDATE c2 = c2 + 1;
select * from t2_lock;

------------------------------
-- UHeapLockTuple returning 7 (TM_SelfUpdated)
------------------------------
INSERT INTO t2_lock (c1)
    SELECT c1 FROM t1_lock
    ON DUPLICATE KEY UPDATE c2 = c2 + 1;
select * from t2_lock;

INSERT INTO t2_lock (c1)
    SELECT c1 FROM t1_lock
    ON DUPLICATE KEY UPDATE c2 = c2 + 1;
select * from t2_lock;

------------------------------
-- cleanup for above 2 cases
------------------------------
drop table t1_lock;
drop table t2_lock;

------------------------------
-- UHeapDelete returning 3 (TM_Updated)
------------------------------
create table t_delete (c1 int) with (storage_type=USTORE);
insert into t_delete values (1);

-- update happens before delete
\parallel on 2
begin
    perform pg_sleep(1);
    delete from t_delete;
end;
/
begin
    update t_delete set c1 = 2;
    perform pg_sleep(2);
end;
/
\parallel off

drop table t_delete;

------------------------------
-- UHeapDelete returning 4 (TM_Deleted)
------------------------------
create table t_delete (c1 int) with (storage_type=USTORE);
insert into t_delete values (1);

-- delete 2 happens before delete 1
\parallel on 2
begin
    perform pg_sleep(1);
    delete from t_delete;
end;
/
begin
    delete from t_delete;
    perform pg_sleep(2);
end;
/
\parallel off

drop table t_delete;

------------------------------
-- UHeapDelete returning 5 (TM_BeingModified)
------------------------------
create table t_delete (c1 int) with (storage_type=USTORE);
insert into t_delete values (1);

-- select for update happens before delete
\parallel on 2
begin
    perform pg_sleep(1);
    delete from t_delete;
end;
/
begin
    perform c1 from t_delete for update;
    perform pg_sleep(2);
end;
/
\parallel off

drop table t_delete;

------------------------------
-- UHeapDelete returning 6 (TM_SelfCreated)
------------------------------

------------------------------
-- UHeapDelete returning 7 (TM_SelfUpdated)
------------------------------

------------------------------
-- UHeapUpdate returning 3 (TM_Updated)
------------------------------
create table t_update (c1 int) with (storage_type=USTORE);
insert into t_update values (1);

-- update 2 happens before update 1
\parallel on 2
begin
    perform pg_sleep(1);
    update t_update set c1 = 2;
end;
/
begin
    update t_update set c1 = 2;
    perform pg_sleep(2);
end;
/
\parallel off

drop table t_update;

------------------------------
-- UHeapUpdate returning 4 (TM_Deleted)
------------------------------
create table t_update (c1 int) with (storage_type=USTORE);
insert into t_update values (1);

-- delete happens before update
\parallel on 2
begin
    perform pg_sleep(1);
    update t_update set c1 = 2;
end;
/
begin
    delete from t_update;
    perform pg_sleep(2);
end;
/
\parallel off

drop table t_update;

------------------------------
-- UHeapUpdate returning 5 (TM_BeingModified)
------------------------------
create table t_update (c1 int) with (storage_type=USTORE);
insert into t_update values (1);

-- select for update happens before update
\parallel on 2
begin
    perform pg_sleep(1);
    update t_update set c1 = 2;
end;
/
begin
    perform c1 from t_update for update;
    perform pg_sleep(2);
end;
/
\parallel off

drop table t_update;

------------------------------
-- UHeapUpdate returning 6 (TM_SelfCreated)
------------------------------

------------------------------
-- UHeapUpdate returning 7 (TM_SelfUpdated)
------------------------------
create table t(a int, b double precision) with (storage_type=USTORE);
create index ta on t(a);
create index tb on t(b);
insert into t values(generate_series(1, 1000), generate_series(1, 1000));

explain (costs off)
update t set a = a + 1 where a >= 1 and a <= 1000;
explain (costs off)
update t set b = b + 1.0 where b <= 1000;

set enable_seqscan to false;
set enable_bitmapscan to false;

explain (costs off)
update t set a = a + 1 where a >= 1 and a <= 1000;
explain (costs off)
update t set b = b + 1.0 where b <= 1000;

update t set a = a + 1 where a >= 1 and a <= 1000;
update t set b = b + 1.0 where b <= 1000;

select count(*) from t where a > 0 and a < 100;
select a from t where a >= 48 and a <= 52;
select b from t where b = 1.0;

drop index ta;
drop index tb;
drop table t;
set enable_seqscan to true;
set enable_bitmapscan to true;

