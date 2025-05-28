create schema functions_test;
set search_path = 'functions_test';

-- test @@rowcount
create table t1 (c1 int);
select @@rowcount;
insert into t1 values(generate_series(1,10));
select @@rowcount;
delete t1 where c1 in (1,3,5,7);
select @@rowcount;
update t1 set c1 = 12 where c1 in (2,4);
select @@rowcount;
select * from t1; 
select @@rowcount;
select count(*) from t1;
select @@rowcount;

do $$
begin
execute 'select * from t1';
RAISE NOTICE '@@rowcount: %', @@rowcount;
end $$;

set enable_set_variable_b_format to on;
select @@rowcount;
reset enable_set_variable_b_format;
select @@rowcount;

begin transaction;
declare c1 cursor for select * from t1;
select @@rowcount;
fetch next from c1;
select @@rowcount;
rollback transaction;

select abcd from t1; -- expect error 
select @@rowcount;

-- rowcount_big()
drop table t1;
select rowcount_big();
create table t1 (c1 int);
select rowcount_big();
insert into t1 values(generate_series(1,10));
select rowcount_big();
delete t1 where c1 in (1,3,5,7);
select rowcount_big();
update t1 set c1 = 12 where c1 in (2,4);
select rowcount_big();
select * from t1; 
select rowcount_big();
select count(*) from t1;
select rowcount_big();

set enable_set_variable_b_format to on;
select rowcount_big();
reset enable_set_variable_b_format;
select rowcount_big();

begin transaction;
declare c1 cursor for select * from t1;
select rowcount_big();
fetch next from c1;
select rowcount_big();
rollback transaction;

select abcd from t1; -- expect error 
select rowcount_big();

-- bypass usecases
set enable_seqscan to off;
set enable_bitmapscan to off;
create index i1 on t1(c1);

explain (costs off) select * from t1;
select @@rowcount;
select * from t1;
select @@rowcount;

explain (costs off) insert into t1 values(20);
insert into t1 values(generate_series(20,26));
select @@rowcount;

explain (costs off) delete from t1 where c1 < 10;
delete from t1 where c1 < 10;
select @@rowcount;

explain (costs off) update t1 set c1 = 30 where c1 > 21;
update t1 set c1 = 30 where c1 > 21;
select @@rowcount;

reset enable_seqscan;
reset enable_bitmapscan;

-- @@spid
select @@spid;

-- @@fetch_status
-- single cursor
begin transaction;
cursor c1 for select * from t1;

fetch next from c1;
select @@fetch_status;
fetch next from c1;
select @@fetch_status;
fetch last from c1;
select @@fetch_status;

fetch next from c2;	-- expect error
select @@fetch_status;
rollback transaction;

-- multi cursors
begin transaction;
cursor c1 for select * from t1;
cursor c2 for select * from t1;

fetch next from c1;
select @@fetch_status;
fetch next from c2;
select @@fetch_status;
fetch last from c1;
select @@fetch_status;
fetch next from c2;
select @@fetch_status;
rollback transaction;

-- pl/pgsql usecases
declare
rowcount int;
rowcount_big bigint;
spid bigint;
begin
spid := @@spid;
RAISE NOTICE '@@spid: %', spid;
execute 'select * from t1';
rowcount := @@rowcount;
RAISE NOTICE '@@rowcount: %', rowcount;
execute 'select * from t1';
rowcount_big := rowcount_big();
RAISE NOTICE '@@rowcount_big: %', rowcount_big;
end;
/

-- pl/tsql usecases
CREATE OR REPLACE FUNCTION test_pltsql RETURNS INT AS
$$
declare
rowcount int;
rowcount_big bigint;
spid bigint;
begin
spid := @@spid;
RAISE NOTICE '@@spid: %', spid;
execute 'select * from t1';
rowcount := @@rowcount;
RAISE NOTICE '@@rowcount: %', rowcount;
execute 'select * from t1';
rowcount_big := rowcount_big();
RAISE NOTICE '@@rowcount_big: %', rowcount_big;
return 0;
end;
$$
LANGUAGE 'pltsql';

select test_pltsql();
drop table t1;
drop function test_pltsql();

-- databasepropertyex
create database test_databasepropertyex dbcompatibility 'd';

select databasepropertyex('test_databasepropertyex', 'Collation');
select pg_typeof(databasepropertyex('test_databasepropertyex', 'Collation'));
select databasepropertyex('test_databasepropertyex', 'ComparisonStyle');
select databasepropertyex('test_databasepropertyex', 'Edition');
select databasepropertyex('test_databasepropertyex', 'IsAnsiNullDefault');
select databasepropertyex('test_databasepropertyex', 'IsAnsiNullsEnabled');
select databasepropertyex('test_databasepropertyex', 'IsAnsiPaddingEnabled');
select databasepropertyex('test_databasepropertyex', 'IsAnsiWarningsEnabled');
select databasepropertyex('test_databasepropertyex', 'IsArithmeticAbortEnabled');
select databasepropertyex('test_databasepropertyex', 'IsAutoClose');
select databasepropertyex('test_databasepropertyex', 'IsAutoCreateStatistics');
select databasepropertyex('test_databasepropertyex', 'IsAutoCreateStatisticsIncremental');
select databasepropertyex('test_databasepropertyex', 'IsAutoShrink');
select databasepropertyex('test_databasepropertyex', 'IsAutoUpdateStatistics');
select databasepropertyex('test_databasepropertyex', 'IsClone');
select databasepropertyex('test_databasepropertyex', 'IsCloseCursorsOnCommitEnabled');
select databasepropertyex('test_databasepropertyex', 'IsDatabaseSuspendedForSnapshotBackup');
select databasepropertyex('test_databasepropertyex', 'IsFulltextEnabled');
select databasepropertyex('test_databasepropertyex', 'IsInStandBy');
select databasepropertyex('test_databasepropertyex', 'IsLocalCursorsDefault');
select databasepropertyex('test_databasepropertyex', 'IsMemoryOptimizedElevateToSnapshotEnabled');
select databasepropertyex('test_databasepropertyex', 'IsMergePublished');
select databasepropertyex('test_databasepropertyex', 'IsNullConcat');
select databasepropertyex('test_databasepropertyex', 'IsNumericRoundAbortEnabled');
select databasepropertyex('test_databasepropertyex', 'IsParameterizationForced');
select databasepropertyex('test_databasepropertyex', 'IsQuotedIdentifiersEnabled');
select databasepropertyex('test_databasepropertyex', 'IsPublished');
select databasepropertyex('test_databasepropertyex', 'IsRecursiveTriggersEnabled');
select databasepropertyex('test_databasepropertyex', 'IsSubscribed');
select databasepropertyex('test_databasepropertyex', 'IsSyncWithBackup');
select databasepropertyex('test_databasepropertyex', 'IsTornPageDetectionEnabled');
select databasepropertyex('test_databasepropertyex', 'IsVerifiedClone');
select databasepropertyex('test_databasepropertyex', 'IsXTPSupported');
select databasepropertyex('test_databasepropertyex', 'LastGoodCheckDbTime');
select databasepropertyex('test_databasepropertyex', 'LCID');
select databasepropertyex('test_databasepropertyex', 'MaxSizeInBytes');
select databasepropertyex('test_databasepropertyex', 'Recovery');
select databasepropertyex('test_databasepropertyex', 'ServiceObjective');
select databasepropertyex('test_databasepropertyex', 'SQLSortOrder');
select databasepropertyex('test_databasepropertyex', 'Status');
select databasepropertyex('test_databasepropertyex', 'Updateability');
select databasepropertyex('test_databasepropertyex', 'UserAccess');
select databasepropertyex('test_databasepropertyex', 'Version');
select databasepropertyex('test_databasepropertyex', 'ReplicaID');
--- expected null
select databasepropertyex('test_databasepropertyex', 'Collation_fake');
select databasepropertyex('test_databasepropertyex_fake', 'Collation');
select databasepropertyex('null', 'Collation');
select databasepropertyex('test_databasepropertyex', 'null');
select databasepropertyex('null', 'null');

drop database test_databasepropertyex;

-- suser_name/suser_sname
SELECT suser_name() AS CurrentLoginName;
create user u1 identified by 'Test@123';
SET SESSION AUTHORIZATION u1 PASSWORD 'Test@123';
SELECT suser_name() AS CurrentLoginName;
RESET SESSION AUTHORIZATION;
create role u2 identified by 'Test@123';
SET SESSION AUTHORIZATION u2 PASSWORD 'Test@123';
SELECT suser_name() AS CurrentLoginName;
RESET SESSION AUTHORIZATION;
select suser_name(oid) from pg_authid where rolname in ('u1', 'u2');
drop user u1;
drop user u2;

SELECT suser_name(-111);
SELECT suser_name('-111');
SELECT suser_name('aaa');
SELECT suser_name(null);
SELECT suser_sname(-111);
SELECT suser_sname('-111');
SELECT suser_sname('aaa');
SELECT suser_sname(null);

-- scope_identity
--- This function returns the last identity value generated for any table in the current session and the current scope.
create schema test_scope_identity;
set search_path to test_scope_identity;

--- base test
create table t1 (c1 int identity(100, 1), c2 int);
insert into t1 values (1);
insert into t1 values (2);
insert into t1 values (3);
insert into t1 values (4);
insert into t1 values (5);
select scope_identity();
insert into t1 values (6);
select scope_identity();

create table t2 (c1 int, c2 int identity(200, 1));
insert into t2 values (1);
insert into t2 values (2);
select scope_identity();
insert into t2 values (3);
select scope_identity();

insert into t1 values (7);
select scope_identity();

insert into t2 values (4);
select scope_identity();

--- function test
create or replace function test_fn_scope_identity() returns int as $$
begin
    insert into t1 values (5);
    return scope_identity();
end; 
$$ language plpgsql;

select test_fn_scope_identity();
select scope_identity();

insert into t2 values (5);
select scope_identity();

--- procedure test
create or replace procedure test_proc_scope_identity() as
declare
    v1 int;
begin
    insert into t1 values (6);
    v1 := scope_identity();
    raise notice 'test_proc_scope_identity, scope_identity: %', v1;
end;
/

call test_proc_scope_identity();
select scope_identity();

insert into t2 values (6);
select scope_identity();

--- before trigger test
create table t3 (c1 int identity(300, 1), c2 int);

create or replace function test_fn_trg_scope_identity_before() returns trigger as $$
declare
    v1 int;
begin
    insert into t1 values (7);
    v1 := scope_identity();
    raise notice 'test_fn_trg_scope_identity_before, scope_identity: %', v1;
    return new;
end;
$$ language pltsql;

create trigger trg_scope_identity_before
    before insert on t3
    for each row execute procedure test_fn_trg_scope_identity_before();

insert into t3 values (1);
insert into t3 values (2);
select scope_identity();

insert into t3 values (3);
select scope_identity();

insert into t2 values (4);
select scope_identity();

insert into t3 values (4);
select scope_identity();

--- after trigger test
create table t4 (c1 int identity(400, 1), c2 int);
create or replace function test_fn_trg_scope_identity_after() returns trigger as $$
declare
    v1 int;
begin
    insert into t1 values (8);
    v1 := scope_identity();
    raise notice 'test_fn_trg_scope_identity_after, scope_identity: %', v1;
    return new;
end;
$$ language pltsql;

create trigger trg_scope_identity_after
    after insert on t4
    for each row execute procedure test_fn_trg_scope_identity_after();

insert into t4 values (1);
insert into t4 values (2);
select scope_identity();
insert into t4 values (3);
select scope_identity();

insert into t2 values (5);
select scope_identity();

insert into t4 values (4);
select scope_identity();

--- test plpgsql trigger
create table t5 (c1 int identity(500, 1), c2 int);
create or replace function test_fn_trg_scope_identity_plpgsql() returns trigger as $$
declare
    v1 int;
begin
    insert into t1 values (9);
    v1 := scope_identity();
    raise notice 'test_fn_trg_scope_identity_plpgsql, scope_identity: %', v1;
    return new;
end;
$$ language plpgsql;

create trigger trg_scope_identity_plpgsql
    before insert on t5
    for each row execute procedure test_fn_trg_scope_identity_plpgsql();

insert into t5 values (1);
insert into t5 values (2);
select scope_identity();
insert into t5 values (3);
select scope_identity();

insert into t2 values (6);
select scope_identity();

insert into t5 values (4);
select scope_identity();

drop table t1;
drop table t2;
drop table t3;
drop table t4; 
drop table t5;
drop function test_fn_scope_identity();
drop function test_proc_scope_identity();
drop function test_fn_trg_scope_identity_before();
drop function test_fn_trg_scope_identity_after();
drop function test_fn_trg_scope_identity_plpgsql();

reset search_path;
drop schema test_scope_identity cascade;

-- @@PROCID
--- test procedure
CREATE OR REPLACE PROCEDURE test_procid 
AS  
DECLARE
	ProcID integer; 
BEGIN
	ProcID = @@PROCID;
	RAISE INFO 'Stored procedure: %', ProcID;
END;
/

select test_procid();
SELECT oid FROM pg_proc WHERE proname = 'test_procid' AND prokind = 'p';

--- test function
CREATE OR REPLACE FUNCTION test_funcid() returns int as $$
begin
    return @@PROCID;
end; 
$$ language pltsql;

select test_funcid();
SELECT oid FROM pg_proc WHERE proname = 'test_funcid' AND prokind = 'f';

--- test trigger
drop table if exists t;
create table t (c1 int identity, c2 int);
create or replace function test_triggerid() returns trigger as $$
declare
    val int;
begin
    val := @@PROCID;
    raise notice 'Triggerid: %', val;
    return new;
end;
$$ language plpgsql;

create trigger trg_procid
    before insert on t
    for each row execute procedure test_triggerid();

insert into t values (1);
SELECT oid FROM pg_proc WHERE proname = 'test_triggerid' AND prokind = 'f';
drop table t;

--- test nested call
CREATE OR REPLACE PROCEDURE test_procid_inner
AS  
DECLARE
	ProcID integer; 
BEGIN
	ProcID = @@PROCID;  
	RAISE INFO 'Inner Stored procedure: %', ProcID;
END;
/

CREATE OR REPLACE PROCEDURE test_procid_outer
AS  
DECLARE
	OuterProcID integer;
	InnerProcID integer;
BEGIN
	OuterProcID = @@PROCID;  
	RAISE INFO 'Outer stored procedure: %', OuterProcID;
	
	PERFORM test_procid_inner();  
END;
/

call test_procid_outer();
SELECT oid FROM pg_proc WHERE proname = 'test_procid_outer' AND prokind = 'p';
SELECT oid FROM pg_proc WHERE proname = 'test_procid_inner' AND prokind = 'p';

--- test anonymous block
declare
    procid int;
begin
 	procid = @@PROCID;  
	RAISE INFO 'anonymous block: %', procid;
end;
/

-- test ident_current
create schema test_ident_current;
set search_path = 'test_ident_current';

select sys.ident_current(null);
select sys.ident_current('t1');

drop table if exists t1;
create table t1 (c1 int identity(100,1), c2 int);
select sys.ident_current('t1');
insert into t1(c2) values(10);
select * from t1;
select sys.ident_current('t1');
insert into t1(c2) values(11);
select * from t1;
select sys.ident_current('t1');

drop table if exists t2;
create table t2 (c1 int identity(200,1), c2 int);
select sys.ident_current('t2');
insert into t2(c2) values(20);
select * from t2;
select sys.ident_current('t2');
insert into t2(c2) values(21);
select * from t2;
select sys.ident_current('t2');

drop table t1;
drop table t2;

reset search_path;
drop schema test_ident_current;

drop schema functions_test cascade;
