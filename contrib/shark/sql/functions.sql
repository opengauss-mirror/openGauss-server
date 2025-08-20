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

create table "T_IDENTITY_0001"(id int identity, name varchar(10));
insert into "T_IDENTITY_0001"(name) values('zhangsan');
insert into "T_IDENTITY_0001"(name) values('lisi');
select * from "T_IDENTITY_0001";
select ident_current('"T_IDENTITY_0001"');
select ident_current("T_IDENTITY_0001");

create or replace view "V_IDENTITY_0001" as select * from "T_IDENTITY_0001";
select * from "V_IDENTITY_0001";
select ident_current('"V_IDENTITY_0001"');
select ident_current('"T_IDENTITY_0001"');

drop view "V_IDENTITY_0001";
drop table "T_IDENTITY_0001";

create table test_identity1(id int identity(100, 1), idd serial, name varchar(20));
insert into test_identity1(name) values('test1');
select * from test_identity1;
select ident_current('test_identity1');
drop table test_identity1;

-- ident_current only for identity, not for serial
create table test_identity2(idd serial, name varchar(20));
insert into test_identity2(name) values('test1');
select * from test_identity2;
select ident_current('test_identity2');
drop table test_identity2;

create table t_identity_0020(id int identity, name varchar(10));
insert into t_identity_0020(name) values('zhangsan');
select * from t_identity_0020;
select ident_current('t_identity_0020');

alter table t_identity_0020 drop column id;
select * from t_identity_0020;
select ident_current('t_identity_0020');

alter table t_identity_0020 add column id int identity;
select * from t_identity_0020;
select ident_current('t_identity_0020');
insert into t_identity_0020(name) values('name1');
select * from t_identity_0020;
select ident_current('t_identity_0020');

alter table t_identity_0020 drop column id;
select * from t_identity_0020;
select ident_current('t_identity_0020');

alter table t_identity_0020 add column id int identity(100, 5);
select * from t_identity_0020;
select ident_current('t_identity_0020');
insert into t_identity_0020(name) values('name5');
select * from t_identity_0020;
select ident_current('t_identity_0020');

drop table t_identity_0020;

create schema schema_ident_current_0001;
create table schema_ident_current_0001.t_identity_0001(id int identity(101,1), name varchar(10));
insert into schema_ident_current_0001.t_identity_0001(name) values('zhangsan');
select * from schema_ident_current_0001.t_identity_0001;
select ident_current('schema_ident_current_0001.t_identity_0001');
drop table schema_ident_current_0001.t_identity_0001;
drop schema schema_ident_current_0001 cascade;

create table t_identity_0033(c1 decimal identity(100,1), c2 varchar(100));
insert into t_identity_0033(c2) values ('a');
insert into t_identity_0033(c2) values ('b');
insert into t_identity_0033(c2) values ('c');
select * from t_identity_0033;
select ident_current('t_identity_0033');

create table t_identity_0033_01(c1 numeric identity(100,1), c2 varchar(100));
insert into t_identity_0033_01(c2) values ('a');
insert into t_identity_0033_01(c2) values ('b');
insert into t_identity_0033_01(c2) values ('c');
select * from t_identity_0033_01;
select ident_current('t_identity_0033_01');

create table t_identity_0033_02(c1 number identity(100,1), c2 varchar(100));
insert into t_identity_0033_02(c2) values ('a');
insert into t_identity_0033_02(c2) values ('b');
insert into t_identity_0033_02(c2) values ('c');
select * from t_identity_0033_02;
select ident_current('t_identity_0033_02');

drop table t_identity_0033;
drop table t_identity_0033_01;
drop table t_identity_0033_02;

drop table if exists t_ident_current0001_01;
drop table if exists t_ident_current0001_02;
create table t_ident_current0001_01(id int identity(100,1) primary key, name varchar(10));
CREATE TABLE t_ident_current0001_02 (log_id int identity PRIMARY KEY, log_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, table_name TEXT, last_identity BIGINT unique);
INSERT INTO t_ident_current0001_02 (table_name, last_identity) SELECT 't_ident_current0001_01', ident_current('t_ident_current0001_01');
select log_id, table_name, last_identity from t_ident_current0001_02;

drop table if exists t_ident_current0001_01;
drop table if exists t_ident_current0001_02;

create table t_ident_current0001_01(id int identity(100,1) primary key, name varchar(10));
CREATE TABLE t_ident_current0001_02 (log_id int identity PRIMARY KEY, log_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, table_name TEXT, last_identity BIGINT);
INSERT INTO t_ident_current0001_02 (table_name, last_identity) SELECT 't_ident_current0001_01', ident_current('t_ident_current0001_01');
select log_id, table_name, last_identity from t_ident_current0001_02;
INSERT INTO t_ident_current0001_02 (table_name, last_identity) SELECT 'hr', ident_current('t_ident_current0001_01');
select log_id, table_name, last_identity from t_ident_current0001_02;

drop table t_ident_current0001_01;
drop table t_ident_current0001_02;

CREATE TABLE Employees (EmployeeID INT IDENTITY(1,1) PRIMARY KEY, FirstName NVARCHAR(50), LastName NVARCHAR(50) );
CREATE VIEW v_Employees AS SELECT EmployeeID, FirstName, LastName FROM Employees;
INSERT INTO Employees (FirstName, LastName) VALUES ('John', 'Doe');
INSERT INTO Employees (FirstName, LastName) VALUES ('Jane', 'Smith');
select * from Employees;
select * from v_Employees;
SELECT IDENT_CURRENT('Employees');
SELECT IDENT_CURRENT('v_Employees');
drop table Employees cascade;

reset search_path;
drop schema test_ident_current;

drop schema functions_test cascade;

-- test datefuncs
--test dateadd
select dateadd(hh,1,timestamp'1997-12-31 23:59:59');
select dateadd(dd,1,timestamp'1997-12-31 23:59:59');
select dateadd(mm,1,timestamp'1997-12-31 23:59:59');
select dateadd(yy,1,timestamp'1997-12-31 23:59:59');
select dateadd(qq,1,timestamp'1997-12-31 23:59:59');
select dateadd(ww,1,timestamptz'1997-12-31 23:59:59');
select dateadd(mi,1,timestamptz'1997-12-31 23:59:59');
select dateadd(ss,1,timestamp'1997-12-31 23:59:59');
select dateadd(ms,1,timestamp'1997-12-31 23:59:59');
select dateadd(mcs,1,timestamp'1997-12-31 23:59:59');
select dateadd(mcs,999999,timestamp'1997-12-31 23:59:59.000002');
select dateadd(mm,1,date'1998-01-30');
select dateadd(dd,2,time'23:20:20');
select dateadd(dd,3,timetz '23:20:20');
select dateadd(dy,2,time'23:20:20');
select dateadd(dw,2,time'23:20:20');
select dateadd(yy,2,time'23:20:20');
select dateadd(mm,2,time'23:20:20');

select dateadd('',1,'2023/3/31');
select dateadd(2022,3,'2023/3/31 11:25:35.123456789');

drop table t;
create table t(id timestamp, id1 int,id2 int);
insert into t values(timestamp'1997-12-11',1,2);
select dateadd(dd,t.id1,id)from t;
select dateadd(mm,t.id1,id)from t;
select dateadd(dd,id1+id1,id)from t;
select dateadd(dd,id1+id1+id2,id)from t;
select dateadd(dd,id1+id1+(id2 -id2),id)from t;
select dateadd(dd,(id1+id1+(id2 -id2)),id)from t;
select dateadd(dd,id1,id)from t;
drop table t;

--test datepart
SELECT DATEPART(year,timestamp'2007-10-30 12:15:32.1234567');
SELECT DATEPART(quarter,timestamp'2007-10-30 12:15:32.1234567');
SELECT DATEPART(month,timestamptz'2007-10-30 12:15:32.1234567');
SELECT DATEPART(dayofyear,timestamptz'2007-10-30 12:15:32.1234567');
SELECT DATEPART(day,timestamp'2007-10-30 12:15:32.1234567');
SELECT DATEPART(week,timestamp'2007-10-30 12:15:32.1234567');
SELECT DATEPART(weekday,timestamp'2007-10-30 12:15:32.1234567');
SELECT DATEPART(hour,timestamp'2007-10-30 12:15:32.1234567');
SELECT DATEPART(minute,timestamp'2007-10-30 12:15:32.1234567');
SELECT DATEPART(second,timestamp'2007-10-30 12:15:32.1234567');
SELECT DATEPART(millisecond,timestamp'2007-10-30 12:15:32.1234567');
SELECT DATEPART(microsecond,timestamp'2007-10-30 12:15:32.1234567');
SELECT DATEPART(nanosecond,timestamp'2007-10-30 12:15:32.1234567');
SELECT DATEPART(iso_week,timestamp'2007-10-30 12:15:32.1234567');
SELECT DATEPART(ISOWW,timestamp'2007-10-30 12:15:32.1234567');
SELECT DATEPART(ISOWK,timestamp'2007-10-30 12:15:32.1234567');
SELECT DATEPART(year,date'2007-10-30');
SELECT DATEPART(year,time'23:20:20');
SELECT DATEPART(year,timetz'23:20:20');
select datepart('','2023/3/31');
select datepart(2022,'2023/3/31 11:25:35.123456789');

--test getdate
select getdate();
select datepart(second,getdate());
select datepart(millisecond,getdate());
select datepart(microsecond,getdate());
select dateadd(d,1,getdate());
select dateadd(day,1,getdate());
select dateadd(hours,1,getdate());
select dateadd("YEAR",1,getdate());
select datepart(HoUR,getdate());
select datepart(DAY,getdate());
select datepart("YEAR",getdate());

--test datename
SELECT DATENAME(year,timestamp'2007-10-30 12:15:32.1234567');
SELECT DATENAME(quarter,timestamp'2007-10-30 12:15:32.1234567');
SELECT DATENAME(mm,timestamp'2007-1-30 12:15:32.1234567');
SELECT DATENAME(m,timestamp'2007-2-28 12:15:32.1234567');
SELECT DATENAME(m,timestamp'2007-3-30 12:15:32.1234567');
SELECT DATENAME(m,timestamp'2007-4-30 12:15:32.1234567');
SELECT DATENAME(m,timestamp'2007-5-30 12:15:32.1234567');
SELECT DATENAME(m,timestamp'2007-6-30 12:15:32.1234567');
SELECT DATENAME(m,timestamp'2007-7-30 12:15:32.1234567');
SELECT DATENAME(m,timestamp'2007-8-30 12:15:32.1234567');
SELECT DATENAME(m,timestamp'2007-9-30 12:15:32.1234567');
SELECT DATENAME(month,timestamp'2007-10-30 12:15:32.1234567');
SELECT DATENAME(m,timestamp'2007-11-30 12:15:32.1234567');
SELECT DATENAME(m,timestamp'2007-12-30 12:15:32.1234567');
SELECT DATENAME(dayofyear,timestamp'2007-10-30 12:15:32.1234567');
SELECT DATENAME(day,timestamp'2007-10-30 12:15:32.1234567');
SELECT DATENAME(week,timestamp'2007-10-30 12:15:32.1234567');
SELECT DATENAME(weekday,timestamp'2007-10-30 12:15:32.1234567');
SELECT DATENAME(dw,timestamp'2007-10-29 12:15:32.1234567');
SELECT DATENAME(w,timestamp'2007-10-28 12:15:32.1234567');
SELECT DATENAME(w,timestamp'2007-10-27 12:15:32.1234567');
SELECT DATENAME(w,timestamp'2007-10-26 12:15:32.1234567');
SELECT DATENAME(w,timestamp'2007-10-25 12:15:32.1234567');
SELECT DATENAME(w,timestamp'2007-10-24 12:15:32.1234567');
SELECT DATENAME(hour,timestamp'2007-10-30 12:15:32.1234567');
SELECT DATENAME(minute,timestamp'2007-10-30 12:15:32.1234567');
SELECT DATENAME(second,timestamp'2007-10-30 12:15:32.1234567');
SELECT DATENAME(millisecond,timestamp'2007-10-30 12:15:32.1234567');
SELECT DATENAME(microsecond,timestamp'2007-10-30 12:15:32.1234567');
SELECT DATENAME(nanosecond,timestamp'2007-10-30 12:15:32.1234567');
SELECT DATENAME(TZoffset,'2007-10-30 12:15:32.1234567 -05:10');
SELECT DATENAME(ISO_WEEK,timestamp'2007-10-30 12:15:32.1234567');
SELECT DATENAME(ISOWW,timestamp'2007-10-30 12:15:32.1234567');
SELECT DATENAME(ISOWK,timestamp'2007-10-30 12:15:32.1234567');
SELECT DATENAME(TZoffset,timestamp'2007-10-30 12:15:32.1234567');
SELECT DATENAME(year, time'12:10:30.123');
SELECT DATENAME(month, time'12:10:30.123');
SELECT DATENAME(day, time'12:10:30.123');
SELECT DATENAME(dayofyear, time'12:10:30.123');
SELECT DATENAME(weekday, time'12:10:30.123');
SELECT DATENAME(weekday, timetz'12:10:30.123');
SELECT DATENAME(weekday, date'2007-10-30');

create view testv as select DATENAME(weekday, date'2007-10-30');
\d+ testv;
select * from testv;
SELECT datename('weekday'::cstring, '10-30-2007'::date) AS datename;
drop view testv;

--test len
SELECT LEN(N'123');
SELECT LEN(N'123   ');
SELECT LEN(N'   123   ');
SELECT LEN(CAST('123' as char(25)));
SELECT LEN('abc');
SELECT LEN('12345678901234567890123456789012345'::varchar);
select len('aa'::varbinary);
