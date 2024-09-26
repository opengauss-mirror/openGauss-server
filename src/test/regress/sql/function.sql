create database pl_test_funcion DBCOMPATIBILITY 'pg';
\c pl_test_funcion;
create schema distribute_function;
set current_schema = distribute_function;

create table function_table_01(f1 int, f2 float, f3 text);
insert into function_table_01 values(1,2.0,'abcde'),(2,4.0,'abcde'),(3,5.0,'affde');
insert into function_table_01 values(4,7.0,'aeede'),(5,1.0,'facde'),(6,3.0,'affde');
analyze function_table_01;

CREATE OR REPLACE FUNCTION test_function_immutable RETURNS BIGINT AS
$body$ 
BEGIN
RETURN 3;
END;
$body$
LANGUAGE 'plpgsql'
IMMUTABLE
CALLED ON NULL INPUT
SECURITY INVOKER
COST 100;

explain (verbose, costs off) select * from test_function_immutable();
select * from test_function_immutable();
CREATE VIEW functionview AS SELECT f1,f2,left(f3,test_function_immutable()::INT) f3 FROM function_table_01;

--targetlist
explain (verbose, costs off) select f1,left(f3,test_function_immutable()::INT) from function_table_01 order by 1 limit 3;
select f1,left(f3,test_function_immutable()::INT) from function_table_01 order by 1 limit 3;

--fromQual
explain (verbose, costs off) select * from function_table_01 where f1 = test_function_immutable();
select * from function_table_01 where f1 = test_function_immutable();

--sortClause
explain (verbose, costs off) select f1,f3 from function_table_01 order by left(f3,test_function_immutable()::INT) limit 3;
select f1,f3 from function_table_01 order by left(f3,test_function_immutable()::INT), f1 limit 3;

--groupClause
explain (verbose, costs off) select avg(f2),left(f3,test_function_immutable()::INT) from function_table_01 group by 2 order by 1;
select avg(f2),left(f3,test_function_immutable()::INT) from function_table_01 group by 2 order by 1;

--havingClause
explain (verbose, costs off) select avg(f2) fa,f3 from function_table_01 group by f3 having avg(f2)>test_function_immutable()  order by 1;
select avg(f2) fa,f3 from function_table_01 group by f3 having avg(f2)>test_function_immutable() order by 1;

--limitClause && offsetClause
explain (verbose, costs off) select * from function_table_01 order by 1 limit test_function_immutable() offset test_function_immutable();
select * from function_table_01 order by 1  limit test_function_immutable() offset test_function_immutable();

explain (verbose, costs off) select avg(f2),left(f3,test_function_immutable()::INT) from function_table_01 group by 2 having avg(f2)>test_function_immutable() order by 1 limit test_function_immutable() offset test_function_immutable()-2;
select avg(f2),left(f3,test_function_immutable()::INT) from function_table_01 group by 2 having avg(f2)>test_function_immutable() order by 1 limit test_function_immutable() offset test_function_immutable()-2;

CREATE OR REPLACE FUNCTION test_function_volatile RETURNS BIGINT AS
$body$ 
DECLARE
cnt BIGINT;
cnt2 BIGINT;
BEGIN
SELECT count(*) INTO cnt FROM(select * from functionview limit 3);
SELECT count(*) INTO cnt2 from function_table_01;
RETURN (cnt+cnt2/2)/2;
END;
$body$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER
COST 100;

explain (verbose on, costs off) select * from test_function_volatile();
select * from test_function_volatile();

explain (verbose on, costs off) select test_function_volatile();
select test_function_volatile();

--targetlist
select f1,left(f3,test_function_volatile()::INT) from function_table_01 order by 1 limit 3;

--fromQual
select * from function_table_01 where f1 = test_function_volatile();

--sortClause
select f1,f3 from function_table_01 order by left(f3,test_function_volatile()::INT) limit 3;

--groupClause
select avg(f2),left(f3,test_function_volatile()::INT) from function_table_01 group by 2 order by 1;

--havingClause
select avg(f2) fa,f3 from function_table_01 group by f3 having avg(f2)>test_function_volatile() order by 1;

--limitClause && offsetClause
select * from function_table_01 order by 1  limit test_function_volatile() offset test_function_volatile();

select avg(f2),left(f3,test_function_volatile()::INT) from function_table_01 group by 2 having avg(f2)>test_function_volatile() order by 1 limit test_function_volatile() offset test_function_volatile()-2;

drop function test_function_volatile;
drop view functionview;
drop function test_function_immutable;
drop table function_table_01;
drop schema distribute_function;

reset current_schema;
create table distribute_function_t1(a int, b int) /*distribute by hash(a)*/;
create table distribute_function_t2(a int, b int) /*distribute by replication*/;
explain (verbose on, costs off) select * from distribute_function_t1, generate_series(1, 1, 5);
explain (verbose on, costs off) select * from distribute_function_t1, generate_series(1, 1, 5);
drop table distribute_function_t1;
drop table distribute_function_t2;


--object inside procedure be searched and new table/view/etc be created in. This is to adapt behaviar of Ora.

create schema dist_func1;
create schema dist_func2;

create or replace function func_create_columnar_partition_table_176_xkey_xpart(part integer) returns boolean as $$ declare
sql_temp text;
begin
sql_temp := 'create table create_columnar_table_176_xkey_'||part||'part ( c_smallint smallint not null,c_double_precision double precision,c_time_without_time_zone time without time zone null,c_time_with_time_zone time with time zone,c_integer integer default 23423,c_bigint bigint default 923423432,c_decimal decimal(19) default 923423423,c_real real,c_numeric numeric(18,12) null,c_varchar varchar(19),c_char char(57) null,c_timestamp_with_timezone timestamp with time zone,c_char2 char default ''0'',c_text text null,c_varchar2 varchar2(20),c_timestamp_without_timezone timestamp without time zone,c_date date,c_varchar22 varchar2(11621),c_numeric2 numeric null)  /*distribute by hash(c_decimal)*/;';
    execute immediate sql_temp;
     return true;
end;
$$ language plpgsql;

create or replace function dist_func1.func_create_columnar_partition_table_176_xkey_xpart(part integer) returns boolean as $$ declare
sql_temp text;
begin
sql_temp := 'create table create_columnar_table_176_xkey_'||part||'part ( c_smallint smallint not null,c_double_precision double precision,c_time_without_time_zone time without time zone null,c_time_with_time_zone time with time zone,c_integer integer default 23423,c_bigint bigint default 923423432,c_decimal decimal(19) default 923423423,c_real real,c_numeric numeric(18,12) null,c_varchar varchar(19),c_char char(57) null,c_timestamp_with_timezone timestamp with time zone,c_char2 char default ''0'',c_text text null,c_varchar2 varchar2(20),c_timestamp_without_timezone timestamp without time zone,c_date date,c_varchar22 varchar2(11621),c_numeric2 numeric null)  /*distribute by hash(c_decimal);*/';
    execute immediate sql_temp;
     return true;
end;
$$ language plpgsql;


call dist_func1.func_create_columnar_partition_table_176_xkey_xpart(1);


set search_path to dist_func1;
call public.func_create_columnar_partition_table_176_xkey_xpart(2);
call func_create_columnar_partition_table_176_xkey_xpart(3);

set search_path to dist_func3;
call public.func_create_columnar_partition_table_176_xkey_xpart(4);
call dist_func1.func_create_columnar_partition_table_176_xkey_xpart(5);

create or replace function dist_func1.test1(part int) returns boolean as $$ declare
sql_temp text;
begin
	sql_temp := 'set search_path to dist_func2;';
	execute immediate sql_temp;
	sql_temp := 'create table create_columnar_table_176_xkey_'||part||'part ( c_smallint smallint not null,c_double_precision double precision,c_time_without_time_zone time without time zone null,c_time_with_time_zone time with time zone,c_integer integer default 23423,c_bigint bigint default 923423432,c_decimal decimal(19) default 923423423,c_real real,c_numeric numeric(18,12) null,c_varchar varchar(19),c_char char(57) null,c_timestamp_with_timezone timestamp with time zone,c_char2 char default ''0'',c_text text null,c_varchar2 varchar2(20),c_timestamp_without_timezone timestamp without time zone,c_date date,c_varchar22 varchar2(11621),c_numeric2 numeric null)  /*distribute by hash(c_decimal)*/;';
	execute immediate sql_temp;
	return true;
end;
$$ language plpgsql;

call dist_func1.test1(6);

select schemaname, tablename from pg_tables where tablename like 'create_columnar_table_176_xkey_%' order by tablename;


create or replace function dist_func1.multi_call1(a integer) returns int as $$
declare
b int;
begin
	select multi_call2(a) + 1 into b;
	return b;
end;
$$ language plpgsql;

create or replace function dist_func1.multi_call2(a integer) returns int as $$
declare
b int;
begin
	select dist_func2.multi_call3(a) + 1 into b;
	return b;
end;
$$ language plpgsql;

create or replace function dist_func2.multi_call3(a integer) returns int as $$
declare
b int;
begin
	select multi_call4() + 1 into b;
	return b;
end;
$$ language plpgsql;

create or replace function dist_func2.multi_call4() returns int as $$
declare
b int;
begin
	return 1;
end;
$$ language plpgsql;

call dist_func1.multi_call1(1);

-- Test cross-schema

-- search_path public, procedure sche1, row table distribute by hash
create schema sche1;

set search_path=sche1;
create or replace function fun_001() returns void as $$
declare
begin
	create table schema_tbl_001(a int) ;
	insert into schema_tbl_001 values(1);
end;
$$ LANGUAGE plpgsql;

set search_path=public;
select sche1.fun_001();
select * from sche1.schema_tbl_001;
select count(*) from pg_class where relname='schema_tbl_001';

drop schema sche1 cascade;

-- search_path sche1, procedure public, row unlogged table distribute by hash
create schema sche1;

set search_path=public;
create or replace function fun_001() returns void as $$
declare
begin
	create unlogged table schema_tbl_001(a int) ;
	insert into schema_tbl_001 values(1);
end;
$$ LANGUAGE plpgsql;

set search_path=sche1;
select public.fun_001();
select * from schema_tbl_001;
select * from public.schema_tbl_001;

drop function public.fun_001;
drop schema sche1 cascade;
drop table public.schema_tbl_001;
reset search_path;

-- search_path sche1, procedure sche2, row partition table
create schema sche1;
create schema sche2;

set search_path=sche2;
create or replace function fun_001() returns void as $$
declare
begin
	create table schema_tbl_001(a int) partition by range(a) (partition p1 values less than(10), partition p2 values less than(20), partition p3 values less than(maxvalue));
	insert into schema_tbl_001 values(1),(10),(20),(100);
end;
$$ LANGUAGE plpgsql;

set search_path=sche1;
select sche2.fun_001();
select * from sche2.schema_tbl_001 order by a;

drop schema sche1 cascade;
drop schema sche2 cascade;
reset search_path;

-- search_path sche1, procedure sche2, table sche3, row table distribute by replication
create schema sche1;
create schema sche2;
create schema sche3;

set search_path=sche2;
create or replace function fun_001() returns void as $$
declare
begin
	create table sche3.schema_tbl_001(a int) /*distribute by replication*/;
	insert into sche3.schema_tbl_001 values(1);
end;
$$ LANGUAGE plpgsql;

set search_path=sche1;
select sche2.fun_001();
select * from sche2.schema_tbl_001;
select * from sche3.schema_tbl_001;

drop schema sche1 cascade;
drop schema sche2 cascade;
drop schema sche3 cascade;
reset search_path;

-- set search_path/current_schema inside procedure is invalid
-- test set search_path
create schema sche1;
create schema sche2;

set search_path=sche2;
create or replace function fun_001() returns void as $$
declare
begin
	set search_path=sche1;
end;
$$ LANGUAGE plpgsql;

reset search_path;
show search_path;

select sche2.fun_001();
show search_path;

-- test set current_schema
set search_path=sche2;
create or replace function fun_002() returns void as $$
declare
begin
	set current_schema=sche1;
end;
$$ LANGUAGE plpgsql;

reset current_schema;
show current_schema;

select sche2.fun_002();
show current_schema;

-- test reset search_path
set search_path=sche2;
create or replace function fun_003() returns void as $$
declare
begin
	reset search_path;
end;
$$ LANGUAGE plpgsql;

show current_schema;
select sche2.fun_003();
show search_path;

-- test set search_path mixed with other operations, column table distribute by hash
set search_path=sche2;
create or replace function fun_004() returns void as $$
declare
begin
	set search_path=public;
	create table schema_tbl_001(a int) with (orientation = column);
	insert into schema_tbl_001 values(1);
end;
$$ LANGUAGE plpgsql;

set search_path=sche1;
select sche2.fun_004();
show search_path;
select * from public.schema_tbl_001;
select * from sche2.schema_tbl_001;

drop schema sche1 cascade;
drop schema sche2 cascade;
reset search_path;

-- test multiple schemas with DDL, DML.
create schema sche1;
create schema sche2;

create or replace function sche1.multi_call1(a integer) returns int as $$
declare
b int;
begin
	select multi_call2(a) + 1 into b;
	insert into sche1_tbl values(1);
	return b;
end;
$$ language plpgsql;

create or replace function sche1.multi_call2(a integer) returns int as $$
declare
b int;
begin
	create table sche1_tbl(id int) with (orientation = column) /*distribute by replication*/;
	select sche2.multi_call3(a) + 1 into b;
	insert into sche2.sche2_tbl values(3);
	return b;
end;
$$ language plpgsql;

create or replace function sche2.multi_call3(a integer) returns int as $$
declare
b int;
begin
	select multi_call4() + 1 into b;
	create table sche2_tbl(id int) /*distribute by hash(id)*/;
	insert into sche2_tbl values(2);
	return b;
end;
$$ language plpgsql;

create or replace function sche2.multi_call4() returns int as $$
declare
b int;
begin
	return 1;
end;
$$ language plpgsql;

select sche1.multi_call1(1);

select * from sche1.sche1_tbl order by id;
select * from sche2.sche2_tbl order by id;

drop schema sche1 cascade;
drop schema sche2 cascade;

-- test temp table in subtransaction
create or replace function fun_002()
returns int
AS $$
DECLARE
x integer;
BEGIN
        create temp table fun_tmp(a int) /*distribute by hash(a)*/;
        perform 1 / 0;
        insert into fun_tmp values(2);
        return 2;
END; $$
LANGUAGE plpgsql;

create or replace function fun_003()
returns int
AS $$
DECLARE
x integer;
BEGIN
        create temp table fun_tmp(a int) /*distribute by hash(a)*/;
        insert into fun_tmp values(3);
        return 3;
END; $$
LANGUAGE plpgsql;

create or replace function fun_001()
returns void
AS $$
DECLARE
x integer;
BEGIN
        BEGIN
                select fun_002() into x;
                insert into fun_tmp values(4);
                exception when others then
        END;
        BEGIN
                select fun_003() into x;
                insert into fun_tmp values(5);
        END;
        insert into fun_tmp values(6);
END; $$
LANGUAGE plpgsql;

call public.fun_001();
select * from fun_tmp order by 1;

drop function fun_001;
drop function fun_002;
drop function fun_003;
drop table fun_tmp;

-- test temp table with exception
create schema sche1;
set search_path = sche1;
create or replace procedure proc_001
as
begin
	create table tt1(a int) /*distribute by hash(a)*/;
	begin
		create temp table tablenamelevel (a int, b int) /*distribute by hash(a)*/;
		select substring('abc', 1, -5);
		exception when others then RAISE NOTICE 'others';
	end;
	begin
		create temp table tablenamelevel2 (a int, b int) /*distribute by hash(a)*/;
		begin
			create temp table tablenamelevel3 (a int, b int) /*distribute by hash(a)*/;
			select substring('abc', 1, -5);
			exception when others then RAISE NOTICE 'others';
			insert into tablenamelevel2 values(2,3);
		end;
		exception when others then RAISE NOTICE 'others';
	end;
	begin
		create temp table tablenamelevel4 (a int, b int) /*distribute by hash(a)*/;
		select substring('abc', 1, -5);
		exception when others then RAISE NOTICE 'others'; 
	end; 
	insert into tt1 values(1);
	exception when others then RAISE NOTICE 'others';
end;
/

reset search_path;
prepare test_cacheplan_pbe as select substring('abc', 1, -5);
start transaction;
savepoint sp1;
create temp table temptable1(a int) /*distribute by hash(a)*/;
rollback to savepoint sp1;
create temp table temptable1(a int) /*distribute by hash(a)*/;
select substring('abc', 1, -5);
rollback to savepoint sp1;
create temp table temptable1(a int) /*distribute by hash(a)*/;
execute test_cacheplan_pbe;
rollback to savepoint sp1;
select sche1.proc_001();
end;

select * from sche1.tt1;
select * from tablenamelevel1;
select * from tablenamelevel2;
select * from tablenamelevel3;
select * from tablenamelevel4;

deallocate test_cacheplan_pbe;
drop table tablenamelevel2;
drop table sche1.tt1;
drop schema sche1 cascade;
reset search_path;

-- test the table with the same name with a pg_catalog table 
create schema sche1;
create table sche1.pg_class(id int);

set search_path=sche1;
insert into pg_class values(1);
select * from sche1.pg_class;

-- invalid to put pg_catalog behind sche1
set search_path=sche1, pg_catalog;
insert into pg_class values(1);
select * from sche1.pg_class;

insert into sche1.pg_class values(1);
select * from sche1.pg_class;

delete from sche1.pg_class;
set search_path=sche1;
create or replace function fun_001() returns void as $$
declare
begin
	insert into pg_class values(1);
end;
$$ LANGUAGE plpgsql;

select sche1.fun_001();
select * from sche1.pg_class;

set search_path=sche1, pg_catalog;
select sche1.fun_001();
select * from sche1.pg_class;

create or replace function sche1.fun_002() returns void as $$
declare
begin
	insert into sche1.pg_class values(1);
end;
$$ LANGUAGE plpgsql;

select sche1.fun_002();
select * from sche1.pg_class;

drop schema sche1 cascade;

-- test the table with the same name with a temp table
create schema sche1;
create schema sche2;

set search_path=sche1;
create or replace function fun_002() returns void as $$
declare
begin
	create table schema_tbl_001(a int) with(orientation=column) partition by range(a) (partition p1 values less than(10),partition p2 values less than(20), partition p3 values less than(maxvalue));
	insert into schema_tbl_001 values(10),(20),(100);
end;
$$ LANGUAGE plpgsql;

set search_path=public;
create or replace function sche1.fun_001() returns void as $$
declare
begin
	create temp table schema_tbl_001(a int) with(orientation=column) /*distribute by replication*/;
	insert into schema_tbl_001 values(4);
end;
$$ LANGUAGE plpgsql;

set search_path=sche2;
select sche1.fun_001();
select sche1.fun_002();

-- always insert into temp table.
select * from sche1.schema_tbl_001;

drop schema sche1 cascade;
drop schema sche2 cascade;
drop table schema_tbl_001;
reset search_path;

-- test create table ... as select
create schema sche1;
set search_path=sche1;
create table sche1.schema_tbl_001(a int);
insert into sche1.schema_tbl_001 values(1),(2);
create or replace function fun_002() returns void as $$
declare
begin
	create table t2 as select * from schema_tbl_001;
	insert into t2 values(3);
end;
$$ LANGUAGE plpgsql;

set search_path=public;
select sche1.fun_002();

select * from sche1.t2 order by 1;

drop schema sche1 cascade;
reset search_path;

-- test insert into select
create schema sche1;
set search_path=sche1;
create table sche1.schema_tbl_001(a int);
insert into sche1.schema_tbl_001 values(1),(2);
create or replace function fun_002() returns void as $$
declare
begin
	create table t2(a int);
	insert into t2 select a from schema_tbl_001;
end;
$$ LANGUAGE plpgsql;

set search_path=public;
select sche1.fun_002();

select * from sche1.t2 order by 1;

drop schema sche1 cascade;
reset search_path;

-- test creating schema inside procedure
create schema sche1;

create or replace function sche1.fun1() returns void as $$
declare
begin
	create table t1(a int);
	create schema sche2 create table t2(a int);
	insert into t1 values(1);
end;
$$ LANGUAGE plpgsql;

set search_path = public;
select sche1.fun1();

select * from sche1.t1;
select * from sche2.t2;

drop schema sche1 cascade;
drop schema sche2 cascade;
reset search_path;

-- test with grammar
create schema sche1;
create schema sche2;
create table sche1.t1(a int) /*distribute by replication*/;

set search_path = sche2;
create or replace function sche1.fun_001()
returns setof sche1.t1
AS $$
DECLARE
        row_data t1%rowtype;
        row_name record;
        query_str text;
        BEGIN
                query_str := 'WITH t AS (
				INSERT INTO t1 VALUES (11),(12),(13) RETURNING *
			)
			SELECT * FROM t order by 1;';
                FOR row_data IN EXECUTE(query_str) LOOP
                      return next row_data;
                END LOOP;
                return;
        END; $$
LANGUAGE 'plpgsql' NOT FENCED;

select sche1.fun_001();
select * from sche1.t1;

drop schema sche1 cascade;
drop schema sche2 cascade;
reset search_path;

-- test cursor
create schema sche1;
set search_path = sche1;
create table schema_tbl_001(a int) /*distribute by replication*/;
insert into schema_tbl_001 values(1),(2);

CREATE OR REPLACE PROCEDURE fun_001()
AS
DECLARE
	b int;
	CURSOR C1 IS
	SELECT a FROM schema_tbl_001 order by 1;
BEGIN
	OPEN C1;
	LOOP
		FETCH C1 INTO b;
		EXIT WHEN C1%NOTFOUND;
	END LOOP;
	CLOSE C1;
END;
/

reset search_path;
select sche1.fun_001();

drop schema sche1 cascade;
reset search_path;

-- test current_schemas()
set search_path to pg_catalog,public;
select current_schemas('f');

set search_path to public,pg_catalog;
select current_schemas('f');

-- test SUPPORT_BIND_SEARCHPATH
SET behavior_compat_options = 'bind_procedure_searchpath';
create schema sche1;
create schema sche2;

set search_path = public;
create table public.bind_t1(a int) /*distribute by hash(a)*/;
create or replace function sche2.fun_001() returns int as $$
declare
begin
        insert into bind_t1 values(2);
        return 2;
end;
$$ LANGUAGE plpgsql;

create or replace function sche1.fun_001() returns int as $$
declare
x int;
begin
        insert into bind_t1 values(1);
	select sche2.fun_001() into x;
        return 1;
end;
$$ LANGUAGE plpgsql;

-- insert into public.bind_t1;
call sche2.fun_001();
select * from public.bind_t1;
delete from public.bind_t1;

-- insert into public.bind_t1;
call sche1.fun_001();
select * from public.bind_t1;
delete from public.bind_t1;

-- cannot find bind_t1. insert fail.
set search_path = sche2;
call sche2.fun_001();

-- insert into sche1.bind_t1;
create table sche1.bind_t1(a int) /*distribute by hash(a)*/;
set search_path = sche1;
call sche2.fun_001();
select * from public.bind_t1;
select * from sche1.bind_t1;

delete from public.bind_t1;
delete from sche1.bind_t1;

-- insert into sche1.bind_t1;
call sche1.fun_001();
select * from public.bind_t1;
select * from sche1.bind_t1;

delete from public.bind_t1;
delete from sche1.bind_t1;

-- cannot insert into sche2.bind_t1.
create table sche2.bind_t1(a int) /*distribute by hash(a)*/;
call sche2.fun_001();
select * from public.bind_t1;
select * from sche1.bind_t1;
select * from sche2.bind_t1;

-- insert into sche2.bind_t1.
set search_path = sche2;
call sche2.fun_001();
select * from public.bind_t1;
select * from sche1.bind_t1;
select * from sche2.bind_t1;

delete from public.bind_t1;
delete from sche1.bind_t1;
delete from sche2.bind_t1;

-- insert into sche1.bind_t1 and sche2.bind_t1.
call sche1.fun_001();
select * from public.bind_t1;
select * from sche1.bind_t1;
select * from sche2.bind_t1;

reset search_path;
drop table public.bind_t1;
drop table sche1.bind_t1;
drop table sche2.bind_t1;
drop schema sche1 cascade;
drop schema sche2 cascade;

-- test DDL under SUPPORT_BIND_SEARCHPATH
create schema sche1;
create table public.sche_t1(a int, b int);
insert into public.sche_t1 values(1, 11);
create or replace procedure sche1.fun_001() as
begin
  insert into sche_t1 values(2, 12);
  truncate table sche_t1;
  insert into sche_t1 values(3, 13);
end;
/
call sche1.fun_001();
select * from public.sche_t1;

create or replace procedure sche1.fun_001() as
begin
  alter table sche_t1 add column c char(10);
  insert into sche_t1 values(4, 14, 'hello');
end;
/
call sche1.fun_001();
select * from public.sche_t1 order by 1,2,3;

drop table public.sche_t1;
drop schema sche1 cascade;

reset behavior_compat_options;

-- test multiple CREATE TABLE AS.
-- avoid cacheplan CREATE TABLE AS statement as INSERT.
create schema sche1;
create table sche1.sche_t1(a int,b varchar);
insert into sche1.sche_t1 values(1,1);
create or replace procedure sche1.subpro() as
begin
    create table test as select * from sche_t1;
    insert into test values(2,2);
end;
/

create or replace procedure sche1.pro() as
begin
    subpro();
    subpro();
end;
/
call sche1.pro();

drop schema sche1 cascade;

-- clear option
reset search_path;

CREATE OR REPLACE FUNCTION func_increment_sql_1(i int, out result_1 bigint, out result_2 bigint)
returns SETOF RECORD
as $$
begin
    result_1 = i + 1;
    result_2 = i * 10;
    raise notice '%', result_1;
    raise notice '%', result_2;
return next;
end;
$$language plpgsql;
SELECT func_increment_sql_1(1);

CREATE OR REPLACE FUNCTION func_increment_sql_2(i int, inout result_1 bigint, out result_2 bigint)
returns SETOF RECORD
as $$
begin
    raise notice '%', result_1;
    result_1 = i + 1;
    result_2 = i * 10;
    raise notice '%', result_1;
    raise notice '%', result_2;
return next;
end;
$$language plpgsql;
--error
SELECT func_increment_sql_2(1);
--success
SELECT func_increment_sql_2(1,2);

CREATE OR REPLACE FUNCTION fun_test_1(i int)
RETURNS void 
as $$
begin
   PERFORM func_increment_sql_1(i);
end;
$$language plpgsql;
select fun_test_1(1);

--error
CREATE OR REPLACE FUNCTION fun_test_2(i int)
RETURNS void 
as $$
begin
   PERFORM func_increment_sql_2(i);
end;
$$language plpgsql;

CREATE OR REPLACE FUNCTION fun_test_2(i int)
RETURNS void 
as $$
begin
   PERFORM func_increment_sql_2(i,i+1);
end;
$$language plpgsql;
select fun_test_2(1);

CREATE FUNCTION w_testfun3 (c_int INOUT int DEFAULT  1)  RETURNS int  AS $$
        BEGIN
            RETURN (c_int);
        END;
$$ LANGUAGE plpgsql;
DROP FUNCTION func_increment_sql_1;
DROP FUNCTION func_increment_sql_2;
DROP FUNCTION fun_test_1;
DROP FUNCTION fun_test_2;

set behavior_compat_options='select_into_return_null';
create table test_table_030(id int, name text);
insert into test_table_030 values(1,'a'),(2,'b');
CREATE OR REPLACE FUNCTION select_into_null_func(canshu varchar(16))
returns int
as $$
DECLARE test_table_030a int;
begin
    SELECT ID into test_table_030a FROM test_table_030 WHERE NAME = canshu;
    return test_table_030a;
end; $$ language plpgsql;
select select_into_null_func('aaa');
select select_into_null_func('aaa') is null;

drop table test_table_030;
drop function select_into_null_func;
reset behavior_compat_options;
\c regression;
drop database IF EXISTS pl_test_funcion;

-- test create internal functions
create function fn_void(cstring) returns void language internal as 'int8in';
select fn_void(null);
select fn_void('');
select fn_void('1234');
select fn_void(1234); --  should error, no function matches
drop function fn_void(cstring);

create function fn_n(cstring) returns int language internal as 'int4in';
select fn_n(null);
select fn_n('');
select fn_n('666');
select fn_n(666); --  should error, no function matches
select fn_n('666666666666666666666666666666666666666666666666666'); -- out of range
select fn_n('fhdsfhdj'); -- invalid input
drop function fn_n(cstring);

create function fn_n2(int8) returns cstring language internal as 'int8out';
select fn_n2(0);
select fn_n2(1::int2);
select fn_n2(1234);
select fn_n2(-1234::int4);
select fn_n2('1234');
drop function fn_n2(int8);

create function fn_n3(int8, cstring) returns cstring language internal as 'int8out'; -- exeeded param is ok
select fn_n3(null, null);
select fn_n3('', '');
select fn_n3(123456789, null);
select fn_n3(12345, ''); -- empty string will be treated as null
select fn_n3(12345, 'rtrgfgf');
drop function fn_n3(int8, cstring);

create function fn_txt(text) returns cstring as 'textout' language internal;
select fn_txt(null);
select fn_txt('');
select fn_txt('084329sdjhfdsffdjf');
select fn_txt('sfiewr0239408497867^&*%&^$%^$*&()&*(&084329sdjhfdsffdjf');
drop function fn_txt(text);

create function fn_txt2(text, ot out int) returns cstring as 'textout' language internal;
select fn_txt2(null);
select fn_txt2('');
select fn_txt2('084329sdjhfdsffdjf');
select fn_txt2('sfiewr0239408497867^&*%&^$%^$*&()&*(&084329sdjhfdsffdjf');
drop function fn_txt2(text, ot out int);

create function fn_txt3(text, ot out cstring) as 'textout' language internal;
select fn_txt3(null);
select fn_txt3('');
select fn_txt3('084329sdjhfdsffdjf');
select fn_txt3('sfiewr0239408497867^&*%&^$%^$*&()&*(&084329sdjhfdsffdjf');
drop function fn_txt3(text, ot out cstring);

create function fn_txt4(ot out cstring, text) as 'textout' language internal;
select fn_txt4(null);
select fn_txt4('');
select fn_txt4('084329sdjhfdsffdjf');
select fn_txt4('sfiewr0239408497867^&*%&^$%^$*&()&*(&084329sdjhfdsffdjf');
drop function fn_txt4(ot out cstring, text);

create function gt(int8, int8) returns bool as 'int8gt' language internal;
select gt(123, 234);
select gt(123, 122);
select gt(123, null);
select gt('', null);
select gt(null, null);
select gt('12', '12');
select gt('123', '13');

select gt(123, 234) = int8gt(123, 234) as eq;
select gt(123, 122) = int8gt(123, 122) as eq;
select gt(123, null) is null and int8gt(123, null) is null as eq;
select gt('', null) is null and int8gt('', null) is null as eq;
select gt(null, null) is null and int8gt(null, null)is null  as eq;
select gt('12', '12') = int8gt('12', '12') as eq;
select gt('123', '13') = int8gt('123', '13') as eq;

drop function gt(int8, int8);

create type circle_tbl_ct;
create function fn_txt4(text) returns circle_tbl_ct as 'textout' language internal; -- OK
create function fn_tfn_txt5(text, ot out circle_tbl_ct) as 'textout' language internal; -- OK
drop type circle_tbl_ct cascade;

create function gt(int, int) returns bool as 'int8gt' language internal; -- should error
create function gt(int8, int) returns bool as 'int8gt' language internal; -- should error
create function fn_p(int, cstring) returns int8 language internal as 'int8in'; -- should error
create function fn_p2(int, cstring) returns int8 language internal as 'int8in'; -- should error
create function fn_r(cstring) returns int language internal as 'int8in'; -- should error
create function fn_r(cstring, o out int) language internal as 'int8in'; -- should error
create function fn_txt_r(int8) returns text language internal as 'int8out'; -- ERROR:  return type cstring
create function fn_txt_p(cstring) returns cstring as 'textout' language internal; -- error: param is text type

-- test function for custum type
create type cc1_type;
create function cc1_in(cstring) returns cc1_type
    strict immutable language internal as 'int8in';

create function cc1_out(cc1_type) returns cstring
    strict immutable language internal as 'int8out';

create function cc1_in_bad(cstring) returns cc1_type
    strict immutable language internal as 'textin';

create function cc1_out_bad(cc1_type) returns cstring
    strict immutable language internal as 'textout';

-- should failed: internallength
create type cc1_type(
    input = cc1_in,
    output = cc1_out
);

-- should failed: internallength
create type cc1_type(
    internallength = 6,
    input = cc1_in,
    output = cc1_out
);
-- should failed: output
create type cc1_type(
    internallength = 8,
    passedbyvalue = true,
    alignment = double,
    storage=plain,
    input = cc1_in,
    output = cc1_out_bad
);
-- should failed
create type cc1_type(
    internallength = 8,
    passedbyvalue = true,
    input = cc1_in,
    output = cc1_out
);
-- should pass
create type cc1_type(
    internallength = 8,
    passedbyvalue = true,
	alignment = double,
    input = cc1_in,
    output = cc1_out
);
create cast (int8 as cc1_type) with inout;
select 1234::int8::cc1_type;

-- case 2
create type cc2_type;
create function cc2_in(cstring) returns cc2_type
    strict immutable language internal as 'textin';

create function cc2_out(cc2_type) returns cstring
    strict immutable language internal as 'textout';

create function cc2_out_bad(cc2_type) returns cstring
    strict immutable language internal as 'int8out';

-- should failed
create type cc2_type(
    input = cc2_in,
    output = cc2_out_bad
);
-- should failed
create type cc2_type(
    internallength = 8,
    input = cc2_in,
    output = cc2_out_bad
);
-- passed
create type cc2_type(
    input = cc2_in,
    output = cc2_out
);
select 1234::cc2_type; -- error
select '1234'::cc2_type;
select 'fdsfdfg#@$'::text::cc2_type;

drop type cc1_type cascade;
drop type cc2_type cascade;

CREATE OR REPLACE PROCEDURE test_debug1 ( IN x INT)
AS
BEGIN
INSERT INTO t1 (a) VALUES (x);
DELETE FROM t1 WHERE a = x;
END;
/

do $$
declare
    funcoid bigint;
begin
    select oid from pg_proc into funcoid where proname = 'test_debug1';
    drop function test_debug1;
    select dbe_pldebugger.turn_on(funcoid);
end;
$$;

drop function test_debug1;

\c postgres

alter system set enable_stmt_track=on;
set log_min_duration_statement=0;
set track_stmt_stat_level='L1,L1';
alter system set instr_unique_sql_count = 10000;
delete from dbe_perf.statement_history;

CREATE TABLE stmt_hist_t1 ( first_name text, last_name text, job_id int, department_id int,d1 bigint ) ;
create index stmt_hist_t1_i1 on stmt_hist_t1 (job_id) ;
create index stmt_hist_t1_i2 on stmt_hist_t1 (department_id) ;
insert into stmt_hist_t1 (first_name,last_name,job_id,department_id) values('Alice', 'Adams', 2, 1 );
insert into stmt_hist_t1 (first_name,last_name,job_id,department_id) values('Beatrice', 'Brand', 3, 1);

CREATE TABLE stmt_hist_t2 ( job_title text, job_id int) ;
insert into stmt_hist_t2 values( 'Job1', 1 );
insert into stmt_hist_t2 values( 'Job2', 2 );
insert into stmt_hist_t2 values( 'Job3', 3 );

analyze stmt_hist_t1;
analyze stmt_hist_t2;

CREATE FUNCTION func_add_sql() RETURNS integer
AS 'select 3;'
LANGUAGE SQL
IMMUTABLE
RETURNS NULL ON NULL INPUT;

explain (verbose, costs off) select func_add_sql() from stmt_hist_t2 join stmt_hist_t1 on stmt_hist_t2.job_id = stmt_hist_t1.job_id where stmt_hist_t1.job_id not in (select department_id from stmt_hist_t1 );
select func_add_sql() from stmt_hist_t2 join stmt_hist_t1 on stmt_hist_t2.job_id = stmt_hist_t1.job_id where stmt_hist_t1.job_id not in (select department_id from stmt_hist_t1 );

call pg_sleep(1);

select query_plan from dbe_perf.statement_history where query ilike '%select func_add_sql() from stmt_hist_t2 join stmt_hist_t1 on stmt_hist_t2.job_id = stmt_hist_t1.job_id where stmt_hist_t1.job_id not in (select department_id from stmt_hist_t1 );%';

drop table stmt_hist_t1, stmt_hist_t2 cascade;
drop function func_add_sql;

alter system set enable_stmt_track=off;
reset log_min_duration_statement;
reset track_stmt_stat_level;
alter system set instr_unique_sql_count = 100;

\c regression
