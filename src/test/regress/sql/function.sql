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

DROP FUNCTION func_increment_sql_1;
DROP FUNCTION func_increment_sql_2;
DROP FUNCTION fun_test_1;
DROP FUNCTION fun_test_2;

\c regression;
drop database IF EXISTS pl_test_funcion;
