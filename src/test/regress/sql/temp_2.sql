
--
-- TEMP
-- Test temp relations and indexes
--

-- Enforce use of COMMIT instead of 2PC for temporary objects

--test other user/schema use temp table
create schema test_temp_2;
set current_schema = test_temp_2;
create temp table test_base2(a int);
insert into test_base2 select generate_series(1, 1000);
create temp table test_temp4(a int, b varchar2(3000));
insert into test_temp4 select a, lpad(a, 3000, '-') from test_base2;
create index temp4_idx on test_temp4(a, b);
select a from test_temp4 order by 1 limit 1;

--test other temp objects. function view etc.
create function pg_temp.test_temp(a int) returns void
as $$
begin
end; $$ language plpgsql;

select test_temp(1);
select test_temp();
select pg_temp.test_temp(1);
select pg_temp_coordinator1_0_1.test_temp(1);
\df

--Test Drop
drop table test_base2;
drop table test_temp4;

--Test Temp schema
create user USER_ALTER_TABLE_OWNER_TO_NEW_OWNER_011_1 password 'Gauss@123';
create user USER_ALTER_TABLE_OWNER_TO_NEW_OWNER_011_2 with sysadmin password 'Gauss@123';
grant USER_ALTER_TABLE_OWNER_TO_NEW_OWNER_011_2 to USER_ALTER_TABLE_OWNER_TO_NEW_OWNER_011_1;
set role USER_ALTER_TABLE_OWNER_TO_NEW_OWNER_011_1 password 'Gauss@123';
create local temp table TAB_ALTER_TABLE_OWNER_TO_NEW_OWNER_011
(ALTER_TABLE_OWNER_COL_1 CHAR(102400),
ALTER_TABLE_OWNER_COL_2 VARCHAR(1024),
ALTER_TABLE_OWNER_COL_3 INTEGER,
ALTER_TABLE_OWNER_COL_4 numeric(10,5),
ALTER_TABLE_OWNER_COL_5 TIMESTAMP WITHOUT TIME ZONE);
alter table TAB_ALTER_TABLE_OWNER_TO_NEW_OWNER_011 owner to USER_ALTER_TABLE_OWNER_TO_NEW_OWNER_011_2;
set role USER_ALTER_TABLE_OWNER_TO_NEW_OWNER_011_2 password 'Gauss@123';
drop table TAB_ALTER_TABLE_OWNER_TO_NEW_OWNER_011 cascade;
reset role;
drop table TAB_ALTER_TABLE_OWNER_TO_NEW_OWNER_011 cascade;
drop user USER_ALTER_TABLE_OWNER_TO_NEW_OWNER_011_1;
drop user USER_ALTER_TABLE_OWNER_TO_NEW_OWNER_011_2;

--test DISCARD
CREATE TEMP TABLE reset_test ( data text );

SELECT relname FROM pg_class WHERE relname = 'reset_test';

DISCARD TEMP;

SELECT relname FROM pg_class WHERE relname = 'reset_test';

CREATE TEMP TABLE tmp_foo (data text);

SELECT relname FROM pg_class WHERE relname = 'tmp_foo';

DISCARD ALL;

SELECT relname FROM pg_class WHERE relname = 'tmp_foo';

select proname from pg_proc where proname = 'test_temp';

select * from test_temp where a = 10;

--test rename
create temporary table alt_colnm_tbl_001(
c_integer integer,
c_char char(10) not null,
c_date date
);

alter table alt_colnm_tbl_001 rename c_integer to _integer;

alter table alt_colnm_tbl_001 add constraint abc check(1=1);

alter table alt_colnm_tbl_001 rename constraint abc to cde;

--test CREATE TABLE AS for common table and temp table with same relname
reset current_schema;
create table tbl_source(a int,b int);
insert into tbl_source values(generate_series(1,100),generate_series(1,100));

create temp table tbl_same_name(a int,b int);
create table tbl_same_name as select * from tbl_source;
select count(*) from tbl_same_name;
select count(*) from public.tbl_same_name;
drop table tbl_same_name;
drop table public.tbl_same_name;

create table tbl_same_name(a int,b int);
create temp table tbl_same_name as select * from tbl_source;
select count(*) from tbl_same_name;
select count(*) from public.tbl_same_name;
drop table tbl_same_name;
drop table public.tbl_same_name;

create schema temp_schema_name;
set current_schema= temp_schema_name;
create temp table tbl_same_name(a int,b int);
create table tbl_same_name as select * from public.tbl_source;
select count(*) from tbl_same_name;
select count(*) from temp_schema_name.tbl_same_name;

--Test if the temp table is still accessible when the connections between CN and DN have been destroyed once.
create temp table pro_cursor_c0019_tb1(val int ,cc clob);
insert into pro_cursor_c0019_tb1 values(1,'daksd');
insert into pro_cursor_c0019_tb1 values(2,'fjdalfj');

create or replace procedure pro_cursor_c00191() as
declare
   cursor cursor1 for fetch pro_cursor_c0019_1;
   c1 integer;
   c2 text;
   
BEGIN
open cursor1;
loop
      fetch cursor1 into c1,c2;
end loop;

close cursor1;
END;
/

select * from pro_cursor_c0019_tb1;
drop table pro_cursor_c0019_tb1;
drop schema temp_schema_name cascade;
