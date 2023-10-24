drop table if exists tab;
create table tab (col SET('a','b')); -- failed
create table tab (col int);
alter table tab modify col SET('a','b');
select find_in_set('a', 'a,b');
select find_in_set('a', 1);

create database test dbcompatibility = 'B';
\c test
SET b_format_behavior_compat_options = 'enable_multi_charset';
drop table if exists tab;

drop table if exists dual;
create table dual (dummy varchar(1));
insert into dual values ('X');

-- check the table's privilage
select relname, relacl[1]::text ~ '=r/*' as haspriv from pg_class where relname='pg_set'; -- expect true

-- _anyset not supported
select count(*) from pg_type where typname = '_anyset'; -- expected 0

-- create table with invalid SET type
-- number of values 1 <= n <= 64;
create table tab (col SET());
create table tab (col SET('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31','32','33','34','35','36','37','38','39','40','41','42','43','44','45','46','47','48','49','50','51','52','53','54','55','56','57','58','59','60','61','62','63','64','65'));
create table tab (col SET('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31','32','33','34','35','36','37','38','39','40','41','42','43','44','45','46','47','48','49','50','51','52','53','54','55','56','57','58','59','60','61','62','63','64'));
drop table tab;

-- set values length <= 255 char
create table tab (col SET('dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd'));
create table tab (col SET('大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大'));
create table tab (col SET('dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd,dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd'));
create table tab (col SET('大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大大'));
drop table tab;

-- set values contails ','
create table tab (col SET('a,b'));
create table tab (col SET('a,b', 'c'));

-- has same set value
create table tab (col SET('a','b','c','a'));  -- unstable testcase

-- set array not support
create table tab (col SET('a', 'b')[]);

-- set value is not const string
create table tab (col set(1,2,3));
create table tab (col set('a'||'b', 'c'));

-- set domain not support
create table tab (col set('a', 'b'));
create domain abc as tab_col_set (value > 10);

-- use exists set type
drop table if exists t1;
create table t1 (c1 tab_col_set);
create table t1 (c1 set('a', 'b'), c2 t1_c1_set);

-- create table use select
create table t1 as select * from tab;
create table t1 (like tab);

-- can not use set column as partition key
create table test_set_tab(c1 set('a', 'b', 'c', 'd'), c2 int) partition by range(c1)
(
	partition test_set_tab_p1 values less than (1),
	partition test_set_tab_p2 values less than (7)
);

create table test_set_tab(c1 set('a', 'b', 'c', 'd'), c2 int) partition by hash(c1)
(
	partition test_set_tab_p1,
	partition test_set_tab_p2
);

-- cstore not support with set type
create table test_set_tab(c1 set('a', 'b', 'c', 'd'), c2 int) with (ORIENTATION = column); -- failed
create table test_set_tab(c1 set('a', 'b', 'c', 'd'), c2 int) with (STORAGE_TYPE = ustore); -- succeed
insert into test_set_tab values (1, 1), (2,2), ('a,b',3), ('c', 4), ('d', 8);

-- local/global temp table set column test
create local temp table temp_set_tab (c1 set('a', 'b'));
create global temp table temp_set_tab (c1 set('a', 'b'));
select count(1) from pg_type where typname='temp_set_tab_c1_set'; -- expect 2
drop table temp_set_tab; -- temp namespace
drop table temp_set_tab; -- public namespace
select count(1) from pg_type where typname='temp_set_tab_c1_set'; -- expect 0

-- trim right space for set labels
drop table if exists trim_test_tab;
create table trim_test_tab(c1 set('   ', 'a    ', ' b  ', '  c')); -- set('', 'a', ' b', '  c')
insert into trim_test_tab values ('   '); -- failed
insert into trim_test_tab values ('a    '); -- failed
insert into trim_test_tab values (' b  '); -- failed
insert into trim_test_tab values ('  c'); -- success
insert into trim_test_tab values (', b,a'); -- success
\dT+ trim_test_tab_c1_set;
-- trime the bpchar right space when cast to set labal
alter table trim_test_tab modify c1 char(10);
insert into trim_test_tab values ('a   ');
insert into trim_test_tab values ('a');
insert into trim_test_tab values ('a   b');
alter table trim_test_tab modify c1 set('', 'a', ' b', '  c', 'a   b');
drop table trim_test_tab;

-- alter table test

-- add
alter table test_set_tab add c3 set();
alter table test_set_tab add c3 set('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31','32','33','34','35','36','37','38','39','40','41','42','43','44','45','46','47','48','49','50','51','52','53','54','55','56','57','58','59','60','61','62','63','64','65');
alter table test_set_tab add c3 set('a,b');
alter table test_set_tab add c3 set('dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd');
-- alter table test_set_tab add c3 set('a','b','c','a'); -- unstable testcase
alter table test_set_tab add c3 set('a','b','c')[];
alter table test_set_tab add c3 set(1, 2, 3);
alter table test_set_tab add c3 test_set_tab_c1_set;

drop type if exists test_set_tab_c3_set;
create type test_set_tab_c3_set as ENUM ('a', 'b');
alter table test_set_tab add c3 set('a', 'b'); -- failed

drop type test_set_tab_c3_set;
alter table test_set_tab add c3 set ('a', 'b', 'c'); -- succeed

-- modify non-set to set type failed when the set type is invalid defined.
alter table test_set_tab modify c2 set();
alter table test_set_tab modify c2 set('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31','32','33','34','35','36','37','38','39','40','41','42','43','44','45','46','47','48','49','50','51','52','53','54','55','56','57','58','59','60','61','62','63','64','65');
alter table test_set_tab modify c2 set('a,b');
alter table test_set_tab modify c2 set('dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd');
-- alter table test_set_tab modify c2 set('a','b','c','a'); -- unstable testcase
alter table test_set_tab modify c2 set('a','b','c')[];
alter table test_set_tab modify c2 test_set_tab_c1_set;

drop type if exists test_set_tab_c2_set;
create type test_set_tab_c2_set as ENUM ('a', 'b');
alter table test_set_tab modify c2 set('a', 'b');  --failed
drop type test_set_tab_c2_set;

-- out of range, here we can test more datatypes such as float, text, varchar, etc..
alter table test_set_tab modify c2 set('a', 'b', 'c'); -- failed, 8 out of range
update test_set_tab set c2 = 7 where c2 = 8;
alter table test_set_tab modify c2 set('a', 'b', 'c'); -- succeed

alter table test_set_tab add c4 float8;
update test_set_tab set c4=479674615.454654;
alter table test_set_tab modify c4 set('a', 'b', 'c'); -- failed
update test_set_tab set c4=5.454654;
alter table test_set_tab modify c4 set('a', 'b', 'c'); -- succeed

-- datatype can not convert to set
alter table test_set_tab add c5 date;
update test_set_tab set c5 = to_date('2022-04-13');
alter table test_set_tab modify c5 set('a', 'b', 'c'); -- failed, no convert path for date to set
alter table test_set_tab modify c5 text;
alter table test_set_tab modify c5 set('a', 'b', 'c'); -- failed, invalid set value
alter table test_set_tab modify c5 set('2022-04-13 00:00:00'); -- succeed

-- can not modify partition key col to set
create table test_set_tab_part(c1 set('a', 'b', 'c', 'd'), c2 int) partition by range(c2)
(
	partition test_set_tab_p1 values less than (1),
	partition test_set_tab_p2 values less than (7)
);
alter table test_set_tab_part modify c2 set('a', 'b', 'c');
select count(1) from pg_type where typname='test_set_tab_part_c2_set'; -- expect 0

-- alter table owner test
reset role;
create user rsr_test01 with password 'Gauss_123';
create user rsr_test02 with password 'Gauss_123';

--schema 的属主是 rsr_test01
create schema rsr_schema01 AUTHORIZATION rsr_test01;
set role rsr_test01 password 'Gauss_123';

--表的属主是rsr_test01
create table rsr_schema01.tab21 (
  col set('beijing','shanghai', 'nanjing', 'wuhan', 'shenzhen')
);

reset role;

--修改表的属主是 rsr_test02
alter type rsr_schema01.tab21_col_set owner to rsr_test02; -- not support
alter table rsr_schema01.tab21 OWNER TO rsr_test02;

-- owner should be the same
select (select typowner from pg_type where typname = 'tab21_col_set') = (select relowner from pg_class where relname = 'tab21');
drop table rsr_schema01.tab21;
select count(*) from pg_type where typname = 'tab21_col_set'; -- expect 0

-- alter table schema test
create user set_test01 with password 'Gauss_123';
create user set_test02 with password 'Gauss_123';
create schema set_schema01 AUTHORIZATION set_test01;
create schema set_schema02 AUTHORIZATION set_test02;
create table set_schema01.set_table (col set('beijing','shanghai', 'nanjing', 'wuhan', 'shenzhen'));
select (select typnamespace from pg_type where typname ='set_table_col_set')=(select relnamespace from pg_class where relname= 'set_table');
-- change the namespace of table
alter table set_schema01.set_table set schema set_schema02;
-- namespace and owner should be the same
select (select typnamespace from pg_type where typname ='set_table_col_set')=(select relnamespace from pg_class where relname= 'set_table');
drop user set_test01 cascade;
select * from set_schema02.set_table;
drop table set_schema02.set_table;

-- modify set col to non-set type
alter table test_set_tab modify c1 date;
alter table test_set_tab modify c1 time;
alter table test_set_tab modify c1 int;
alter table test_set_tab modify c2 char(10);
select c2, length(c2) from test_set_tab order by c2;
alter table test_set_tab modify c4 float4;
alter table test_set_tab modify c5 float8;
select count(distinct settypid) from pg_set; -- expect 3, some set types have deleted.

-- drop table's set column
drop type test_set_tab_part_c1_set; -- failed
alter table test_set_tab_part drop c1;
select count(distinct settypid) from pg_set; -- expect 2

-- drop table constains set column
drop table test_set_tab;
select count(distinct settypid) from pg_set; -- expect 1

-- flash back test for ustore table
create table test_ustore_set (c1 set('n1', 'n2', 'n3'), c2 int) with (storage_type=ustore);
select count(*) from pg_set where settypid = (select oid from pg_type where typname='test_ustore_set_c1_set'); -- expect 3
drop table test_ustore_set;
select count(*) from pg_set where settypid = (select oid from pg_type where typname='test_ustore_set_c1_set'); -- expect 0
select * from gs_recyclebin where rcyoriginname='test_ustore_set_c1_set'; -- no output

-- drop schema test
create schema s1;
create table s1.tab (col set('s1.a', 's1.b'));
drop schema s1; -- failed
drop schema s1 cascade;

drop table if exists test_cons_set;
create table test_cons_set (c1 int check (c1 >= 10), c2 varchar(30) check(regexp_count(c2, 'a.b') = 1), c3 set('a', 'b', 'c', 'd') check (c3 > c1));
insert into test_cons_set values (10, 'a,b,d', 11);
insert into test_cons_set values (11, 'a-b', 'c,d');

alter table test_cons_set modify c1 set('a', 'b'); -- failed
alter table test_cons_set modify c1 set('a', 'b', 'c', 'd'); -- succeed
\d test_cons_set
alter table test_cons_set modify c2 set('a','b','c','d'); -- failed
delete from test_cons_set where c2 = 'a-b';
insert into test_cons_set values (12, 'a,b', 'a,c,d');
alter table test_cons_set modify c2 set('d', 'c', 'b', 'a'); -- failed, constaint conflict

-- test insert
drop table if exists ins_test_set;
create table ins_test_set (
  c1 set('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31','32','33','34','35','36','37','38','39','40','41','42','43','44','45','46','47','48','49','50','51','52','53','54','55','56','57','58','59','60','61','62','63','64'),
  c2 set('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31','32','33','34','35','36','37','38','39','40','41','42','43','44','45','46','47','48','49','50','51','52','53','54','55','56','57','58','59','60','61','62','63'),
  c3 set('a', 'b', '', 'd', 'e', 'f')
);

-- display the new set type and its members
\dT+ ins_test_set_c1_set;
\dT+ ins_test_set_c3_set;

insert into ins_test_set (c1) values (0); -- empty set
insert into ins_test_set (c1) values (-1); -- succeed to set all 64-bit
insert into ins_test_set (c1) values (9223372036854775808); -- out of range, different from M*
insert into ins_test_set (c1) values (-9223372036854775808); -- succeed to set 64th-bit
insert into ins_test_set (c1) values (9223372036854775807); -- succeed to set all 63-bit
insert into ins_test_set (c1) values (-9223372036854775809); -- out of range, different from M*
select c1::bigint as digit, c1 from ins_test_set where c1 is not null order by c1;


insert into ins_test_set (c2) values (0); -- empty set
insert into ins_test_set (c2) values (-1); -- failed to set all 64-bit, out of the set range
insert into ins_test_set (c2) values (4611686018427387904); -- succeed to set 63th-bit
insert into ins_test_set (c2) values (9223372036854775807); -- succeed to set all 63-bit
insert into ins_test_set (c2) values (-9223372036854775808); -- failed to set 64th-bit, out of set range

insert into ins_test_set (c3) values (0);
insert into ins_test_set (c3) values (-1); -- failed, out of range
insert into ins_test_set (c3) values (31);
insert into ins_test_set (c3) values (32); -- failed, out of range
insert into ins_test_set (c3) values (31.49); -- to 31
insert into ins_test_set (c3) values (31.50); -- ceil(32), different from M*

insert into ins_test_set (c1)values ('2,1,1'); -- duplicate set member
insert into ins_test_set (c1) values ('1');
insert into ins_test_set (c1) values ('a'); -- fail
insert into ins_test_set (c1) values ('65'); -- fail

insert into ins_test_set (c3) values (''); -- null if compatible with A
insert into ins_test_set (c3) values (',');
insert into ins_test_set (c3) values ('d,e,');
insert into ins_test_set (c3) values ('d,,e');
insert into ins_test_set (c3) values (',d,e');
insert into ins_test_set (c3) values ('A,a'); -- fail
insert into ins_test_set (c3) values ('E,e');

drop table if exists source_tab;
create table source_tab (c_int2 int2, c_int4 int, c_int8 bigint, c_bool bool, c_char char(20), c_varchar varchar(20), c_nvarchar2 nvarchar2(20), c_numberic number, c_text text, c_float4 float4, c_float8 float8, c_date date, c_time time, c_interval interval, c_set set ('1','2'));
insert into source_tab values (
  12,
  3432,
  4611686018427387904,
  true,
  '1,2,3,64',
  '1,2,3,64',
  '1,2,3,64',
  12.5,
  '1,2,3,64',
  12.5,
  4611606018427387904.43,
  '2022-04-08 00:00:00',
  '16:20:12',
  '1 day',
  3
);
truncate table ins_test_set;
insert into ins_test_set (c1,c2) select c_int2,c_float4 from source_tab;
insert into ins_test_set (c1,c2) select c_int4,c_bool::int from source_tab;
insert into ins_test_set (c1,c2) select c_int8,'63' from source_tab;
insert into ins_test_set (c1,c2) select c_bool::int,'62' from source_tab;
--insert into ins_test_set (c1,c2) select c_char,c_numberic from source_tab; -- fail
insert into ins_test_set (c1,c2) select c_varchar,c_float8 from source_tab;
insert into ins_test_set (c1,c2) select c_nvarchar2,c_int2 from source_tab;
insert into ins_test_set (c1,c2) select c_numberic,c_int4 from source_tab;
insert into ins_test_set (c1,c2) select c_text, c_set from source_tab; -- fail
insert into ins_test_set (c1,c2) select c_text, c_float8 from source_tab;
insert into ins_test_set (c1,c2) select c_float4,substr(c_text,1,5) from source_tab;
insert into ins_test_set (c1,c2) select c_float8,c_int8 from source_tab;
insert into ins_test_set (c1,c2) select c_date,',' from source_tab; -- fail, no convert path
insert into ins_test_set (c1,c2) select c_time,1 from source_tab; -- fail, no convert path
insert into ins_test_set (c1,c2) select c_interval,1 from source_tab; -- fail, no convert path
insert into ins_test_set (c1,c2) select c_set,1 from source_tab; -- fail, no convert path, different from M*
insert into ins_test_set (c1,c2) select c_set::text, c_set::int from source_tab;
insert into ins_test_set (c1,c2) select c1,c2 from ins_test_set;

-- select test (operator(<,<=,=,!=,>,>=), type cast, join, index scan)
select c1::bigint as num, c1 from ins_test_set order by c1; -- 22 rows expect

-- operator test
-- <
select c1+0, c2+0 from ins_test_set where c1 < c2 order by 1;  -- set < other set
select c1+0, c2+0 from ins_test_set where c1 < 0::int2 order by c1;  -- set < int2
select c1+0, c2+0 from ins_test_set where c1 < (1<<30) order by c1; -- set < int4
select c1+0, c2+0 from ins_test_set where c1 < (1::bigint<<62) order by 1; -- set < int8
select c1 from ins_test_set where c1 < '4,3' order by 1; -- set < text
select c1 from ins_test_set where c1 < true order by 1; -- set < bool
select c1 from ins_test_set where c1 < false order by 1; -- set < bool
select c1+0, c2+0 from ins_test_set where 0::int2 < c1 order by c1;  -- int2 < set
select c1+0, c2+0 from ins_test_set where (1<<30) < c1 order by c1; --  int4 < set
select c1+0, c2+0 from ins_test_set where (1::bigint<<62) < c1 order by 1; -- int8 < set
select c1 from ins_test_set where '4,3' < c1 order by 1; -- text < set
select c1 from ins_test_set where true < c1 order by 1; -- bool < set

-- <=
select c1+0, c2+0 from ins_test_set where c1 <= c2 order by 1;  -- set <= other set
select c1+0, c2+0 from ins_test_set where c1 <= 0::int2 order by c1;  -- set <= int2
select c1+0, c2+0 from ins_test_set where c1 <= (1<<30) order by c1; -- set <= int4
select c1+0, c2+0 from ins_test_set where c1 <= (1::bigint<<62) order by 1; -- set <= int8
select c1 from ins_test_set where c1 <= '4,3' order by 1; -- set <= text
select c1 from ins_test_set where c1 <= true order by 1; -- set <= bool
select c1 from ins_test_set where c1 <= false order by 1; -- set <= bool
select c1+0, c2+0 from ins_test_set where 0::int2 <= c1 order by c1;  -- int2 <= set
select c1+0, c2+0 from ins_test_set where (1<<30) <= c1 order by c1; --  int4 <= set
select c1+0, c2+0 from ins_test_set where (1::bigint<<62) <= c1 order by 1; -- int8 <= set
select c1 from ins_test_set where '4,3' <= c1 order by 1; -- text <= set
select c1 from ins_test_set where true <= c1 order by 1; -- bool <= set

-- <>
select c1+0, c2+0 from ins_test_set where c1 != c2 order by 1;  -- set <> other set
select c1+0, c2+0 from ins_test_set where c1 <> 12::int2 order by c1;  -- set <> int2
select c1+0, c2+0 from ins_test_set where c1 != (1<<30) order by c1; -- set <> int4
select c1+0, c2+0 from ins_test_set where c1 != 4611606018427387904 order by 1; -- set <> int8
select c1 from ins_test_set where c1 != '1,2,3,64' order by 1; -- set <> text
select c1 from ins_test_set where c1 <> true order by 1; -- set < bool
select c1 from ins_test_set where c1 != false order by 1; -- set <> bool
select c1+0, c2+0 from ins_test_set where 12::int2 != c1 order by c1;  -- int2 <> set
select c1+0, c2+0 from ins_test_set where (1<<30) != c1 order by c1; --  int4 <> set
select c1+0, c2+0 from ins_test_set where 4611606018427387904 != c1 order by 1; -- int8 <> set
select c1 from ins_test_set where '1,2,3,64' != c1 order by 1; -- text <> set
select c1 from ins_test_set where true <> c1 order by 1; -- bool < set

-- >
select c1+0, c2+0 from ins_test_set where c1 > c2 order by 1;  -- set > other set
select c1+0, c2+0 from ins_test_set where c1 > 0::int2 order by c1;  -- set > int2
select c1+0, c2+0 from ins_test_set where c1 > (1<<30) order by c1; -- set > int4
select c1+0, c2+0 from ins_test_set where c1 > (1::bigint<<62) order by 1; -- set > int8
select c1 from ins_test_set where c1 > '4,3' order by 1; -- set > text
select c1 from ins_test_set where c1 > true order by 1; -- set > bool
select c1+0, c2+0 from ins_test_set where 0::int2 > c1 order by c1;  -- int2 > set
select c1+0, c2+0 from ins_test_set where (1<<30) > c1 order by c1; --  int4 > set
select c1+0, c2+0 from ins_test_set where (1::bigint<<62) > c1 order by 1; -- int8 > set
select c1 from ins_test_set where '4,3' > c1 order by 1; -- text > set
select c1 from ins_test_set where true > c1 order by 1; -- bool > set

-- >=
select c1+0, c2+0 from ins_test_set where c1 >= c2 order by 1;  -- set >= other set
select c1+0, c2+0 from ins_test_set where c1 >= 0::int2 order by c1;  -- set >= int2
select c1+0, c2+0 from ins_test_set where c1 >= (1<<30) order by c1; -- set >= int4
select c1+0, c2+0 from ins_test_set where c1 >= (1::bigint<<62) order by 1; -- set >= int8
select c1 from ins_test_set where c1 >= '4,3' order by 1; -- set >= text
select c1 from ins_test_set where c1 >= true order by 1; -- set >= bool
select c1+0, c2+0 from ins_test_set where 0::int2 >= c1 order by c1;  -- int2 >= set
select c1+0, c2+0 from ins_test_set where (1<<30) >= c1 order by c1; --  int4 >= set
select c1+0, c2+0 from ins_test_set where (1::bigint<<62) >= c1 order by 1; -- int8 >= set
select c1 from ins_test_set where '4,3' >= c1 order by 1; -- text >= set
select c1 from ins_test_set where true >= c1 order by 1; -- bool >= set

-- =
select c1+0, c2+0 from ins_test_set where c1 = c2 order by 1;  -- set = other set
select c1+0, c2+0 from ins_test_set where c1 = 12::int2 order by c1;  -- set = int2
select c1+0, c2+0 from ins_test_set where c1 = (1<<30) order by c1; -- set = int4
select c1+0, c2+0 from ins_test_set where c1 = (1::bigint<<62) order by 1; -- set = int8
select c1 from ins_test_set where c1 = '1,2,3,64' order by 1; -- set = text
select c1 from ins_test_set where c1 = true order by 1; -- set = bool
select c1+0, c2+0 from ins_test_set where 12::int2 = c1 order by c1;  -- int2 = set
select c1+0, c2+0 from ins_test_set where (1<<30) = c1 order by c1; --  int4 = set
select c1+0, c2+0 from ins_test_set where (1::bigint<<62) = c1 order by 1; -- int8 = set
select c1 from ins_test_set where '1,2,3,64' = c1 order by 1; -- text < set
select c1 from ins_test_set where true = c1 order by 1; -- bool = set

-- set convert to other type
select c1::int8,
       c1::text,
       c1::numeric,
       c1::float4,
       c1::float8,
       c1::varchar(30), -- truncate
       c1::char(30),
       c1::nvarchar2(30)
from ins_test_set order by 1;

-- join test
select a.c1, a.c1::bigint, b.c_int2 from ins_test_set a join source_tab b on a.c1 = b.c_int2 order by 1; -- join with int2, hash by value
select a.c1, a.c1::bigint, b.c_int2 from ins_test_set a join source_tab b on a.c1 > b.c_int2 order by 1; -- join with int2
select a.c1, a.c1::bigint, b.c_int2 from ins_test_set a join source_tab b on a.c1 >= b.c_int2 order by 1; -- join with int2
select a.c1, a.c1::bigint, b.c_int2 from ins_test_set a join source_tab b on a.c1 < b.c_int2 order by 1; -- join with int2
select a.c1, a.c1::bigint, b.c_int2 from ins_test_set a join source_tab b on a.c1 <= b.c_int2 order by 1; -- join with int2

select a.c1, a.c1::bigint, b.c_int4 from ins_test_set a join source_tab b on a.c1 = b.c_int4 order by 1; -- join with int4, hash by value
select a.c1, a.c1::bigint, b.c_int4 from ins_test_set a join source_tab b on a.c1 > b.c_int4 order by 1; -- join with int4
select a.c1, a.c1::bigint, b.c_int4 from ins_test_set a join source_tab b on a.c1 >= b.c_int4 order by 1; -- join with int4
select a.c1, a.c1::bigint, b.c_int4 from ins_test_set a join source_tab b on a.c1 < b.c_int4 order by 1; -- join with int4
select a.c1, a.c1::bigint, b.c_int4 from ins_test_set a join source_tab b on a.c1 <= b.c_int4 order by 1; -- join with int4

select a.c1, a.c1::bigint, b.c_int8 from ins_test_set a join source_tab b on a.c1 = b.c_int8 order by 1; -- join with int8, hash by value
select a.c1, a.c1::bigint, b.c_int8 from ins_test_set a join source_tab b on a.c1 > b.c_int8 order by 1; -- join with int8
select a.c1, a.c1::bigint, b.c_int8 from ins_test_set a join source_tab b on a.c1 >= b.c_int8 order by 1; -- join with int8
select a.c1, a.c1::bigint, b.c_int8 from ins_test_set a join source_tab b on a.c1 < b.c_int8 order by 1; -- join with int8
select a.c1, a.c1::bigint, b.c_int8 from ins_test_set a join source_tab b on a.c1 <= b.c_int8 order by 1; -- join with int8

select a.c1, a.c1::bigint, b.c_varchar from ins_test_set a join source_tab b on a.c1 = b.c_varchar order by 1; -- join with varchar
select a.c1, a.c1::bigint, b.c_varchar from ins_test_set a join source_tab b on a.c1 > b.c_varchar order by 1; -- join with varchar
select a.c1, a.c1::bigint, b.c_varchar from ins_test_set a join source_tab b on a.c1 >= b.c_varchar order by 1; -- join with varchar
select a.c1, a.c1::bigint, b.c_varchar from ins_test_set a join source_tab b on a.c1 < b.c_varchar order by 1; -- join with varchar
select a.c1, a.c1::bigint, b.c_varchar from ins_test_set a join source_tab b on a.c1 <= b.c_varchar order by 1; -- join with varchar

select a.c1, a.c1::bigint, b.c_nvarchar2 from ins_test_set a join source_tab b on a.c1 = b.c_nvarchar2 order by 1; -- join with nvarchar2
select a.c1, a.c1::bigint, b.c_nvarchar2 from ins_test_set a join source_tab b on a.c1 > b.c_nvarchar2 order by 1; -- join with nvarchar2
select a.c1, a.c1::bigint, b.c_nvarchar2 from ins_test_set a join source_tab b on a.c1 >= b.c_nvarchar2 order by 1; -- join with nvarchar2
select a.c1, a.c1::bigint, b.c_nvarchar2 from ins_test_set a join source_tab b on a.c1 < b.c_nvarchar2 order by 1; -- join with nvarchar2
select a.c1, a.c1::bigint, b.c_nvarchar2 from ins_test_set a join source_tab b on a.c1 <= b.c_nvarchar2 order by 1; -- join with nvarchar2

select a.c1, a.c1::bigint, b.c_char from ins_test_set a join source_tab b on a.c1 =  b.c_char order by 1; -- join with char
select a.c1, a.c1::bigint, b.c_char from ins_test_set a join source_tab b on a.c1 >  b.c_char order by 1; -- join with char
select a.c1, a.c1::bigint, b.c_char from ins_test_set a join source_tab b on a.c1 >= b.c_char order by 1; -- join with char
select a.c1, a.c1::bigint, b.c_char from ins_test_set a join source_tab b on a.c1 <  b.c_char order by 1; -- join with char
select a.c1, a.c1::bigint, b.c_char from ins_test_set a join source_tab b on a.c1 <= b.c_char order by 1; -- join with char

select a.c1, a.c1::bigint, b.c_text from ins_test_set a join source_tab b on a.c1 =  b.c_text order by 1; -- join with text
select a.c1, a.c1::bigint, b.c_text from ins_test_set a join source_tab b on a.c1 >  b.c_text order by 1; -- join with text
select a.c1, a.c1::bigint, b.c_text from ins_test_set a join source_tab b on a.c1 >= b.c_text order by 1; -- join with text
select a.c1, a.c1::bigint, b.c_text from ins_test_set a join source_tab b on a.c1 <  b.c_text order by 1; -- join with text
select a.c1, a.c1::bigint, b.c_text from ins_test_set a join source_tab b on a.c1 <= b.c_text order by 1; -- join with text

select a.c1, a.c1::bigint, b.c_bool from ins_test_set a join source_tab b on a.c1 =  b.c_bool order by 1; -- join with bool
select a.c1, a.c1::bigint, b.c_bool from ins_test_set a join source_tab b on a.c1 >  b.c_bool order by 1; -- join with bool
select a.c1, a.c1::bigint, b.c_bool from ins_test_set a join source_tab b on a.c1 >= b.c_bool order by 1; -- join with bool
select a.c1, a.c1::bigint, b.c_bool from ins_test_set a join source_tab b on a.c1 <  b.c_bool order by 1; -- join with bool
select a.c1, a.c1::bigint, b.c_bool from ins_test_set a join source_tab b on a.c1 <= b.c_bool order by 1; -- join with bool

select a.c1, a.c1::bigint, b.c_set from ins_test_set a join source_tab b on a.c1 =  b.c_set order by 1; -- join with set
select a.c1, a.c1::bigint, b.c_set from ins_test_set a join source_tab b on a.c1 >  b.c_set order by 1; -- join with set
select a.c1, a.c1::bigint, b.c_set from ins_test_set a join source_tab b on a.c1 >= b.c_set order by 1; -- join with set
select a.c1, a.c1::bigint, b.c_set from ins_test_set a join source_tab b on a.c1 <  b.c_set order by 1; -- join with set
select a.c1, a.c1::bigint, b.c_set from ins_test_set a join source_tab b on a.c1 <= b.c_set order by 1; -- join with set

-- aggr/group/order/distinct/having/union/except/minus/limit/offset/case when/decode
select distinct c1::bigint as num, c1 from ins_test_set order by c1; -- 8 rows expect
select c1::bigint as num, c1 from ins_test_set group by c1 order by c1;
select c1::bigint as num, c1 from ins_test_set group by c1 + 1; -- fail
select c1::bigint + 1 as num, c1 + 1 from ins_test_set group by c1 + 1 order by c1 + 1;
select sum(c1) from ins_test_set;
select sum(c1) from ins_test_set group by c1 order by c1;
select avg(c1), c1::bigint + 1 as num, c1 + 1 from ins_test_set group by c1 + 1 having avg(c1) > 20 order by c1 + 1;

select c1::bigint as num, c1 from ins_test_set where exists (select c_int2 from source_tab where c_int2 < c1) order by 1;
select c1::bigint as num, c1 from ins_test_set where not exists (select c_int2 from source_tab where c_int2 < c1) order by 1;
select c1::bigint as num, c1 from ins_test_set where c1 in (select c_int2 from source_tab) order by 1;
select c1::bigint as num, c1 from ins_test_set where c1 not in (select c_int2 from source_tab) order by 1;

-- union test
-- set union set
select c1 from ins_test_set union all select c2 from ins_test_set order by 1;
select c1 from ins_test_set union select c2 from ins_test_set order by 1; -- hash by string, not set value
select c1 from ins_test_set union select c2 from ins_test_set union select c3 from ins_test_set order by 1; -- hash by string, not set value

-- set1 union set2
select c1 from ins_test_set union all select c_set from source_tab order by 1;
select c1 from ins_test_set union select c_set from source_tab order by 1;
drop table if exists union_test;
create table union_test (c1 set('a','b','c','d'));
insert into union_test values ('a'),('b'),('c'),('d');
select c1 from ins_test_set union all select c1 from union_test order by 1;
select c1 from ins_test_set union select c1 from union_test order by 1; -- set values is the same, but string is different. hash by string
drop table union_test;

-- set union unknownoid(const string)
select c1 from ins_test_set union select 'ab' from dual order by 1;
select 'ab' from dual union select c1 from ins_test_set order by 1;
select 'ab' from dual union select c_int4 from source_tab order by 1; -- not changed
select '1' from dual union select c_int4 from source_tab order by 1; -- not changed

-- set union with datatypes in other type category
select c1 from ins_test_set union select c_int4 from source_tab order by 1;
select c1 from ins_test_set union select c_varchar from source_tab order by 1;
select c1::bigint from ins_test_set union select c_int4 from source_tab order by 1;

-- case when
select case c1 when c1 <=0 then c1 + 1 when c1 > 0 and c1 < 100 then c1 || 'abc' else c1 end from ins_test_set order by 1; -- || is not string concat in M*
select c2, c3, case when c1 <=0 then c1 when c1 > 0 and c1 > 100 then c2 else c3 end as result from ins_test_set order by 1,2,3;
select c1::bigint, c2, c3, case c1 when 0 then c1 when true then c2 when '1,2,3,64' then c3 else null end as result from ins_test_set order by 1,2,3,4;

-- decode
select c1::bigint, c2, c3, decode(c1, 0, c1, 1, c2, '1,2,3,64', 'c3', 'default') from ins_test_set order by 1,2,3,4;
select c1::bigint, c2, c3, decode(c1, 0, 1, 1, c2, '1,2,3,64', 'c3', 'default') from ins_test_set order by 1,2,3,4;

-- between...and
select c1 from ins_test_set where c1 between 'a,b,c' and '1,2,3' order by 1;
select c1 from ins_test_set where c1 between 1 and '1,2,3' order by 1; -- different from M*
select c1 from ins_test_set where c1 between '1,2' and 56.54 order by 1;

-- greatest/least/nvl/exists/not exists/in(subquery)/not in(subquery)/any/some/all/in(values)/
select greatest(c1, c2, c3) from ins_test_set order by 1; -- convert to text
select least(c1, c2, c3) from ins_test_set order by 1; -- convert to text
select greatest(c1, c2, 1) from ins_test_set order by 1;
select least(c1, c2, 1) from ins_test_set order by 1;
select nvl(c1, 1) from ins_test_set order by 1; -- order by set value

select c1 from ins_test_set where c1 in (1, '1,2,3', 323.42, '2020-2-12'::date);
select distinct c1 from ins_test_set where c1 not in (1, '1,2,3', 323.42, '2020-2-12'::date, 4611606018427387904) order by 1;

select c1 from ins_test_set where c1 in (select c_set from source_tab);
select c1 from ins_test_set a where exists (select 1 from source_tab b where b.c_set = a.c1);

select c1 from ins_test_set where c1 not in (select c_set from source_tab) order by 1;
select distinct c1 from ins_test_set where c1 not in (select c_set from source_tab) order by 1;
select distinct c1 from ins_test_set a where not exists (select 1 from source_tab b where b.c_set = a.c1) order by 1;

select distinct c1 from ins_test_set where c1=any(select c_set from source_tab);
select distinct c1 from ins_test_set where c1=any(select c_bool from source_tab);
select distinct c1 from ins_test_set where c1=any(select c_int8 from source_tab);
select distinct c1 from ins_test_set where c1=all(select c_set from source_tab);
select distinct c1 from ins_test_set where c1=all(select c_bool from source_tab);
select distinct c1 from ins_test_set where c1=all(select c_int8 from source_tab);

-- aggr function: sum/max/min/avg/count/median/array_agg/string_agg/listagg/bit_and/bit_or
select array_agg(c1) from ins_test_set; -- fail, not support
select array_agg(c1) from (select distinct c1::int8 as c1 from ins_test_set order by c1);

select sum(c1), max(c1+1), min(c1-1), avg(c1), count(c1), median(c1) from ins_test_set;
select string_agg(c1, '->' order by c1) from ins_test_set; -- order by string, result is also string format
select listagg(c1, '->') within group(order by c1) from ins_test_set; -- order by set value, and the result is set values
select listagg(c1, '->') within group(order by c1) over (partition by c2) from ins_test_set;
select c2, listagg(c1, '->') within group(order by c1) over (partition by c2) from ins_test_set order by c2;

select bit_and(c1) from ins_test_set group by c2 having bit_and(c1) > 0 order by 1;
select bit_or(c1) from ins_test_set group by c2 having bit_or(c1) > 0 order by 1;

-- update test
update ins_test_set set c1 = 0 where c1=-9223372036854775801 returning c1;
update ins_test_set set c1 = -1 where c1='1' returning c1+1;
update ins_test_set set c1 = 9223372036854775808 where c1=3; -- fail, out of range
update ins_test_set set c1 = -9223372036854775808 where c1=3 returning to_char(c1);
update ins_test_set set c1 = 9223372036854775807 where c1='3,4' returning (c1::bigint % 10) / 16;
update ins_test_set set c1 = '65'; -- fail
update ins_test_set set c1 = '1,64' where c1='1,3,4' returning (c1-1);
update ins_test_set set c1 = '63' where c1=3432.49 returning c1 * 0.01; -- update 0
update ins_test_set set c1 = '63' where c1=3432 returning c1 * 0.01; -- update 2
update ins_test_set set c1 = true::int where c1 = 9223372036854775808; -- update 0
update ins_test_set set c1 = false::int where c1 = 9223372036854775807; -- update 4
update ins_test_set set c1 = 1.23 where c1 = '63';
update ins_test_set set c1 = '6,6,6' where c1 = -1 or c1 = '64';
update ins_test_set set c1 = -9223372036854775809 where c1 = 1;
update ins_test_set set c1 = (select c_float8 from source_tab) where c1 = 1;
update ins_test_set set c1 = (select c_nvarchar2 from source_tab) where c1 = (select c_float8 from source_tab);
select c1::bigint as num, c1 from ins_test_set order by c1;

-- delete test
delete from ins_test_set where c1 > '1,2,3,64' returning c1::bigint;
delete from ins_test_set where c1 = -9223372036854775807 returning -c1::bigint;
delete from ins_test_set where c1 = (select 0);
delete from ins_test_set where c1 = 32.156; -- 0 row
delete from ins_test_set where c1 = 32 returning *; -- 0 row

-- index scan test
create index idx_test_c1 on ins_test_set (c1);
select * from ins_test_set where c1=-9223372036854775801 order by c2, c3;
select * from ins_test_set where c1='1,2,3,64' order by c2, c3;
select c1, c2 from ins_test_set a, source_tab b where a.c1 = b.c_set order by 1,2;
set enable_hashjoin=off;
select c1, c2 from ins_test_set a, source_tab b where a.c1 = b.c_set order by 1,2;
set enable_mergejoin=off;
select c1, c2 from ins_test_set a, source_tab b where a.c1 = b.c_set order by 1,2;
select c1, c2 from ins_test_set a, source_tab b where a.c1 = b.c_int8;
set enable_hashjoin=on;
set enable_mergejoin=on;
select c1, c2 from ins_test_set a, source_tab b where a.c1 = b.c_float4; -- no index scan
select c1, c2 from ins_test_set a, source_tab b where a.c1 = b.c_numberic; -- no index scan
select c1, c2 from ins_test_set a, source_tab b where a.c1 = b.c_text order by c2; -- no index scan
set enable_seqscan = off;
explain (costs off) select c2 from ins_test_set where c1 = 3 order by c1; -- eliminate sort by index

-- hash index is not supportted for row store
create index idx_test_c2 on ins_test_set using hash (c2);

-- test find_in_set()
select c1, find_in_set(null, c1) from ins_test_set order by 1,2;
select c1, find_in_set(3, null) from ins_test_set order by 1,2;
select c1, find_in_set(',', c1) from ins_test_set order by 1,2;
select c1, find_in_set('a,b', c1) from ins_test_set order by 1,2;
select c1, find_in_set('3', c1) from ins_test_set order by 1,2;
select c1, find_in_set(3, c1) from ins_test_set order by 1,2;

-- test compare rules for set
drop table if exists test1;
create table test1 (c set('d','c','b','a'));
insert into test1 values ('a'),('b'),('c'),('d');
select c, c+0 from test1 order by c; -- order by value

drop table if exists test1;
drop table if exists test2;
create table test1 (c set('a','b','c','d'));
insert into test1 values ('a'),('b'),('c'),('d');
create table test2 (c set('d','c','b','a'));
insert into test2 values ('a'),('b'),('c'),('d');
select test1.c + 0, test1.c as c1, test2.c + 0, test2.c as c2, test1.c > test2.c from test1, test2 where test1.c > test2.c order by 1,3;
select test1.c + 0, test1.c as c1, test2.c + 0, test2.c as c2, test1.c > test2.c from test1, test2 where test1.c >= test2.c order by 1,3;
select test1.c + 0, test1.c as c1, test2.c + 0, test2.c as c2, test1.c > test2.c from test1, test2 where test1.c < test2.c order by 1,3;
select test1.c + 0, test1.c as c1, test2.c + 0, test2.c as c2, test1.c > test2.c from test1, test2 where test1.c <= test2.c order by 1,3;
select test1.c + 0, test1.c as c1, test2.c + 0, test2.c as c2, test1.c > test2.c from test1, test2 where test1.c != test2.c order by 1,3;
select test1.c + 0, test1.c as c1, test2.c + 0, test2.c as c2, test1.c > test2.c from test1, test2 where test1.c = test2.c order by 1,3;

drop table if exists test1;
create table test1 (c1 set('a','b','c','d'), c2 set('d','c','b','a'));
insert into test1 values ('a', 'a'), ('b', 'b'),('c', 'c'),('d', 'd');
select test1.c1+0, test1.c1, test1.c2+0, test1.c2 from test1 where test1.c1 > test1.c2;
select test1.c1+0, test1.c1, test1.c2+0, test1.c2 from test1 where test1.c1 >= test1.c2;
select test1.c1+0, test1.c1, test1.c2+0, test1.c2 from test1 where test1.c1 < test1.c2;
select test1.c1+0, test1.c1, test1.c2+0, test1.c2 from test1 where test1.c1 <= test1.c2;
select test1.c1+0, test1.c1, test1.c2+0, test1.c2 from test1 where test1.c1 != test1.c2;
select test1.c1+0, test1.c1, test1.c2+0, test1.c2 from test1 where test1.c1 = test1.c2;
select t1.c1+0, t1.c1, t2.c2+0, t2.c2 from test1 as t1, test1 as t2 where t1.c1 > t2.c2 order by 1,3;
select t1.c1+0, t1.c1, t2.c2+0, t2.c2 from test1 as t1, test1 as t2 where t1.c1 >= t2.c2 order by 1,3;
select t1.c1+0, t1.c1, t2.c2+0, t2.c2 from test1 as t1, test1 as t2 where t1.c1 < t2.c2 order by 1,3;
select t1.c1+0, t1.c1, t2.c2+0, t2.c2 from test1 as t1, test1 as t2 where t1.c1 <= t2.c2 order by 1,3;
select t1.c1+0, t1.c1, t2.c2+0, t2.c2 from test1 as t1, test1 as t2 where t1.c1 != t2.c2 order by 1,3;
select t1.c1+0, t1.c1, t2.c2+0, t2.c2 from test1 as t1, test1 as t2 where t1.c1 = t2.c2 order by 1,3;

drop table if exists t1;
drop table if exists t2;
create table t1 (c1 set('a', 'b'));
create table t2 (c1 set('c', 'd'));
insert into t1 values ('b');
insert into t2 values ('d'); 
select * from t1 join t2 on t1.c1 = t2.c1;  -- hash by string
explain (costs off) select * from t1 join t2 on t1.c1 = t2.c1;
set enable_hashjoin = off;
select * from t1 join t2 on t1.c1 = t2.c1;  -- sort merge join, compare by string
set enable_mergejoin = off;
explain (costs off) select * from t1 join t2 on t1.c1 = t2.c1; -- nest loop, compare by string
select * from t1 join t2 on t1.c1 = t2.c1;
select * from t1 join t2 on t1.c1 != t2.c1;
set enable_mergejoin = on;
select * from t1 join t2 on t1.c1 != t2.c1;
explain (costs off) select * from t1 join t2 on t1.c1 != t2.c1;

-- set label length >= 127 select test
drop table if exists t1;
create table t1 (
  c1 set('111111111111111111111a111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111'),
  c2 set('字符串长度测试111111111111111111111a111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊')
);
insert into t1 values ('111111111111111111111a111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111', null);
insert into t1 values (1, '字符串长度测试111111111111111111111a111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊');
select c1 from t1;
prepare sel_set1(int) as select * from t1 where c1 = $1; -- bind as int
execute sel_set1(1);
prepare sel_set2(int) as select * from t1 where c2 = $1;
execute sel_set2(1);
prepare sel_set3 as select * from t1 where c2 = $1;
execute sel_set3('字符串长度测试111111111111111111111a111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊');
select find_in_set('111111111111111111111a111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111', c1),
       find_in_set('字符串长度测试111111111111111111111a111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊', c2)
  from t1;

drop table if exists t2;
create table t2 (
  c1 set('', '1', '111111111111111111111a111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111', '字符串长度测试111111111111111111111a111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊'),
  c2 set('1', '', '3'),
  c3 set('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31','32','33','34','35','36','37','38','39','40','41','42','43','44','45','46','47','48','49','50','51','52','53','54','55','56','57','58','59','60','61','62','63','64')
);
select pg_catalog.pg_get_tabledef('t2');

-- test '' as set member
-- 1. define set type not include ''
drop table if exists t1;
create table t1 (c1 set('a', 'b') not null default '');
insert into t1 values ('');
insert into t1 values (default);
select c1, c1+0 from t1; -- expect value 0
insert into t1 values ('a,,b'); -- expect error
COPY t1(c1) FROM stdin WITH NULL 'NULL' CSV QUOTE '"' DELIMITER ',' ESCAPE '"';
""
\.

-- 2. define set type include ''
drop table if exists t2;
create table t2 (c1 set('a', '', 'b') default '');
insert into t2 values (''); -- expect value 0, not 2
insert into t2 values (default);
select c1, c1+0 from t2;
insert into t2 values ('a,,b'); -- expect 2, not 0
insert into t2 values (',a,b'); -- expect 2, not 0
COPY t2(c1) FROM stdin WITH NULL 'NULL' CSV QUOTE '"' DELIMITER ',' ESCAPE '"';
""
\.
select c1, c1+0 from t2 order by 2;
drop table t1;
drop table t2;

-- test check duplicate set scenes
-- 1. column charset/collate
drop table if exists test_set_key1;
create table test_set_key1(a set('', '  '));
create table test_set_key1(a set('', '  ') collate utf8_bin);
create table test_set_key1(a set('a', 'a  '));
create table test_set_key1(a set('a', 'a  ') collate utf8_bin);
create table test_set_key1(a set('aaa', 'AAA  ') collate utf8_general_ci);
create table test_set_key1(a set('高斯sS', '高斯ŠŠ  ') collate utf8_general_ci);
create table test_set_key1(a set('aaa', 'aaA   ') charset 'utf8');
create table test_set_key1(a set('aaa', 'aaA   ') charset 'gbk');
create table test_set_key1(a set('aaa', 'aaA   ') charset 'gb18030');

--succeed scenes
create table t_column_collation1(a set('a', 'b', 'A','B') collate utf8_bin);
create table t_column_collation2(a set('a', 'b', 'A','B') collate utf8_bin);
create table t_column_collation3(a set('aaa', 'bbb') charset 'utf8');
create table t_column_collation4(a set('a', 'b', 'A','B') collate utf8_bin);
create table t_column_collation5(a set('高斯DB','1') character set utf8mb4 collate utf8mb4_general_ci default '高斯db');
drop table t_column_collation1;
drop table t_column_collation2;
drop table t_column_collation3;
drop table t_column_collation4;
drop table t_column_collation5;

-- 2. table charset/collate
create table test_set_key1(a set('aaa', 'aaA   ')) charset 'utf8';
create table test_set_key1(a set('aaa', 'aaA   ')) collate 'utf8_general_ci';
create table test_set_key1(a set('aaa', 'aaA   ')) charset 'gbk';
create table test_set_key1(a set('aaa', 'aaa   ')) charset 'gbk' collate gbk_bin;
create table test_set_key1(a set('aaa', 'aaA   ')) charset 'gb18030';
create table test_set_key1(a set('aaa', 'aaa   ')) charset 'gb18030' collate gb18030_bin;

--succeed scenes
create table t_table_collation1(a set('aaa', 'bbb')) charset 'utf8';
create table t_table_collation2(a set('aaa', 'aaA   ') collate 'utf8_bin')charset 'utf8';
drop table t_table_collation1;
drop table t_table_collation2;

-- 3. schema charset/collate
create schema set_test_schema1  charset = utf8;
set current_schema='set_test_schema1';

create table t(a set('a', 'A'));  --fail
create table t(a set('a', 'b'));  -- succeed
alter table t add column b set('a', 'A', 'b'); -- fail
alter table t add column b set('a', 'A', 'b') collate utf8_bin; --succeed
insert into t values('a  ', 'a  ');
insert into t values('a  ', 'A  ');
insert into t values('B  ', 'A  ');
insert into t values('a  ', 'B  ');  --fail
drop table t;

create schema set_test_schema2  charset = utf8mb4 collate = utf8mb4_bin;
set current_schema='set_test_schema2';
create table t(a set('a', 'A'));  -- succeed
alter table t add column b set('a', 'A', 'b') collate utf8_general_ci; --fail
alter table t add column b set('a', 'A', 'b'); -- succeed
insert into t values ('a  ', 'b  ');
insert into t values ('a  ', 'B  '); -- fail
drop table t;

drop schema set_test_schema1;
drop schema set_test_schema2;
\c test
SET b_format_behavior_compat_options = 'enable_multi_charset';

-- test insert value with collation
-- 1. insert value length ignore end space
create table test_set_len (col SET('ddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd') collate utf8_bin);
insert into test_set_len values ('ddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd                     ');
drop table test_set_len;

-- 2. insert value while collate is binary

-- 3. insert value while collate like _bin
create table test_collation2(a set('aaa', 'AAA','高斯SS', '高斯ss') collate utf8_bin);
insert into test_collation2 values('aaa  ');
insert into test_collation2 values('aaa');
insert into test_collation2 values('AAA');
insert into test_collation2 values('AAA  ');
insert into test_collation2 values('高斯ss   ');
insert into test_collation2 values('高斯SS   ');
insert into test_collation2 values('高斯sS   '); --failed
insert into test_collation2 values('高斯ss,高斯ss   ');

-- 4. insert value while collate like _ci
create table test_collation3(a set('高斯sS', '汉字sS', 'aaa', 'bbb')) charset utf8;
select pg_get_tabledef('test_collation3');

insert into test_collation3 values('高斯sS   ');
insert into test_collation3 values('高斯ss,高斯ss   ');
insert into test_collation3 values('高斯ŠŠ');
insert into test_collation3 values('汉字sS   ');
insert into test_collation3 values('汉字ss,汉字ss   ');
insert into test_collation3 values('汉字ŠŠ');
insert into test_collation3 values('aaa  ');
insert into test_collation3 values('aaa');
insert into test_collation3 values('AAA  ');

-- test update with colalte
create table test_update(a set('a', 'A', 'b'))collate utf8_bin;
insert into test_update values('a'),('A');
update test_update set a = 'b' where a = 'a';
update test_update set a = 'b' where a = 'a' collate utf8_general_ci;
select * from test_update;
update test_update set a = 'a' where a = 'B' collate utf8_general_ci;
select * from test_update;
drop table test_update;

-- test operator
create table test_collation_op(a set('a', 'A', 'b', 'B'), b set('a', 'A', 'b', 'B')) collate utf8_bin;
insert into test_collation_op values('a', 'A'),('A','a'),('b','B'),('B','b');

-- test '='
select a from test_collation2 where a = 'aaa';
select a from test_collation2 where a = '高斯ss' collate utf8_bin;
select a from test_collation2 where a = 'aaa' collate "utf8_general_ci"; -- set = text
select a from test_collation2 where a = '高斯ss' collate utf8_general_ci;

select a from test_collation3 where a = '高斯ss';
select a from test_collation3 where a = '高斯ss' collate utf8_bin;
select a from test_collation3 where a = 'aaa' collate "utf8_general_ci";

select a from test_collation2 where 'aaa' = a collate "utf8_general_ci"; -- text = set

select
  distinct t2.a, t3.a 
from
  test_collation2 as t2,
  test_collation2 as t3
where t2.a = t3.a  collate "utf8_general_ci"; -- set = set

select
  distinct t2.a, t3.a 
from
  test_collation2 as t2,
  test_collation2 as t3
where t2.a = t3.a;

-- '<>'
select a, b from test_collation_op where a <> b; -- set <> other set
select a, b from test_collation_op where a <> b collate 'utf8_general_ci';
select a, b from test_collation_op where a <> 'a'; -- set <> text
select a, b from test_collation_op where a <> 'a' collate 'utf8_general_ci';
select a, b from test_collation_op where 'a' <> a;
select a, b from test_collation_op where 'a' <> a collate 'utf8_general_ci'; -- text <> set

-- '<'
select a, b from test_collation_op where a < b; -- set < other set
select a from test_collation_op where a < b collate 'utf8_general_ci';
select a from test_collation_op where a < 'C'; -- set < text
select a from test_collation_op where a < 'C' collate 'utf8_general_ci';
select a from test_collation_op where 'B' < a;
select a from test_collation_op where 'B' < a collate 'utf8_general_ci';

-- '<='
select a, b from test_collation_op where a <= b; -- set <= other set
select a from test_collation_op where a <= b collate 'utf8_general_ci';
select a from test_collation_op where a <= 'A'; -- set <= text
select a from test_collation_op where a <= 'A' collate 'utf8_general_ci';
select a from test_collation_op where 'B' <= a; -- text <= set
select a from test_collation_op where 'B' <= a collate 'utf8_general_ci';

-- '>'
select a, b from test_collation_op where a > b; -- set > other set
select a from test_collation_op where a > b collate 'utf8_general_ci';
select a from test_collation_op where a > 'B'; -- set > text
select a from test_collation_op where a > 'B' collate 'utf8_general_ci';
select a from test_collation_op where 'C' > a;
select a from test_collation_op where 'C' > a collate 'utf8_general_ci';

-- '>='
select a, b from test_collation_op where a >= b; -- set >= other set
select a from test_collation_op where a >= b collate 'utf8_general_ci';
select a from test_collation_op where a >= 'B'; -- set >= text
select a from test_collation_op where a >= 'B' collate 'utf8_general_ci';
select a from test_collation_op where 'C' >= a;
select a from test_collation_op where 'C' >= a collate 'utf8_general_ci';

-- test order by
select a from test_collation2 order by a;
select a from test_collation2 order by a collate 'utf8_general_ci';
select a from test_collation2 order by a collate 'utf8_bin';

select a from test_collation3 order by a;
select a from test_collation3 order by a collate 'utf8_general_ci';
select a from test_collation3 order by a collate 'utf8_bin';

-- test distinct
select distinct a from test_collation2;
select distinct a collate 'utf8_bin' from test_collation2;
select distinct a collate 'utf8_general_ci' from test_collation2;

select distinct a from test_collation3;
select distinct a collate 'utf8_bin' from test_collation3;
select distinct a collate 'utf8_general_ci' from test_collation3;

-- test like
select a from test_collation2 where a like 'aa%';
select a from test_collation2 where a like 'aa%' collate 'utf8_general_ci';

select a from test_collation3 where a like 'aa%';
select a from test_collation3 where a like 'aa%' collate 'utf8_general_ci';

-- test alter table modify/add
alter table test_collation3 modify a char(10) collate utf8_bin;
select a from test_collation3 where a = '高斯ss';
select a from test_collation3 where a = '高斯sS' collate utf8_bin;
alter table test_collation3 modify a set('高斯sS', '汉字sS', 'aaa', 'bbb');
select pg_get_tabledef('test_collation3');
insert into test_collation3 values('高斯ŠŠ  ');
alter table test_collation3 add b set('a','b', 'c');
insert into test_collation3(b) values('A'), ('b  '), (3);

-- test unique/primary key
create table t_collation_set5(a set('aaa', 'bbb') collate utf8_general_ci unique);
insert into t_collation_set5 values('aaa');
insert into t_collation_set5 values('aaa  ');
insert into t_collation_set5 values('AAA  ');

create table t_collation_set6(a set('aaa', 'bbb') collate utf8_general_ci primary key);
insert into t_collation_set6 values('aaa  ');
insert into t_collation_set6 values('aaa');
insert into t_collation_set6 values('AAA  ');

drop table t_collation_set5;
drop table t_collation_set6;

-- test join
create table test_join1(
  c_char char(10),
  c_varchar varchar(10),
  c_nvarchar2 nvarchar2(10),
  c_text text,
  c_set set('a')
) collate utf8_bin;

create table test_join2(
  c_set set('A')
) collate utf8_bin;

insert into test_join1 values('a  ', 'a  ', 'a  ', 'a  ','a  ');
insert into test_join2 values('A');

select t1.c_char, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set = t1.c_char;
select t1.c_char, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set = t1.c_char collate 'utf8_general_ci';
select t1.c_char, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set <> t1.c_char;
select t1.c_char, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set <> t1.c_char collate 'utf8_general_ci';
select t1.c_char, t2.c_set from test_join1 t1 join test_join2 t2 on t1.c_char > t2.c_set;
select t1.c_char, t2.c_set from test_join1 t1 join test_join2 t2 on t1.c_char > t2.c_set collate 'utf8_general_ci';
select t1.c_char, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set >= t1.c_char;
select t1.c_char, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set >= t1.c_char collate 'utf8_general_ci';
select t1.c_char, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set < t1.c_char;
select t1.c_char, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set < t1.c_char collate 'utf8_general_ci';
select t1.c_char, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set <= t1.c_char;
select t1.c_char, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set <= t1.c_char collate 'utf8_general_ci';

select t1.c_varchar, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set = t1.c_varchar;
select t1.c_varchar, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set = t1.c_varchar collate 'utf8_general_ci';
select t1.c_varchar, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set <> t1.c_varchar;
select t1.c_varchar, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set <> t1.c_varchar collate 'utf8_general_ci';
select t1.c_varchar, t2.c_set from test_join1 t1 join test_join2 t2 on t1.c_varchar > t2.c_set;
select t1.c_varchar, t2.c_set from test_join1 t1 join test_join2 t2 on t1.c_varchar > t2.c_set collate 'utf8_general_ci';
select t1.c_varchar, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set >= t1.c_varchar;
select t1.c_varchar, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set >= t1.c_varchar collate 'utf8_general_ci';
select t1.c_varchar, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set < t1.c_varchar;
select t1.c_varchar, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set < t1.c_varchar collate 'utf8_general_ci';
select t1.c_varchar, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set <= t1.c_varchar;
select t1.c_varchar, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set <= t1.c_varchar collate 'utf8_general_ci';

select t1.c_nvarchar2, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set = t1.c_nvarchar2;
select t1.c_nvarchar2, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set = t1.c_nvarchar2 collate 'utf8_general_ci';
select t1.c_nvarchar2, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set <> t1.c_nvarchar2;
select t1.c_nvarchar2, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set <> t1.c_nvarchar2 collate 'utf8_general_ci';
select t1.c_nvarchar2, t2.c_set from test_join1 t1 join test_join2 t2 on t1.c_nvarchar2 > t2.c_set;
select t1.c_nvarchar2, t2.c_set from test_join1 t1 join test_join2 t2 on t1.c_nvarchar2 > t2.c_set collate 'utf8_general_ci';
select t1.c_nvarchar2, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set >= t1.c_nvarchar2;
select t1.c_nvarchar2, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set >= t1.c_nvarchar2 collate 'utf8_general_ci';
select t1.c_nvarchar2, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set < t1.c_nvarchar2;
select t1.c_nvarchar2, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set < t1.c_nvarchar2 collate 'utf8_general_ci';
select t1.c_nvarchar2, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set <= t1.c_nvarchar2;
select t1.c_nvarchar2, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set <= t1.c_nvarchar2 collate 'utf8_general_ci';

select t1.c_text, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set = t1.c_text;
select t1.c_text, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set = t1.c_text collate 'utf8_general_ci';
select t1.c_text, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set <> t1.c_text;
select t1.c_text, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set <> t1.c_text collate 'utf8_general_ci';
select t1.c_text, t2.c_set from test_join1 t1 join test_join2 t2 on t1.c_text > t2.c_set;
select t1.c_text, t2.c_set from test_join1 t1 join test_join2 t2 on t1.c_text > t2.c_set collate 'utf8_general_ci';
select t1.c_text, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set >= t1.c_text;
select t1.c_text, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set >= t1.c_text collate 'utf8_general_ci';
select t1.c_text, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set < t1.c_text;
select t1.c_text, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set < t1.c_text collate 'utf8_general_ci';
select t1.c_text, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set <= t1.c_text;
select t1.c_text, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set <= t1.c_text collate 'utf8_general_ci';

select t1.c_set, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set = t1.c_set;
select t1.c_set, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set = t1.c_set collate 'utf8_general_ci';
select t1.c_set, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set <> t1.c_set;
select t1.c_set, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set <> t1.c_set collate 'utf8_general_ci';
select t1.c_set, t2.c_set from test_join1 t1 join test_join2 t2 on t1.c_set > t2.c_set;
select t1.c_set, t2.c_set from test_join1 t1 join test_join2 t2 on t1.c_set > t2.c_set collate 'utf8_general_ci';
select t1.c_set, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set >= t1.c_set;
select t1.c_set, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set >= t1.c_set collate 'utf8_general_ci';
select t1.c_set, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set < t1.c_set;
select t1.c_set, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set < t1.c_set collate 'utf8_general_ci';
select t1.c_set, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set <= t1.c_set;
select t1.c_set, t2.c_set from test_join1 t1 join test_join2 t2 on t2.c_set <= t1.c_set collate 'utf8_general_ci';

drop table test_join1;
drop table test_join2;
-- test partition table with collate
create table test_part_tab1(c1 int, c2 set('a', 'b', 'A', 'B'))charset utf8 partition by range(c1)  --fail
(
    partition test_set_tab_p1 values less than (5),
    partition test_set_tab_p2 values less than (10)
);

create table test_part_tab1(c1 int, c2 set('a', 'b', 'A', 'B'))charset utf8 collate utf8_bin partition by range(c1)  -- success
(
    partition test_set_tab_p1 values less than (5),
    partition test_set_tab_p2 values less than (10)
);
select pg_get_tabledef('test_part_tab1');
insert into test_part_tab1 values(1, 'a  ');
insert into test_part_tab1 values(6, 'A  ');
insert into test_part_tab1 values(2, 'b  ');
insert into test_part_tab1 values(7, 'B  ');

create table test_part_tab2(c1 int, c2 set('a', 'b', 'c', 'd'))charset utf8 partition by range(c1) -- success
(
    partition test_set_tab_p1 values less than (5),
    partition test_set_tab_p2 values less than (10)
);
insert into test_part_tab2 values(1, 'a  ');
insert into test_part_tab2 values(6, 'A  ');
insert into test_part_tab2 values(2, 'b  ');
insert into test_part_tab2 values(7, 'B  ');

drop table test_part_tab1;
drop table test_part_tab2;

-- test local/global temp table with collate
create local temp table test_temp_tab1 (c1 set('a', 'b')) charset utf8;
insert into test_temp_tab1 values('a  ');
insert into test_temp_tab1 values('A  ');

create global temp table test_temp_tab2 (c1 set('a', 'b')) charset utf8 collate utf8_bin;
insert into test_temp_tab2 values('a  ');
insert into test_temp_tab2 values('A  '); --fail

drop table test_temp_tab1;
drop table test_temp_tab2;
-- test ustore
create table test_ustore1(
  c1 set('a', 'b', 'c', 'd'),
  c2 set('a', 'b','A', 'B') collate 'gbk_bin'
) charset utf8 with (STORAGE_TYPE = ustore);

select pg_get_tabledef('test_ustore1');
insert into test_ustore1 values('a  ', 'a  ');
insert into test_ustore1 values('A  ', 'A  ');
select c2 from test_ustore1 where c2 = 'a';
select c2 from test_ustore1 where c2 = 'a' collate 'utf8_general_ci';
select c2 from test_ustore1 where c2 = c1;
drop table test_ustore1;

-- test cstore(not supported)
create table test_cstore1(c1 set('a', 'b', 'c', 'd') collate 'utf8_bin') with (ORIENTATION = column);

-- test check constraint with collate
create table test_check1(a set('a', 'b', 'c') check(a >= 'b'))charset utf8;
insert into test_check1 values('a');
insert into test_check1 values('B  ');
insert into test_check1 values('C');
insert into test_check1 values('c');
drop table test_check1;