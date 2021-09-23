set enable_default_ustore_table = on;

create schema test_unione_delete_multi_col;
set current_schema='test_unione_delete_multi_col';

-- create composite type
create type composite_type as (col_int integer, col_decimal decimal, col_bool boolean, col_money money, col_text text, col_date date, col_geo point, col_net cidr, col_circle circle);

-- create tables
create table integer_table (
  c1 tinyint,
  c2 smallint,
  c3 integer not null,
  c4 bigint,
  c5 int1,
  c6 int2,
  c7 int4,
  c8 int8,
  c9 int,
  c10 binary_integer
) with (storage_type=USTORE);

create table decimal_table (
  c1 decimal,
  c2 decimal(38),
  c3 decimal(38,7),
  c4 numeric,
  c5 numeric(38),
  c6 numeric(38,7),
  c7 number,
  c8 number(38),
  c9 number(38,7),
  c10 dec,
  c11 dec(38),
  c12 dec(38,7)
) with (storage_type=USTORE);

create table numeric_table (
  c1 real,
  c2 double precision,
  c3 smallserial,
  c4 serial,
  c5 bigserial,
  c6 float,
  c7 float4,
  c8 float8,
  c9 binary_double
) with (storage_type=USTORE);

create table mix_table (
  c1 money,
  c2 boolean,
  c3 bytea,
  c4 Oid
) with (storage_type=USTORE);

create table time_table (
  c1 date,
  c2 time,
  c3 time(6),
  c4 time without time zone,
  c5 time with time zone,
  c6 time(6) without time zone,
  c7 time(6) with time zone,
  c8 timestamp,
  c9 timestamp(6),
  c10 timestamp without time zone,
  c11 timestamp with time zone,
  c12 timestamp(6) without time zone,
  c13 timestamp(6) with time zone,
  c14 smalldatetime,
  c15 interval,
  c16 interval day (3) to second (4),
  c17 interval year,
  c18 interval second (6),
  c19 interval hour to second(6),
  c20 reltime,
  c21 timetz,
  c22 tinterval,
  c23 abstime
) with (storage_type=USTORE);

create table character_table (
  c1 char,
  c2 char(10),
  c3 character,
  c4 character(20),
  c5 nchar,
  c6 nchar(30),
  c7 varchar,
  c8 varchar(40),
  c9 character varying(200),
  c10 varchar2(50),
  c11 nvarchar2(60),
  c12 clob,
  c13 text,
  c14 "char"
) with (storage_type=USTORE);

create table network_table (
  c1 cidr not null,
  c2 inet
) with (storage_type=USTORE);

create table bit_table (
  c1 bit,
  c2 bit varying
) with (storage_type=USTORE);

create table text_search_table (
  c1 tsvector,
  c2 tsquery
) with (storage_type=USTORE);

create table geometric_table (
  c1 point,
  c2 lseg,
  c3 box,
  c4 path,
  c5 path,
  c6 polygon,
  c7 circle
) with (storage_type=USTORE);

create table t1 (
  c1 int[],
  c2 int[][],
  c3 blob,
  c4 raw,
  c5 composite_type,
  c6 box,
  c7 name,
  c8 macaddr,
  c9 uuid
) with (storage_type=USTORE);

-- create tables for verification
create table v_integer_table (
  c1 tinyint,
  c2 smallint,
  c3 integer not null,
  c4 bigint,
  c5 int1,
  c6 int2,
  c7 int4,
  c8 int8,
  c9 int,
  c10 binary_integer
) with (orientation=row);

create table v_decimal_table (
  c1 decimal,
  c2 decimal(38),
  c3 decimal(38,7),
  c4 numeric,
  c5 numeric(38),
  c6 numeric(38,7),
  c7 number,
  c8 number(38),
  c9 number(38,7),
  c10 dec,
  c11 dec(38),
  c12 dec(38,7)
) with (orientation=row);

create table v_numeric_table (
  c1 real,
  c2 double precision,
  c3 smallserial,
  c4 serial,
  c5 bigserial,
  c6 float,
  c7 float4,
  c8 float8,
  c9 binary_double
) with (orientation=row);

create table v_mix_table (
  c1 money,
  c2 boolean,
  c3 bytea,
  c4 Oid
) with (orientation=row);

create table v_time_table (
  c1 date,
  c2 time,
  c3 time(6),
  c4 time without time zone,
  c5 time with time zone,
  c6 time(6) without time zone,
  c7 time(6) with time zone,
  c8 timestamp,
  c9 timestamp(6),
  c10 timestamp without time zone,
  c11 timestamp with time zone,
  c12 timestamp(6) without time zone,
  c13 timestamp(6) with time zone,
  c14 smalldatetime,
  c15 interval,
  c16 interval day (3) to second (4),
  c17 interval year,
  c18 interval second (6),
  c19 interval hour to second(6),
  c20 reltime,
  c21 timetz,
  c22 tinterval,
  c23 abstime
) with (orientation=row);

create table v_character_table (
  c1 char,
  c2 char(10),
  c3 character,
  c4 character(20),
  c5 nchar,
  c6 nchar(30),
  c7 varchar,
  c8 varchar(40),
  c9 character varying(200),
  c10 varchar2(50),
  c11 nvarchar2(60),
  c12 clob,
  c13 text,
  c14 "char"
) with (orientation=row);

create table v_network_table (
  c1 cidr not null,
  c2 inet
) with (orientation=row);

create table v_bit_table (
  c1 bit,
  c2 bit varying
) with (orientation=row);

create table v_text_search_table (
  c1 tsvector,
  c2 tsquery
) with (orientation=row);

create table v_geometric_table (
  c1 point,
  c2 lseg,
  c3 box,
  c4 path,
  c5 path,
  c6 polygon,
  c7 circle
) with (orientation=row);

create table v_t1 (
  c1 int[],
  c2 int[][],
  c3 blob,
  c4 raw,
  c5 composite_type,
  c6 box,
  c7 name,
  c8 macaddr,
  c9 uuid
) with (orientation=row);

-- insert
insert into integer_table values(1,1,-2147483648,-9223372036854775808,1,1,1,1,1,1);
insert into v_integer_table values(1,1,-2147483648,-9223372036854775808,1,1,1,1,1,1);
insert into integer_table values(2,2,-224748364,-9223372036854775808,2,2,2,2,2,2);
insert into v_integer_table values(2,2,-224748364,-9223372036854775808,2,2,2,2,2,2);
insert into integer_table values(3,3,-334748364,-933337303685477580,3,3,3,3,3,3);
insert into v_integer_table values(3,3,-334748364,-933337303685477580,3,3,3,3,3,3);

insert into decimal_table values (123.456789,123.456789,123.456789,123.456789,123.456789,123.456789,246.12345,246.12345,246.12345,246.12345,246.12345,246.12345);
insert into v_decimal_table values (123.456789,123.456789,123.456789,123.456789,123.456789,123.456789,246.12345,246.12345,246.12345,246.12345,246.12345,246.12345);
insert into decimal_table values (234.456789,234.456789,234.456789,234.456789,234.456789,234.456789,246.23445,246.23445,246.23445,246.23445,246.23445,246.23445);
insert into v_decimal_table values (234.456789,234.456789,234.456789,234.456789,234.456789,234.456789,246.23445,246.23445,246.23445,246.23445,246.23445,246.23445);
insert into decimal_table values (345.456789,345.456789,345.456789,345.456789,345.456789,345.456789,246.34545,246.34545,246.34545,246.34545,246.34545,246.34545);
insert into v_decimal_table values (345.456789,345.456789,345.456789,345.456789,345.456789,345.456789,246.34545,246.34545,246.34545,246.34545,246.34545,246.34545);

insert into numeric_table values (123.456789,123.456789,123.456789,123.456789,246.12345,246.12345,246.12345,246.12345,246.12345);
insert into v_numeric_table values (123.456789,123.456789,123.456789,123.456789,246.12345,246.12345,246.12345,246.12345,246.12345);
insert into numeric_table values (234.456789,234.456789,234.456789,234.456789,246.23445,246.23445,246.23445,246.23445,246.23445);
insert into v_numeric_table values (234.456789,234.456789,234.456789,234.456789,246.23445,246.23445,246.23445,246.23445,246.23445);
insert into numeric_table values (345.456789,345.456789,345.456789,345.456789,246.34545,246.34545,246.34545,246.34545,246.34545);
insert into v_numeric_table values (345.456789,345.456789,345.456789,345.456789,246.34545,246.34545,246.34545,246.34545,246.34545);

insert into mix_table values (144,true,E'\\000',19981122);
insert into v_mix_table values (144,true,E'\\000',19981122);
insert into mix_table values (154,true,E'\\110',19981122);
insert into v_mix_table values (154,true,E'\\110',19981122);
insert into mix_table values (1444,true,E'\\100',19981122);
insert into v_mix_table values (1444,true,E'\\100',19981122);

insert into network_table values ('192.168.1','192.168.1.226/24');
insert into v_network_table values ('192.168.1','192.168.1.226/24');
insert into network_table values ('192.169.1','192.169.1.226/24');
insert into v_network_table values ('192.169.1','192.169.1.226/24');
insert into network_table values ('192.169.1','192.169.1.226/12');
insert into v_network_table values ('192.169.1','192.169.1.226/12');

insert into bit_table values (B'0',B'00');
insert into v_bit_table values (B'0',B'00');
insert into bit_table values (B'0',B'10');
insert into v_bit_table values (B'0',B'10');
insert into bit_table values (B'0',B'01');
insert into v_bit_table values (B'0',B'01');

insert into text_search_table values (' a:1 s:2 d g','1|2|4|5|6');
insert into v_text_search_table values (' a:1 s:2 d g','1|2|4|5|6');
insert into text_search_table values (' a:1 s:2 d g','1|2|4|5|7');
insert into v_text_search_table values (' a:1 s:2 d g','1|2|4|5|7');
insert into text_search_table values (' a:1 s:2 d g','1|2|4|5|8');
insert into v_text_search_table values (' a:1 s:2 d g','1|2|4|5|8');

insert into geometric_table values ('(0.0000009,0.0000009)','((0.0000009,0.0000009),(0.0000018,0.0000018))','(0,0,100,100)','((1,1),(2,2),(3,3))','[(0,0),(-10,0),(-10,10)]','(2.0,0.0),(2.0,4.0),(0.0,0.0)','<(500,500),500>');
insert into v_geometric_table values ('(0.0000009,0.0000009)','((0.0000009,0.0000009),(0.0000018,0.0000018))','(0,0,100,100)','((1,1),(2,2),(3,3))','[(0,0),(-10,0),(-10,10)]','(2.0,0.0),(2.0,4.0),(0.0,0.0)','<(500,500),500>');
insert into geometric_table values ('(0.0000009,0.0000009)','((0.0000009,0.0000009),(0.0000006,0.0000006))','(0,0,100,100)','((1,1),(2,2),(3,3))','[(0,0),(-10,0),(-10,10)]','(2.0,0.0),(2.0,4.0),(0.0,0.0)','<(500,500),500>');
insert into v_geometric_table values ('(0.0000009,0.0000009)','((0.0000009,0.0000009),(0.0000006,0.0000006))','(0,0,100,100)','((1,1),(2,2),(3,3))','[(0,0),(-10,0),(-10,10)]','(2.0,0.0),(2.0,4.0),(0.0,0.0)','<(500,500),500>');
insert into geometric_table values ('(0.0000009,0.0000009)','((0.0000009,0.0000009),(0.0000006,0.0000006))','(0,0,100,100)','((1,1),(2,2),(3,3))','[(0,0),(-10,0),(-10,10)]','(2.0,0.0),(2.0,4.0),(0.0,0.0)','<(250,250),250>');
insert into v_geometric_table values ('(0.0000009,0.0000009)','((0.0000009,0.0000009),(0.0000006,0.0000006))','(0,0,100,100)','((1,1),(2,2),(3,3))','[(0,0),(-10,0),(-10,10)]','(2.0,0.0),(2.0,4.0),(0.0,0.0)','<(250,250),250>');

insert into time_table values ('2020-09-04','00:00:01','21:21:21','21:21:21', '21:21:21 pst','04:05:06','04:05:06 PST','2010-12-12','2010-12-12','2010-12-12','2010-12-12 pst','2010-12-12','2010-12-12 pst','2003-04-12 04:05:06','2 day 13:24:56',INTERVAL '3' DAY,interval '2' year,INTERVAL '2333' second,INTERVAL '5:23:35.5555' hour to second,'3 days','1984-2-6 01:00:30+8','["Feb 10, 1947 23:59:12" "Jan 14, 1973 03:14:21"]','Aug 15 14:23:19 1983');
insert into v_time_table values ('2020-09-04','00:00:01','21:21:21','21:21:21', '21:21:21 pst','04:05:06','04:05:06 PST','2010-12-12','2010-12-12','2010-12-12','2010-12-12 pst','2010-12-12','2010-12-12 pst','2003-04-12 04:05:06','2 day 13:24:56',INTERVAL '3' DAY,interval '2' year,INTERVAL '2333' second,INTERVAL '5:23:35.5555' hour to second,'3 days','1984-2-6 01:00:30+8','["Feb 10, 1947 23:59:12" "Jan 14, 1973 03:14:21"]','Aug 15 14:23:19 1983');
insert into time_table values ('2020-09-04','00:00:01','21:21:21','21:21:21', '21:21:21 pst','04:05:06','04:05:06 PST','2010-12-12','2010-12-12','2010-12-12','2010-12-12 pst','2010-12-12','2010-12-12 pst','2003-04-12 04:05:06','2 day 13:24:56',INTERVAL '3' DAY,interval '2' year,INTERVAL '2333' second,INTERVAL '5:23:35.5555' hour to second,'3 days','1984-2-6 01:00:30+8','["Feb 10, 1947 23:59:12" "Jan 14, 1973 03:14:21"]','Aug 15 14:23:19 1998');
insert into v_time_table values ('2020-09-04','00:00:01','21:21:21','21:21:21', '21:21:21 pst','04:05:06','04:05:06 PST','2010-12-12','2010-12-12','2010-12-12','2010-12-12 pst','2010-12-12','2010-12-12 pst','2003-04-12 04:05:06','2 day 13:24:56',INTERVAL '3' DAY,interval '2' year,INTERVAL '2333' second,INTERVAL '5:23:35.5555' hour to second,'3 days','1984-2-6 01:00:30+8','["Feb 10, 1947 23:59:12" "Jan 14, 1973 03:14:21"]','Aug 15 14:23:19 1998');
insert into time_table values ('2020-09-04','00:00:01','21:21:21','21:21:21', '21:21:21 pst','04:05:06','04:05:06 PST','2010-12-12','2010-12-12','2010-12-12','2010-12-12 pst','2010-12-12','2010-12-12 pst','2003-04-12 04:05:06','2 day 13:24:56',INTERVAL '3' DAY,interval '2' year,INTERVAL '2333' second,INTERVAL '5:23:35.5555' hour to second,'3 days','1984-2-6 01:00:30+8','["Feb 10, 1947 23:59:12" "Jan 14, 2019 03:14:21"]','Mar 15 14:23:19 1998');
insert into v_time_table values ('2020-09-04','00:00:01','21:21:21','21:21:21', '21:21:21 pst','04:05:06','04:05:06 PST','2010-12-12','2010-12-12','2010-12-12','2010-12-12 pst','2010-12-12','2010-12-12 pst','2003-04-12 04:05:06','2 day 13:24:56',INTERVAL '3' DAY,interval '2' year,INTERVAL '2333' second,INTERVAL '5:23:35.5555' hour to second,'3 days','1984-2-6 01:00:30+8','["Feb 10, 1947 23:59:12" "Jan 14, 2019 03:14:21"]','Mar 15 14:23:19 1998');

insert into character_table values ('a','hello','a','hello','a','hello','a','hello','99991231','99991231','99991231','clob','hello how are you','HIHIHI');
insert into v_character_table values ('a','hello','a','hello','a','hello','a','hello','99991231','99991231','99991231','clob','hello how are you','HIHIHI');
insert into character_table values ('b','hello','b','hello','b','hello','b','hello','99991231','99991231','99991231','clob','hello how are you','HIHIHI');
insert into v_character_table values ('b','hello','b','hello','b','hello','b','hello','99991231','99991231','99991231','clob','hello how are you','HIHIHI');
insert into character_table values ('c','hello','c','hello','c','hello','c','hello','99991231','99991231','99991231','clob','hello how are you','HIHIHI');
insert into v_character_table values ('c','hello','c','hello','c','hello','c','hello','99991231','99991231','99991231','clob','hello how are you','HIHIHI');

insert into t1 values('{1,2,3}','{{1},{2}}','aaaaaaaaaaa','DEADBEEF',row(214748,12345.122,false,2000,'hello','2020-09-04','(0.0000009,0.0000009)','192.168.1','<(250,250),250>'),'(0,0,100,100)','AAAAAA','08:00:2b:01:02:03','A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11');
insert into v_t1 values('{1,2,3}','{{1},{2}}','aaaaaaaaaaa','DEADBEEF',row(214748,12345.122,false,2000,'hello','2020-09-04','(0.0000009,0.0000009)','192.168.1','<(250,250),250>'),'(0,0,100,100)','AAAAAA','08:00:2b:01:02:03','A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11');
insert into t1 values('{1,2,3}','{{1},{2}}','aaaaaaaaaaa','DEADBEEF',row(214748,135.2468,true,2000,'helloo','2020-09-04','(0.0000009,0.0000009)','192.168.1','<(500,500),500>'),'(0,0,100,100)','BBBBBB','08:00:2b:01:02:03','A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11');
insert into v_t1 values('{1,2,3}','{{1},{2}}','aaaaaaaaaaa','DEADBEEF',row(214748,135.2468,true,2000,'helloo','2020-09-04','(0.0000009,0.0000009)','192.168.1','<(500,500),500>'),'(0,0,100,100)','BBBBBB','08:00:2b:01:02:03','A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11');

-- delete
delete from integer_table where c1=1;
delete from v_integer_table where c1=1;

delete from decimal_table where c6<200;
delete from v_decimal_table where c6<200;

delete from numeric_table where c3=123;
delete from v_numeric_table where c3=123;

delete from mix_table where c2=false;
delete from v_mix_table where c2=false;

delete from network_table where c2='192.169.1.226/12';
delete from v_network_table where c2='192.169.1.226/12';

delete from bit_table where c2=B'01';
delete from v_bit_table where c2=B'01';

delete from text_search_table where c2='1|2|4|5|6';
delete from v_text_search_table where c2='1|2|4|5|6';

delete from geometric_table where c7<'<(400,400)400>';
delete from v_geometric_table where c7<'<(400,400)400>';

delete from time_table where c3>'00:00:02';
delete from v_time_table where c3>'00:00:02';

delete from character_table where c1 <> 'a';
delete from v_character_table where c1 <> 'a';

delete from t1 where c4='DEADBEEF';
delete from v_t1 where c4='DEADBEEF';

create view view_insert_1 as (select * from integer_table except select * from v_integer_table);
create view view_insert_2 as (select * from decimal_table except select * from v_decimal_table);
create view view_insert_3 as (select * from numeric_table except select * from v_numeric_table);
create view view_insert_4 as (select * from mix_table except select * from v_mix_table);
create view view_insert_5 as (select * from network_table except select * from v_network_table);
create view view_insert_6 as (select * from bit_table except select * from v_bit_table);
create view view_insert_7 as (select * from text_search_table except select * from v_text_search_table);
create view view_insert_9 as (select * from time_table except select * from v_time_table);
create view view_insert_10 as (select * from character_table except select * from v_character_table);

select * from view_insert_1;
select * from view_insert_2;
select * from view_insert_3;
select * from view_insert_4;
select * from view_insert_5;
select * from view_insert_6;
select * from view_insert_7;
select * from view_insert_9;
select * from view_insert_10;

select * from geometric_table;
select * from v_geometric_table;

select * from t1;
select * from v_t1;

-- drop table, type and view
drop type composite_type cascade;

drop view view_insert_1 cascade;
drop view view_insert_2 cascade;
drop view view_insert_3 cascade;
drop view view_insert_4 cascade;
drop view view_insert_5 cascade;
drop view view_insert_6 cascade;
drop view view_insert_7 cascade;
drop view view_insert_9 cascade;
drop view view_insert_10 cascade;

drop table v_integer_table;
drop table v_decimal_table;
drop table v_numeric_table;
drop table v_mix_table;
drop table v_network_table;
drop table v_bit_table;
drop table v_text_search_table;
drop table v_geometric_table;
drop table v_time_table;
drop table v_character_table;
drop table v_t1;

drop table integer_table;
drop table decimal_table;
drop table numeric_table;
drop table mix_table;
drop table network_table;
drop table bit_table;
drop table text_search_table;
drop table geometric_table;
drop table time_table;
drop table character_table;
drop table t1;
