set enable_default_ustore_table = on;

create schema test_unione_composite_all;
set current_schema='test_unione_composite_all';

-- create type 
create type composite as (c1 int, c2 text, c3 boolean);
create type nested_composite_type as (c1 int, c2 composite);
create type composite_type_all as (
  c1 tinyint,
  c2 smallint, 
  c3 integer,
  c4 bigint,
  c5 int1,
  c6 int2,
  c7 int4,
  c8 int8,
  c9 int,
  c10 int[],
  c11 int[][],
  c12 binary_integer,
  c13 decimal,
  c14 decimal(38),
  c15 decimal(38,7),
  c16 numeric,
  c17 numeric(38),
  c18 numeric(38,7),
  c19 dec,
  c20 dec(38),
  c21 dec(38,7),
  c22 real,
  c23 double precision,
--  c24 smallserial,
--  c25 serial,
--  c26 bigserial,
  c27 float,
  c28 float4,
  c29 float8,
  c30 binary_double,
  c31 money,
  c32 boolean,
  c33 bytea,
  c34 Oid,
  c35 date,
  c36 time,
  c37 time(6),
  c38 time without time zone,
  c39 time with time zone,
  c40 time(6) without time zone,
  c41 time(6) with time zone,
  c42 timestamp,
  c43 timestamp(6),
  c44 timestamp without time zone,
  c45 timestamp with time zone,
  c46 timestamp(6) without time zone,
  c47 timestamp(6) with time zone,
  c48 smalldatetime,
  c49 interval,
  c50 interval day (3) to second (4),
  c51 interval year,
  c52 interval second (6),
  c53 interval hour to second (6),
  c54 reltime,
  c55 timetz,
  c56 tinterval,
  c57 abstime,
  c58 char,
  c59 char(10),
  c60 character,
  c61 character(20),
  c62 nchar,
  c63 nchar(30),
  c64 varchar,
  c65 varchar(40),
  c66 character varying(200),
  c67 varchar2(50),
  c68 nvarchar2(60),
  c69 clob,
  c70 text,
  c71 "char",
  c72 cidr,
  c73 inet,
  c74 bit,
  c75 bit varying,
  c76 tsvector,
  c77 tsquery,
  c78 point,
  c79 lseg,
  c80 box,
  c81 path,
  c82 path,
  c83 polygon,
  c84 circle,
  c85 blob,
  c86 raw,
  c87 box,
  c88 name,
  c89 macaddr,
  c90 uuid,
  c91 composite,
  c92 nested_composite_type);
  
-- create table 
create table t1 (c1 composite_type_all) with (storage_type=USTORE);
create table v_t1 (c1 composite_type_all) with (orientation=row);

-- insert
insert into t1 values (row(1,1,-2147483648,-9223372036854775808,1,1,1,1,1,'{1,2,3}','{{1},{2}}',123.456789,123.456789,123.456789,123.456789,123.456789,123.456789,246.12345,246.12345,246.12345,246.12345,123.456789,123.456789,246.12345,246.12345,246.12345,246.12345,144,true,E'\\000',19981122,'2020-09-04','00:00:01','21:21:21','21:21:21', '21:21:21 pst','04:05:06','04:05:06 PST','2010-12-12','2010-12-12','2010-12-12','2010-12-12 pst','2010-12-12','2010-12-12 pst','2003-04-12 04:05:06','2 day 13:24:56',INTERVAL '3' DAY,interval '2' year,INTERVAL '2333' second,INTERVAL '5:23:35.5555' hour to second,'3 days','1984-2-6 01:00:30+8','["Feb 10, 1947 23:59:12" "Jan 14, 1973 03:14:21"]','Aug 15 14:23:19 1983','a','hello','a','hello','a','hello','a','hello','99991231','99991231','99991231','clob','hello how are you','HIHIHI','192.168.1','192.168.1.226/24',B'0',B'00',' a:1 s:2 d g','1|2|4|5|6','(0.0000009,0.0000009)','((0.0000009,0.0000009),(0.0000018,0.0000018))','(0,0,100,100)','((1,1),(2,2),(3,3))','[(0,0),(-10,0),(-10,10)]','(2.0,0.0),(2.0,4.0),(0.0,0.0)','<(500,500),500>','aaaaaaaaaaa','DEADBEEF','(0,0,100,100)','AAAAAA','08:00:2b:01:02:03','A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11',row(1,'hi',true),row(1,row(1,'hello',true))));

insert into v_t1 values (row(1,1,-2147483648,-9223372036854775808,1,1,1,1,1,'{1,2,3}','{{1},{2}}',123.456789,123.456789,123.456789,123.456789,123.456789,123.456789,246.12345,246.12345,246.12345,246.12345,123.456789,123.456789,246.12345,246.12345,246.12345,246.12345,144,true,E'\\000',19981122,'2020-09-04','00:00:01','21:21:21','21:21:21', '21:21:21 pst','04:05:06','04:05:06 PST','2010-12-12','2010-12-12','2010-12-12','2010-12-12 pst','2010-12-12','2010-12-12 pst','2003-04-12 04:05:06','2 day 13:24:56',INTERVAL '3' DAY,interval '2' year,INTERVAL '2333' second,INTERVAL '5:23:35.5555' hour to second,'3 days','1984-2-6 01:00:30+8','["Feb 10, 1947 23:59:12" "Jan 14, 1973 03:14:2dro1"]','Aug 15 14:23:19 1983','a','hello','a','hello','a','hello','a','hello','99991231','99991231','99991231','clob','hello how are you','HIHIHI','192.168.1','192.168.1.226/24',B'0',B'00',' a:1 s:2 d g','1|2|4|5|6','(0.0000009,0.0000009)','((0.0000009,0.0000009),(0.0000018,0.0000018))','(0,0,100,100)','((1,1),(2,2),(3,3))','[(0,0),(-10,0),(-10,10)]','(2.0,0.0),(2.0,4.0),(0.0,0.0)','<(500,500),500>','aaaaaaaaaaa','DEADBEEF','(0,0,100,100)','AAAAAA','08:00:2b:01:02:03','A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11',row(1,'hi',true),row(1,row(1,'hello',true))));

-- drop tables
drop table t1;
drop table v_t1;

drop type composite_type_all;
drop type nested_composite_type;
drop type composite;
