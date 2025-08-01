create database test_charset dbcompatibility='D';
\c test_charset
create extension shark;
--支持排序规则
create schema sch_ddl_0078 character set = utf8 collate utf8_unicode_ci;
drop schema sch_ddl_0078;
drop table if EXISTS test_table;
CREATE TABLE test_table (
    a VARCHAR(100),
    b VARCHAR(100)
) COLLATE utf8mb4_general_ci;
insert into test_table values('abc', 'ABC');
select * from test_table;
select a = b from test_table;
drop table test_table;
CREATE TABLE test_table (
    id INT  PRIMARY KEY,
    name VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci
);
insert into test_table values(1, 'ABC');
insert into test_table values(2, 'abc');
select * from test_table where name = 'abc';
drop table test_table;

create table t_charset_0 (c1 varchar(20));
select pg_get_tabledef('t_charset_0');
set b_format_behavior_compat_options = 'default_collation';
-- schema level charset and collate
create schema s_charset_1  charset   utf8mb4 collate   utf8mb4_unicode_ci;
create schema s_charset_2  charset = utf8mb4 collate = utf8mb4_unicode_ci;
create schema s_charset_3  charset = utf8mb4 collate = utf8mb4_general_ci;
create schema s_charset_4  charset = utf8mb4;
create schema s_charset_5  charset = utf8;
create schema s_charset_6  charset = gbk;
create schema s_charset_7  default charset = utf8mb4 default collate = utf8mb4_unicode_ci;
create schema s_charset_8  charset = "binary";
create schema s_charset_9  character set = utf8mb4;
create schema s_charset_10 collate = utf8mb4_general_ci;
create schema s_charset_11 collate = utf8mb4_bin;
create schema s_charset_12 collate = binary;
create schema s_charset_13 collate = "binary";
create schema s_charset_14  charset = binary;
create schema s_charset_16 charset = gbk collate = utf8mb4_general_ci; -- error
create schema s_charset_16 default charset utf8mb4 default collate utf8mb4_unicode_ci;
create schema s_charset_17 CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
create schema s_charset_18 collate = utf8mb4_unicode_ci charset = utf8mb4;
create schema s_charset_19 collate = utf8mb4_unicode_ci collate = utf8mb4_general_ci;
create schema s_charset_20 charset = gbk charset = utf8mb4;
create schema s_charset_21 collate = utf8mb4_unicode_ci charset = utf8mb4 collate = utf8mb4_general_ci;
create schema s_charset_22 collate = "zh_CN.gbk" charset = utf8mb4 collate = utf8mb4_general_ci;
create schema s_charset_23 charset utf8mb4 collate "aa_DJ.utf8"; -- error
create schema s_charset_23 collate "aa_DJ"; -- error
select * from pg_namespace where nspname like 's_charset_%' order by 1;
create schema s_charset_14 collate = "zh_CN.gbk"; -- error
create schema s_charset_15 charset = gbk collate = "zh_CN.gbk"; -- error
alter schema s_charset_1 charset utf8mb4 collate utf8mb4_general_ci;
alter schema s_charset_2 charset = utf8mb4 collate = utf8mb4_general_ci;
alter schema s_charset_3 collate = utf8mb4_unicode_ci;
alter schema s_charset_5 charset = gbk;
alter schema s_charset_5 charset = gbk collate = "zh_CN.gbk"; -- error
alter schema s_charset_9 character set = utf8 collate = utf8mb4_unicode_ci;
select * from pg_namespace where nspname like 's_charset_%' order by 1;

-- relation level charset and collate
create table t_charset_1 (c1 varchar(20)) charset   utf8mb4 collate   utf8mb4_unicode_ci;
create table t_charset_2 (c1 varchar(20)) charset = utf8mb4 collate = utf8mb4_unicode_ci;
create table t_charset_3 (c1 varchar(20)) charset = utf8mb4 collate = utf8mb4_general_ci;
create table t_charset_4 (c1 varchar(20)) charset = utf8mb4;
create table t_charset_5 (c1 varchar(20)) charset = utf8;
create table t_charset_6 (c1 varchar(20)) charset = gbk;
create table t_charset_7 (c1 varchar(20)) default charset = utf8mb4 default collate = utf8mb4_unicode_ci;
create table t_charset_8 (c1 varchar(20)) charset = binary; -- error
create table t_charset_8 (c1 text) charset = binary;
select pg_get_tabledef('t_charset_8');
create table t_charset_9 (c1 varchar(20)) character set = utf8mb4;
create table t_charset_10(c1 varchar(20)) collate = utf8mb4_general_ci;
create table t_charset_11(c1 varchar(20)) collate = utf8mb4_bin;
create table t_charset_12(c1 varchar(20)) collate = binary;
create table t_charset_12(c1 varchar(20)) default charset utf8mb4 default collate utf8mb4_unicode_ci;
create table t_charset_13(c1 varchar(20)) collate = "binary";
create table t_charset_16(c1 varchar(20)) charset = gbk collate = utf8mb4_general_ci; -- error
create table t_charset_17(c1 varchar(20)) CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
create table t_charset_18(c1 varchar(20)) collate = utf8mb4_unicode_ci charset = utf8mb4;
create table t_charset_19(c1 varchar(20)) collate = utf8mb4_unicode_ci collate = utf8mb4_general_ci;
create table t_charset_20(c1 varchar(20)) charset = gbk charset = utf8mb4;
create table t_charset_21(c1 varchar(20)) collate = utf8mb4_unicode_ci charset = utf8mb4 collate = utf8mb4_general_ci;
create table t_charset_22(c1 varchar(20)) collate = "zh_CN.gbk" charset = utf8mb4 collate = utf8mb4_general_ci;
create table t_charset_23(like t_charset_22);
select r.relname,r.reloptions,a.attcollation from pg_class r,pg_attribute a where r.oid=a.attrelid and r.relname='t_charset_23';
create table t_charset_24(c1 varchar(20) character set binary); -- error
create table t_charset_24(c1 varchar(20) character set "binary"); -- error
create table t_charset_25(c1 varchar(20)) with(collate = 7);
create table t_charset_26(c1 varchar(20)) charset utf8mb4 collate "aa_DJ.utf8"; -- error
create table t_charset_26(c1 varchar(20)) collate "aa_DJ"; -- error
create table t_charset_27(like t_charset_18 including reloptions);
select relname, reloptions from pg_class where relname like 't_charset_%' order by 1;
alter table t_charset_1 convert to charset binary; -- error
alter table t_charset_1 convert to charset utf8mb4;
alter table t_charset_1 convert to character set utf8mb4;
alter table t_charset_1 convert to character set utf8mb4 collate utf8mb4_general_ci;
select relname, reloptions from pg_class where relname = 't_charset_1';
alter table t_charset_1 convert to character set default collate utf8mb4_unicode_ci;
select relname, reloptions from pg_class where relname = 't_charset_1';
alter table t_charset_1 charset utf8mb4;
alter table t_charset_1 character set utf8mb4;
alter table t_charset_1 character set utf8mb4 collate utf8mb4_bin;
select relname, reloptions from pg_class where relname = 't_charset_1';
alter table t_charset_1 collate utf8mb4_unicode_ci;
select relname, reloptions from pg_class where relname = 't_charset_1';
alter table t_charset_1 change c1 c2 varchar(30) charset gbk collate utf8mb4_bin; -- error
alter table t_charset_1 change c1 c2 varchar(30) charset utf8mb4 collate utf8mb4_bin;
select pg_get_tabledef('t_charset_1');

-- attribute level charset and collate
create table a_charset_1 (
a1 varchar(20) charset utf8mb4 collate utf8mb4_general_ci,
a2 varchar(20) charset utf8mb4 collate utf8mb4_unicode_ci,
a3 varchar(20) charset utf8mb4 collate utf8mb4_bin
)
charset utf8mb4 collate utf8mb4_general_ci;
select pg_get_tabledef('a_charset_1');
insert into a_charset_1 values('中国','中国','中国');
insert into a_charset_1 select a2,a1 from a_charset_1;
select *,rawtohex(a1),rawtohex(a2),length(a1),length(a2),length(a3),lengthb(a1),lengthb(a2),lengthb(a3) from a_charset_1;
alter table a_charset_1 convert to charset gbk collate "zh_CN.gbk";
select rawtohex(a1),rawtohex(a2),rawtohex(a3) from a_charset_1;
alter table a_charset_1 convert to charset utf8mb4;
select rawtohex(a1),rawtohex(a2),rawtohex(a3) from a_charset_1;

create table a_charset_2(
a1 character(20) charset utf8mb4 collate utf8mb4_general_ci,
a2 char(20) charset utf8mb4 collate utf8mb4_general_ci,
a3 nchar(20) charset utf8mb4 collate utf8mb4_general_ci,
a4 varchar(20) charset utf8mb4 collate utf8mb4_general_ci,
a5 character varying(20) charset utf8mb4 collate utf8mb4_general_ci,
a6 varchar2(20) charset utf8mb4 collate utf8mb4_general_ci,
a7 nvarchar2(20) charset utf8mb4 collate utf8mb4_general_ci,
a8 text,
a9 blob
) charset binary;
select pg_get_tabledef('a_charset_2');
alter table a_charset_2 add a8 varchar(20) charset utf8mb4;
alter table a_charset_2 add a9 varchar(20) character set utf8mb4;
alter table a_charset_2 add a10 varchar(20) character set utf8mb4 collate utf8mb4_unicode_ci;
alter table a_charset_2 add a11 varchar(20) collate utf8mb4_bin;
alter table a_charset_2 add a12 varchar(20);
alter table a_charset_2 add a13 int;
alter table a_charset_2 add a14 varchar(20) charset utf8mb4 collate "aa_DJ.utf8";
alter table a_charset_2 add a15 varchar(20) collate "aa_DJ.utf8";
alter table a_charset_2 add a16 varchar(20) collate "aa_DJ";
alter table a_charset_2 add a17 text charset utf8mb4 collate utf8mb4_general_ci;
alter table a_charset_2 add a18 clob charset utf8mb4 collate utf8mb4_general_ci; -- error
alter table a_charset_2 add a19 name charset utf8mb4 collate utf8mb4_general_ci; -- error
alter table a_charset_2 add a20 "char" charset utf8mb4 collate utf8mb4_general_ci; -- error
alter table a_charset_2 add a21 BLOB charset utf8mb4 collate utf8mb4_general_ci; -- error
alter table a_charset_2 add a22 RAW charset utf8mb4 collate utf8mb4_general_ci; -- error
alter table a_charset_2 add a23 BYTEA charset utf8mb4 collate utf8mb4_general_ci; -- error
alter table a_charset_2 add a24 varchar(20) collate "zh_CN.gbk"; -- error;
select pg_get_tabledef('a_charset_2');
alter table a_charset_2 add a8 varchar(20) charset utf8mb4 charset utf8mb4; -- error
alter table a_charset_2 modify a1 int;
alter table a_charset_2 modify a9 varchar(20) character set gbk;
alter table a_charset_2 modify a10 varchar(20);
alter table a_charset_2 modify a11 varchar(20) collate utf8mb4_unicode_ci;
alter table a_charset_2 modify a12 varchar(20) charset utf8mb4;
select pg_get_tabledef('a_charset_2');

create table a_charset_3(
a1 varchar(20) collate "C",
a2 varchar(20) collate "default",
a3 varchar(20) collate "POSIX"
);
create table a_charset_4(a1 blob);

-- divergence test
\h create schema;
\h alter schema;
\h create table;
\h create table partition;
\h create table subpartition;
\h alter table;
alter session set current_schema = s_charset_1;
create table s_t_charset_1(s1 varchar(20));
select pg_get_tabledef('s_t_charset_1');
create table s_t_charset_2(s1 varchar(20));
select pg_get_tabledef('s_t_charset_2');
create table s_t_charset_3 (like s_t_charset_1);
select pg_get_tabledef('s_t_charset_3');
create table s_t_charset_4(s1 varchar(20) charset utf8mb4 collate "aa_DJ");
create table s_t_charset_5(s1 varchar(20) collate "aa_DJ");
create table s_t_charset_6(s1 int);
alter table s_t_charset_6 charset binary;
alter table s_t_charset_6 convert to charset default collate binary; -- error
alter table s_t_charset_6 convert to charset default collate utf8mb4_bin;
select pg_get_tabledef('s_t_charset_6');
create table s_t_charset_7 as table s_t_charset_1;
\d+ s_t_charset_7;
create table s_t_charset_8 as select '123';
\d+ s_t_charset_8;
alter session set current_schema = s_charset_12;
create table s_t_charset_9(s1 varchar(20) charset utf8mb4);
alter table s_t_charset_9 convert to charset default collate utf8mb4_bin; -- error
alter session set current_schema = s_charset_1;

-- partition table
create table p_charset_1(c1 varchar(20),c2 varchar(20),c3 int)
character set = utf8mb4 collate = utf8mb4_general_ci
partition by hash(c1)
(
partition p1,
partition p2
);
select * from pg_get_tabledef('p_charset_1');
alter table p_charset_1 convert to character set utf8mb4;
alter table p_charset_1 collate utf8mb4_unicode_ci;
insert into p_charset_1 values('a中国a');
select * from p_charset_1;
\d+ p_charset_1;

-- temporary table
create temporary table tem_charset_1(c1 varchar(20),c2 varchar(20),c3 int) character set = utf8mb4;
select r.relname,r.reloptions,a.attcollation from pg_class r,pg_attribute a where r.oid=a.attrelid and r.relname='tem_charset_1';
alter table tem_charset_1 convert to character set utf8mb4;
alter table tem_charset_1 collate utf8mb4_unicode_ci;
insert into tem_charset_1 values('a中国a');
select r.relname,r.reloptions,a.attcollation from pg_class r,pg_attribute a where r.oid=a.attrelid and r.relname='tem_charset_1';

-- cstore not supported
SET b_format_behavior_compat_options = 'default_collation, enable_multi_charset';
create schema s_charset_multi  charset = gbk;
create table s_charset_multi.cstore_charset_1(c1 varchar(20),c2 varchar(20),c3 int) with (ORIENTATION=column); -- ERROR
DROP schema s_charset_multi;
create table cstore_charset_1(c1 varchar(20),c2 varchar(20),c3 int) character set = gbk with (ORIENTATION=column); -- ERROR
create table cstore_charset_1(c1 varchar(20),c2 varchar(20) character set gbk,c3 int) with (ORIENTATION=column); -- ERROR
create table cstore_charset_1(c1 varchar(20),c2 varchar(20),c3 int) with (ORIENTATION=column);
ALTER TABLE cstore_charset_1 ADD COLUMN c4 varchar(20) character set gbk; -- ERROR
ALTER TABLE cstore_charset_1 MODIFY COLUMN c1 varchar(20) character set gbk; -- ERROR
drop table cstore_charset_1;

\c postgres
drop database test_charset;