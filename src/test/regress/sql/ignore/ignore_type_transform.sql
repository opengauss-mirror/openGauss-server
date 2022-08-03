create database sql_ignore_type_transform_test dbcompatibility 'B';
\c sql_ignore_type_transform_test;

-- test for tinyint
drop table if exists t;
create table t(num tinyint);
insert into t values(10000);
insert into t values(-10000);
insert into t values(100);
insert into t values(-100);
insert /*+ ignore_error */ into t values(10000);
insert /*+ ignore_error */ into t values(-10000);
insert /*+ ignore_error */ into t values(100);
insert /*+ ignore_error */ into t values(-100);
select * from t;
update /*+ ignore_error */ t set num = 100000 where num = 100;
select * from t;

-- insert numeric table into tinyint table
drop table if exists t_tinyint;
drop table if exists t_numeric;
create table t_tinyint(num tinyint);
create table t_numeric(num numeric);
insert into t_numeric values (38764378634891278678324089237898634298778923472389687.123874);
insert into t_numeric values (-38764378634891278678324089237898634298778923472389687.123874);
insert /*+ ignore_error */ into t_tinyint select num from t_numeric;
select * from t_tinyint;

-- test for smallint
drop table if exists t;
create table t(num smallint);
insert into t values(100000);
insert into t values(-100000);
insert into t values(10000);
insert into t values(-10000);
insert /*+ ignore_error */ into t values(100000);
insert /*+ ignore_error */ into t values(-100000);
insert /*+ ignore_error */ into t values(10000);
insert /*+ ignore_error */ into t values(-10000);
select * from t;
update /*+ ignore_error */ t set num = 1000000 where num = 10000;
select * from t;

-- insert numeric table into smallint table
drop table if exists t_smallint;
drop table if exists t_numeric;
create table t_smallint(num smallint);
create table t_numeric(num numeric);
insert into t_numeric values (38764378634891278678324089237898634298778923472389687.123874);
insert into t_numeric values (-38764378634891278678324089237898634298778923472389687.123874);
insert /*+ ignore_error */ into t_smallint select num from t_numeric;
select * from t_smallint;

-- test for int
drop table if exists t;
create table t(num int);
insert into t values(10000000000);
insert into t values(-10000000000);
insert into t values(1000000);
insert into t values(-1000000);
insert /*+ ignore_error */ into t values(10000000000);
insert /*+ ignore_error */ into t values(-10000000000);
insert /*+ ignore_error */ into t values(1000000);
insert /*+ ignore_error */ into t values(-1000000);
select * from t;
update /*+ ignore_error */ t set num = 99999999999999999999 where num = 1000000;
select * from t;

-- insert numeric table into int table
drop table if exists t_int;
drop table if exists t_numeric;
create table t_int(num int);
create table t_numeric(num numeric);
insert into t_numeric values (38764378634891278678324089237898634298778923472389687.123874);
insert into t_numeric values (-38764378634891278678324089237898634298778923472389687.123874);
insert /*+ ignore_error */ into t_int select num from t_numeric;
select * from t_int;

-- test for bigint
drop table if exists t;
create table t(num bigint);
insert into t values(847329847839274398574389574398579834759384504385094386073456058984);
insert into t values(-439058439498375898437567893745893275984375984375984375984345498759438);
insert into t values(10000000::numeric);
insert into t values(-10000000::numeric);
insert /*+ ignore_error */ into t values(847329847839274398574389574398579834759384504385094386073456058984);
insert /*+ ignore_error */ into t values(-439058439498375898437567893745893275984375984375984375984345498759438);
insert /*+ ignore_error */ into t values(10000000::numeric);
insert /*+ ignore_error */ into t values(-10000000::numeric);
select * from t;
update /*+ ignore_error */ t set num = 99999999999999999999999999999999999999999999999999999999999999999999999999 where num = 10000000;
select * from t;

-- insert numeric table into bigint table
drop table if exists t_bigint;
drop table if exists t_numeric;
create table t_bigint(num bigint);
create table t_numeric(num numeric);
insert into t_numeric values (38764378634891278678324089237898634298778923472389687.123874);
insert into t_numeric values (-38764378634891278678324089237898634298778923472389687.123874);
insert /*+ ignore_error */ into t_bigint select num from t_numeric;
select * from t_bigint;

-- insert int table into tinyint table
drop table if exists t_int;
drop table if exists t_tinyint;
create table t_int(num int);
create table t_tinyint(num tinyint);
insert into t_int values(10000);
insert into t_int values(-10000);
insert /*+ ignore_error */ into t_tinyint select num from t_int;
select * from t_tinyint;

-- insert int table into smallint table
drop table if exists t_int;
drop table if exists t_smallint;
create table t_int(num int);
create table t_smallint(num smallint);
insert into t_int values(1000000);
insert into t_int values(-1000000);
insert /*+ ignore_error */ into t_smallint select num from t_int;
select * from t_smallint;

-- test for float4 to tinyint
drop table if exists t_tinyint;
create table t_tinyint(num tinyint);
insert into t_tinyint values (10000.023::float4);
insert into t_tinyint values (-10000.023::float4);
insert into t_tinyint values (123.023::float4);
insert into t_tinyint values (-123.023::float4);
insert /*+ ignore_error */ into t_tinyint values (10000.023::float4);
insert /*+ ignore_error */ into t_tinyint values (-10000.023::float4);
insert /*+ ignore_error */ into t_tinyint values (123.023::float4);
insert /*+ ignore_error */ into t_tinyint values (-123.023::float4);
select * from t_tinyint;

-- test for float4 to smallint
drop table if exists t_smallint;
create table t_smallint(num smallint);
insert into t_smallint values (1000000.023::float4);
insert into t_smallint values (-1000000.023::float4);
insert into t_smallint values (10000.023::float4);
insert into t_smallint values (-10000.023::float4);
insert /*+ ignore_error */ into t_smallint values (1000000.023::float4);
insert /*+ ignore_error */ into t_smallint values (-1000000.023::float4);
insert /*+ ignore_error */ into t_smallint values (10000.023::float4);
insert /*+ ignore_error */ into t_smallint values (-10000.023::float4);
select * from t_smallint;

-- test for float4 to int
drop table if exists t_int;
create table t_int(num int);
insert into t_int values (72348787598743985743895.023::float4);
insert into t_int values (-72348787598743985743895.023::float4);
insert into t_int values (123123.023::float4);
insert into t_int values (-123123.023::float4);
insert /*+ ignore_error */ into t_int values (72348787598743985743895.023::float4);
insert /*+ ignore_error */ into t_int values (-72348787598743985743895.023::float4);
insert /*+ ignore_error */ into t_int values (123123.023::float4);
insert /*+ ignore_error */ into t_int values (-123123.023::float4);
select * from t_int;

-- test for float4 to bigint
drop table if exists t_bigint;
create table t_bigint(num bigint);
insert into t_bigint values (238947289573489758943455436587549686.023::float4);
insert into t_bigint values (-238947289573489758943455436587549686.023::float4);
insert into t_bigint values (100000.023::float4);
insert into t_bigint values (-100000.023::float4);
insert /*+ ignore_error */ into t_bigint values (238947289573489758943455436587549686.023::float4);
insert /*+ ignore_error */ into t_bigint values (-238947289573489758943455436587549686.023::float4);
insert /*+ ignore_error */ into t_bigint values (100000.023::float4);
insert /*+ ignore_error */ into t_bigint values (-100000.023::float4);
select * from t_bigint;

-- test for float8 to tinyint
drop table if exists t_tinyint;
create table t_tinyint(num tinyint);
insert into t_tinyint values (238947289573489758943455436587549686.023::float8);
insert into t_tinyint values (-238947289573489758943455436587549686.023::float8);
insert into t_tinyint values (100.123::float8);
insert into t_tinyint values (-100.123::float8);
insert /*+ ignore_error */ into t_tinyint values (238947289573489758943455436587549686.023::float8);
insert /*+ ignore_error */ into t_tinyint values (-238947289573489758943455436587549686.023::float8);
insert /*+ ignore_error */ into t_tinyint values (100.123::float8);
insert /*+ ignore_error */ into t_tinyint values (-100.123::float8);
select * from t_tinyint;

-- test for float8 to smallint
drop table if exists t_smallint;
create table t_smallint(num smallint);
insert into t_smallint values (238947289573489758943455436587549686.023::float8);
insert into t_smallint values (-238947289573489758943455436587549686.023::float8);
insert into t_smallint values (100.023::float8);
insert into t_smallint values (-100.023::float8);
insert /*+ ignore_error */ into t_smallint values (238947289573489758943455436587549686.023::float8);
insert /*+ ignore_error */ into t_smallint values (-238947289573489758943455436587549686.023::float8);
insert /*+ ignore_error */ into t_smallint values (100.023::float8);
insert /*+ ignore_error */ into t_smallint values (-100.023::float8);
select * from t_smallint;

-- test for float8 to int
drop table if exists t_int;
create table t_int(num int);
insert into t_int values (23894728957345798437986755479865476509486804965449876953489758943455436587549686.023::float8);
insert into t_int values (-23894728957345798437986755479865476509486804965449876953489758943455436587549686.023::float8);
insert into t_int values (123.023::float8);
insert into t_int values (-123.023::float8);
insert /*+ ignore_error */ into t_int values (23894728957345798437986755479865476509486804965449876953489758943455436587549686.023::float8);
insert /*+ ignore_error */ into t_int values (-23894728957345798437986755479865476509486804965449876953489758943455436587549686.023::float8);
insert /*+ ignore_error */ into t_int values (123.023::float8);
insert /*+ ignore_error */ into t_int values (-123.023::float8);
select * from t_int;

-- test for float8 to bigint
drop table if exists t_bigint;
create table t_bigint(num bigint);
insert into t_bigint values (238947289573457984379867554798658438574983768504985454690874578759765898798798709854353476509486804965449876953489758943455436587549686.023::float8);
insert into t_bigint values (-238947289573457984379867554798658438574983768504985454690874578759765898798798709854353476509486804965449876953489758943455436587549686.023::float8);
insert into t_bigint values (123.123::float8);
insert into t_bigint values (-123.123::float8);
insert /*+ ignore_error */ into t_bigint values (238947289573457984379867554798658438574983768504985454690874578759765898798798709854353476509486804965449876953489758943455436587549686.023::float8);
insert /*+ ignore_error */ into t_bigint values (-238947289573457984379867554798658438574983768504985454690874578759765898798798709854353476509486804965449876953489758943455436587549686.023::float8);
insert /*+ ignore_error */ into t_bigint values (123.123::float8);
insert /*+ ignore_error */ into t_bigint values (-123.123::float8);
select * from t_bigint;

-- test for float8 to float4
drop table if exists t_float4;
create table t_float4(num float4);
insert into t_float4 values (1892038097432987589432759843769348605436304758493758943758943758943759843756983760945860948605948765487689547893475893475918920380974329875894327598437693486054363047584937589437589437589437598437569837609458609486059487654876895478934758934759627346378267863475863875648365843734895749837589437589473988.18920380974329875894327598437693486054363047584937589437589437589437598437569837609458609486059487654876895478934758934759189203809743298758943275984376934860543630475849375894375894375894375984375698376094586094860594876548768954789347589347593874894375984::float8);
insert into t_float4 values (-1892038097432987589432759843769348605436304758493758943758943758943759843756983760945860948605948765487689547893475893475918920380974329875894327598437693486054363047584937589437589437589437598437569837609458609486059487654876895478934758934759627346378267863475863875648365843734895749837589437589473988.18920380974329875894327598437693486054363047584937589437589437589437598437569837609458609486059487654876895478934758934759189203809743298758943275984376934860543630475849375894375894375894375984375698376094586094860594876548768954789347589347593874894375984::float8);
insert into t_float4 values(123.123::float8);
insert into t_float4 values(-123.123::float8);
insert /*+ ignore_error */ into t_float4 values (1892038097432987589432759843769348605436304758493758943758943758943759843756983760945860948605948765487689547893475893475918920380974329875894327598437693486054363047584937589437589437589437598437569837609458609486059487654876895478934758934759627346378267863475863875648365843734895749837589437589473988.18920380974329875894327598437693486054363047584937589437589437589437598437569837609458609486059487654876895478934758934759189203809743298758943275984376934860543630475849375894375894375894375984375698376094586094860594876548768954789347589347593874894375984::float8);
insert /*+ ignore_error */ into t_float4 values (-1892038097432987589432759843769348605436304758493758943758943758943759843756983760945860948605948765487689547893475893475918920380974329875894327598437693486054363047584937589437589437589437598437569837609458609486059487654876895478934758934759627346378267863475863875648365843734895749837589437589473988.18920380974329875894327598437693486054363047584937589437589437589437598437569837609458609486059487654876895478934758934759189203809743298758943275984376934860543630475849375894375894375894375984375698376094586094860594876548768954789347589347593874894375984::float8);
insert /*+ ignore_error */ into t_float4 values(123.123::float8);
insert /*+ ignore_error */ into t_float4 values(-123.123::float8);
select * from t_float4;

-- test for numeric to float4
drop table if exists t_numeric;
drop table if exists t_float4;
create table t_numeric(num numeric);
create table t_float4(num float4);
insert into t_numeric values(1892038097432987589432759843769348605436304758493758943758943758943759843756983760945860948605948765487689547893475893475918920380974329875894327598437693486054363047584937589437589437589437598437569837609458609486059487654876895478934758934759627346378267863475863875648365843734895749837589437589473988.18920380974329875894327598437693486054363047584937589437589437589437598437569837609458609486059487654876895478934758934759189203809743298758943275984376934860543630475849375894375894375894375984375698376094586094860594876548768954789347589347593874894375984);
insert into t_numeric values(-1892038097432987589432759843769348605436304758493758943758943758943759843756983760945860948605948765487689547893475893475918920380974329875894327598437693486054363047584937589437589437589437598437569837609458609486059487654876895478934758934759627346378267863475863875648365843734895749837589437589473988.18920380974329875894327598437693486054363047584937589437589437589437598437569837609458609486059487654876895478934758934759189203809743298758943275984376934860543630475849375894375894375894375984375698376094586094860594876548768954789347589347593874894375984);
insert /*+ ignore_error */ into t_float4 select num from t_numeric;
select * from t_float4;

-- test for char(n) converting
drop table if exists t_char;
drop table if exists t_text;
create table t_char(cont char(6));
create table t_text(cont text);
insert into t_text values('abcdef');
insert into t_text values('abcdefghj');
insert into t_text values(123456789123456789);
insert /*+ ignore_error */ into t_char select cont from t_text;
select * from t_char;

-- test for varchar(n) converting
drop table if exists t_varchar;
drop table if exists t_text;
create table t_varchar(cont varchar(6));
create table t_text(cont text);
insert into t_text values('abcdef');
insert into t_text values('abcdefghj');
insert into t_text values(123456789123456789);
insert /*+ ignore_error */ into t_varchar select cont from t_text;
select * from t_varchar;

-- test for character(n) converting
drop table if exists t_character;
drop table if exists t_text;
create table t_character(cont character(6));
create table t_text(cont text);
insert into t_text values('abcdef');
insert into t_text values('abcdefghj');
insert into t_text values(123456789123456789);
insert /*+ ignore_error */ into t_character select cont from t_text;
select * from t_character;

-- test for nchar(n) converting
drop table if exists t_nchar;
drop table if exists t_text;
create table t_nchar(cont nchar(6));
create table t_text(cont text);
insert into t_text values('abcdef');
insert into t_text values('abcdefghj');
insert into t_text values(123456789123456789);
insert /*+ ignore_error */ into t_nchar select cont from t_text;
select * from t_nchar;

-- test for character converting
drop table if exists t_character;
drop table if exists t_text;
create table t_character(cont character);
create table t_text(cont text);
insert into t_text values('abcdef');
insert into t_text values('abcdefghj');
insert into t_text values(123456789123456789);
insert /*+ ignore_error */ into t_character select cont from t_text;
select * from t_character;

-- test for varchar2(n) converting
drop table if exists t_varchar2;
drop table if exists t_text;
create table t_varchar2(cont varchar2(6));
create table t_text(cont text);
insert into t_text values('abcdef');
insert into t_text values('abcdefghj');
insert into t_text values(123456789123456789);
insert /*+ ignore_error */ into t_varchar2 select cont from t_text;
select * from t_varchar2;

-- test for nvarchar2(n) converting
drop table if exists t_nvarchar2;
drop table if exists t_text;
create table t_nvarchar2(cont nvarchar2(6));
create table t_text(cont text);
insert into t_text values('abcdef');
insert into t_text values('abcdefghj');
insert into t_text values(123456789123456789);
insert /*+ ignore_error */ into t_nvarchar2 select cont from t_text;
select * from t_nvarchar2;

-- test for nvarchar2 converting
drop table if exists t_nvarchar2;
drop table if exists t_text;
create table t_nvarchar2(cont nvarchar2);
create table t_text(cont text);
insert into t_text values('abcdef');
insert into t_text values('abcdefghj');
insert into t_text values(123456789123456789);
insert /*+ ignore_error */ into t_nvarchar2 select cont from t_text;
select * from t_nvarchar2;

-- test for integer-string-mixed value in
drop table if exists t_int;
create table t_int(num int);
insert /*+ ignore_error */ into t_int values('12a34');
select * from t_int;

-- test for inconvertible type transform.
drop table if exists t_int;
create table t_int(num int);
insert /*+ ignore_error */ into t_int values('2011-8-2'::timestamp);
select * from t_int;

delete from t_int;
drop table if exists t_timestamp;
create table t_timestamp(val timestamp);
insert into t_timestamp values('2011-8-2');
insert /*+ ignore_error */ into t_int select val from t_timestamp;
select * from t_int;

delete from t_int;
insert into t_int values(999);
update /*+ ignore_error */ t_int set num = '2011-8-2'::timestamp;
select * from t_int;

drop table if exists t_multi;
create table t_multi(c1 int unique, c2 int);
insert /*+ ignore_error */ into t_multi values('2011-8-2'::timestamp, 1);
insert /*+ ignore_error */ into t_multi values(0, 0) on duplicate key update c2 = 2;
select * from t_multi;
insert /*+ ignore_error */ into t_multi values('2011-8-2'::timestamp , 3) on duplicate key update c2 = 3;
select * from t_multi;

drop table if exists t_float8;
create table t_float8(num float8);
insert /*+ ignore_error */ into t_float8 values('2011-8-2'::timestamp);
select * from t_float8;

delete from t_float8;
insert into t_float8 values(123.999);
update /*+ ignore_error */ t_float8 set num = '2011-8-2'::timestamp;
select * from t_float8;

drop table if exists t_uuid;
create table t_uuid(val uuid);
insert /*+ ignore_error */ into t_uuid values(0);
select * from t_uuid;

drop table if exists t_date;
create table t_date(val date);
insert /*+ ignore_error */ into t_date values(0);
select * from t_date;

delete from t_date;
insert into t_date values('2011-8-2');
update /*+ ignore_error */ t_date set val = 0;
select * from t_date;

\c postgres
drop database if exists sql_ignore_type_transform_test;