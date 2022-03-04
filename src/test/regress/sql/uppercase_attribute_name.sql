drop table if exists test_tb;
create table test_tb(col1 int, Col_2 int, "col_第三列" int, "CoL_Four" int);

insert into test_tb values(1, 1, 1, 1);
insert into test_tb values(2, 2, 2, 2);
insert into test_tb values(3, 3, 3, 3);

select * from test_tb order by col1;

set uppercase_attribute_name=true;

-- During \d(+) one table, PQfnumber() is matched according to the lowercase column name.
-- So we need to restrict uppercase_attribute_name to not take effect in \d(+).
\d+ pg_class

select * from test_tb order by col1;

reset uppercase_attribute_name;

drop table test_tb;

-- utf8 encoding
create database utf8 encoding='utf8' LC_COLLATE='en_US.UTF-8' LC_CTYPE ='en_US.UTF-8' TEMPLATE=template0 dbcompatibility='A';
\c utf8
set client_encoding=utf8;

drop table if exists test_tb;
create table test_tb(col1 int, Col_2 int, "col_第三列" int, "CoL_Four" int);

insert into test_tb values(1, 1, 1, 1);
insert into test_tb values(2, 2, 2, 2);
insert into test_tb values(3, 3, 3, 3);

select * from test_tb order by col1;

set uppercase_attribute_name=true;

select * from test_tb order by col1;

reset uppercase_attribute_name;

drop table test_tb;

-- gbk encoding
create database gbk encoding='gbk' LC_COLLATE='zh_CN.GBK' LC_CTYPE ='zh_CN.GBK' TEMPLATE=template0 dbcompatibility='A';
\c gbk
set client_encoding=utf8;

drop table if exists test_tb;
create table test_tb(col1 int, Col_2 int, "col_第三列" int, "CoL_Four" int);

insert into test_tb values(1, 1, 1, 1);
insert into test_tb values(2, 2, 2, 2);
insert into test_tb values(3, 3, 3, 3);

select * from test_tb order by col1;

set uppercase_attribute_name=true;

select * from test_tb order by col1;

reset uppercase_attribute_name;

drop table test_tb;

-- gb18030 encoding
create database gb18030 encoding='gb18030' LC_COLLATE='zh_CN.GB18030' LC_CTYPE ='zh_CN.GB18030' TEMPLATE=template0 dbcompatibility='A';
\c gb18030
set client_encoding=utf8;

drop table if exists test_tb;
create table test_tb(col1 int, Col_2 int, "col_第三列" int, "CoL_Four" int);

insert into test_tb values(1, 1, 1, 1);
insert into test_tb values(2, 2, 2, 2);
insert into test_tb values(3, 3, 3, 3);

select * from test_tb order by col1;

set uppercase_attribute_name=true;

select * from test_tb order by col1;

reset uppercase_attribute_name;

drop table test_tb;

-- 'B' dbcompatibility
create database b_dbcompatibility TEMPLATE=template0 dbcompatibility='B';
\c b_dbcompatibility
set client_encoding=utf8;

drop table if exists test_tb;
create table test_tb(col1 int, Col_2 int, "col_第三列" int, "CoL_Four" int);

insert into test_tb values(1, 1, 1, 1);
insert into test_tb values(2, 2, 2, 2);
insert into test_tb values(3, 3, 3, 3);

select * from test_tb order by col1;

set uppercase_attribute_name=true;

select * from test_tb order by col1;

reset uppercase_attribute_name;

drop table test_tb;

\c regression
clean connection to all force for database utf8;
clean connection to all force for database gbk;
clean connection to all force for database gb18030;
clean connection to all force for database b_dbcompatibility;
drop database utf8;
drop database gbk;
drop database gb18030;
drop database b_dbcompatibility;