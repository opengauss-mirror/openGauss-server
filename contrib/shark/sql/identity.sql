create schema identity_schema;
set current_schema = identity_schema;

drop table if exists t1;
create table t1(id serial primary key,name varchar(10));
insert into t1(name) values('zhangsan');
insert into t1(name) values('lisi');
insert into t1(name) values('wangwu');
select SCOPE_IDENTITY();
select ident_current('t1');
drop table t1;

CREATE TABLE book(bookId int IDENTITY, bookname NVARCHAR(50), author NVARCHAR(50));
INSERT INTO book VALUES('book1','author1'),('book2','author2');
INSERT INTO book(bookname,author) VALUES('book3','author3'),('book4','author4');
INSERT INTO book VALUES(3,'book5','author5');
select * from book;
ALTER table book add column id int identity;
ALTER table book alter bookId type text;
drop table if exists book;

CREATE TABLE book(id int identity, bookId int IDENTITY, bookname NVARCHAR(50), author NVARCHAR(50));

CREATE TABLE book
(
    bookId int NOT NULL PRIMARY KEY IDENTITY, 
    bookname NVARCHAR(50), 
    author NVARCHAR(50)
);
INSERT INTO book VALUES('book1','author1'),('book2','author2');
select * from book;
drop table if exists book;

CREATE TABLE book
(
    bookId int NOT NULL PRIMARY KEY IDENTITY(3,5), 
    bookname NVARCHAR(50), 
    author NVARCHAR(50)
);
INSERT INTO book VALUES('book1','author1'),('book2','author2');
select * from book;
delete book where bookId=8;
select * from book;
INSERT INTO book(bookname,author) VALUES('book3','author3'),('book4','author4');
select * from book;
drop table if exists book;

CREATE TABLE book
(
    bookId tinyint NOT NULL PRIMARY KEY IDENTITY(3,5), 
    bookname NVARCHAR(50), 
    author NVARCHAR(50)
);
drop table if exists book;
CREATE TABLE book
(
    bookId smallint NOT NULL PRIMARY KEY IDENTITY(3,5), 
    bookname NVARCHAR(50), 
    author NVARCHAR(50)
);
drop table if exists book;
CREATE TABLE book
(
    bookId bigint NOT NULL PRIMARY KEY IDENTITY(3,5), 
    bookname NVARCHAR(50), 
    author NVARCHAR(50)
);
drop table if exists book;
CREATE TABLE book
(
    bookId numeric NOT NULL PRIMARY KEY IDENTITY(3,5), 
    bookname NVARCHAR(50), 
    author NVARCHAR(50)
);
drop table if exists book;
CREATE TABLE book
(
    bookId decimal NOT NULL PRIMARY KEY IDENTITY(3,5), 
    bookname NVARCHAR(50), 
    author NVARCHAR(50)
);
drop table if exists book;
CREATE TABLE book
(
    bookname NVARCHAR(50),
    author NVARCHAR(50)
);
ALTER TABLE book add bookId int not null primary key identity;
INSERT INTO book VALUES('book1','author1'),('book2','author2');
select * from book;
delete book where bookId=8;
select * from book;
INSERT INTO book(bookname,author) VALUES('book3','author3'),('book4','author4');
select * from book;
drop table if exists book;

CREATE TABLE book
(
    bookId int NOT NULL PRIMARY KEY IDENTITY(100,100),
    bookname NVARCHAR(50),
    author NVARCHAR(50)
);
INSERT INTO book VALUES('book1','author1'),('book2','author2');
INSERT INTO book(bookname,author) VALUES('book3','author3'),('book4','author4');
select * from book;
drop table if exists book;


CREATE TABLE book
(
    bookId tinyint NOT NULL PRIMARY KEY IDENTITY(-1,1), 
    bookname NVARCHAR(50), 
    author NVARCHAR(50)
);
drop table if exists book;
CREATE TABLE book
(
    bookId tinyint NOT NULL PRIMARY KEY IDENTITY(0,-1), 
    bookname NVARCHAR(50), 
    author NVARCHAR(50)
);
drop table if exists book;
CREATE TABLE book
(
    bookId tinyint NOT NULL PRIMARY KEY IDENTITY(255,256), 
    bookname NVARCHAR(50), 
    author NVARCHAR(50)
);
drop table if exists book;
CREATE TABLE book
(
    bookId smallint NOT NULL PRIMARY KEY IDENTITY(32767,32768), 
    bookname NVARCHAR(50), 
    author NVARCHAR(50)
);
drop table if exists book;
CREATE TABLE book
(
    bookId bigint NOT NULL PRIMARY KEY IDENTITY(-9223372036854775808,1),
    bookname NVARCHAR(50), 
    author NVARCHAR(50)
);
drop table if exists book;
CREATE TABLE book
(
    bookId text NOT NULL PRIMARY KEY IDENTITY, 
    bookname NVARCHAR(50), 
    author NVARCHAR(50)
);
drop table if exists book;
CREATE TABLE book
(
    bookId numeric(4,2) NOT NULL PRIMARY KEY IDENTITY, 
    bookname NVARCHAR(50), 
    author NVARCHAR(50)
);

CREATE TABLE book(id serial, bookId int identity default 5, bookname char(20));

CREATE TABLE t_identity(id serial, col int identity(5,10), col2 text)
partition by range(col)(
partition p1 VALUES LESS THAN (10),
partition p2 VALUES LESS THAN (20),
partition p3 VALUES LESS THAN (30),
partition p4 VALUES LESS THAN (MAXVALUE)
);
insert into t_identity(col2) values('abc');
insert into t_identity(col2) values('def');
insert into t_identity(col2) values('ghi');
insert into t_identity(col2) values('jkl');
select * from t_identity partition(p1);
select * from t_identity partition(p2);
select * from t_identity partition(p3);
select * from t_identity partition(p4);
drop table t_identity;

create table t_identity_0020(id int identity, name varchar(10));
insert into t_identity_0020(name) values('zhangsan');
select scope_identity();
alter table t_identity_0020 drop column id;
select scope_identity();
drop table t_identity_0020;

create table t_identity_0032(id int identity(1,2), name varchar(10));
insert into t_identity_0032(name) values('zhangsan');
select SCOPE_IDENTITY();
drop table t_identity_0032;
select SCOPE_IDENTITY();

create table t_identity_0020(id int identity, name varchar(10));
insert into t_identity_0020(name) values('zhangsan');
insert into t_identity_0020(name) values('lisi');
insert into t_identity_0020(name) values('wangwu');
insert into t_identity_0020(name) values('zhaoliu');
select scope_identity();
drop table t_identity_0020;
select scope_identity();

create table t_identity_0021(id int identity, name varchar(10));
insert into t_identity_0021(name) values('zhangsan');
insert into t_identity_0021(name) values('lisi');
insert into t_identity_0021(name) values('wangwu');
insert into t_identity_0021(name) values('zhaoliu');
insert into t_identity_0021(name) values('qianqi');
select scope_identity();
drop table t_identity_0021;
select scope_identity();

CREATE TABLE t_identity(id decimal(12) not null identity(1,1), col text);
insert into t_identity values('aaa');
select * from t_identity;
drop table if exists t_identity;

CREATE TABLE t_identity(id decimal(12,2) not null identity(1,1), col text);

create schema sch_1130412;
create table sch_1130412.tab_1130412(a1 sql_variant);
insert into sch_1130412.tab_1130412 values('aa'::char(8)),('cc'::char(8));
select * from sch_1130412.tab_1130412 order by a1;
drop table sch_1130412.tab_1130412;
drop schema sch_1130412;

drop table if exists t_identity_0020;
create table t_identity_0020(id int identity(1,50), name varchar(10));

DO $$ 
DECLARE last_id date; 
BEGIN 
INSERT INTO t_identity_0020 (name) VALUES ('示例'); 
last_id := scope_identity(); 
RAISE NOTICE 'NOTICE:新插入记录的 ID 为: %', last_id; 
END; $$;

DO $$ 
DECLARE last_id time; 
BEGIN 
INSERT INTO t_identity_0020 (name) VALUES ('示例'); 
last_id := scope_identity(); 
RAISE NOTICE 'NOTICE:新插入记录的 ID 为: %', last_id; 
END; $$;

DO $$ DECLARE last_id timestamp; 
BEGIN 
INSERT INTO t_identity_0020 (name) VALUES ('示例'); 
last_id := scope_identity(); 
RAISE NOTICE 'NOTICE:新插入记录的 ID 为: %', last_id; 
END; $$;

DO $$ DECLARE last_id varchar2(2); 
BEGIN 
INSERT INTO t_identity_0020 (name) VALUES ('示例'); 
last_id := scope_identity(); 
RAISE NOTICE 'NOTICE:新插入记录的 ID 为: %', last_id; 
END; $$;

DO $$ 
DECLARE last_id date; 
BEGIN 
last_id := ident_current('t_identity_0020'); 
RAISE NOTICE 'NOTICE:新插入记录的 ID 为: %', last_id; 
END; $$;

DO $$ 
DECLARE last_id time; 
BEGIN 
last_id := ident_current('t_identity_0020'); 
RAISE NOTICE 'NOTICE:新插入记录的 ID 为: %', last_id; 
END; $$;

DO $$ 
DECLARE last_id timestamp;
BEGIN 
last_id := ident_current('t_identity_0020'); 
RAISE NOTICE 'NOTICE:新插入记录的 ID 为: %', last_id; 
END; $$;

DO $$ 
DECLARE last_id varchar2(2); 
BEGIN 
last_id := ident_current('t_identity_0020'); 
RAISE NOTICE 'NOTICE:新插入记录的 ID 为: %', last_id; 
END; $$;

drop table t_identity_0020;

create table t_identity_0013(id int identity, name varchar(10));
insert into t_identity_0013(name) values('zhangsan');
insert into t_identity_0013(name) values('lisi');
insert into t_identity_0013(name) values('wangwu');
--error
update t_identity_0013 set id=4 where name='zhangsan';
drop table t_identity_0013;

-- ## ident_current after truncate table. expected: success.
create table t_identity_0021(id int identity(100,1), name varchar(10));
insert into t_identity_0021(name) values('zhangsan');
insert into t_identity_0021(name) values('lisi');
select ident_current('t_identity_0021');
truncate table t_identity_0021;
select ident_current('t_identity_0021');
insert into t_identity_0021(name) values('zhangsan');
select ident_current('t_identity_0021');
insert into t_identity_0021(name) values('lisi');
select ident_current('t_identity_0021');
drop table t_identity_0021;

-- ## create table like, copy identity attr, but no data.
create table t_identity_0023(id int identity(100,1), name varchar(10));
insert into t_identity_0023(name) values('x1');
insert into t_identity_0023(name) values('x2');
insert into t_identity_0023(name) values('x3');
select ident_current('t_identity_0023');
select scope_identity();
-- no option.
create table t_identity_0023_1_1 (like t_identity_0023);
\d+ t_identity_0023_1_1
insert t_identity_0023_1_1 (name) values('xxx1');
-- including IDENTITY.
create table t_identity_0023_1 (like t_identity_0023 INCLUDING IDENTITY);
\d+ t_identity_0023_1
select ident_current('t_identity_0023_1');
insert t_identity_0023_1 (name) values('xxx1');
insert t_identity_0023_1 (name) values('xxx1');
select ident_current('t_identity_0023_1');
select scope_identity();
truncate table t_identity_0023_1;
select ident_current('t_identity_0023_1');
insert t_identity_0023_1 (name) values('xxx1');
insert t_identity_0023_1 (name) values('xxx1');
select ident_current('t_identity_0023_1');
select * from t_identity_0023_1 order by id;
-- including DEFAULTS.
create table t_identity_0023_2 (like t_identity_0023 INCLUDING DEFAULTS);
\d+ t_identity_0023_2
insert t_identity_0023_2 (name) values('xxx1');
insert t_identity_0023_2 (name) values('xxx1');
select ident_current('t_identity_0023_2');
select scope_identity();

drop table t_identity_0023, t_identity_0023_1_1;
drop table t_identity_0023_1;
drop table t_identity_0023_2;
-- ## can't insert identity column, expected error.
drop table if exists t_identity;
create table t_identity(id int identity, col text);
-- expected error.
insert into t_identity(id, col) values(2,'zshan');
select ident_current('t_identity');
-- expected success.
set identity_insert = on;
insert into t_identity(id, col) values(2,'zshan');
select ident_current('t_identity');
select * from t_identity order by id;
set identity_insert = off;

drop table if exists t_identity;
create table t_identity(id int identity(100, 1), col text);
select ident_current('t_identity');
-- expected success.
insert into t_identity(col) select 'test';
insert into t_identity(col) select 'test';
-- expected error.
insert into t_identity(id, col) select 1, 'test';
-- expected success.
set identity_insert = on;
insert into t_identity(id, col) select 1, 'test';
set identity_insert = off;
-- expected error.
insert into t_identity(col) select 1, 'test';
-- expected error.
insert into t_identity select 1, 'test';
select ident_current('t_identity');
select * from t_identity order by id;

-- expected error.
insert into t_identity(id, col) values(100, 'newtest')
    on duplicate key update col = VALUES(col);
-- expected error.
insert into t_identity(col) values(100, 'newtest')
    on duplicate key update col = VALUES(col);
select ident_current('t_identity');
drop table t_identity;

drop table if exists target_table;
create table target_table (
    id int identity(1,1) PRIMARY KEY,
    name varchar(50), 
    age int
);
drop table if exists source_table;
create table source_table (id int, name varchar(50), age int);
insert into source_table values (10, 'zliu', 28), (11, 'sqi', 30);
-- expected: success. let id generated by system.
merge into target_table AS t using source_table AS s ON t.id = s.id
when not matched then
    insert (name, age) values (s.name, s.age); -- no identity column.
-- expected: success. let id generated by system.
merge into target_table AS t using source_table AS s ON t.id = s.id
when not matched then
    insert values (s.name, s.age);
-- expected error.
merge into target_table AS t using source_table AS s ON t.id = s.id
when not matched then
    insert (id, name, age) values (s.id, s.name, s.age);
-- expected success.
set identity_insert = on;
merge into target_table AS t using source_table AS s ON t.id = s.id
when not matched then
    insert (id, name, age) values (s.id, s.name, s.age);
set identity_insert = off;
-- expected error.
merge into target_table AS t using source_table AS s ON t.id = s.id
when not matched then
    insert values (s.id, s.name, s.age);
select * from target_table order by id;

drop table source_table;
drop table target_table;

-- ## SELECT INTO, copy identity attr of single source table.
create table t_identity_0024(id int identity(100, 1), name varchar(10), age int);
insert into t_identity_0024 (name, age) values('xx', 11);
insert into t_identity_0024 (name, age) values('xxx', 12);
create table t_identity_0025(id1 int, name varchar(10));
-- expected: successs
select name, age into t_identity_0024_01 from t_identity_0024;
\d+ t_identity_0024_01

-- expected: success
set identity_insert = on;
select id into t_identity_0024_02 from t_identity_0024;

select id as id_alias into t_identity_0024_02_1 from t_identity_0024;
\d+ t_identity_0024_02_1
select ident_current('t_identity_0024_02_1');

select * into t_identity_0024_03 from t_identity_0024;
\d+ t_identity_0024_03
select ident_current('t_identity_0024_03');

select id, age as alias_age into t_identity_0024_04 from t_identity_0024;
\d+ t_identity_0024_04
select * from t_identity_0024_04 order by id;
select ident_current('t_identity_0024_04');
insert into t_identity_0024_04(alias_age) values (23);
select * from t_identity_0024_04 order by id;
set identity_insert = off;
-- expected: error
insert into t_identity_0024_04(id, alias_age) values (1000, 22);
set identity_insert = on;
-- expected: success
insert into t_identity_0024_04(id, alias_age) values (1000, 22);
set identity_insert = off;
select ident_current('t_identity_0024_04');
select * from t_identity_0024_04 order by id;

-- expected: success, multiple tables no need to copy identity, thus not to set identity_insert.
select i1.id, i2.id1, i1.age as alias_age into t_identity_0024_05 from t_identity_0024 i1, t_identity_0025 i2 where i1.id = i2.id1;
\d+ t_identity_0024_05

-- same to serial.
-- expected: success
create table t_serial_001(id serial, name varchar(10));
select * into t_serial_001_1 from t_serial_001;
\d+ t_serial_001_1
-- expected: success
create table t_serial_002(id serial, id1 serial, name varchar(10));
select * into t_serial_002_1 from t_serial_002;
\d+ t_serial_002_1
create table t_serial_identity_001(id int identity(200, 2), id1 serial, name varchar(10));
-- expected: error
select * into t_serial_identity_001_1 from t_serial_identity_001;
set identity_insert = on;
-- expected: success
select * into t_serial_identity_001_1 from t_serial_identity_001;
set identity_insert = off;
\d+ t_serial_identity_001_1


drop table t_identity_0024;
drop table t_identity_0024_01;
drop table t_identity_0024_02;
drop table t_identity_0024_02_1;
drop table t_identity_0024_03;
drop table t_identity_0024_04;
drop table t_identity_0024_05;
drop table t_identity_0025;
drop table t_serial_001;
drop table t_serial_001_1;
drop table t_serial_002;
drop table t_serial_002_1;
drop table t_serial_identity_001;
drop table t_serial_identity_001_1;

-- numeric identity test.
---- select into
-- max/min value
create table t_identity_numeric_t1(id numeric(25, 0) identity(200, 2), id1 serial, name varchar(10));
set identity_insert = on;
select id, name into t_identity_numeric_t1_1 from t_identity_numeric_t1;
\d+ t_identity_numeric_t1_1
insert into t_identity_numeric_t1_1(id, name) values(987654321098765432109876, 'xxx'); -- over int64
select * from t_identity_numeric_t1_1 order by id;
set identity_insert = off;
drop table t_identity_numeric_t1;
drop table t_identity_numeric_t1_1;

-- start
create table t_identity_numeric_t2(id numeric(25, 0) identity(987654321098765432100000, 2), id1 serial, name varchar(10));
set identity_insert = on;
select id, name into t_identity_numeric_t2_1 from t_identity_numeric_t2;
set identity_insert = off;
\d+ t_identity_numeric_t2_1
insert into t_identity_numeric_t2_1(name) values('xxx'); -- over int64
select * from t_identity_numeric_t2_1 order by id;
drop table t_identity_numeric_t2;
drop table t_identity_numeric_t2_1;

-- increment
create table t_identity_numeric_t3(id numeric(25, 0) identity(2, 987654321098765432100000), id1 serial, name varchar(10));
set identity_insert = on;
select id, name into t_identity_numeric_t3_1 from t_identity_numeric_t3;
set identity_insert = off;
\d+ t_identity_numeric_t3_1
insert into t_identity_numeric_t3_1(name) values('xxx'); -- over int64
insert into t_identity_numeric_t3_1(name) values('xxx1');
select * from t_identity_numeric_t3_1 order by id;
drop table t_identity_numeric_t3;
drop table t_identity_numeric_t3_1;

---- create table like.
-- max/min value
create table t_identity_numeric_t4(id numeric(38, 0) identity, name varchar(10));
create table t_identity_numeric_t4_1 (like t_identity_numeric_t4 including identity);
\d+ t_identity_numeric_t4_1;
set identity_insert = on;
insert into t_identity_numeric_t4_1(id, name) values(987654321098765432109876543210, 'xxx'); -- over int64
set identity_insert = off;
select * from t_identity_numeric_t4_1 order by id;
drop table t_identity_numeric_t4;
drop table t_identity_numeric_t4_1;

-- start
create table t_identity_numeric_t5(id numeric(38, 0) identity(987654321098765432100000, 2), id1 serial, name varchar(10));
create table t_identity_numeric_t5_1 (like t_identity_numeric_t5 including identity);
insert into t_identity_numeric_t5_1(name) values('xxx'); -- over int64
select * from t_identity_numeric_t5_1 order by id;
drop table t_identity_numeric_t5;
drop table t_identity_numeric_t5_1;

-- increment
create table t_identity_numeric_t6(id numeric(38, 0) identity(2, 987654321098765432100000), id1 serial, name varchar(10));
create table t_identity_numeric_t6_1 (like t_identity_numeric_t6 including identity);
\d+ t_identity_numeric_t6_1
insert into t_identity_numeric_t6_1(name) values('xxx'); -- over int64
insert into t_identity_numeric_t6_1(name) values('xxx1');
select * from t_identity_numeric_t6_1 order by id;
drop table t_identity_numeric_t6;
drop table t_identity_numeric_t6_1;

-- column storage
create table t_ident (id int identity, col text) with (orientation=column);
\d+ t_ident;
insert into t_ident(col) values('column1');
insert into t_ident(col) values('column2');
select SCOPE_IDENTITY();
select ident_current('t_ident');
drop table t_ident;


-- ### test internal sequence name used by identity column
-- create table like
create sequence identity_t2_id_seq_identity;
create table identity_t2(id numeric(38, 0) identity(10, 2), b int);
create table identity_t2_1 (like identity_t2 including identity);
insert into identity_t2_1(b) values (10);
insert into identity_t2_1(b) values (10);
select id, b from identity_t2_1;
drop sequence identity_t2_id_seq_identity;
drop table identity_t2;
drop table identity_t2_1;
-- select into
create sequence identity_t3_id_seq_identity;
create table identity_t3(id numeric(38, 0) identity(10, 3), b int);
set identity_insert = on;
select id, b into identity_t3_1 from identity_t3;
insert into identity_t3_1(b) values (10);
insert into identity_t3_1(b) values (10);
select id, b from identity_t3_1;
drop sequence identity_t3_id_seq_identity;
drop table identity_t3;
drop table identity_t3_1;


-- compatiable with pg identity
-- error
create table identity_t1(id numeric(38, 0) identity(1, 2) generated always as identity);
-- error
create table identity_t1(id numeric(38, 0) identity(1, 2), id2 numeric(37, 0) generated always as identity);


reset current_schema;
drop schema identity_schema;
