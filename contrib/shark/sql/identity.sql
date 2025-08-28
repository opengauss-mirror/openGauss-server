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

-- ident_current after truncate table. expected: success.
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

reset current_schema;
drop schema identity_schema;
