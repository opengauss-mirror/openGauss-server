create schema identity_schema;
set current_schema = identity_schema;
CREATE TABLE book(bookId int IDENTITY, bookname NVARCHAR(50), author NVARCHAR(50));
INSERT INTO book VALUES('book1','author1'),('book2','author2');
INSERT INTO book(bookname,author) VALUES('book3','author3'),('book4','author4');
INSERT INTO book VALUES(3,'book5','author5');
select * from book;
ALTER table book add column id int identity;
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

reset current_schema;
drop schema identity_schema;