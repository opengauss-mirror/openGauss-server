-- Note: about upsert test
-- test point:
--    grammer
--    table, unlogged table, temp table, matview
--    constraint(primary key, index, foreign key, null ...)
--    trigger
--    sequence

CREATE DATABASE upsert WITH TEMPLATE template0 ENCODING 'UTF8';
\c upsert

-- grammer test
CREATE SCHEMA upsert_test;
SET CURRENT_SCHEMA TO upsert_test;
CREATE TYPE atype AS(a int, b int);
CREATE TYPE btype AS(a int, b atype, c varchar[3]);
CREATE TABLE t_grammer (c1 INT PRIMARY KEY, c2 INT DEFAULT -100, c3 int[3], c4 atype, c5 int[2]);
CREATE TABLE t_default (c1 INT PRIMARY KEY DEFAULT 10, c2 FLOAT DEFAULT random(), c3 TIMESTAMP DEFAULT current_timestamp);
CREATE TABLE t_data (c_int INT PRIMARY KEY, c_tiny TINYINT, c_smallint SMALLINT, c_bigint BIGINT, c_numeric NUMERIC,
	c_var VARCHAR, c_text TEXT, c_bytea BYTEA, c_date DATE, c_timestamp TIMESTAMP, c_time TIME,
	c_intarray INT[3], c_com atype);
CREATE TABLE t_trigger (key INT PRIMARY KEY, color TEXT);
CREATE TABLE excluded (a int primary key, b int);
CREATE TABLE "excluded" (a int primary key, b int);


-- unlogged table
CREATE SCHEMA upsert_test_unlog;
SET CURRENT_SCHEMA TO upsert_test_unlog;
CREATE TYPE atype AS(a int, b int);
CREATE TYPE btype AS(a int, b atype, c varchar[3]);
CREATE UNLOGGED TABLE t_hash_unlog_0 (c1 INT, c2 INT, c3 VARCHAR, c4 INT[3], c5 INT[5], c6 atype, c7 btype);
CREATE UNLOGGED TABLE t_hash_unlog_1 (c1 INT, c2 INT PRIMARY KEY, c3 VARCHAR, c4 INT[3], c5 INT[5], c6 atype, c7 btype);
CREATE UNLOGGED TABLE t_rep_unlog_0 (c1 INT, c2 INT, c3 VARCHAR, c4 INT[3], c5 INT[5], c6 atype, c7 btype) ;
CREATE UNLOGGED TABLE t_rep_unlog_1 (c1 INT, c2 INT PRIMARY KEY, c3 VARCHAR, c4 INT[3], c5 INT[5], c6 atype, c7 btype);

-- temp table
-- temp table can not be created here, we create it in upsert_tmp_test.


-- restriction test
CREATE SCHEMA upsert_test_etc;
SET CURRENT_SCHEMA TO upsert_test_etc;
create table up_neg_01 (c1 int, c2 int, c3 int) with (ORIENTATION = COLUMN);
create table up_neg_02 (c1 int, c2 int, c3 int) with (ORIENTATION = COLUMN);
create table up_neg_03  (c1 int, c2 int UNIQUE DEFERRABLE, c3 int);
create table up_neg_04  (c1 int, c2 int UNIQUE, c3 int);

create table up_neg_05(c1 int, c2 int, c3 int, c4 int unique, c5 int primary key, unique(c2,c3));

create table up_neg_0(c1 int, c2 int, c3 int, c4 int unique, c5 int primary key, unique(c2,c3)) ;

create table up_neg_06(c1 int, c2 int, c3 int unique) ;
create table up_neg_07(c1 int, c2 int, c3 int unique) ;
create table up_neg_08(c1 int, c2 int, c3 int unique) ;
create table up_neg_09(c1 int, c2 int, c3 int, unique(c2,c3)) ;
create table up_neg_10(c1 int, c2 int, c3 int, unique(c2,c3)) ;
create view up_view as select *from up_neg_01;
create materialized view mat_view as select *from up_neg_01;
create table pkt (a int primary key, b int, c int);
create table fkt (a int primary key, b int references pkt, c int);
create table up_neg_11(c1 int, c2 int) partition by range(c1)(partition p1 values less than(10), partition p2 values less than(maxvalue));
create unique index index_up_neg_11 on up_neg_11(c2);

-- procedure test
CREATE SCHEMA upsert_test_procedure;


-- explain test
CREATE SCHEMA upsert_test_explain;
SET CURRENT_SCHEMA TO upsert_test_explain;
create table up_expl_hash(c1 int, c2 int, c3 int unique) ;
create table up_expl_repl(c1 int, c2 int, c3 int unique) ;
create table up_expl_part(c1 int, c2 int, c3 int unique) ;
create unlogged table up_expl_unlog(c1 int, c2 int, c3 int unique) ;
create table up_expl_node(c1 int, c2 int, c3 int unique) ;
create table up_expl_repl(c1 int, c2 int, c3 int unique) ;
create table up_expl_repl2(c1 int, c2 int, c3 int, c4 int unique, c5 int primary key, unique(c2,c3));
-- create temp table up_expl_temp(c1 int, c2 int, c3 int unique);

