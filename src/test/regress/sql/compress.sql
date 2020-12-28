--
-- COMPRESS
--

-- ******************testing basic system tables ********************

SELECT DISTINCT relcmprs FROM pg_class WHERE OID<16384;

-- ****************** CREATE TABLE ********************

-- case 1: At default table is NO-COMPRESS
CREATE TABLE defcmprs(id int, name text);
SELECT relcmprs FROM pg_class WHERE relname='defcmprs';
INSERT INTO defcmprs VALUES(1, 'Cat');
INSERT INTO defcmprs VALUES(2, 'Dog');
INSERT INTO defcmprs VALUES(3, 'Polly');
INSERT INTO defcmprs VALUES(4, 'Cow');
SELECT * FROM defcmprs ORDER BY id;
DROP TABLE defcmprs;

-- case 2: Given nocompress/NOCOMPRESS
CREATE TABLE uncmprs(id int, name text) nocompress;
SELECT relcmprs FROM pg_class WHERE relname='uncmprs';
DROP TABLE uncmprs;

CREATE TABLE uncmprs(id int, name text) NOCOMPRESS;
SELECT relcmprs FROM pg_class WHERE relname='uncmprs';
INSERT INTO uncmprs VALUES(1, 'Cat');
INSERT INTO uncmprs VALUES(2, 'Dog');
INSERT INTO uncmprs VALUES(3, 'Polly');
INSERT INTO uncmprs VALUES(4, 'Cow');
SELECT * FROM uncmprs ORDER BY id;
DROP TABLE uncmprs;

-- case 3:  Given compress/COMPRESS
CREATE TABLE cmprs(id int, name text) ;
SELECT relcmprs FROM pg_class WHERE relname='cmprs';
DROP TABLE cmprs;
CREATE TABLE cmprs(id int, name text) ;
SELECT relcmprs FROM pg_class WHERE relname='cmprs';
INSERT INTO cmprs VALUES(1, 'Cat');
INSERT INTO cmprs VALUES(2, 'Dog');
INSERT INTO cmprs VALUES(3, 'Polly');
INSERT INTO cmprs VALUES(4, 'Cow');
SELECT * FROM cmprs ORDER BY id;
-- View don't support COMPRESS
CREATE VIEW myview AS SELECT name FROM cmprs;
SELECT relcmprs FROM pg_class WHERE relname='myview';
SELECT * FROM myview ORDER BY name;
-- Index don't support COMPRESS
CREATE INDEX myindex ON cmprs(name);
SELECT relcmprs FROM pg_class WHERE relname='myview';

DROP TABLE cmprs CASCADE;

-- ****************** ALTER TABLE ********************
-- case 1: COMPRESS -> NOCOMPRESS -> COMPRESS
CREATE TABLE compresstbl(id int, name text, addr text) ;
SELECT relcmprs FROM pg_class WHERE relname='compresstbl';
-- 	case 1.1: COMPRESS -> NOCOMPRESS
ALTER TABLE compresstbl SET NOCOMPRESS;
SELECT relcmprs FROM pg_class WHERE relname='compresstbl';
-- 	case 1.2: NOCOMPRESS -> NOCOMPRESS
ALTER TABLE compresstbl SET NOCOMPRESS;
SELECT relcmprs FROM pg_class WHERE relname='compresstbl';
-- 	case 1.3: NOCOMPRESS -> COMPRESS
ALTER TABLE compresstbl SET COMPRESS;
SELECT relcmprs FROM pg_class WHERE relname='compresstbl';
-- 	case 1.4: COMPRESS -> COMPRESS
ALTER TABLE compresstbl SET COMPRESS;
SELECT relcmprs FROM pg_class WHERE relname='compresstbl';
DROP TABLE compresstbl;

-- case 2: NOCOMPRESS -> COMPRESS -> NOCOMPRESS
CREATE TABLE uncompresstbl(id int, name text, addr text);
SELECT relcmprs FROM pg_class WHERE relname='uncompresstbl';
ALTER TABLE uncompresstbl SET COMPRESS;
SELECT relcmprs FROM pg_class WHERE relname='uncompresstbl';
ALTER TABLE uncompresstbl RENAME COLUMN addr TO addrress;
SELECT relcmprs FROM pg_class WHERE relname='uncompresstbl';
ALTER TABLE uncompresstbl SET NOCOMPRESS;
SELECT relcmprs FROM pg_class WHERE relname='uncompresstbl';
ALTER TABLE uncompresstbl RENAME COLUMN name TO tblname;
SELECT relcmprs FROM pg_class WHERE relname='uncompresstbl';
DROP TABLE uncompresstbl;

-- specify the compress method
CREATE TABLE cmpr_methods
(
	id int,
	a varchar(20) prefix,
	b int delta,
	c numeric(20) dictionary,
	d varchar(100) numstr,
	e date nocompress,
	f timestamp
) ;
SELECT relcmprs FROM pg_class WHERE relname='cmpr_methods';
SELECT attcmprmode FROM pg_attribute WHERE attname = 'id' AND attrelid=(SELECT oid FROM pg_class WHERE relname='cmpr_methods');
SELECT attcmprmode FROM pg_attribute WHERE attname = 'a' AND attrelid=(SELECT oid FROM pg_class WHERE relname='cmpr_methods');
SELECT attcmprmode FROM pg_attribute WHERE attname = 'b' AND attrelid=(SELECT oid FROM pg_class WHERE relname='cmpr_methods');
SELECT attcmprmode FROM pg_attribute WHERE attname = 'c' AND attrelid=(SELECT oid FROM pg_class WHERE relname='cmpr_methods');
SELECT attcmprmode FROM pg_attribute WHERE attname = 'd' AND attrelid=(SELECT oid FROM pg_class WHERE relname='cmpr_methods');
SELECT attcmprmode FROM pg_attribute WHERE attname = 'e' AND attrelid=(SELECT oid FROM pg_class WHERE relname='cmpr_methods');
SELECT attcmprmode FROM pg_attribute WHERE attname = 'f' AND attrelid=(SELECT oid FROM pg_class WHERE relname='cmpr_methods');
DROP TABLE cmpr_methods;

-- ****************** About partition ********************
drop table if exists rp;
create table rp
(
	c1 int,
	c2 int
)

partition by range (c1)
(
	partition p0 values less than (50),
	partition p1 values less than (100),
	partition p2 values less than (150)
);
INSERT INTO rp VALUES(1, 50);
INSERT INTO rp VALUES(25, 50);
INSERT INTO rp VALUES(49, 50);
INSERT INTO rp VALUES(50, 50);
INSERT INTO rp VALUES(75, 50);
INSERT INTO rp VALUES(99, 50);
INSERT INTO rp VALUES(100, 50);
INSERT INTO rp VALUES(120, 50);
INSERT INTO rp VALUES(149, 50);
SELECT relcmprs FROM pg_class WHERE relname='rp';
SELECT * FROM rp ORDER BY c1;
ALTER TABLE rp SET NOCOMPRESS;
SELECT relcmprs FROM pg_class WHERE relname='rp';
DROP TABLE rp;

-- test infterface pg_relation_with_compression()
create table row_tbl ( a int , b int ) ;
select pg_relation_with_compression('row_tbl');
alter table row_tbl set nocompress;
select pg_relation_with_compression('row_tbl');
drop table row_tbl;
select pg_relation_with_compression('pg_class');
create table column_tbl ( a int, b int) with ( orientation = column ) ;
select pg_relation_with_compression('column_tbl');
alter table column_tbl set ( compression = no);
select pg_relation_with_compression('column_tbl');
drop table column_tbl;
