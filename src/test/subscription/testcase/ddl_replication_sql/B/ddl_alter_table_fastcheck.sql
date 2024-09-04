--
--FOR BLACKLIST FEATURE: REFERENCES/WITH OIDS/RULE/CREATE TYPE/DOMAIN is not supported.
--

--
-- ALTER_TABLE
-- add attribute
--
set search_path=fastcheck;
CREATE TABLE atmp1 (initial int4);

COMMENT ON TABLE tmp_wrong IS 'table comment';
COMMENT ON TABLE atmp1 IS 'table comment';
COMMENT ON TABLE atmp1 IS NULL;

ALTER TABLE atmp1 ADD COLUMN xmin integer; -- fails

ALTER TABLE atmp1 ADD COLUMN a int4 default 3;

ALTER TABLE atmp1 ADD COLUMN b name;

ALTER TABLE atmp1 ADD COLUMN c text;

ALTER TABLE atmp1 ADD COLUMN d float8;

ALTER TABLE atmp1 ADD COLUMN e float4;

ALTER TABLE atmp1 ADD COLUMN f int2;

ALTER TABLE atmp1 ADD COLUMN g polygon;

ALTER TABLE atmp1 ADD COLUMN h abstime;

ALTER TABLE atmp1 ADD COLUMN i char;

ALTER TABLE atmp1 ADD COLUMN j abstime[];

ALTER TABLE atmp1 ADD COLUMN k int4;

ALTER TABLE atmp1 ADD COLUMN l tid;

ALTER TABLE atmp1 ADD COLUMN m xid;

ALTER TABLE atmp1 ADD COLUMN n oidvector;

--ALTER TABLE atmp1 ADD COLUMN o lock;
ALTER TABLE atmp1 ADD COLUMN p smgr;

ALTER TABLE atmp1 ADD COLUMN q point;

ALTER TABLE atmp1 ADD COLUMN r lseg;

ALTER TABLE atmp1 ADD COLUMN s path;

ALTER TABLE atmp1 ADD COLUMN t box;

ALTER TABLE atmp1 ADD COLUMN u tinterval;

ALTER TABLE atmp1 ADD COLUMN v timestamp;

ALTER TABLE atmp1 ADD COLUMN w interval;

ALTER TABLE atmp1 ADD COLUMN x float8[];

ALTER TABLE atmp1 ADD COLUMN y float4[];

ALTER TABLE atmp1 ADD COLUMN z int2[];

INSERT INTO atmp1 (a, b, c, d, e, f, g, h, i, j, k, l, m, n, p, q, r, s, t, u,
	v, w, x, y, z)
   VALUES (4, 'name', 'text', 4.1, 4.1, 2, '(4.1,4.1,3.1,3.1)',
        'Mon May  1 00:30:30 1995', 'c', '{Mon May  1 00:30:30 1995, Monday Aug 24 14:43:07 1992, epoch}',
	314159, '(1,1)', '512',
	'1 2 3 4 5 6 7 8', 'magnetic disk', '(1.1,1.1)', '(4.1,4.1,3.1,3.1)',
	'(0,2,4.1,4.1,3.1,3.1)', '(4.1,4.1,3.1,3.1)', '["epoch" "infinity"]',
	'epoch', '01:00:10', '{1.0,2.0,3.0,4.0}', '{1.0,2.0,3.0,4.0}', '{1,2,3,4}');

SELECT * FROM atmp1;

----drop table tmp;

-- the wolf bug - schema mods caused inconsistent row descriptors
CREATE TABLE atmp2 (
	initial 	int4
);

ALTER TABLE atmp2 ADD COLUMN a int4;

ALTER TABLE atmp2 ADD COLUMN b name;

ALTER TABLE atmp2 ADD COLUMN c text;

ALTER TABLE atmp2 ADD COLUMN d float8;

ALTER TABLE atmp2 ADD COLUMN e float4;

ALTER TABLE atmp2 ADD COLUMN f int2;

ALTER TABLE atmp2 ADD COLUMN g polygon;

ALTER TABLE atmp2 ADD COLUMN h abstime;

ALTER TABLE atmp2 ADD COLUMN i char;

ALTER TABLE atmp2 ADD COLUMN j abstime[];

ALTER TABLE atmp2 ADD COLUMN k int4;

ALTER TABLE atmp2 ADD COLUMN l tid;

ALTER TABLE atmp2 ADD COLUMN m xid;

ALTER TABLE atmp2 ADD COLUMN n oidvector;

--ALTER TABLE atmp2 ADD COLUMN o lock;
ALTER TABLE atmp2 ADD COLUMN p smgr;

ALTER TABLE atmp2 ADD COLUMN q point;

ALTER TABLE atmp2 ADD COLUMN r lseg;

ALTER TABLE atmp2 ADD COLUMN s path;

ALTER TABLE atmp2 ADD COLUMN t box;

ALTER TABLE atmp2 ADD COLUMN u tinterval;

ALTER TABLE atmp2 ADD COLUMN v timestamp;

ALTER TABLE atmp2 ADD COLUMN w interval;

ALTER TABLE atmp2 ADD COLUMN x float8[];

ALTER TABLE atmp2 ADD COLUMN y float4[];

ALTER TABLE atmp2 ADD COLUMN z int2[];

INSERT INTO atmp2 (a, b, c, d, e, f, g, h, i, j, k, l, m, n, p, q, r, s, t, u,
	v, w, x, y, z)
   VALUES (4, 'name', 'text', 4.1, 4.1, 2, '(4.1,4.1,3.1,3.1)',
        'Mon May  1 00:30:30 1995', 'c', '{Mon May  1 00:30:30 1995, Monday Aug 24 14:43:07 1992, epoch}',
	314159, '(1,1)', '512',
	'1 2 3 4 5 6 7 8', 'magnetic disk', '(1.1,1.1)', '(4.1,4.1,3.1,3.1)',
	'(0,2,4.1,4.1,3.1,3.1)', '(4.1,4.1,3.1,3.1)', '["epoch" "infinity"]',
	'epoch', '01:00:10', '{1.0,2.0,3.0,4.0}', '{1.0,2.0,3.0,4.0}', '{1,2,3,4}');

SELECT * FROM atmp2;

----drop table tmp;


--
-- rename - check on both non-temp and temp tables
--
CREATE TABLE atmp3 (regtable int);
-- Enforce use of COMMIT instead of 2PC for temporary objects


CREATE TABLE onek (
	unique1		int4,
	unique2		int4,
	two			int4,
	four		int4,
	ten			int4,
	twenty		int4,
	hundred		int4,
	thousand	int4,
	twothousand	int4,
	fivethous	int4,
	tenthous	int4,
	odd			int4,
	even		int4,
	stringu1	name,
	stringu2	name,
	string4		name
) with(autovacuum_enabled = off);
CREATE INDEX onek_unique1 ON onek USING btree(unique1 int4_ops);

CREATE TABLE tenk1 (
	unique1		int4,
	unique2		int4,
	two			int4,
	four		int4,
	ten			int4,
	twenty		int4,
	hundred		int4,
	thousand	int4,
	twothousand	int4,
	fivethous	int4,
	tenthous	int4,
	odd			int4,
	even		int4,
	stringu1	name,
	stringu2	name,
	string4		name
) with(autovacuum_enabled = off);

CREATE TABLE stud_emp (
	name 		text,
	age			int4,
	location 	point,
	salary		int4,
	manager		name,
	gpa 		float8,
	percent		int4
) with(autovacuum_enabled = off);

-- ALTER TABLE ... RENAME on non-table relations
-- renaming indexes (FIXME: this should probably test the index's functionality)
ALTER INDEX IF EXISTS __onek_unique1 RENAME TO tmp_onek_unique1;
ALTER INDEX IF EXISTS __tmp_onek_unique1 RENAME TO onek_unique1;

ALTER INDEX onek_unique1 RENAME TO tmp_onek_unique1;
ALTER INDEX tmp_onek_unique1 RENAME TO onek_unique1;

-- renaming views
CREATE VIEW tmp_view (unique1) AS SELECT unique1 FROM tenk1;
ALTER TABLE tmp_view RENAME TO tmp_view_new;

DROP VIEW tmp_view_new;
-- toast-like relation name
alter table stud_emp rename to pg_toast_stud_emp;
alter table pg_toast_stud_emp rename to stud_emp;

-- renaming index should rename constraint as well
ALTER TABLE onek ADD CONSTRAINT onek_unique1_constraint UNIQUE (unique1);
ALTER INDEX onek_unique1_constraint RENAME TO onek_unique1_constraint_foo;
ALTER TABLE onek DROP CONSTRAINT onek_unique1_constraint_foo;

-- renaming constraint
ALTER TABLE onek ADD CONSTRAINT onek_check_constraint CHECK (unique1 >= 0);
ALTER TABLE onek RENAME CONSTRAINT onek_check_constraint TO onek_check_constraint_foo;
ALTER TABLE onek DROP CONSTRAINT onek_check_constraint_foo;

-- renaming constraint should rename index as well
ALTER TABLE onek ADD CONSTRAINT onek_unique1_constraint UNIQUE (unique1);
DROP INDEX onek_unique1_constraint;  -- to see whether it's there
ALTER TABLE onek RENAME CONSTRAINT onek_unique1_constraint TO onek_unique1_constraint_foo;
DROP INDEX onek_unique1_constraint_foo;  -- to see whether it's there
ALTER TABLE onek DROP CONSTRAINT onek_unique1_constraint_foo;

-- renaming constraints vs. inheritance
CREATE TABLE constraint_rename_test (a int CONSTRAINT con1 CHECK (a > 0), b int, c int);
\d constraint_rename_test

create table test_modify (a int, b int);
alter table test_modify replica identity full;
alter table test_modify modify (b not null enable);
insert into test_modify(b) values (null);
insert into test_modify values (1, null);
alter table test_modify modify(b null);
insert into test_modify values (1, null);
alter table test_modify modify (b not null enable);
alter table test_modify replica identity full;
delete from test_modify;
alter table test_modify modify (a not null, b not null);
insert into test_modify values (1,null);
insert into test_modify values (null,1);
alter table test_modify modify (a null, b null);
insert into test_modify values (1,null);
insert into test_modify values (null,1);
alter table test_modify modify (b constraint ak not null);
delete from test_modify;
alter table test_modify modify (b constraint ak not null);
insert into test_modify values(1,1);
insert into test_modify values(1,null);
alter table test_modify modify (b constraint ak null);
insert into test_modify values(1,null);
alter table test_modify modify (a null, a not null);
-- try alter view should fail
create view test_modify_view as select * from test_modify;
alter table test_modify_view modify (a not null enable);
drop view test_modify_view;
--drop table test_modify;


-- test setting and removing default values
create table def_test (
	c1	int4 default 5,
	c2	text default 'initial_default'
);
insert into def_test default values;
alter table def_test alter column c1 drop default;
insert into def_test default values;
alter table def_test alter column c2 drop default;
insert into def_test default values;
alter table def_test alter column c1 set default 10;
alter table def_test alter column c2 set default 'new_default';
insert into def_test default values;
select * from def_test order by 1, 2;

-- set defaults to an incorrect type: this should fail
alter table def_test alter column c1 set default 'wrong_datatype';
alter table def_test alter column c2 set default 20;

-- set defaults on a non-existent column: this should fail
alter table def_test alter column c3 set default 30;

create type mytype as (a text);
create table foo (f1 text, f2 mytype, f3 text);

insert into foo values('bb','cc','dd');
select * from foo order by f1;

-- drop domain mytype cascade;

select * from foo order by f1;
insert into foo values('qq','rr');
select * from foo order by f1;
alter table foo replica identity full;
update foo set f3 = 'zz';
select * from foo order by f1;
select f3,max(f1) from foo group by f3;

-- Simple tests for alter table column type
alter table foo replica identity full;
delete from foo where f1 = 'qq';
alter table foo alter f1 TYPE integer; -- fails
alter table foo alter f1 TYPE varchar(10);
--drop table foo;


CREATE TABLE TBL_DOMAIN
(
  IDOMAINID   NUMBER(10) NOT NULL,
  SDOMAINNAME VARCHAR2(30) NOT NULL
);
--create/recreate primary, unique and foreign key constraints 
ALTER TABLE TBL_DOMAIN
  ADD CONSTRAINT PK_TBL_DOMAIN PRIMARY KEY (IDOMAINID)
  USING INDEX ;
  
ALTER TABLE TBL_DOMAIN
  ADD CONSTRAINT IX_TBL_DOMAIN UNIQUE (SDOMAINNAME)
  USING INDEX ;
\d+ TBL_DOMAIN
--drop table TBL_DOMAIN;

--create table
CREATE TABLE TBL_CM_MAXTSENDTOHOST
(
  I_MODULETYPE  NUMBER(38) NOT NULL,
  I_MODULENO    NUMBER(38) NOT NULL,
  I_PLAMODULENO NUMBER(38) NOT NULL,
  I_TABLEID     NUMBER(38) NOT NULL,
  I_OLDMAXTUPLE NUMBER(38) NOT NULL,
  I_NEWMAXTUPLE NUMBER(38) NOT NULL,
  I_RESERVED1   NUMBER(38) DEFAULT 0,
  I_RESERVED2   NUMBER(38) DEFAULT 0,
  I_RESERVED3   NUMBER(38) DEFAULT 0,
  I_RESERVED4   NUMBER(38) DEFAULT 0,
  I_RESERVED5   NUMBER(38) DEFAULT 0,
  I_RESERVED6   NUMBER(38) DEFAULT 0,
  I_RESERVED7   NUMBER(38) DEFAULT 0,
  SV_RESERVED8  VARCHAR2(32) DEFAULT '',
  SV_RESERVED9  VARCHAR2(32) DEFAULT '',
  SV_RESERVED10 VARCHAR2(32) DEFAULT ''
)
  PCTFREE 10
  INITRANS 1
  MAXTRANS 255
  STORAGE
  (
    INITIAL 64K
    MINEXTENTS 1
    MAXEXTENTS UNLIMITED
  )
 ;
--add primary key
ALTER TABLE TBL_CM_MAXTSENDTOHOST
  ADD PRIMARY KEY (I_PLAMODULENO, I_TABLEID)
  USING INDEX 
  PCTFREE 10
  INITRANS 2
  MAXTRANS 255
  STORAGE
  (
    INITIAL 64K
    MINEXTENTS 1
    MAXEXTENTS UNLIMITED
  );
 \d+ TBL_CM_MAXTSENDTOHOST
 --drop table TBL_CM_MAXTSENDTOHOST;

--create table
CREATE TABLE TBL_LICCTRLDESC_DEFAULT
(
  I_INDEX        NUMBER(38) NOT NULL,
  SV_FEATURENAME VARCHAR2(64) NOT NULL,
  SV_ITEMNAME    VARCHAR2(64) NOT NULL,
  I_ITEMTYPE     NUMBER(38) NOT NULL,
  I_ITEMVALUEMIN NUMBER(38) NOT NULL,
  I_ITEMVALUEMAX NUMBER(38) NOT NULL,
  I_RESERVED1    NUMBER(38) DEFAULT 0,
  I_RESERVED2    NUMBER(38) DEFAULT 0,
  I_RESERVED3    NUMBER(38) DEFAULT 0,
  I_RESERVED4    NUMBER(38) DEFAULT 0,
  I_RESERVED5    NUMBER(38) DEFAULT 0,
  I_RESERVED6    NUMBER(38) DEFAULT 0,
  I_RESERVED7    NUMBER(38) DEFAULT 0,
  SV_RESERVED8   VARCHAR2(32) DEFAULT '',
  SV_RESERVED9   VARCHAR2(32) DEFAULT '',
  SV_RESERVED10  VARCHAR2(32) DEFAULT '',
  I_STATUS       NUMBER(38) NOT NULL
)
  PCTFREE 10
  INITRANS 1
  MAXTRANS 255
  STORAGE
  (
    INITIAL 64K
    MINEXTENTS 1
    MAXEXTENTS UNLIMITED
  )
 ;
--add primary key
ALTER TABLE TBL_LICCTRLDESC_DEFAULT
  ADD PRIMARY KEY (I_INDEX)
  USING INDEX 
  PCTFREE 10
  INITRANS 2
  MAXTRANS 255
  STORAGE
  (
    INITIAL 64K
    MINEXTENTS 1
    MAXEXTENTS UNLIMITED
  );
--add unique index
CREATE UNIQUE INDEX IDX_TBL_LICCTRL_DEF ON TBL_LICCTRLDESC_DEFAULT (I_INDEX DESC, I_STATUS)
  PCTFREE 10
  INITRANS 2
  MAXTRANS 255
  STORAGE
  (
    INITIAL 64K
    MINEXTENTS 1
    MAXEXTENTS UNLIMITED
  );
\d+ TBL_LICCTRLDESC_DEFAULT
 --drop table TBL_LICCTRLDESC_DEFAULT;
--using index clause
CREATE TABLE STUDENTS
(
	ID INT,
	NAME VARCHAR2(20),
	AGE INT,
	ADDRESS VARCHAR(30)
);
 --alter table to add unique index or primary key 
ALTER TABLE STUDENTS ADD UNIQUE (ID)
USING INDEX
PCTFREE 10
INITRANS 2
MAXTRANS 255
STORAGE
(
  INITIAL 64K
  MINEXTENTS 1
  MAXEXTENTS UNLIMITED
);

ALTER TABLE STUDENTS ADD CONSTRAINT ZHANGYG UNIQUE (AGE, ADDRESS)
USING INDEX
PCTFREE 10
INITRANS 2
MAXTRANS 255
STORAGE
(
  INITIAL 64K
  MINEXTENTS 1
  MAXEXTENTS UNLIMITED
);

ALTER TABLE STUDENTS ADD PRIMARY KEY (AGE)
USING INDEX
PCTFREE 10
INITRANS 2
MAXTRANS 255
STORAGE
(
  INITIAL 64K
  MINEXTENTS 1
  MAXEXTENTS UNLIMITED
);
\d+ STUDENTS
--drop table STUDENTS;
--simulate A db's ALTER TABLE gram
CREATE TABLE MODIFY_TABLE_A(I INTEGER);
ALTER TABLE MODIFY_TABLE_A ADD (mychar CHAR); 
ALTER TABLE MODIFY_TABLE_A ADD (myint1 INT, mychar1 CHAR);
ALTER TABLE MODIFY_TABLE_A ADD (myint2 INT, mychar2 CHAR, mychar3 CHAR);
ALTER TABLE MODIFY_TABLE_A ADD a CHAR, ADD b CHAR;
\d MODIFY_TABLE_A
ALTER TABLE MODIFY_TABLE_A ADD mychar4 CHAR;
\d MODIFY_TABLE_A
ALTER TABLE MODIFY_TABLE_A MODIFY I VARCHAR2(64);
\d MODIFY_TABLE_A
ALTER TABLE MODIFY_TABLE_A MODIFY I CHAR, MODIFY myint1 CHAR;
\d MODIFY_TABLE_A
ALTER TABLE MODIFY_TABLE_A MODIFY (myint1 VARCHAR(12));
\d MODIFY_TABLE_A
ALTER TABLE MODIFY_TABLE_A MODIFY (myint1 VARCHAR(13), mychar1 INT);
\d MODIFY_TABLE_A
ALTER TABLE MODIFY_TABLE_A MODIFY (myint1 VARCHAR(13), myint1 INT);
--drop table MODIFY_TABLE_A;


CREATE SCHEMA test_sche;
CREATE TABLE test_sche.logical_TB1(
c1 integer,
c2 date,
c3 text)
partition by system
(
partition p1,
partition p2,
partition p3);

insert into test_sche.logical_TB1 partition(p1) values(1,'2022-01-01','p1');
insert into test_sche.logical_TB1 partition(p2) values(2,'2022-02-01','p2');
insert into test_sche.logical_TB1 partition(p2) values(3,'2022-02-01','p3');
truncate test_sche.logical_TB1;
--drop table test_sche.logical_TB1;

CREATE TABLE MODIFY_TABLE_A(I INTEGER);
\d MODIFY_TABLE_A
create table aaa(a integer);
\d aaa
create table bbb(B integer);
\d bbb
create table CCC(c integer);
\d CCC
create table DDD(D integer);
\d DDD
create table EEE("E" integer);
\d EEE
create table FFF("FF" integer);
\d FFF
create table HHH("HH" integer);

alter table aaa rename a to AA;
\d aaa
create table GGG("GdGG" integer);
alter table CCC rename c to "CC";
alter table FFF rename FF to ff; -- differnt in b compatibility
alter table HHH rename "HH" to gg;

rename table public.HHH to public.hhh;
rename table public.hhh to public.hhh1;

insert into t1_full values (4,'d');
insert into t1_full values (5, 'e');
create type mytyp as (a int, b text);
alter table t1_full add column c timestamp default now() not null first;
alter table t1_full add column d timestamp on update current_timestamp;

alter table t1_full add column e int auto_increment unique;
alter table t1_full alter column b set data type timestamp using now();
alter table t1_full add column ff mytyp default(1, now()::text);
alter table t1_full add column ff33 mytyp default(1, current_timestamp(3)::text);

alter table t1_full rename to t1_repl_index;
alter table t1_repl_index add constraint t1_pkey_a primary key (a);
alter table t1_repl_index replica identity default;
alter table t1_repl_index add column f int auto_increment unique;
alter table t1_repl_index add column f int auto_increment null unique;
alter table t1_repl_index alter column b set data type timestamp using now();
alter table t1_repl_index add column e timestamp default now() not null;
alter table t1_repl_index alter column e set data type float using random();
alter table t1_repl_index add column h int default random();
alter table t1_repl_index add column h int;
alter table t1_repl_index alter column h set data type float;
update t1_repl_index set h=random();
alter table t1_repl_index add column g timestamp generated always as (b + '1 year');
insert into t1_repl_index (a) values (200), (201), (202);
-- drop table t1_repl_index;

insert into tkey1 values (10), (12);
alter table tkey1 modify column b float4 auto_increment unique;
alter table tkey1 modify column b int auto_increment null unique;
drop table tkey1;

create table blobtbl (id int primary key, a blob, b raw, c clob, d bytea);
alter table blobtbl replica identity default;
insert into blobtbl values (1, utl_raw.cast_to_raw('this is blob'), utl_raw.cast_to_raw('this is raw'), 'this is clob', decode('this is bytea', 'escape'));
insert into blobtbl values (2, utl_raw.cast_to_raw('this is blob2'), utl_raw.cast_to_raw('this is raw2'), 'this is clob2', decode('this is bytea2', 'escape'));
insert into blobtbl values (3, utl_raw.cast_to_raw('this is blob3'), utl_raw.cast_to_raw('this is raw3'), 'this is clob3', decode('this is bytea3', 'escape'));

update blobtbl set a=utl_raw.cast_to_raw('this is blob after update'), b=utl_raw.cast_to_raw('this is raw after update'), c='this is clob after update', d=decode('this is bytea after i[date]', 'escape') where id=2;
delete from blobtbl where id=3;

select utl_raw.cast_to_varchar2(a) as blob_col, utl_raw.cast_to_varchar2(b) as raw_col, cast(c as varchar)  as clob_col, encode(d, 'escape') as bytea_col into blobtbl_1 from blobtbl;

create table blobtbl_2 as (select utl_raw.cast_to_varchar2(a) as blob_col, utl_raw.cast_to_varchar2(b) as raw_col, cast(c as varchar) as clob_col, encode(d, 'escape') as bytea_col from blobtbl);

create schema testb;
set search_path='testb';
create table t1 (a int, b timestamp without time zone);
alter table t1 alter column b set default now();
alter table t1 modify column b timestamp on update current_timestamp;
insert into t1 (a,b) values  (1,default), (2,default),(3,'1900-01-01 1:00:00');
alter table t1 replica identity full;
create type typ1 as (a int, b text);

alter table t1 add column c typ1 default(1, now()::text);
alter type typ1 add attribute c timestamp;
alter table t1 add constraint t1_pkey primary key (a);
alter table t1 replica identity default;
alter table t1 alter column b set data type timestamp using now() - a;
create type typ2;
create type typ2 as (a int, b int);
alter type typ2 drop attribute a;
drop type typ2;

create table tab1_1163900(id int not null,a1 text) partition by range(id);
create table tab2_1163900(id int not null,a1 text) partition by list(id);
create table tab3_1163900(id int not null,a1 text) partition by hash(id);
--create table;
create table t1_1163900(id int not null,a1 text);
create table t2_1163900(id int not null,a1 text);
create table t3_1163900(id int not null,a1 text);
--insert;
insert into t1_1163900(id,a1) select generate_series(1,100),'a';
--t3_1163900;
insert into t3_1163900(id,a1) select generate_series(1,100),'a';
--t2_1163900;
do $$
declare
begin
for i in 1..100 loop
insert into t2_1163900 values(20,'a');
end loop;
end $$;

--attach;
alter table tab1_1163900 attach partition t1_1163900 for values from (1) to (1000);
alter table tab2_1163900 attach partition t2_1163900 for values in(20);
alter table tab3_1163900 attach partition t3_1163900 for values with(modulus 1,remainder 0);

create table aaaaa1 (b int generated by default as identity (cycle increment by 10),c int);
-- \dS aaaaa_b_seq
-- insert into aaaaa(c) values(213);
-- insert into aaaaa(c) values(21);
-- insert into aaaaa values(3,121);
-- insert into aaaaa(c) values(111);
-- insert into aaaaa values(null,212);
-- alter table aaaaa alter column b drop default;
-- drop sequence aaaaa_b_seq;

create table bbbb (a int not null);
alter table bbbb alter column a add generated by default as identity;

create table genalways(id bigint generated always as identity (start 68 cycle maxvalue 70),name varchar(40));

create table genalways2(id smallint generated always as identity (start 68 cycle maxvalue 70),name varchar(40));

drop table if exists gentest;
create table gentest(id integer PRIMARY KEY, name varchar(40));
/* AT_AddIdentity */
ALTER TABLE gentest ALTER  id ADD GENERATED ALWAYS AS IDENTITY (start 12 maxvalue 322);
/* AT_SetIdentity in pg compatibility */
ALTER TABLE gentest ALTER  id SET GENERATED ALWAYS;
ALTER TABLE gentest ALTER  id DROP IDENTITY;
ALTER TABLE gentest ALTER  id ADD GENERATED BY DEFAULT AS IDENTITY (start 99 maxvalue 1000);
ALTER TABLE gentest ALTER  id DROP IDENTITY IF EXISTS;
ALTER TABLE gentest ALTER  id ADD GENERATED ALWAYS AS IDENTITY (start 33 maxvalue 333);
ALTER TABLE gentest ALTER  id SET GENERATED BY DEFAULT;
ALTER TABLE gentest ALTER  id RESTART WITH 123;
ALTER TABLE gentest ALTER  id RESTART;



CREATE TABLE range_sales
(
    product_id     INT4 NOT NULL,
    customer_id    INT4 PRIMARY KEY,
    time_id        DATE,
    channel_id     CHAR(1),
    type_id        INT4,
    quantity_sold  NUMERIC(3),
    amount_sold    NUMERIC(10,2)
)
PARTITION BY RANGE (time_id)
(
    PARTITION time_2008 VALUES LESS THAN ('2009-01-01'),
    PARTITION time_2009 VALUES LESS THAN ('2010-01-01'),
    PARTITION time_2010 VALUES LESS THAN ('2011-01-01'),
    PARTITION time_2011 VALUES LESS THAN ('2012-01-01')
);
INSERT INTO range_sales SELECT generate_series(1,1000),
                               generate_series(1,1000),
                               date_pli('2008-01-01', generate_series(1,1000)),
                               generate_series(1,1000)%10,
                               generate_series(1,1000)%10,
                               generate_series(1,1000)%1000,
                               generate_series(1,1000);
CREATE INDEX range_sales_idx ON range_sales(product_id) LOCAL;
--success, add 1 partition
ALTER TABLE range_sales ADD PARTITION time_2012 VALUES LESS THAN ('2013-01-01');
--success, add 1 partition
ALTER TABLE range_sales ADD PARTITION time_end VALUES LESS THAN (MAXVALUE);

ALTER TABLE range_sales DROP PARTITION time_2009;
--success, drop partition time_2011
ALTER TABLE range_sales DROP PARTITION FOR ('2011-06-01');
ALTER TABLE range_sales DROP PARTITION time_2012 update global index;


create table t_tinyint0018 (
    c1 tinyint,
    c2 tinyint(1) default null,
    c3 tinyint(10) not null default '0',
    c4 tinyint default '0',
        c5 text
);
alter table t_tinyint0018 add unique index i_tinyint0018(c1, c2, c5(10));

create table t1_addkey (a int, b int, c int, d int);
alter table t1_addkey add primary key (a, b);
alter table t1_addkey add unique (c);



CREATE TABLE test_alter_autoinc_col(col int unsigned primary key);
INSERT INTO test_alter_autoinc_col VALUES(1);
ALTER TABLE test_alter_autoinc_col ADD COLUMN id int unsigned AUTO_INCREMENT unique;



create table alter_table_tbl1 (a int primary key, b int);
create table alter_table_tbl2 (c int primary key, d int);
alter table alter_table_tbl2 add constraint alter_table_tbl_fk foreign key (d) references alter_table_tbl1 (a);

create index alter_table_tbl_b_ind on alter_table_tbl1(b);

-- disbale/enable keys
alter table alter_table_tbl1 disable keys;


alter table alter_table_tbl1 enable keys;

-- drop index/key index_name
alter table alter_table_tbl1 drop index alter_table_tbl_b_ind;

create index alter_table_tbl_b_ind on alter_table_tbl1(b);
alter table alter_table_tbl1 drop key alter_table_tbl_b_ind;
alter table alter_table_tbl2 drop primary key;

alter table alter_table_tbl2 drop foreign key alter_table_tbl_fk;

create index alter_table_tbl_b_ind on alter_table_tbl1(b);
alter table alter_table_tbl1 rename index alter_table_tbl_b_ind to new_alter_table_tbl_b_ind;


alter table alter_table_tbl1 rename to new_alter_table_tbl1;
alter table new_alter_table_tbl1 rename as new_new_alter_table_tbl1;
alter table new_new_alter_table_tbl1 rename new_new_new_alter_table_tbl1;
alter table if exists new_new_new_alter_table_tbl1 rename alter_table_tbl1;
alter table if exists not_exists_tbl rename new_not_exists_tbl;


alter table alter_table_tbl1 add column key int, rename index new_alter_table_tbl_b_ind to alter_table_tbl_b_ind;
alter table alter_table_tbl1 drop column key, drop key alter_table_tbl_b_ind;

ALTER TABLE alter_table_tbl1 RENAME COLUMN a TO AB;
ALTER TABLE alter_table_tbl1 RENAME COLUMN ab TO Ab;
ALTER TABLE alter_table_tbl1 RENAME AB TO AB;
ALTER TABLE alter_table_tbl1 RENAME ab TO ab;
ALTER TABLE if exists alter_table_tbl1 RENAME COLUMN AB TO Ab;
ALTER TABLE if exists alter_table_tbl1 RENAME COLUMN Ab TO ab;
ALTER TABLE if exists alter_table_tbl1 RENAME AB TO ab;
ALTER TABLE if exists alter_table_tbl1 RENAME Ab TO AB;
ALTER TABLE if exists alter_table_tbl1 RENAME Ab AS AB;


ALTER TABLE alter_table_tbl1 CHANGE AB ab int;
ALTER TABLE alter_table_tbl1 CHANGE COLUMN AB ABCC int;
ALTER TABLE alter_table_tbl1 CHANGE COLUMN ABCCC AB varchar;


CREATE TABLE t_alter_test(c text);
ALTER TABLE t_alter_test DEFAULT COLLATE = test_collate;
ALTER TABLE t_alter_test DEFAULT CHARACTER SET = test_charset;
ALTER TABLE t_alter_test DEFAULT CHARSET = test_charset;
ALTER TABLE t_alter_test default CHARACTER SET = utf_8;
ALTER TABLE t_alter_test CHARACTER SET = utf_8;
ALTER TABLE t_alter_test convert to CHARACTER SET utf_8; 

CREATE TABLE IF NOT EXISTS test_part
(
a int primary key not null default 5,
b int,
c int,
d int
) 
PARTITION BY RANGE(a)
(
    PARTITION p0 VALUES LESS THAN (1000),
    PARTITION p1 VALUES LESS THAN (2000),
    PARTITION p2 VALUES LESS THAN (3000)
);

create unique index idx_c on test_part (c);
create index idx_b on test_part using btree(b) local;
alter table test_part add constraint uidx_d unique(d);
alter table test_part add constraint uidx_c unique using index idx_c;

insert into test_part (with RECURSIVE t_r(i,j,k,m) as(values(0,1,2,3) union all select i+1,j+2,k+3,m+4 from t_r where i < 2500) select * from t_r);

ALTER TABLE test_part REBUILD PARTITION p0, p1;
ALTER TABLE test_part REBUILD PARTITION all;

ALTER TABLE test_part ANALYZE PARTITION p0, p1;
ALTER TABLE test_part ANALYZE PARTITION all;


ALTER TABLE test_part remove PARTITIONING;


CREATE TABLE bcomp_t1(id int, t text, ref int);
CREATE TABLE bcomp_t2(id int, t text);

alter table bcomp_t2 add constraint unique_id unique(id);
alter table bcomp_t1 add foreign key(ref) references bcomp_t2(id);
alter table bcomp_t1 drop foreign key bcomp_t1_ref_fkey;

CREATE TABLE bcomp_test_table_1 (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255),
    age INT
);
ALTER TABLE bcomp_test_table_1 ADD INDEX idx_age (age);
ALTER TABLE bcomp_test_table_1 rename index idx_age to index_age;
ALTER TABLE bcomp_test_table_1 DROP INDEX index_age;

CREATE TABLE test (
id int unsigned auto_increment not null primary key,
title varchar,
boby text,
name name
);
CREATE FULLTEXT INDEX test_index_1 ON test (title, boby) WITH PARSER ngram;
CREATE FULLTEXT INDEX test_index_2 ON test (title, boby, name);
ALTER TABLE test ADD FULLTEXT INDEX test_index_1 (title, boby) WITH PARSER ngram;

CREATE TABLE bcomp_test_table_2 (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255),
    age INT
);
ALTER TABLE bcomp_test_table_2 ADD CONSTRAINT chk_age_range CHECK (age BETWEEN 18 AND 65);
ALTER TABLE bcomp_test_table_2 DROP CONSTRAINT chk_age_range;

CREATE TABLE bcomp_test_table_3 (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255),
    age INT
);
CREATE INDEX idx_age ON bcomp_test_table_3(age);
ALTER TABLE bcomp_test_table_3 ADD KEY idx_age (age);
ALTER TABLE bcomp_test_table_3 DROP KEY idx_age;

CREATE TABLE tt (a int primary key);
alter table tt drop primary key;

CREATE TABLE bcomp_test_table_4 (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255),
    age INT);
ALTER TABLE bcomp_test_table_4 DISABLE KEYS;
ALTER TABLE bcomp_test_table_4 ENABLE KEYS;

CREATE TABLE bcomp_test_table_5 (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255),
    age INT);
CREATE INDEX idx_age ON bcomp_test_table_5(age);
ALTER TABLE bcomp_test_table_5 RENAME INDEX idx_age TO idx_age_1;
ALTER TABLE bcomp_test_table_5 DROP INDEX idx_age_1;

CREATE TABLE bcomp_test_table_6 (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255),
    created_at DATE
) PARTITION BY RANGE COLUMNS(id) (
    PARTITION p0 VALUES LESS THAN (100),
    PARTITION p1 VALUES LESS THAN (200),
    PARTITION p2 VALUES LESS THAN (MAXVALUE)
);
ALTER TABLE bcomp_test_table_6 REMOVE PARTITIONING;

CREATE DATABASE test_db;
ALTER DATABASE test_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE TABLE t(num int);


create definer = ddl_test_user event IF NOT EXISTS ee11 on schedule EVERY 1 day at '2022-12-09 17:24:11' disable do insert into t values(0);

create event IF NOT EXISTS ee12 on schedule EVERY 2 day at '2022-12-09 17:24:11' ends '2028-12-09 17:24:11' disable do insert into t values(0);

create event IF NOT EXISTS ee13 on schedule EVERY 2 day at '2022-12-09 17:24:11' disable do insert into t values(0);

alter definer = ddl_test_user event ee13 on schedule AT '2099-12-11 17:24:11' comment 'jhhh' do insert into t values(1);

alter definer = ddl_test_user event ee13  on schedule EVERY 1 day starts '2022-12-09 17:24:11' ends '2028-12-09 17:24:11' ON COMPLETION PRESERVE enable do insert into t values(1);

alter event ee13 on schedule AT '2055-12-11 17:24:11' enable comment 'jhhh' do insert into t values(1);

alter event ee12 on schedule at '2055-12-09 17:24:11' enable;


select  job_name, nspname from pg_job where dbname='event_b';
drop event if exists ee11;
drop event if exists ee13;

create event IF NOT EXISTS ee14 on schedule EVERY 2 day at '2022-12-09 17:24:11' disable do insert into t values(0);

alter event ee14 on schedule at '2055-12-09 17:24:11' disable;
alter event ee14 on schedule at '2055-12-09 17:24:11' enable;
alter event ee14 on schedule at '2055-12-09 17:24:11' DISABLE ON SLAVE;
alter event ee14 rename to  ee142;
drop event if exists ee142;

create table t1_z (col1 int primary key auto_increment , col2 text,col3 bigint);
insert into t1_z(col1,col2) values(3, 'aaa');
alter table t1_z auto_increment = 3;

drop table t1_z;