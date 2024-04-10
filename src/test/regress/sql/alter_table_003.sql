--
--FOR BLACKLIST FEATURE: REFERENCES/INHERITS/WITH OIDS/RULE/CREATE TYPE/DOMAIN is not supported.
--
-- test inheritance

create table dropColumn (a int, b int, e int);
create table dropColumnChild (c int) inherits (dropColumn);
create table dropColumnAnother (d int) inherits (dropColumnChild);

-- these two should fail
alter table dropColumnchild drop column a;
alter table only dropColumnChild drop column b;



-- these three should work
alter table only dropColumn drop column e;
alter table dropColumnChild drop column c;
alter table dropColumn drop column a;

create table renameColumn (a int);
create table renameColumnChild (b int) inherits (renameColumn);
create table renameColumnAnother (c int) inherits (renameColumnChild);

-- these three should fail
alter table renameColumnChild rename column a to d;
alter table only renameColumnChild rename column a to d;
alter table only renameColumn rename column a to d;

-- these should work
alter table renameColumn rename column a to d;
alter table renameColumnChild rename column b to a;

-- these should work
alter table if exists doesnt_exist_tab rename column a to d;
alter table if exists doesnt_exist_tab rename column b to a;

-- this should work
alter table renameColumn add column w int;

-- this should fail
alter table only renameColumn add column x int;


-- Test corner cases in dropping of inherited columns

create table p1 (f1 int, f2 int);
create table c1 (f1 int not null) inherits(p1);

-- should be rejected since c1.f1 is inherited
alter table c1 drop column f1;
-- should work
alter table p1 drop column f1;
-- c1.f1 is still there, but no longer inherited
select f1 from c1;
alter table c1 drop column f1;
select f1 from c1;

drop table p1 cascade;

create table p1 (f1 int, f2 int);
create table c1 () inherits(p1);

-- should be rejected since c1.f1 is inherited
alter table c1 drop column f1;
alter table p1 drop column f1;
-- c1.f1 is dropped now, since there is no local definition for it
select f1 from c1;

drop table p1 cascade;

create table p1 (f1 int, f2 int);
create table c1 () inherits(p1);

-- should be rejected since c1.f1 is inherited
alter table c1 drop column f1;
alter table only p1 drop column f1;
-- c1.f1 is NOT dropped, but must now be considered non-inherited
alter table c1 drop column f1;

drop table p1 cascade;

create table p1 (f1 int, f2 int);
create table c1 (f1 int not null) inherits(p1);

-- should be rejected since c1.f1 is inherited
alter table c1 drop column f1;
alter table only p1 drop column f1;
-- c1.f1 is still there, but no longer inherited
alter table c1 drop column f1;

drop table p1 cascade;

create table p1(id int, name text);
create table p2(id2 int, name text, height int);
create table c1(age int) inherits(p1,p2);
create table gc1() inherits (c1);

select relname, attname, attinhcount, attislocal
from pg_class join pg_attribute on (pg_class.oid = pg_attribute.attrelid)
where relname in ('p1','p2','c1','gc1') and attnum > 0 and not attisdropped
order by relname, attnum;

-- should work
alter table only p1 drop column name;
-- should work. Now c1.name is local and inhcount is 0.
alter table p2 drop column name;
-- should be rejected since its inherited
alter table gc1 drop column name;
-- should work, and drop gc1.name along
alter table c1 drop column name;
-- should fail: column does not exist
alter table gc1 drop column name;
-- should work and drop the attribute in all tables
alter table p2 drop column height;

select relname, attname, attinhcount, attislocal
from pg_class join pg_attribute on (pg_class.oid = pg_attribute.attrelid)
where relname in ('p1','p2','c1','gc1') and attnum > 0 and not attisdropped
order by relname, attnum;

drop table p1, p2 cascade;

--
-- Test the ALTER TABLE SET WITH/WITHOUT OIDS command
--
create table altstartwith (col integer) with oids;

insert into altstartwith values (1);

select oid > 0, * from altstartwith;

alter table altstartwith set without oids;

select oid > 0, * from altstartwith; -- fails
select * from altstartwith;

alter table altstartwith set with oids;

select oid > 0, * from altstartwith;

drop table altstartwith;

-- Check inheritance cases
create table altwithoid (col integer) with oids;

-- Inherits parents oid column anyway
create table altinhoid () inherits (altwithoid) without oids;

insert into altinhoid values (1);

select oid > 0, * from altwithoid;
select oid > 0, * from altinhoid;

alter table altwithoid set without oids;

select oid > 0, * from altwithoid; -- fails
select oid > 0, * from altinhoid; -- fails
select * from altwithoid;
select * from altinhoid;

alter table altwithoid set with oids;

select oid > 0, * from altwithoid;
select oid > 0, * from altinhoid;

drop table altwithoid cascade;

create table altwithoid (col integer) without oids;

-- child can have local oid column
create table altinhoid () inherits (altwithoid) with oids;

insert into altinhoid values (1);

select oid > 0, * from altwithoid; -- fails
select oid > 0, * from altinhoid;

alter table altwithoid set with oids;

select oid > 0, * from altwithoid;
select oid > 0, * from altinhoid;

-- the child's local definition should remain
alter table altwithoid set without oids;

select oid > 0, * from altwithoid; -- fails
select oid > 0, * from altinhoid;

drop table altwithoid cascade;

-- test renumbering of child-table columns in inherited operations

create table p1 (f1 int);
create table c1 (f2 text, f3 int) inherits (p1);

alter table p1 add column a1 int check (a1 > 0);
alter table p1 add column f2 text;

insert into p1 values (1,2,'abc');
insert into c1 values(11,'xyz',33,0); -- should fail
insert into c1 values(11,'xyz',33,22);

select * from p1 order by f1;
update p1 set a1 = a1 + 1, f2 = upper(f2);
select * from p1 order by f1;

drop table p1 cascade;

-- test that operations with a dropped column do not try to reference
-- its datatype

--create domain mytype as text;
--create table foo (f1 text, f2 mytype, f3 text);;

insert into foo values('bb','cc','dd');
select * from foo order by f1;

--drop domain mytype cascade;

--select * from foo order by f1;
--insert into foo values('qq','rr');
--select * from foo order by f1;
--update foo set f3 = 'zz';
--select * from foo order by f1;
--select f3,max(f1) from foo group by f3;

-- Simple tests for alter table column type
--delete from foo where f1 = 'qq';
--alter table foo alter f1 TYPE integer; -- fails
--alter table foo alter f1 TYPE varchar(10);
--drop table foo;

create table anothertab (atcol1 serial8, atcol2 boolean,
	constraint anothertab_chk check (atcol1 <= 3));;

insert into anothertab (atcol1, atcol2) values (default, true);
insert into anothertab (atcol1, atcol2) values (default, false);
select * from anothertab order by atcol1, atcol2;

alter table anothertab alter column atcol1 type boolean; -- we cannot support this cast with numeric nextval
alter table anothertab alter column atcol1 type integer;

select * from anothertab order by atcol1, atcol2;

insert into anothertab (atcol1, atcol2) values (45, null); -- fails
insert into anothertab (atcol1, atcol2) values (default, null);

select * from anothertab order by atcol1, atcol2;

alter table anothertab alter column atcol2 type text
      using case when atcol2 is true then 'IT WAS TRUE'
                 when atcol2 is false then 'IT WAS FALSE'
                 else 'IT WAS NULL!' end;

select * from anothertab order by atcol1, atcol2;
alter table anothertab alter column atcol1 type boolean
        using case when atcol1 % 2 = 0 then true else false end; -- fails
alter table anothertab alter column atcol1 drop default;
alter table anothertab alter column atcol1 type boolean
        using case when atcol1 % 2 = 0 then true else false end; -- fails
alter table anothertab drop constraint anothertab_chk;
alter table anothertab drop constraint anothertab_chk; -- fails
alter table anothertab drop constraint IF EXISTS anothertab_chk; -- succeeds

alter table anothertab alter column atcol1 type boolean
        using case when atcol1 % 2 = 0 then true else false end;

select * from anothertab order by atcol1, atcol2;

drop table anothertab;

create table another (f1 int, f2 text);;

insert into another values(1, 'one');
insert into another values(2, 'two');
insert into another values(3, 'three');

select * from another order by f1, f2;

alter table another
  alter f1 type text using f2 || ' more',
  alter f2 type bigint using f1 * 10;

select * from another order by f1, f2;

drop table another;

-- table's row type
create table tab1 (a int, b text);
create table tab2 (x int, y tab1);
alter table tab1 alter column b type varchar; -- fails

-- disallow recursive containment of row types
create table recur1 (f1 int);
alter table recur1 add column f2 recur1; -- fails
alter table recur1 add column f2 recur1[]; -- fails
--create domain array_of_recur1 as recur1[];
--alter table recur1 add column f2 array_of_recur1; -- fails
create table recur2 (f1 int, f2 recur1);
alter table recur1 add column f2 recur2; -- fails
alter table recur1 add column f2 int;
alter table recur1 alter column f2 type recur2; -- fails

-- SET STORAGE may need to add a TOAST table
create table test_storage (a text);
alter table test_storage alter a set storage plain;
alter table test_storage add b int default 0; -- rewrite table to remove its TOAST table
alter table test_storage alter a set storage extended; -- re-add TOAST table

select reltoastrelid <> 0 as has_toast_table
from pg_class
where oid = 'test_storage'::regclass;

-- ALTER TYPE with a check constraint and a child table (bug before Nov 2012)
CREATE TABLE test_inh_check (a float check (a > 10.2));
CREATE TABLE test_inh_check_child() INHERITS(test_inh_check);
ALTER TABLE test_inh_check ALTER COLUMN a TYPE numeric;
\d test_inh_check
\d test_inh_check_child

--
-- lock levels
--
drop type lockmodes;
create type lockmodes as enum (
 'AccessShareLock'
,'RowShareLock'
,'RowExclusiveLock'
,'ShareUpdateExclusiveLock'
,'ShareLock'
,'ShareRowExclusiveLock'
,'ExclusiveLock'
,'AccessExclusiveLock'
);

drop view my_locks;
create or replace view my_locks as
select case when c.relname like 'pg_toast%' then 'pg_toast' else c.relname end, max(mode::lockmodes) as max_lockmode
from pg_locks l join pg_class c on l.relation = c.oid
where virtualtransaction = (
        select virtualtransaction
        from pg_locks
        where transactionid = txid_current()::integer)
and locktype = 'relation'
and relnamespace != (select oid from pg_namespace where nspname = 'pg_catalog')
and c.relname != 'my_locks'
group by c.relname;

create table alterlock (f1 int primary key, f2 text);

start transaction; alter table alterlock alter column f2 set statistics 150;
select * from my_locks order by 1;
rollback;

start transaction; alter table alterlock cluster on alterlock_pkey;
select * from my_locks order by 1;
commit;

start transaction; alter table alterlock set without cluster;
select * from my_locks order by 1;
commit;

start transaction; alter table alterlock set (fillfactor = 100);
select * from my_locks order by 1;
commit;

start transaction; alter table alterlock reset (fillfactor);
select * from my_locks order by 1;
commit;

start transaction; alter table alterlock set (toast.autovacuum_enabled = off);
select * from my_locks order by 1;
commit;

start transaction; alter table alterlock set (autovacuum_enabled = off);
select * from my_locks order by 1;
commit;

start transaction; alter table alterlock alter column f2 set (n_distinct = 1);
select * from my_locks order by 1;
rollback;

start transaction; alter table alterlock alter column f2 set storage extended;
select * from my_locks order by 1;
rollback;

start transaction; alter table alterlock alter column f2 set default 'x';
select * from my_locks order by 1;
rollback;

-- cleanup
drop table alterlock;
drop view my_locks;
drop type lockmodes;

--
-- alter function
--
create function test_strict(text) returns text as
    'select coalesce($1, ''got passed a null'');'
    language sql returns null on null input;
select test_strict(NULL);
alter function test_strict(text) called on null input;
select test_strict(NULL);

create function non_strict(text) returns text as
    'select coalesce($1, ''got passed a null'');'
    language sql called on null input;
select non_strict(NULL);
alter function non_strict(text) returns null on null input;
select non_strict(NULL);

--
-- alter object set schema
--

create schema alter1;
create schema alter2;

create table alter1.t1(f1 serial primary key, f2 int check (f2 > 0));

create view alter1.v1 as select * from alter1.t1;

create function alter1.plus1(int) returns int as 'select $1+1' language sql;

--create domain alter1.posint integer check (value > 0);

create type alter1.ctype as (f1 int, f2 text);

create function alter1.same(alter1.ctype, alter1.ctype) returns boolean language sql
as 'select $1.f1 is not distinct from $2.f1 and $1.f2 is not distinct from $2.f2';

create operator alter1.=(procedure = alter1.same, leftarg  = alter1.ctype, rightarg = alter1.ctype);

create operator class alter1.ctype_hash_ops default for type alter1.ctype using hash as
  operator 1 alter1.=(alter1.ctype, alter1.ctype);

create conversion alter1.ascii_to_utf8 for 'sql_ascii' to 'utf8' from ascii_to_utf8;

create text search parser alter1.prs(start = prsd_start, gettoken = prsd_nexttoken, end = prsd_end, lextypes = prsd_lextype);
create text search configuration alter1.cfg(parser = alter1.prs);
create text search template alter1.tmpl(init = dsimple_init, lexize = dsimple_lexize);
create text search dictionary alter1.dict(template = alter1.tmpl);

insert into alter1.t1(f2) values(11);
insert into alter1.t1(f2) values(12);

alter table alter1.t1 set schema alter2;
alter table alter1.v1 set schema alter2;
alter function alter1.plus1(int) set schema alter2;
--alter domain alter1.posint set schema alter2;
alter operator class alter1.ctype_hash_ops using hash set schema alter2;
alter operator family alter1.ctype_hash_ops using hash set schema alter2;
alter operator alter1.=(alter1.ctype, alter1.ctype) set schema alter2;
alter function alter1.same(alter1.ctype, alter1.ctype) set schema alter2;
alter type alter1.ctype set schema alter2;
alter conversion alter1.ascii_to_utf8 set schema alter2;
alter text search parser alter1.prs set schema alter2;
alter text search configuration alter1.cfg set schema alter2;
alter text search template alter1.tmpl set schema alter2;
alter text search dictionary alter1.dict set schema alter2;

-- this should succeed because nothing is left in alter1
drop schema alter1;

insert into alter2.t1(f2) values(13);
insert into alter2.t1(f2) values(14);

select * from alter2.t1 order by f1, f2;

select * from alter2.v1 order by f1, f2;

select alter2.plus1(41);

-- clean up
drop schema alter2 cascade;
drop schema alter1 cascade;

--
-- composite types
--

CREATE TYPE test_type AS (a int);
\d test_type

ALTER TYPE nosuchtype ADD ATTRIBUTE b text; -- fails

ALTER TYPE test_type ADD ATTRIBUTE b text;
\d test_type

ALTER TYPE test_type ADD ATTRIBUTE b text; -- fails

ALTER TYPE test_type ALTER ATTRIBUTE b SET DATA TYPE varchar;
\d test_type

ALTER TYPE test_type ALTER ATTRIBUTE b SET DATA TYPE integer;
\d test_type

ALTER TYPE test_type DROP ATTRIBUTE b;
\d test_type

ALTER TYPE test_type DROP ATTRIBUTE c; -- fails

ALTER TYPE test_type DROP ATTRIBUTE IF EXISTS c;

ALTER TYPE test_type DROP ATTRIBUTE a, ADD ATTRIBUTE d boolean;
\d test_type

ALTER TYPE test_type RENAME ATTRIBUTE a TO aa;
ALTER TYPE test_type RENAME ATTRIBUTE d TO dd;
\d test_type

DROP TYPE test_type;

CREATE TYPE test_type1 AS (a int, b text);
CREATE TABLE test_tbl1 (x int, y test_type1);
ALTER TYPE test_type1 ALTER ATTRIBUTE b TYPE varchar; -- fails

CREATE TYPE test_type2 AS (a int, b text);
CREATE TABLE test_tbl2 OF test_type2;
CREATE TABLE test_tbl2_subclass () INHERITS (test_tbl2);
\d test_type2
\d test_tbl2

ALTER TYPE test_type2 ADD ATTRIBUTE c text; -- fails
ALTER TYPE test_type2 ADD ATTRIBUTE c text CASCADE;
\d test_type2
\d test_tbl2

ALTER TYPE test_type2 ALTER ATTRIBUTE b TYPE varchar; -- fails
ALTER TYPE test_type2 ALTER ATTRIBUTE b TYPE varchar CASCADE;
\d test_type2
\d test_tbl2

ALTER TYPE test_type2 DROP ATTRIBUTE b; -- fails
ALTER TYPE test_type2 DROP ATTRIBUTE b CASCADE;
\d test_type2
\d test_tbl2

ALTER TYPE test_type2 RENAME ATTRIBUTE a TO aa; -- fails
ALTER TYPE test_type2 RENAME ATTRIBUTE a TO aa CASCADE;
\d test_type2
\d test_tbl2
\d test_tbl2_subclass

DROP TABLE test_tbl2_subclass;

-- This test isn't that interesting on its own, but the purpose is to leave
-- behind a table to test pg_upgrade with. The table has a composite type
-- column in it, and the composite type has a dropped attribute.
CREATE TYPE test_type3 AS (a int);
CREATE TABLE test_tbl3 (c) AS SELECT '(1)'::test_type3;
ALTER TYPE test_type3 DROP ATTRIBUTE a, ADD ATTRIBUTE b int;

CREATE TYPE test_type_empty AS ();

--
-- typed tables: OF / NOT OF
--

CREATE TYPE tt_t0 AS (z inet, x int, y numeric(8,2));
ALTER TYPE tt_t0 DROP ATTRIBUTE z;
CREATE TABLE tt0 (x int NOT NULL, y numeric(8,2));	-- OK
CREATE TABLE tt1 (x int, y bigint);					-- wrong base type
CREATE TABLE tt2 (x int, y numeric(9,2));			-- wrong typmod
CREATE TABLE tt3 (y numeric(8,2), x int);			-- wrong column order
CREATE TABLE tt4 (x int);							-- too few columns
CREATE TABLE tt5 (x int, y numeric(8,2), z int);	-- too few columns
CREATE TABLE tt6 () INHERITS (tt0);					-- can't have a parent
CREATE TABLE tt7 (x int, q text, y numeric(8,2)) WITH OIDS;
ALTER TABLE tt7 DROP q;								-- OK

ALTER TABLE tt0 OF tt_t0;
ALTER TABLE tt1 OF tt_t0;
ALTER TABLE tt2 OF tt_t0;
ALTER TABLE tt3 OF tt_t0;
ALTER TABLE tt4 OF tt_t0;
ALTER TABLE tt5 OF tt_t0;
ALTER TABLE tt6 OF tt_t0;
ALTER TABLE tt7 OF tt_t0;

CREATE TYPE tt_t1 AS (x int, y numeric(8,2));
ALTER TABLE tt7 OF tt_t1;			-- reassign an already-typed table
ALTER TABLE tt7 NOT OF;
\d tt7

-- make sure we can drop a constraint on the parent but it remains on the child
CREATE TABLE test_drop_constr_parent (c text CHECK (c IS NOT NULL));
CREATE TABLE test_drop_constr_child () INHERITS (test_drop_constr_parent);
ALTER TABLE ONLY test_drop_constr_parent DROP CONSTRAINT "test_drop_constr_parent_c_check";
-- should fail
INSERT INTO test_drop_constr_child (c) VALUES (NULL);
DROP TABLE test_drop_constr_parent CASCADE;

--
-- IF EXISTS test
--
ALTER TABLE IF EXISTS tt8 ADD COLUMN f int;
ALTER TABLE IF EXISTS tt8 ADD CONSTRAINT xxx PRIMARY KEY(f);
ALTER TABLE IF EXISTS tt8 ADD CHECK (f BETWEEN 0 AND 10);
ALTER TABLE IF EXISTS tt8 ALTER COLUMN f SET DEFAULT 0;
ALTER TABLE IF EXISTS tt8 RENAME COLUMN f TO f1;
ALTER TABLE IF EXISTS tt8 SET SCHEMA alter2;

CREATE TABLE tt8(a int);
CREATE SCHEMA alter2;

ALTER TABLE IF EXISTS tt8 ADD COLUMN f int;
ALTER TABLE IF EXISTS tt8 ADD CONSTRAINT xxx PRIMARY KEY(f);
ALTER TABLE IF EXISTS tt8 ADD CHECK (f BETWEEN 0 AND 10);
ALTER TABLE IF EXISTS tt8 ALTER COLUMN f SET DEFAULT 0;
ALTER TABLE IF EXISTS tt8 RENAME COLUMN f TO f1;
ALTER TABLE IF EXISTS tt8 SET SCHEMA alter2;

\d alter2.tt8

DROP TABLE alter2.tt8;
DROP SCHEMA alter2;

create database test_first_after_A dbcompatibility 'A';
\c test_first_after_A

-- test add column ... first | after columnname
-- common scenatios
drop table if exists t1 cascade;
create table t1(f1 int, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 add f6 clob first, add f7 blob after f2;
alter table t1 add f8 int, add f9 text first, add f10 float after f3;
\d+ t1
select * from t1;

-- 1 primary key
drop table if exists t1 cascade;
create table t1(f1 int primary key, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 add f6 blob first, add f7 clob after f2;
alter table t1 add f8 int, add f9 text first, add f10 float after f3;
select * from t1;

drop table if exists t1 cascade;
create table t1(f1 int primary key, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
alter table t1 drop f1, add f6 text, add f7 int primary key first, add f8 float after f3;
\d+ t1;

-- 2 unique index
drop table if exists t1 cascade;
create table t1(f1 int unique, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 drop f1, add f6 int unique first, add f7 float unique after f3;
select * from t1;

-- 3 default and generated column
drop table if exists t1 cascade;
create table t1(f1 int default 1, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 drop f1, add f6 int default 1 first, add f7 float default 7 after f3;
select * from t1;

drop table if exists t1 cascade;
create table t1(f1 int, f2 int default 2, f3 int default 3, f4 int generated always as (f1 + f2) stored);
insert into t1 values(1, 2, 3), (11, 22, 33);
alter table t1 add f5 int generated always as (f2 + f3) stored first, add f6 int generated always as (f1 + f3) stored after f5;
select * from t1;

-- 5 NULL and NOT NULL
drop table if exists t1 cascade;
create table t1(f1 int null, f2 varchar(20) not null, f3 timestamp, f4 bit(8), f5 bool);
alter table t1 drop f1, drop f2, add f6 int null first, add f7 float not null after f3;
\d+ t1

-- 6 check constraint
drop table if exists t1 cascade;
create table t1(f1 int check(f1 = 1), f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (1, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 drop f2, add f6 text check (f1 > 0) first, add f7 int check(f7 - f1 > 0) after f3;
select * from t1;

-- 7 foreign key
drop table if exists t1 cascade;
drop table if exists t_pri1 cascade;
drop table if exists t_pri2 cascade;
create table t_pri1(f1 text, f2 int primary key);
insert into t_pri1 values('a', 1), ('b', 2);
create table t_pri2(f1 text, f2 bool, f4 int primary key);
insert into t_pri2 values('a', true, 1), ('b', false, 2);
create table t1(f1 int, f2 int references t_pri1(f2), f3 bool);
insert into t1 values(1, 2, true), (2, 2, false);
alter table t1 drop f2, add f4 int references t_pri2(f4) first;
select * from t1;
alter table t1 drop f4, add f4 int references t_pri2(f4) after f1;
select * from t1;

-- partition table
drop table if exists t1 cascade;
create table t1
(f1 int, f2 int, f3 int)
partition by range(f1, f2)
(
        partition t1_p0 values less than (10, 0),
        partition t1_p1 values less than (20, 0),
        partition t1_p2 values less than (30, 0)
);
select relname, parttype, partkey from pg_partition where parentid=(select oid from pg_class where relname='t1') order by relname;

alter table t1 add f4 int first, add f5 int after f1;
\d+ t1
select relname, parttype, partkey from pg_partition where parentid=(select oid from pg_class where relname='t1') order by relname;

-- subpartition table
drop table if exists range_range cascade;
create table range_range(id int, gender varchar not null, birthday date not null)
partition by range (id) subpartition by range (birthday)
(
        partition p_1 values less than(100)
        (
                subpartition p_1_a values less than('2022-01-01'),
                subpartition p_1_b values less than(MAXVALUE)
        ),
        partition p_2 values less than(200)
        (
                subpartition p_2_a values less than('2022-01-01'),
                subpartition p_2_b values less than(MAXVALUE)
        ),
        partition p_3 values less than(MAXVALUE)
        (
                subpartition p_3_a values less than('2022-01-01'),
                subpartition p_3_b values less than(MAXVALUE)
        )
);
insert into range_range values(198,'boy','2010-02-15'),(33,'boy','2003-08-11'),(78,'girl','2014-06-24');
insert into range_range values(233,'girl','2010-01-01'),(360,'boy','2007-05-14'),(146,'girl','2005-03-08');
insert into range_range values(111,'girl','2013-11-19'),(15,'girl','2009-01-12'),(156,'boy','2011-05-21');

-- test pg_partition
select relname, parttype, partkey from pg_partition where parentid=(select oid from pg_class where relname='range_range') order by relname;
alter table range_range add f1 int default 1 first, add f2 text after id;
\d+ range_range
select relname, parttype, partkey from pg_partition where parentid=(select oid from pg_class where relname='range_range') order by relname;
select * from range_range;

-- pg_constraint test
set enable_default_ustore_table = on;
drop table if exists t_pri cascade;
drop table if exists t1 cascade;
create table t_pri(f1 int, f2 int, f3 int, primary key(f2, f3));
create table t1
(
        f1 int, f2 int, f3 varchar(20), f4 int, f5 int, f6 int, f7 int,
        foreign key(f1, f2) references t_pri(f2, f3),
        unique(f3, f4),
        check(f5 = 10),
        unique(f4, f5) include(f6, f7)
);
select conname, contype, conkey, confkey, conbin, consrc, conincluding from pg_constraint 
        where conrelid = (select oid from pg_class where relname = 't1') order by conname;

alter table t_pri add f4 int first, add f5 int after f2;
alter table t1 add f8 int primary key first, add f9 int unique after f3;
\d+ t_pri
select conname, contype, conkey, confkey, conbin, consrc, conincluding from pg_constraint 
        where conrelid = (select oid from pg_class where relname = 't_pri') order by conname;
\d+ t1
select conname, contype, conkey, confkey, conbin, consrc, conincluding from pg_constraint 
        where conrelid = (select oid from pg_class where relname = 't1') order by conname;

set enable_default_ustore_table = off;

-- pg_index test
drop table if exists t1 cascade;
create table t1
(
        f1 int, f2 int, f3 varchar(20), f4 int, f5 int, f6 int, f7 int,
        primary key(f1, f2),
        unique(f3, f4),
        check(f5 = 10)
);
create unique index partial_t1_idx on t1(f5, abs(f6)) where f5 + f6 - abs(f7) > 0;

select indkey, indexprs, indpred from pg_index where indrelid = (select oid from pg_class where relname = 't1') order by 1, 2, 3;

alter table t1 add f8 int first, add f9 int unique after f1;
\d+ t1
select indkey, indexprs, indpred from pg_index where indrelid = (select oid from pg_class where relname = 't1') order by 1, 2, 3;

-- pg_attribute test
drop table if exists t1 cascade;
create table t1(f1 int, f2 int, f3 int);
select attname, attnum, atthasdef, attisdropped from pg_attribute where attrelid = (select oid from pg_class where relname = 't1') and attnum > 0 order by attnum;

alter table t1 add f4 int default 4 first;
\d+ t1
select attname, attnum, atthasdef, attisdropped from pg_attribute where attrelid = (select oid from pg_class where relname = 't1') and attnum > 0 order by attnum;

alter table t1 drop f2, add f5 int default 5 after f1;
\d+ t1
select attname, attnum, atthasdef, attisdropped from pg_attribute where attrelid = (select oid from pg_class where relname = 't1') and attnum > 0 order by attnum;

-- pg_attrdef test
drop table if exists t1 cascade;
create table t1(f1 int primary key, f2 int, f3 int default 3, f4 int generated always as (f2 + f3) stored);
select adnum, adsrc, adgencol from pg_attrdef where adrelid = (select oid from pg_class where relname = 't1') order by adnum;

alter table t1 add f5 text default 'aaa' first;
\d+ t1
select adnum, adsrc, adgencol from pg_attrdef where adrelid = (select oid from pg_class where relname = 't1') order by adnum;

alter table t1 drop f2, add f6 int generated always as (f1 + abs(f3)) stored after f1;
\d+ t1
select adnum, adsrc, adgencol from pg_attrdef where adrelid = (select oid from pg_class where relname = 't1') order by adnum;

-- pg_depend test
drop table if exists t1 cascade;
create table t1(f1 int default 10, f2 int primary key, f3 int generated always as (f1 + f2) stored);
select classid, objsubid, refclassid, refobjsubid, deptype from pg_depend
        where refobjid = (select oid from pg_class where relname='t1') or objid = (select oid from pg_class where relname='t1') order by 1, 2, 3, 4, 5;

alter table t1 add t1 add f4 int first;
\d+ t1
select classid, objsubid, refclassid, refobjsubid, deptype from pg_depend
        where refobjid = (select oid from pg_class where relname='t1') or objid = (select oid from pg_class where relname='t1') order by 1, 2, 3, 4, 5;
alter table t1 drop f2, add f6 int, add f7 int generated always as (f1 + f6) stored after f1;
\d+ t1
select classid, objsubid, refclassid, refobjsubid, deptype from pg_depend
        where refobjid = (select oid from pg_class where relname='t1') or objid = (select oid from pg_class where relname='t1') order by 1, 2, 3, 4, 5;

-- pg_rewrite test
drop table if exists t1 cascade;
create table t1(f1 int, f2 int, f3 int);
insert into t1 values(1, 2, 3), (11, 22, 33);
create view t1_view1 as select * from t1;
create view t1_view2 as select f1, f2 from t1;
\d+ t1_view1
\d+ t1_view2
\d+ t1
select pg_get_viewdef('t1_view1');
select pg_get_viewdef('t1_view2');
select * from t1_view1;
select * from t1_view2;
select * from t1;
alter table t1 add f4 int first, add f5 int after f1;
\d+ t1_view1
\d+ t1_view2
\d+ t1
select pg_get_viewdef('t1_view1');
select pg_get_viewdef('t1_view2');
select * from t1_view1;
select * from t1_view2;
select * from t1;

-- pg_trigger test
drop table if exists t1 cascade;
create table t1(f1 boolean not null, f2 text, f3 int, f4 date);
alter table t1 add primary key(f1);
create or replace function dummy_update_func() returns trigger as $$
begin
        raise notice 'dummy_update_func(%) called: action = %, oid = %, new = %', TG_ARGV[0], TG_OP, OLD, NEW;
        return new;
end;
$$ language plpgsql;

drop trigger if exists f1_trig_update on t1;
drop trigger if exists f1_trig_insert on t1;

create trigger f1_trig_update after update of f1 on t1 for each row
        when (not old.f1 and new.f1) execute procedure dummy_update_func('update');
create trigger f1_trig_insert after insert on t1 for each row
        when (not new.f1) execute procedure dummy_update_func('insert');

select tgname, tgattr, tgqual from pg_trigger where tgrelid = (select oid from pg_class where relname='t1') order by tgname;

alter table t1 add f5 int after f1, add f6 boolean first;
\d+ t1
select tgname, tgattr, tgqual from pg_trigger where tgrelid = (select oid from pg_class where relname='t1') order by tgname;

-- pg_rlspolicy test
drop table if exists t1 cascade;
drop role if exists test_rlspolicy;
create role test_rlspolicy nologin password 'Gauss_234';
create table t1 (f1 int, f2 int, f3 text) partition by range (f1)
(
        partition t1_p0 values less than(10),
        partition t1_p1 values less than(50),
        partition t1_p2 values less than(100),
        partition t1_p3 values less than(MAXVALUE)
);

INSERT INTO t1 VALUES (generate_series(1, 150) % 24, generate_series(1, 150), 'huawei');
grant select on t1 to public;

create row level security policy t1_rls1 on t1 as permissive to public using (f2 <= 20);
create row level security policy t1_rls2 on t1 as restrictive to test_rlspolicy using (f1 < 30);

\d+ t1
select * from t1 limit 10;
select polname, polqual from pg_rlspolicy where polrelid = (select oid from pg_class where relname='t1');

alter table t1 add f4 int generated always as (f1 + 100) stored after f1, add f5 int generated always as (f2 + 100) stored first;
\d+ t1
select * from t1 limit 10;
select polname, polqual from pg_rlspolicy where polrelid = (select oid from pg_class where relname='t1');
drop table if exists t1 cascade;

\c postgres
drop database test_first_after_A;

-- test add column ... first | after columnname in B compatibility
create database test_first_after_B dbcompatibility 'b';
\c test_first_after_B

-- test add column ... first | after columnname in astore table
-- ASTORE table
-- common scenatios
drop table if exists t1 cascade;
create table t1(f1 int, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 add f6 clob first, add f7 blob after f2;
alter table t1 add f8 int, add f9 text first, add f10 float after f3;
\d+ t1
select * from t1;

-- 1 primary key
-- 1.1.1 primary key in original table without data, add column without constraints
drop table if exists t1 cascade;
create table t1(f1 int primary key, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
alter table t1 add f6 clob first, add f7 blob after f2;
alter table t1 add f8 int, add f9 text first, add f10 float after f3;
\d+ t1

-- 1.1.2 primary key in original table with data, add column without constraints
drop table if exists t1 cascade;
create table t1(f1 int primary key, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 add f6 blob first, add f7 clob after f2;
alter table t1 add f8 int, add f9 text first, add f10 float after f3;
select * from t1;

-- 1.2.1 primary key in a table without data, add column with primary key
drop table if exists t1 cascade;
create table t1(f1 int primary key, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
-- error
alter table t1 add f6 text, add f7 int primary key first, add f8 float after f3;
select * from t1;

-- 1.2.2 primary key in a table with data, add column with primary key
drop table if exists t1 cascade;
create table t1(f1 int primary key, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
-- error
alter table t1 add f6 text, add f7 int primary key first, add f8 float after f3;
select * from t1;

-- 1.3.1 primary key in a table without data, drop primary key, then add column with primary key
drop table if exists t1 cascade;
create table t1(f1 int primary key, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
alter table t1 drop f1, add f6 text, add f7 int primary key first, add f8 float after f3;
\d+ t1;

-- 1.3.2 primary key in a table with data, drop primary key, then add column with primary key
drop table if exists t1 cascade;
create table t1(f1 int primary key, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 drop f1;
-- error
alter table t1 add f6 text, add f7 int primary key first, add f8 float after f3;
select * from t1;

-- 1.4.1 primary key in a table without data, drop primary key, the add column with primary key and default
drop table if exists t1 cascade;
create table t1(f1 int primary key, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
alter table t1 drop f1, add f6 text, add f7 int primary key default 7 first, add f8 float after f3;
\d+ t1

-- 1.4.2 primary key in a table with data, drop primary key, then add column with primary key and default
drop table if exists t1 cascade;
create table t1(f1 int primary key, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 drop f1;
-- error
alter table t1 add f6 text, add f7 int primary key default 7 first, add f8 float after f3;
select * from t1;

-- 1.5.1 primary key in a table without data, drop primary key, the add column with primary key and auto_increment
drop table if exists t1 cascade;
create table t1(f1 int primary key, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
alter table t1 drop f1, add f6 text, add f7 int primary key auto_increment first, add f8 float after f3;
\d+ t1

-- 1.5.2 primary key in a table with data, drop primary key, the add column with primary key and auto_increment
drop table if exists t1 cascade;
create table t1(f1 int primary key, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 drop f1, add f6 text, add f7 int primary key auto_increment first, add f8 float after f3;
select * from t1;

-- 2 unique index
-- 2.1.1 unique index in a table without data, add column without constraints
drop table if exists t1 cascade;
create table t1(f1 int unique, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
alter table t1 add f6 int first, add f7 float after f3;
\d+ t1

-- 2.1.2 unique index in a table with data, add column without constraints
drop table if exists t1 cascade;
create table t1(f1 int unique, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 add f6 int first, add f7 float after f3;
select * from t1;

-- 2.2.1 unique index in a table without data, add column with unique index
drop table if exists t1 cascade;
create table t1(f1 int unique, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
alter table t1 add f6 int unique first, add f7 float unique after f3;
\d+ t1

-- 2.2.2 unique index in a table with data, add column with unique index
drop table if exists t1 cascade;
create table t1(f1 int unique, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 add f6 int unique first, add f7 float unique after f3;
select * from t1;

-- 2.3.1 unique index in a table without data, drop unique index, add column with unique index
drop table if exists t1 cascade;
create table t1(f1 int unique, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
alter table t1 drop f1, add f6 int unique first, add f7 float unique after f3;
\d+ t1

-- 2.3.2 unique index in a table with data, drop unique index, add column with unique index
drop table if exists t1 cascade;
create table t1(f1 int unique, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 drop f1, add f6 int unique first, add f7 float unique after f3;
select * from t1;

-- 2.4.1 unique index in a table without data, drop unique index, add column with unique index and default
drop table if exists t1 cascade;
create table t1(f1 int unique, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
alter table t1 add f6 int unique default 6 first;
alter table t1 drop f1, add f7 float unique default 7 after f3;
\d+ t1

-- 2.4.2 unique index in a table with data, drop unique index, add column with unique index and default
drop table if exists t1 cascade;
create table t1(f1 int unique, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
-- error
alter table t1 add f6 int unique default 6 first;
alter table t1 drop f1;
-- error
alter table t1 add f7 float unique default 7 after f3;
select * from t1;

-- 3 default and generated column
-- 3.1.1 default in a table without data, add column without constraints
drop table if exists t1 cascade;
create table t1(f1 int default 1, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
alter table t1 add f6 int first, add f7 float after f3;
\d+ t1

-- 3.1.2 default in a table with data, add column without constraints
drop table if exists t1 cascade;
create table t1(f1 int default 1, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 add f6 int first, add f7 float after f3;
select * from t1;

-- 3.2.1 default in a table without data, add column with default
drop table if exists t1 cascade;
create table t1(f1 int default 1, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
alter table t1 add f6 int default 6 first, add f7 float default 7 after f3;
\d+ t1

-- 3.2.2 default in a table with data, add column with default
drop table if exists t1 cascade;
create table t1(f1 int default 1, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 add f6 int default 6 first, add f7 float default 7 after f3;
select * from t1;

-- 3.3.1 default in a table without data, drop default, add column with default
drop table if exists t1 cascade;
create table t1(f1 int default 1, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
alter table t1 drop f1, add f6 int default 6 first, add f7 float default 7 after f3;
\d+ t1

-- 3.3.2 default in a table with data, drop default, add column with default
drop table if exists t1 cascade;
create table t1(f1 int default 1, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 drop f1, add f6 int default 1 first, add f7 float default 7 after f3;
select * from t1;

-- 3.4.1 generated column in a table without data, drop generated column
drop table if exists t1 cascade;
create table t1(f1 int, f2 int default 2, f3 int default 3, f4 int generated always as (f1 + f2) stored);
alter table t1 drop f1, add f5 int generated always as (f2 + f3) stored first, add f6 int generated always as (f3*10) stored after f5;
\d+ t1

-- 3.4.1 generated column in a table with data, drop generated column
drop table if exists t1 cascade;
create table t1(f1 int, f2 int default 2, f3 int default 3, f4 int generated always as (f1 + f2) stored);
insert into t1 values(1, 2, 3), (11, 22, 33);
alter table t1 drop f1, add f5 int generated always as (f2 + f3) stored first, add f6 int generated always as (f3*10) stored after f5;
select * from t1;

-- 3.5.1 generated column in a table without data, add generated column
drop table if exists t1 cascade;
create table t1(f1 int, f2 int default 2, f3 int default 3, f4 int generated always as (f1 + f2) stored);
alter table t1 add f5 int generated always as (f2 + f3) stored first, add f6 int generated always as (f2 + f3) stored after f5;
\d+ t1;

-- 3.5.2 generated column in table with data, add generated column
drop table if exists t1 cascade;
create table t1(f1 int, f2 int default 2, f3 int default 3, f4 int generated always as (f1 + f2) stored);
insert into t1 values(1, 2, 3), (11, 22, 33);
alter table t1 add f5 int generated always as (f2 + f3) stored first, add f6 int generated always as (f1 + f3) stored after f5;
select * from t1;

-- 4 auto_increment
-- 4.1.1 auto_increment in a table without data, add column without constraints
drop table if exists t1 cascade;
create table t1(f1 int primary key auto_increment, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
alter table t1 add f6 text first, add f7 float after f3;
\d+ t1

-- 4.1.2 auto_increment in a table with data, add column without constraints
drop table if exists t1 cascade;
create table t1(f1 int primary key auto_increment, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 add f6 text first, add f7 float after f3;
select * from t1;

-- 4.2.1 auto_increment in a table without data, add column with auto_increment
drop table if exists t1 cascade;
create table t1(f1 int primary key auto_increment, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
-- error
alter table t1 add f6 int primary key auto_increment first;
-- error
alter table t1 add f7 int primary key auto_increment after f3;
\d+ t1

-- 4.2.2 auto_increment in a table with data, add column with auto_increment
drop table if exists t1 cascade;
create table t1(f1 int primary key auto_increment, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
-- error
alter table t1 add f6 int primary key auto_increment first;
-- error
alter table t1 add f7 int primary key auto_increment after f3;
select * from t1;

-- 4.3.1 auto_increment in a table without data, drop auto_increment, add column with auto_increment
drop table if exists t1 cascade;
create table t1(f1 int primary key auto_increment, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
alter table t1 drop f1, add f6 int primary key auto_increment first;
\d+ t1

-- 4.3.2 auto_increment in a table with data, drop auto_increment, add column with auto_increment
drop table if exists t1 cascade;
create table t1(f1 int primary key auto_increment, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 drop f1, add f6 int primary key auto_increment first;

-- 4.4.1 auto_increment in a table without data, drop auto_increment, add column with auto_increment and default
drop table if exists t1 cascade;
create table t1(f1 int primary key auto_increment, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
alter table t1 drop f1;
-- error
alter table t1 add f6 int primary key auto_increment default 6 first;
\d+ t1

-- 4.4.2 auto_increment in a table with data, drop auto_increment, add column with auto_increment and default
drop table if exists t1 cascade;
create table t1(f1 int primary key auto_increment, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 drop f1;
-- error
alter table t1 add f6 int primary key auto_increment default 6 first;
select * from t1;

-- 5 NULL and NOT NULL
-- 5.1.1 null and not null in a table without data, add column without constraints
drop table if exists t1 cascade;
alter table t1(f1 int null, f2 varchar(20) not null, f3 timestamp, f4 bit(8), f5 bool);
alter table t1 add f6 text first, add f7 float after f3;
\d+ t1

-- 5.1.2 null and not null in a table with data, add column without constraints
drop table if exists t1 cascade;
alter table t1(f1 int null, f2 varchar(20) not null, f3 timestamp, f4 bit(8), f5 bool);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 add f6 text first, add f7 float after f3;
select * from t1;

-- 5.2.1 null and not null in table without data, add column with null or not null
drop table if exists t1 cascade;
alter table t1(f1 int null, f2 varchar(20) not null, f3 timestamp, f4 bit(8), f5 bool);
alter table t1 add f6 int null first;
alter table t1 add f7 float not null after f3;
\d+ t1

-- 5.2.2 null and not null in a table with data, add column with null or not null
drop table if exists t1 cascade;
create table t1(f1 int null, f2 varchar(20) not null, f3 timestamp, f4 bit(8), f5 bool);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 add f6 int null first;
-- error
alter table t1 add f7 float not null after f3;
select * from t1;

-- 5.3.1 null and not null in a table without data, drop null, add column with null or not null
drop table if exists t1 cascade;
create table t1(f1 int null, f2 varchar(20) not null, f3 timestamp, f4 bit(8), f5 bool);
alter table t1 drop f1, add f6 int null first, add f7 float not null after f3;
\d+ t1

-- 5.3.2 null and not null in a table with data, drop null, add column with null or not null
drop table if exists t1 cascade;
create table t1(f1 int null, f2 varchar(20) not null, f3 timestamp, f4 bit(8), f5 bool);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 drop f1, add f6 int null first;
-- error
alter table t1 add f7 float not null after f3;
select * from t1;

-- 5.4.1 null and not null in a table without data, drop null and not null, add column with null or not null
drop table if exists t1 cascade;
create table t1(f1 int null, f2 varchar(20) not null, f3 timestamp, f4 bit(8), f5 bool);
alter table t1 drop f1, drop f2, add f6 int null first, add f7 float not null after f3;
\d+ t1

-- 5.4.2 null and not null in a table without data, drop null and not null, add column with null or not null
drop table if exists t1 cascade;
create table t1(f1 int null, f2 varchar(20) not null, f3 timestamp, f4 bit(8), f5 bool);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 drop f1, drop f2, add f6 int null first;
-- error
alter table t1 add f7 float not null after f3;
select * from t1;

-- 6 check constraint
-- 6.1.1 check constraint in a table without data, add column without constraint
drop table if exists t1 cascade;
create table t1(f1 int check(f1 = 1), f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
alter table t1 add f6 text first, add f7 float after f3;
\d+ t1

-- 6.1.2 check constraint in a table with data, add column without constraint
drop table if exists t1 cascade;
create table t1(f1 int check(f1 = 1), f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 add f6 text first, add f7 float after f3;
select * from t1;

-- 6.2.1 check constraint in a table without data, add column with check
drop table if exists t1 cascade;
create table t1(f1 int check(f1 = 1), f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
alter table t1 add f6 int default 6, add f7 text check(f6 = 6) first, add f8 float check(f1 + f2 == 7);
\d+ t1

-- 6.2.2 check constraint in a table with data, add column with check
drop table if exists t1 cascade;
create table t1(f1 int check(f1 = 1), f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (1, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 add f6 int default 6, add f7 text check(f6 = 6) first, add f8 float check(f1 + f2 == 7) after f3;
select * from t1;

-- 6.3.1 check constraint in a table without data, drop check, add column with check
drop table if exists t1 cascade;
create table t1(f1 int check(f1 = 1), f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
alter table t1 drop f2, add f6 text check (f1 > 0) first, add f7 int check (f7 - f1 > 0) after f3;
\d+ t1

-- 6.3.2 check constraint in a table with data, drop check, add column with with check
drop table if exists t1 cascade;
create table t1(f1 int check(f1 = 1), f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (1, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 drop f2, add f6 text check (f1 > 0) first, add f7 int check(f7 - f1 > 0) after f3;
select * from t1;

-- 7 foreign key
-- 7.1.1 foreign key constraint in a table without data, add column without constraint
drop table if exists t_pri1 cascade;
create table t_pri1(f1 int, f2 int primary key);
drop table if exists t1 cascade;
create table t1(f1 int, f2 int references t_pri1(f2), f3 bool);
alter table t1 add f4 int, add f5 text first, f6 float after f2;
\d+ t1

-- 7.1.2 foreign key constraint in a table with data, add column without constraint
drop table if exists t1 cascade;
drop table if exists t_pri1 cascade;
create table t_pri1(f1 text, f2 int primary key);
insert into t_pri1 values('a', 1), ('b', 2);
create table t1(f1 text, f2 int references t_pri1(f2), f3 bool);
insert into t1 values('a', 1, true), ('b', 2, false);
alter table t1 add f4 int, add f5 text first, f6 float after f2;
select * from t1;

-- 7.2.1 foreign key constraint in a table without data, add column with foreign key
drop table if exists t1 cascade;
drop table if exists t_pri1 cascade;
drop table if exists t_pri2 cascade;
create table t_pri1(f1 text, f2 int primary key);
create table t_pri2(f1 int, f2 int, f4 int primary key);
create table t1(f1 int, f2 int references t_pri1(f2), f3 bool);
alter table t1 add f4 int references t_pri2(f4) first;
\d+ t1
alter table t1 drop f4, add f4 int references t_pri2(f4) after f2;
\d+ t1

-- 7.2.2 foreign key constraint in a table with data, add column with foreign key
drop table if exists t1 cascade;
drop table if exists t_pri1 cascade;
drop table if exists t_pri2 cascade;
create table t_pri1(f1 text, f2 int primary key);
insert into t_pri1 values('a', 1), ('b', 2);
create table t_pri2(f1 int, f2 bool, f4 int primary key);
insert into t_pri2 values(11, true, 1), (22, false, 2);
create table t1(f1 int, f2 int references t_pri1(f2), f3 bool);
insert into t1 values(1, 1, true), (2, 2, false);
alter table t1 add f4 int references t_pri2(f4) first;
select * from t1;
alter table t1 drop f4, add f4 int references t_pri2(f4) after f2;
select * from t1;

-- 7.3.1 foreign key constraint in a table without data, drop foreign key, add column with foreign key
drop table if exists t1 cascade;
drop table if exists t_pri1 cascade;
drop table if exists t_pri2 cascade;
create table t_pri1(f1 int, f2 int primary key);
create table t_pri2(f1 int, f2 int, f4 int primary key);
create table t1(f1 int, f2 int references t_pri1(f2), f3 bool);
alter table t1 drop f2, add f4 int references t_pri2(f4) first;
\d+ t1
alter table t1 drop f4, add f4 int references t_pri2(f4) after f1;
\d+ t1

-- 7.3.2 foreign key constraint in a table with data, drop foreign key, add column with foreign key
drop table if exists t1 cascade;
drop table if exists t_pri1 cascade;
drop table if exists t_pri2 cascade;
create table t_pri1(f1 text, f2 int primary key);
insert into t_pri1 values('a', 1), ('b', 2);
create table t_pri2(f1 text, f2 bool, f4 int primary key);
insert into t_pri2 values('a', true, 1), ('b', false, 2);
create table t1(f1 int, f2 int references t_pri1(f2), f3 bool);
insert into t1 values(1, 2, true), (2, 2, false);
alter table t1 drop f2, add f4 int references t_pri2(f4) first;
select * from t1;
alter table t1 drop f4, add f4 int references t_pri2(f4) after f1;
select * from t1;

-- partition table
drop table if exists t1 cascade;
create table t1
(f1 int, f2 int, f3 int)
partition by range(f1, f2)
(
        partition t1_p0 values less than (10, 0),
        partition t1_p1 values less than (20, 0),
        partition t1_p2 values less than (30, 0)
);
select relname, parttype, partkey from pg_partition where parentid=(select oid from pg_class where relname='t1') order by relname;

alter table t1 add f4 int first, add f5 int after f1;
\d+ t1
select relname, parttype, partkey from pg_partition where parentid=(select oid from pg_class where relname='t1') order by relname;

-- subpartition table
drop table if exists range_range cascade;
create table range_range(id int, gender varchar not null, birthday date not null)
partition by range (id) subpartition by range (birthday)
(
        partition p_1 values less than(100)
        (
                subpartition p_1_a values less than('2022-01-01'),
                subpartition p_1_b values less than(MAXVALUE)
        ),
        partition p_2 values less than(200)
        (
                subpartition p_2_a values less than('2022-01-01'),
                subpartition p_2_b values less than(MAXVALUE)
        ),
        partition p_3 values less than(MAXVALUE)
        (
                subpartition p_3_a values less than('2022-01-01'),
                subpartition p_3_b values less than(MAXVALUE)
        )
);
insert into range_range values(198,'boy','2010-02-15'),(33,'boy','2003-08-11'),(78,'girl','2014-06-24');
insert into range_range values(233,'girl','2010-01-01'),(360,'boy','2007-05-14'),(146,'girl','2005-03-08');
insert into range_range values(111,'girl','2013-11-19'),(15,'girl','2009-01-12'),(156,'boy','2011-05-21');

-- test pg_partition
select relname, parttype, partkey from pg_partition where parentid=(select oid from pg_class where relname='range_range') order by relname;
alter table range_range add f1 int default 1 first, add f2 text after id;
\d+ range_range
select relname, parttype, partkey from pg_partition where parentid=(select oid from pg_class where relname='range_range') order by relname;
select * from range_range;

-- USTORE table
-- common scenatios
drop table if exists t1 cascade;
create table t1(f1 int, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 add f6 clob first, add f7 blob after f2;
alter table t1 add f8 int, add f9 text first, add f10 float after f3;
\d+ t1
select * from t1;

-- 1 primary key
-- 1.1.1 primary key in original table without data, add column without constraints
drop table if exists t1 cascade;
create table t1(f1 int primary key, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
alter table t1 add f6 clob first, add f7 blob after f2;
alter table t1 add f8 int, add f9 text first, add f10 float after f3;
\d+ t1

-- 1.1.2 primary key in original table with data, add column without constraints
drop table if exists t1 cascade;
create table t1(f1 int primary key, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 add f6 blob first, add f7 clob after f2;
alter table t1 add f8 int, add f9 text first, add f10 float after f3;
select * from t1;

-- 1.2.1 primary key in a table without data, add column with primary key
drop table if exists t1 cascade;
create table t1(f1 int primary key, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
-- error
alter table t1 add f6 text, add f7 int primary key first, add f8 float after f3;
select * from t1;

-- 1.2.2 primary key in a table with data, add column with primary key
drop table if exists t1 cascade;
create table t1(f1 int primary key, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
-- error
alter table t1 add f6 text, add f7 int primary key first, add f8 float after f3;
select * from t1;

-- 1.3.1 primary key in a table without data, drop primary key, then add column with primary key
drop table if exists t1 cascade;
create table t1(f1 int primary key, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
alter table t1 drop f1, add f6 text, add f7 int primary key first, add f8 float after f3;
\d+ t1;

-- 1.3.2 primary key in a table with data, drop primary key, then add column with primary key
drop table if exists t1 cascade;
create table t1(f1 int primary key, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 drop f1;
-- error
alter table t1 add f6 text, add f7 int primary key first, add f8 float after f3;
select * from t1;

-- 1.4.1 primary key in a table without data, drop primary key, the add column with primary key and default
drop table if exists t1 cascade;
create table t1(f1 int primary key, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
alter table t1 drop f1, add f6 text, add f7 int primary key default 7 first, add f8 float after f3;
\d+ t1

-- 1.4.2 primary key in a table with data, drop primary key, then add column with primary key and default
drop table if exists t1 cascade;
create table t1(f1 int primary key, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 drop f1;
-- error
alter table t1 add f6 text, add f7 int primary key default 7 first, add f8 float after f3;
select * from t1;

-- 1.5.1 primary key in a table without data, drop primary key, the add column with primary key and auto_increment
drop table if exists t1 cascade;
create table t1(f1 int primary key, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
alter table t1 drop f1, add f6 text, add f7 int primary key auto_increment first, add f8 float after f3;
\d+ t1

-- 1.5.2 primary key in a table with data, drop primary key, the add column with primary key and auto_increment
drop table if exists t1 cascade;
create table t1(f1 int primary key, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 drop f1, add f6 text, add f7 int primary key auto_increment first, add f8 float after f3;
select * from t1;

-- 2 unique index
-- 2.1.1 unique index in a table without data, add column without constraints
drop table if exists t1 cascade;
create table t1(f1 int unique, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
alter table t1 add f6 int first, add f7 float after f3;
\d+ t1

-- 2.1.2 unique index in a table with data, add column without constraints
drop table if exists t1 cascade;
create table t1(f1 int unique, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 add f6 int first, add f7 float after f3;
select * from t1;

-- 2.2.1 unique index in a table without data, add column with unique index
drop table if exists t1 cascade;
create table t1(f1 int unique, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
alter table t1 add f6 int unique first, add f7 float unique after f3;
\d+ t1

-- 2.2.2 unique index in a table with data, add column with unique index
drop table if exists t1 cascade;
create table t1(f1 int unique, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 add f6 int unique first, add f7 float unique after f3;
select * from t1;

-- 2.3.1 unique index in a table without data, drop unique index, add column with unique index
drop table if exists t1 cascade;
create table t1(f1 int unique, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
alter table t1 drop f1, add f6 int unique first, add f7 float unique after f3;
\d+ t1

-- 2.3.2 unique index in a table with data, drop unique index, add column with unique index
drop table if exists t1 cascade;
create table t1(f1 int unique, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 drop f1, add f6 int unique first, add f7 float unique after f3;
select * from t1;

-- 2.4.1 unique index in a table without data, drop unique index, add column with unique index and default
drop table if exists t1 cascade;
create table t1(f1 int unique, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
alter table t1 add f6 int unique default 6 first;
alter table t1 drop f1, add f7 float unique default 7 after f3;
\d+ t1

-- 2.4.2 unique index in a table with data, drop unique index, add column with unique index and default
drop table if exists t1 cascade;
create table t1(f1 int unique, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
-- error
alter table t1 add f6 int unique default 6 first;
alter table t1 drop f1;
-- error
alter table t1 add f7 float unique default 7 after f3;
select * from t1;

-- 3 default and generated column
-- 3.1.1 default in a table without data, add column without constraints
drop table if exists t1 cascade;
create table t1(f1 int default 1, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
alter table t1 add f6 int first, add f7 float after f3;
\d+ t1

-- 3.1.2 default in a table with data, add column without constraints
drop table if exists t1 cascade;
create table t1(f1 int default 1, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 add f6 int first, add f7 float after f3;
select * from t1;

-- 3.2.1 default in a table without data, add column with default
drop table if exists t1 cascade;
create table t1(f1 int default 1, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
alter table t1 add f6 int default 6 first, add f7 float default 7 after f3;
\d+ t1

-- 3.2.2 default in a table with data, add column with default
drop table if exists t1 cascade;
create table t1(f1 int default 1, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 add f6 int default 6 first, add f7 float default 7 after f3;
select * from t1;

-- 3.3.1 default in a table without data, drop default, add column with default
drop table if exists t1 cascade;
create table t1(f1 int default 1, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
alter table t1 drop f1, add f6 int default 6 first, add f7 float default 7 after f3;
\d+ t1

-- 3.3.2 default in a table with data, drop default, add column with default
drop table if exists t1 cascade;
create table t1(f1 int default 1, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 drop f1, add f6 int default 1 first, add f7 float default 7 after f3;
select * from t1;

-- 3.4.1 generated column in a table without data, drop generated column
drop table if exists t1 cascade;
create table t1(f1 int, f2 int default 2, f3 int default 3, f4 int generated always as (f1 + f2) stored) with (storage_type = ustore);
alter table t1 drop f1, add f5 int generated always as (f2 + f3) stored first, add f6 int generated always as (f3*10) stored after f5;
\d+ t1

-- 3.4.1 generated column in a table with data, drop generated column
drop table if exists t1 cascade;
create table t1(f1 int, f2 int default 2, f3 int default 3, f4 int generated always as (f1 + f2) stored) with (storage_type = ustore);
insert into t1 values(1, 2, 3), (11, 22, 33);
alter table t1 drop f1, add f5 int generated always as (f2 + f3) stored first, add f6 int generated always as (f3*10) stored after f5;
select * from t1;

-- 3.5.1 generated column in a table without data, add generated column
drop table if exists t1 cascade;
create table t1(f1 int, f2 int default 2, f3 int default 3, f4 int generated always as (f1 + f2) stored) with (storage_type = ustore);
alter table t1 add f5 int generated always as (f2 + f3) stored first, add f6 int generated always as (f1 + f3) stored after f5;
\d+ t1;

-- 3.5.2 generated column in table with data, add generated column
drop table if exists t1 cascade;
create table t1(f1 int, f2 int default 2, f3 int default 3, f4 int generated always as (f1 + f2) stored) with (storage_type = ustore);
insert into t1 values(1, 2, 3), (11, 22, 33);
alter table t1 add f5 int generated always as (f2 + f3) stored first, add f6 int generated always as (f1 + f3) stored after f5;
select * from t1 cascade;

-- 4 auto_increment
-- 4.1.1 auto_increment in a table without data, add column without constraints
drop table if exists t1 cascade;
create table t1(f1 int primary key auto_increment, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
alter table t1 add f6 text first, add f7 float after f3;
\d+ t1

-- 4.1.2 auto_increment in a table with data, add column without constraints
drop table if exists t1 cascade;
create table t1(f1 int primary key auto_increment, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 add f6 text first, add f7 float after f3;
select * from t1;

-- 4.2.1 auto_increment in a table without data, add column with auto_increment
drop table if exists t1 cascade;
create table t1(f1 int primary key auto_increment, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
-- error
alter table t1 add f6 int primary key auto_increment first;
-- error
alter table t1 add f7 int primary key auto_increment after f3;
\d+ t1

-- 4.2.2 auto_increment in a table with data, add column with auto_increment
drop table if exists t1 cascade;
create table t1(f1 int primary key auto_increment, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
-- error
alter table t1 add f6 int primary key auto_increment first;
-- error
alter table t1 add f7 int primary key auto_increment after f3;
select * from t1;

-- 4.3.1 auto_increment in a table without data, drop auto_increment, add column with auto_increment
drop table if exists t1 cascade;
create table t1(f1 int primary key auto_increment, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
alter table t1 drop f1, add f6 int primary key auto_increment first;
\d+ t1

-- 4.3.2 auto_increment in a table with data, drop auto_increment, add column with auto_increment
drop table if exists t1 cascade;
create table t1(f1 int primary key auto_increment, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 drop f1, add f6 int primary key auto_increment first;

-- 4.4.1 auto_increment in a table without data, drop auto_increment, add column with auto_increment and default
drop table if exists t1 cascade;
create table t1(f1 int primary key auto_increment, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
alter table t1 drop f1;
-- error
alter table t1 add f6 int primary key auto_increment default 6 first;
\d+ t1

-- 4.4.2 auto_increment in a table with data, drop auto_increment, add column with auto_increment and default
drop table if exists t1 cascade;
create table t1(f1 int primary key auto_increment, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 drop f1;
-- error
alter table t1 add f6 int primary key auto_increment default 6 first;
select * from t1;

-- 5 NULL and NOT NULL
-- 5.1.1 null and not null in a table without data, add column without constraints
drop table if exists t1 cascade;
alter table t1(f1 int null, f2 varchar(20) not null, f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
alter table t1 add f6 text first, add f7 float after f3;
\d+ t1

-- 5.1.2 null and not null in a table with data, add column without constraints
drop table if exists t1 cascade;
alter table t1(f1 int null, f2 varchar(20) not null, f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 add f6 text first, add f7 float after f3;
select * from t1;

-- 5.2.1 null and not null in table without data, add column with null or not null
drop table if exists t1 cascade;
alter table t1(f1 int null, f2 varchar(20) not null, f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
alter table t1 add f6 int null first, add f7 float not null after f3;
\d+ t1

-- 5.2.2 null and not null in a table with data, add column with null or not null
drop table if exists t1 cascade;
create table t1(f1 int null, f2 varchar(20) not null, f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 add f6 int null first;
-- error
alter table t1 add f7 float not null after f3;
select * from t1;

-- 5.3.1 null and not null in a table without data, drop null, add column with null or not null
drop table if exists t1 cascade;
create table t1(f1 int null, f2 varchar(20) not null, f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
alter table t1 drop f1, add f6 int null first, add f7 float not null after f3;
\d+ t1

-- 5.3.2 null and not null in a table with data, drop null, add column with null or not null
drop table if exists t1 cascade;
create table t1(f1 int null, f2 varchar(20) not null, f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 drop f1, add f6 int null first;
-- error
alter table t1 add f7 float not null after f3;
select * from t1;

-- 5.4.1 null and not null in a table without data, drop null and not null, add column with null or not null
drop table if exists t1 cascade;
create table t1(f1 int null, f2 varchar(20) not null, f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
alter table t1 drop f1, drop f2, add f6 int null first, add f7 float not null after f3;
\d+ t1

-- 5.4.2 null and not null in a table without data, drop null and not null, add column with null or not null
drop table if exists t1 cascade;
create table t1(f1 int null, f2 varchar(20) not null, f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 drop f1, drop f2, add f6 int null first;
-- error
alter table t1 add f7 float not null after f3;
select * from t1;

-- 6 check constraint
-- 6.1.1 check constraint in a table without data, add column without constraint
drop table if exists t1 cascade;
create table t1(f1 int check(f1 = 1), f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
alter table t1 add f6 text first, add f7 float after f3;
\d+ t1

-- 6.1.2 check constraint in a table with data, add column without constraint
drop table if exists t1 cascade;
create table t1(f1 int check(f1 = 1), f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 add f6 text first, add f7 float after f3;
select * from t1;

-- 6.2.1 check constraint in a table without data, add column with check
drop table if exists t1 cascade;
create table t1(f1 int check(f1 = 1), f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
alter table t1 add f6 int default 6, add f7 text check(f6 = 6) first, add f8 float check(f1 + f2 == 7);
\d+ t1

-- 6.2.2 check constraint in a table with data, add column with check
drop table if exists t1 cascade;
create table t1(f1 int check(f1 = 1), f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (1, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 add f6 int default 6, add f7 text check(f6 = 6) first, add f8 float check(f1 + f2 == 7) after f3;
select * from t1;

-- 6.3.1 check constraint in a table without data, drop check, add column with check
drop table if exists t1 cascade;
create table t1(f1 int check(f1 = 1), f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
alter table t1 drop f2, add f6 text check (f1 > 0) first, add f7 int check (f7 - f1 > 0) after f3;
\d+ t1

-- 6.3.2 check constraint in a table with data, drop check, add column with with check
drop table if exists t1 cascade;
create table t1(f1 int check(f1 = 1), f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with (storage_type = ustore);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (1, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 drop f2, add f6 text check (f1 > 0) first, add f7 int check(f7 - f1 > 0) after f3;
select * from t1;

-- 7 foreign key
-- 7.1.1 foreign key constraint in a table without data, add column without constraint
drop table if exists t_pri1 cascade;
create table t_pri1(f1 int, f2 int primary key) with (storage_type = ustore);
drop table if exists t1 cascade;
create table t1(f1 int, f2 int references t_pri1(f2), f3 bool) with (storage_type = ustore);
alter table t1 add f4 int, add f5 text first, f6 float after f2;
\d+ t1

-- 7.1.2 foreign key constraint in a table with data, add column without constraint
drop table if exists t1 cascade;
drop table if exists t_pri1 cascade;
create table t_pri1(f1 text, f2 int primary key) with (storage_type = ustore);
insert into t_pri1 values('a', 1), ('b', 2);
create table t1(f1 text, f2 int references t_pri1(f2), f3 bool) with (storage_type = ustore);
insert into t1 values('a', 1, true), ('b', 2, false);
alter table t1 add f4 int, add f5 text first, f6 float after f2;
select * from t1;

-- 7.2.1 foreign key constraint in a table without data, add column with foreign key
drop table if exists t1 cascade;
drop table if exists t_pri1 cascade;
drop table if exists t_pri2 cascade;
create table t_pri1(f1 text, f2 int primary key) with (storage_type = ustore);
create table t_pri2(f1 int, f2 int, f4 int primary key) with (storage_type = ustore);
create table t1(f1 int, f2 int references t_pri1(f2), f3 bool) with (storage_type = ustore);
alter table t1 add f4 int references t_pri2(f4) first;
\d+ t1
alter table t1 drop f4, add f4 int references t_pri2(f4) after f2;
\d+ t1

-- 7.2.2 foreign key constraint in a table with data, add column with foreign key
drop table if exists t1 cascade;
drop table if exists t_pri1 cascade;
drop table if exists t_pri2 cascade;
create table t_pri1(f1 text, f2 int primary key) with (storage_type = ustore);
insert into t_pri1 values('a', 1), ('b', 2);
create table t_pri2(f1 int, f2 bool, f4 int primary key) with (storage_type = ustore);
insert into t_pri2 values(11, true, 1), (22, false, 2);
create table t1(f1 int, f2 int references t_pri1(f2), f3 bool) with (storage_type = ustore);
insert into t1 values(1, 1, true), (2, 2, false);
alter table t1 add f4 int references t_pri2(f4) first;
select * from t1;
alter table t1 drop f4, add f4 int references t_pri2(f4) after f2;
select * from t1;

-- 7.3.1 foreign key constraint in a table without data, drop foreign key, add column with foreign key
drop table if exists t1 cascade;
drop table if exists t_pri1 cascade;
drop table if exists t_pri2 cascade;
create table t_pri1(f1 int, f2 int primary key) with (storage_type = ustore);
create table t_pri2(f1 int, f2 int, f4 int primary key) with (storage_type = ustore);
create table t1(f1 int, f2 int references t_pri1(f2), f3 bool) with (storage_type = ustore);
alter table t1 drop f2, add f4 int references t_pri2(f4) first;
\d+ t1
alter table t1 drop f4, add f4 int references t_pri2(f4) after f1;
\d+ t1

-- 7.3.2 foreign key constraint in a table with data, drop foreign key, add column with foreign key
drop table if exists t1 cascade;
drop table if exists t_pri1 cascade;
drop table if exists t_pri2 cascade;
create table t_pri1(f1 text, f2 int primary key) with (storage_type = ustore);
insert into t_pri1 values('a', 1), ('b', 2);
create table t_pri2(f1 text, f2 bool, f4 int primary key) with (storage_type = ustore);
insert into t_pri2 values('a', true, 1), ('b', false, 2);
create table t1(f1 int, f2 int references t_pri1(f2), f3 bool) with (storage_type = ustore);
insert into t1 values(1, 2, true), (2, 2, false);
alter table t1 drop f2, add f4 int references t_pri2(f4) first;
select * from t1;
alter table t1 drop f4, add f4 int references t_pri2(f4) after f1;
select * from t1;

-- partition table
drop table if exists t1 cascade;
create table t1
(f1 int, f2 int, f3 int) with (storage_type = ustore)
partition by range(f1, f2)
(
        partition t1_p0 values less than (10, 0),
        partition t1_p1 values less than (20, 0),
        partition t1_p2 values less than (30, 0)
);
select relname, parttype, partkey from pg_partition where parentid=(select oid from pg_class where relname='t1') order by relname;

alter table t1 add f4 int first, add f5 int after f1;
\d+ t1
select relname, parttype, partkey from pg_partition where parentid=(select oid from pg_class where relname='t1') order by relname;

-- subpartition table
drop table if exists range_range cascade;
create table range_range(id int, gender varchar not null, birthday date not null) with (storage_type = ustore)
partition by range (id) subpartition by range (birthday)
(
        partition p_1 values less than(100)
        (
                subpartition p_1_a values less than('2022-01-01'),
                subpartition p_1_b values less than(MAXVALUE)
        ),
        partition p_2 values less than(200)
        (
                subpartition p_2_a values less than('2022-01-01'),
                subpartition p_2_b values less than(MAXVALUE)
        ),
        partition p_3 values less than(MAXVALUE)
        (
                subpartition p_3_a values less than('2022-01-01'),
                subpartition p_3_b values less than(MAXVALUE)
        )
);
insert into range_range values(198,'boy','2010-02-15'),(33,'boy','2003-08-11'),(78,'girl','2014-06-24');
insert into range_range values(233,'girl','2010-01-01'),(360,'boy','2007-05-14'),(146,'girl','2005-03-08');
insert into range_range values(111,'girl','2013-11-19'),(15,'girl','2009-01-12'),(156,'boy','2011-05-21');


-- test pg_partition
select relname, parttype, partkey from pg_partition where parentid=(select oid from pg_class where relname='range_range') order by relname;
alter table range_range add f1 int default 1 first, add f2 text after id;
\d+ range_range
select relname, parttype, partkey from pg_partition where parentid=(select oid from pg_class where relname='range_range') order by relname;
select * from range_range;

-- orientation = column not support
drop table if exists t1 cascade;
create table t1(f1 int, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with (orientation = column);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
-- error
alter table t1 add f6 text first;
-- error
alter table t1 add f6 text after f1;

-- pg_constraint test
set enable_default_ustore_table = on;
drop table if exists t_pri cascade;
drop table if exists t1 cascade;
create table t_pri(f1 int, f2 int, f3 int, primary key(f2, f3));
create table t1
(
        f1 int, f2 int, f3 varchar(20), f4 int, f5 int, f6 int, f7 int,
        foreign key(f1, f2) references t_pri(f2, f3),
        unique((lower(f3)), (abs(f4))),
        check(f5 = 10),
        unique(f4, f5) include(f6, f7)
);
select conname, contype, conkey, confkey, conbin, consrc, conincluding from pg_constraint 
        where conrelid = (select oid from pg_class where relname = 't1') order by conname;

alter table t_pri add f4 int first, add f5 int after f2;
alter table t1 add f8 int primary key first, add f9 int unique after f3;
\d+ t_pri
select conname, contype, conkey, confkey, conbin, consrc, conincluding from pg_constraint 
        where conrelid = (select oid from pg_class where relname = 't_pri') order by conname;
\d+ t1
select conname, contype, conkey, confkey, conbin, consrc, conincluding from pg_constraint 
        where conrelid = (select oid from pg_class where relname = 't1') order by conname;

set enable_default_ustore_table = off;

-- pg_index test
drop table if exists t1 cascade;
create table t1
(
        f1 int, f2 int, f3 varchar(20), f4 int, f5 int, f6 int, f7 int,
        primary key(f1, f2),
        unique((lower(f3)), (abs(f4))),
        check(f5 = 10)
);
create unique index partial_t1_idx on t1(f5, abs(f6)) where f5 + f6 - abs(f7) > 0;

select indkey, indexprs, indpred from pg_index where indrelid = (select oid from pg_class where relname = 't1') order by 1, 2, 3;

alter table t1 add f8 int first, add f9 int unique after f1;
\d+ t1
select indkey, indexprs, indpred from pg_index where indrelid = (select oid from pg_class where relname = 't1') order by 1, 2, 3;

-- pg_attribute test
drop table if exists t1 cascade;
create table t1(f1 int, f2 int, f3 int);
select attname, attnum, atthasdef, attisdropped from pg_attribute where attrelid = (select oid from pg_class where relname = 't1') and attnum > 0 order by attnum;

alter table t1 add f4 int default 4 first;
\d+ t1
select attname, attnum, atthasdef, attisdropped from pg_attribute where attrelid = (select oid from pg_class where relname = 't1') and attnum > 0 order by attnum;

alter table t1 drop f2, add f5 int default 5 after f1;
\d+ t1
select attname, attnum, atthasdef, attisdropped from pg_attribute where attrelid = (select oid from pg_class where relname = 't1') and attnum > 0 order by attnum;

-- pg_attrdef test
drop table if exists t1 cascade;
create table t1(f1 int primary key auto_increment, f2 int, f3 int default 3, f4 int generated always as (f2 + f3) stored);
select adnum, adsrc, adgencol from pg_attrdef where adrelid = (select oid from pg_class where relname = 't1') order by adnum;

alter table t1 add f5 text default 'aaa' first;
\d+ t1
select adnum, adsrc, adgencol from pg_attrdef where adrelid = (select oid from pg_class where relname = 't1') order by adnum;

alter table t1 drop f2, add f6 int generated always as (f1 + abs(f3)) stored after f1; -- ERROR

-- pg_rewrite test
drop table if exists t1 cascade;
create table t1(f1 int, f2 int, f3 int);
insert into t1 values(1, 2, 3), (11, 22, 33);
create view t1_view1 as select * from t1;
create view t1_view2 as select f1, f2 from t1;
\d+ t1_view1
\d+ t1_view2
\d+ t1
select pg_get_viewdef('t1_view1');
select pg_get_viewdef('t1_view2');
select * from t1_view1;
select * from t1_view2;
select * from t1;
alter table t1 add f4 int first, add f5 int after f1;
\d+ t1_view1
\d+ t1_view2
\d+ t1
select pg_get_viewdef('t1_view1');
select pg_get_viewdef('t1_view2');
select * from t1_view1;
select * from t1_view2;
select * from t1;

-- pg_trigger test
drop table if exists t1 cascade;
create table t1(f1 boolean not null, f2 text, f3 int, f4 date);
alter table t1 add primary key(f1);
create or replace function dummy_update_func() returns trigger as $$
begin
        raise notice 'dummy_update_func(%) called: action = %, oid = %, new = %', TG_ARGV[0], TG_OP, OLD, NEW;
        return new;
end;
$$ language plpgsql;

drop trigger if exists f1_trig_update on t1;
drop trigger if exists f1_trig_insert on t1;

create trigger f1_trig_update after update of f1 on t1 for each row
        when (not old.f1 and new.f1) execute procedure dummy_update_func('update');
create trigger f1_trig_insert after insert on t1 for each row
        when (not new.f1) execute procedure dummy_update_func('insert');

select tgname, tgattr, tgqual from pg_trigger where tgrelid = (select oid from pg_class where relname='t1') order by tgname;

alter table t1 add f5 int after f1, add f6 boolean first;
\d+ t1
select tgname, tgattr, tgqual from pg_trigger where tgrelid = (select oid from pg_class where relname='t1') order by tgname;

-- pg_rlspolicy test
drop table if exists t1 cascade;
drop role if exists test_rlspolicy2;
create role test_rlspolicy2 nologin password 'Gauss_234';
create table t1 (f1 int, f2 int, f3 text) partition by range (f1)
(
        partition t1_p0 values less than(10),
        partition t1_p1 values less than(50),
        partition t1_p2 values less than(100),
        partition t1_p3 values less than(MAXVALUE)
);

INSERT INTO t1 VALUES (generate_series(1, 150) % 24, generate_series(1, 150), 'huawei');
grant select on t1 to public;

create row level security policy t1_rls1 on t1 as permissive to public using (f2 <= 20);
create row level security policy t1_rls2 on t1 as restrictive to test_rlspolicy2 using (f1 < 30);

\d+ t1
select * from t1 limit 10;
select polname, polqual from pg_rlspolicy where polrelid = (select oid from pg_class where relname='t1');

alter table t1 add f4 int generated always as (f1 + 100) stored after f1, add f5 int generated always as (f2 + 100) stored first;
\d+ t1
select * from t1 limit 10;
select polname, polqual from pg_rlspolicy where polrelid = (select oid from pg_class where relname='t1');

-- expression test
drop table if exists t1 cascade;
create table t1(f1 int, f2 int, f3 int, f4 bool, f5 text, f6 text);
insert into t1 values(1, 2, 3, true, 'nanjin', 'huawei');
-- T_FuncExpr
create index t1_idx1 on t1(abs(f1), f2);
-- T_OpExpr
create index t1_idx2 on t1((f1 + f2), (f1 - f3));
-- T_BooleanTest
create index t1_idx3 on t1((f4 is true));
-- T_CaseExpr and T_CaseWhen
create index t1_idx4 on t1((case f1 when f2 then 'yes' when f3 then 'no' else 'unknow' end));
-- T_ArrayExpr
create index t1_idx5 on t1((array[f1, f2, f3]));
-- T_TypeCast
create index t1_idx6 on t1(((f1 + f2 + 1) :: text));
-- T_BoolExpr
create index t1_idx7 on t1((f1 and f2), (f2 or f3));
-- T_ArrayRef
create index t1_idx8 on t1((f1 = (array[f1, f2, 3])[1]));
-- T_ScalarArrayOpExpr
create index t1_idx9 on t1((f1 = ANY(ARRAY[f2, 1, f1 + 10])));
-- T_RowCompareExpr
create index t1_idx10 on t1((row(f1, f5) < row(f2, f6)));
-- T_MinMaxExpr
create index t1_idx11 on t1(greatest(f1, f2, f3), least(f1, f2, f3));
-- T_RowExpr
drop table if exists mytable cascade;
create table mytable(f1 int, f2 int, f3 text);
create function getf1(mytable) returns int as 'select $1.f1' language sql;
create index t1_idx12 on t1(getf1(row(f1, 2, 'a')));
-- T_CoalesceExpr
create index t1_idx13 on t1(nvl(f1, f2));
-- T_NullTest
create index t1_idx14 on t1((f1 is null));
-- T_ScalarArrayOpExpr
create index t1_idx16 on t1((f1 in (1,2,3)));
-- T_NullIfExpr
create index t1_idx17 on t1(nullif(f5,f6));
-- T_RelabelType
alter table t1 add f7 oid;
create index t1_idx18 on t1((f7::int4));
-- T_CoerceViaIO
alter table t1 add f8 json;
create index t1_idx19 on t1((f8::jsonb));
-- T_ArrayCoerceExpr
alter table t1 add f9 float[];
create index t1_idx20 on t1((f9::int[]));
-- T_PrefixKey
create index t1_idx21 on t1(f6(5));

\d+ t1
select * from t1;

alter table t1 add f10 int primary key auto_increment after f4,
        add f11 int generated always as (f1 + f2) stored after f1,
        add f12 date default '2023-01-05' first,
        add f13 int not null default 13 first;

\d+ t1
select * from t1;

-- test modify column ... first | after column in astore table
-- ASTORE table
-- common scenatios
drop table if exists t1 cascade;
create table t1(f1 int, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 modify f3 timestamp first, modify f1 int after f4, modify f5 bool after f2;
\d+ t1
select * from t1;
alter table t1 modify 

-- 1 primary key
drop table if exists t1 cascade;
create table t1(f1 int primary key, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
alter table t1 modify f3 timestamp first, modify f1 int after f4;
\d+ t1
alter table t1 modify f1 int after f3;
\d+ t1
alter table t1 drop f1, modify f5 bool first;
\d+ t1

drop table if exists t1 cascade;
create table t1(f1 int primary key, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 modify f3 timestamp first, modify f1 int after f4;
\d+ t1
select * from t1;
alter table t1 modify f1 int after f3;
\d+ t1
select * from t1;
alter table t1 drop f1, modify f5 bool first;
\d+ t1
select * from t1;

-- 2 unique index
drop table if exists t1 cascade;
create table t1(f1 int unique, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
alter table t1 modify f3 timestamp first, modify f1 int after f4;
\d+ t1
alter table t1 modify f1 int after f3;
\d+ t1
alter table t1 drop f1, modify f5 bool first;
\d+ t1

drop table if exists t1 cascade;
create table t1(f1 int unique, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 modify f3 timestamp first, modify f1 int after f4;
\d+ t1
select * from t1;
alter table t1 modify f1 int after f3;
\d+ t1
select * from t1;
alter table t1 drop f1, modify f5 bool first;
\d+ t1
select * from t1;

-- 3 default and generated column
drop table if exists t1 cascade;
create table t1(f1 int default 1, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
alter table t1 modify f3 timestamp first, modify f1 int after f4;
\d+ t1
alter table t1 modify f1 int after f3;
\d+ t1
alter table t1 drop f1, modify f5 bool first;
\d+ t1

drop table if exists t1 cascade;
create table t1(f1 int default 1, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 modify f3 timestamp first, modify f1 int after f4;
\d+ t1
select * from t1;
alter table t1 modify f1 int after f3;
\d+ t1
select * from t1;
alter table t1 drop f1, modify f5 bool first;
\d+ t1
select * from t1;

drop table if exists t1 cascade;
create table t1(f1 int, f2 int default 2, f3 int default 3, f4 int generated always as (f1 + f2) stored);
alter table t1 modify f4 int after f2, modify f1 int after f3, modify f3 int first;
\d+ t1
alter table t1 drop f1;
\d+ t1

drop table if exists t1 cascade;
create table t1(f1 int, f2 int default 2, f3 int default 3, f4 int generated always as (f1 + f2) stored);
insert into t1 values(1,2,3),(11,22,33);
alter table t1 modify f4 int after f2, modify f1 int after f3, modify f3 int first;
\d+ t1
select * from t1;
alter table t1 drop f1;
\d+ t1
select * from t1;

-- 4 auto_increment
drop table if exists t1 cascade;
create table t1(f1 int primary key auto_increment, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
\d+ t1
alter table t1 modify f3 timestamp first, modify f1 int after f4;
\d+ t1

drop table if exists t1 cascade;
create table t1(f1 int primary key auto_increment, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
insert into t1(f2, f3, f4, f5) values('a', '2022-11-08 19:56:10.158564', x'41', true), ('b', '2022-11-09 19:56:10.158564', x'42', false);
\d+ t1
select * from t1;
alter table t1 modify f3 timestamp first, modify f1 int after f4;
\d+ t1
select * from t1;
insert into t1(f3, f2, f4, f5, f1) values('2022-11-10 19:56:10.158564', 'c', x'43', false, 3);
select f1 from t1;

-- 5 NULL and NOT NULL
drop table if exists t1 cascade;
alter table t1(f1 int null, f2 varchar(20) not null, f3 timestamp, f4 bit(8), f5 bool);
alter table t1 modify f3 timestamp first, modify f1 int after f4;
\d+ t1
alter table t1 modify f1 int after f3;
\d+ t1
alter table t1 drop f1, modify f5 bool first;
\d+ t1
alter table t1 modify f2 varchar(20) after f3;
\d+ t1

drop table if exists t1 cascade;
alter table t1(f1 int null, f2 varchar(20) not null, f3 timestamp, f4 bit(8), f5 bool);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 modify f3 timestamp first, modify f1 int after f4;
\d+ t1
select * from t1;
alter table t1 modify f1 int after f3;
\d+ t1
select * from t1;
alter table t1 drop f1, modify f5 bool first;
\d+ t1
select * from t1;
alter table t1 modify f2 varchar(20) after f3;
\d+ t1
select * from t1;

-- 6 check constraint
drop table if exists t1 cascade;
create table t1(f1 int check(f1 = 1), f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
alter table t1 modify f3 timestamp first, modify f1 int after f4;
\d+ t1
alter table t1 modify f1 int after f3;
\d+ t1
alter table t1 drop f1, modify f5 bool first;
\d+ t1

drop table if exists t1 cascade;
create table t1(f1 int check(f1 = 1), f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 modify f3 timestamp first, modify f1 int after f4;
\d+ t1
select * from t1;
alter table t1 modify f1 int after f3;
\d+ t1
select * from t1;
alter table t1 drop f1, modify f5 bool first;
\d+ t1
select * from t1;

-- 7 foreign key
drop table if exists t_pri1 cascade;
create table t_pri1(f1 int, f2 int primary key);
drop table if exists t1 cascade;
create table t1(f1 int, f2 int references t_pri1(f2), f3 bool);
alter table t1 modify f2 int first;
\d+ t1
alter table t1 modify f2 int after f3;
\d+ t1

drop table if exists t_pri1 cascade;
create table t_pri1(f1 int, f2 int primary key);
insert into t_pri1 values(1,1),(2,2);
drop table if exists t1 cascade;
create table t1(f1 int, f2 int references t_pri1(f2), f3 bool);
insert into t1 values(1, 1, true), (2, 2, false);
alter table t1 modify f2 int first;
\d+ t1
select * from t1;
alter table t1 modify f2 int after f3;
\d+ t1
select * from t1;

-- partition table
drop table if exists t1 cascade;
create table t1
(f1 int, f2 int, f3 int, primary key (f1, f2))
partition by range(f1, f2)
(
        partition t1_p0 values less than (10, 0),
        partition t1_p1 values less than (20, 0),
        partition t1_p2 values less than (30, 0)
);
select relname, parttype, partkey from pg_partition where parentid=(select oid from pg_class where relname='t1') order by relname;

alter table t1 modify f1 int after f2, modify f3 int first, modify f2 int first;
select relname, parttype, partkey from pg_partition where parentid=(select oid from pg_class where relname='t1') order by relname;

alter table t1 modify f1 int after f2;
select relname, parttype, partkey from pg_partition where parentid=(select oid from pg_class where relname='t1') order by relname;

-- modify operation before add
alter table t1 add f4 int after f2, modify f1 int after f2;
select relname, parttype, partkey from pg_partition where parentid=(select oid from pg_class where relname='t1');

drop table if exists t1 cascade;
create table t1
(f1 int, f2 int, f3 int, primary key (f1, f2))
partition by range(f1, f2)
(
        partition t1_p0 values less than (10, 0),
        partition t1_p1 values less than (20, 0),
        partition t1_p2 values less than (30, 0)
);
insert into t1 values(9, -1, 1), (19, -1, 2), (29, -1, 3);
\d+ t1
select relname, parttype, partkey from pg_partition where parentid=(select oid from pg_class where relname='t1');
select * from t1 partition (t1_p0);
select * from t1 partition (t1_p1);
select * from t1 partition (t1_p2);

alter table t1 modify f1 int after f2, modify f3 int first, modify f2 int first;
\d+ t1
select relname, parttype, partkey from pg_partition where parentid=(select oid from pg_class where relname='t1');
select * from t1 partition (t1_p0);
select * from t1 partition (t1_p1);
select * from t1 partition (t1_p2);

alter table t1 modify f1 int after f2;
\d+ t1
select relname, parttype, partkey from pg_partition where parentid=(select oid from pg_class where relname='t1');
select * from t1 partition (t1_p0);
select * from t1 partition (t1_p1);
select * from t1 partition (t1_p2);

alter table t1 add f4 int after f2, modify f1 int after f2;
\d+ t1
select relname, parttype, partkey from pg_partition where parentid=(select oid from pg_class where relname='t1');
select * from t1 partition (t1_p0);
select * from t1 partition (t1_p1);
select * from t1 partition (t1_p2);

-- subpartition table
drop table if exists range_range cascade;
create table range_range(id int, gender varchar not null, birthday date not null, primary key(id, birthday))
partition by range (id) subpartition by range (birthday)
(
        partition p_1 values less than(100)
        (
                subpartition p_1_a values less than('2022-01-01'),
                subpartition p_1_b values less than(MAXVALUE)
        ),
        partition p_2 values less than(200)
        (
                subpartition p_2_a values less than('2022-01-01'),
                subpartition p_2_b values less than(MAXVALUE)
        ),
        partition p_3 values less than(MAXVALUE)
        (
                subpartition p_3_a values less than('2022-01-01'),
                subpartition p_3_b values less than(MAXVALUE)
        )
);
select relname, parttype, partkey from pg_partition where parentid=(select oid from pg_class where relname='range_range') order by relname;

alter table range_range modify birthday date first, modify id int after gender;
\d+ range_range
select relname, parttype, partkey from pg_partition where parentid=(select oid from pg_class where relname='range_range') order by relname;


drop table if exists range_range cascade;
create table range_range(id int, gender varchar not null, birthday date not null, primary key(id, birthday))
partition by range (id) subpartition by range (birthday)
(
        partition p_1 values less than(100)
        (
                subpartition p_1_a values less than('2022-01-01'),
                subpartition p_1_b values less than(MAXVALUE)
        ),
        partition p_2 values less than(200)
        (
                subpartition p_2_a values less than('2022-01-01'),
                subpartition p_2_b values less than(MAXVALUE)
        ),
        partition p_3 values less than(MAXVALUE)
        (
                subpartition p_3_a values less than('2022-01-01'),
                subpartition p_3_b values less than(MAXVALUE)
        )
);

insert into range_range values(198,'boy','2010-02-15'),(33,'boy','2003-08-11'),(78,'girl','2014-06-24');
insert into range_range values(233,'girl','2010-01-01'),(360,'boy','2007-05-14'),(146,'girl','2005-03-08');
insert into range_range values(111,'girl','2013-11-19'),(15,'girl','2009-01-12'),(156,'boy','2011-05-21');

select relname, parttype, partkey from pg_partition where parentid=(select oid from pg_class where relname='range_range') order by relname;

alter table range_range modify birthday date first, modify id int after gender;
\d+ range_range
select relname, parttype, partkey from pg_partition where parentid=(select oid from pg_class where relname='range_range') order by relname;

select * from range_range;

-- USTORE table
-- common scenatios
drop table if exists t1 cascade;
create table t1(f1 int, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with(storage_type = ustore);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 modify f3 timestamp first, modify f1 int after f4, modify f5 bool after f2;
\d+ t1
select * from t1;
alter table t1 modify 

-- 1 primary key
drop table if exists t1 cascade;
create table t1(f1 int primary key, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with(storage_type = ustore);
alter table t1 modify f3 timestamp first, modify f1 int after f4;
\d+ t1
alter table t1 modify f1 int after f3;
\d+ t1
alter table t1 drop f1, modify f5 bool first;
\d+ t1

drop table if exists t1 cascade;
create table t1(f1 int primary key, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with(storage_type = ustore);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 modify f3 timestamp first, modify f1 int after f4;
\d+ t1
select * from t1;
alter table t1 modify f1 int after f3;
\d+ t1
select * from t1;
alter table t1 drop f1, modify f5 bool first;
\d+ t1
select * from t1;

-- 2 unique index
drop table if exists t1 cascade;
create table t1(f1 int unique, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with(storage_type = ustore);
alter table t1 modify f3 timestamp first, modify f1 int after f4;
\d+ t1
alter table t1 modify f1 int after f3;
\d+ t1
alter table t1 drop f1, modify f5 bool first;
\d+ t1

drop table if exists t1 cascade;
create table t1(f1 int unique, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with(storage_type = ustore);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 modify f3 timestamp first, modify f1 int after f4;
\d+ t1
select * from t1;
alter table t1 modify f1 int after f3;
\d+ t1
select * from t1;
alter table t1 drop f1, modify f5 bool first;
\d+ t1
select * from t1;

-- 3 default and generated column
drop table if exists t1 cascade;
create table t1(f1 int default 1, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with(storage_type = ustore);
alter table t1 modify f3 timestamp first, modify f1 int after f4;
\d+ t1
alter table t1 modify f1 int after f3;
\d+ t1
alter table t1 drop f1, modify f5 bool first;
\d+ t1

drop table if exists t1 cascade;
create table t1(f1 int default 1, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with(storage_type = ustore);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 modify f3 timestamp first, modify f1 int after f4;
\d+ t1
select * from t1;
alter table t1 modify f1 int after f3;
\d+ t1
select * from t1;
alter table t1 drop f1, modify f5 bool first;
\d+ t1
select * from t1;

drop table if exists t1 cascade;
create table t1(f1 int, f2 int default 2, f3 int default 3, f4 int generated always as (f1 + f2) stored) with(storage_type = ustore);
alter table t1 modify f4 int after f2, modify f1 int after f3, modify f3 int first;
\d+ t1
alter table t1 drop f1;
\d+ t1

drop table if exists t1 cascade;
create table t1(f1 int, f2 int default 2, f3 int default 3, f4 int generated always as (f1 + f2) stored) with(storage_type = ustore);
insert into t1 values(1,2,3),(11,22,33);
alter table t1 modify f4 int after f2, modify f1 int after f3, modify f3 int first;
\d+ t1
select * from t1;
alter table t1 drop f1;
\d+ t1
select * from t1;

-- 4 auto_increment
drop table if exists t1 cascade;
create table t1(f1 int primary key auto_increment, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with(storage_type = ustore);
\d+ t1
alter table t1 modify f3 timestamp first, modify f1 int after f4;
\d+ t1

drop table if exists t1 cascade;
create table t1(f1 int primary key auto_increment, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with(storage_type = ustore);
insert into t1(f2, f3, f4, f5) values('a', '2022-11-08 19:56:10.158564', x'41', true), ('b', '2022-11-09 19:56:10.158564', x'42', false);
\d+ t1
select * from t1;
alter table t1 modify f3 timestamp first, modify f1 int after f4;
\d+ t1
select * from t1;
insert into t1(f3, f2, f4, f5, f1) values('2022-11-10 19:56:10.158564', 'c', x'43', false, 3);
select f1 from t1;

-- 5 NULL and NOT NULL
drop table if exists t1 cascade;
alter table t1(f1 int null, f2 varchar(20) not null, f3 timestamp, f4 bit(8), f5 bool) with(storage_type = ustore);
alter table t1 modify f3 timestamp first, modify f1 int after f4;
\d+ t1
alter table t1 modify f1 int after f3;
\d+ t1
alter table t1 drop f1, modify f5 bool first;
\d+ t1
alter table t1 modify f2 varchar(20) after f3;
\d+ t1

drop table if exists t1 cascade;
alter table t1(f1 int null, f2 varchar(20) not null, f3 timestamp, f4 bit(8), f5 bool) with(storage_type = ustore);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 modify f3 timestamp first, modify f1 int after f4;
\d+ t1
select * from t1;
alter table t1 modify f1 int after f3;
\d+ t1
select * from t1;
alter table t1 drop f1, modify f5 bool first;
\d+ t1
select * from t1;
alter table t1 modify f2 varchar(20) after f3;
\d+ t1
select * from t1;

-- 6 check constraint
drop table if exists t1 cascade;
create table t1(f1 int check(f1 = 1), f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with(storage_type = ustore);
alter table t1 modify f3 timestamp first, modify f1 int after f4;
\d+ t1
alter table t1 modify f1 int after f3;
\d+ t1
alter table t1 drop f1, modify f5 bool first;
\d+ t1

drop table if exists t1 cascade;
create table t1(f1 int check(f1 = 1), f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with(storage_type = ustore);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
alter table t1 modify f3 timestamp first, modify f1 int after f4;
\d+ t1
select * from t1;
alter table t1 modify f1 int after f3;
\d+ t1
select * from t1;
alter table t1 drop f1, modify f5 bool first;
\d+ t1
select * from t1;

-- 7 foreign key
drop table if exists t_pri1 cascade;
create table t_pri1(f1 int, f2 int primary key) with(storage_type = ustore);
drop table if exists t1 cascade;
create table t1(f1 int, f2 int references t_pri1(f2), f3 bool) with(storage_type = ustore);
alter table t1 modify f2 int first;
\d+ t1
alter table t1 modify f2 int after f3;
\d+ t1

drop table if exists t_pri1 cascade;
create table t_pri1(f1 int, f2 int primary key) with(storage_type = ustore);
insert into t_pri1 values(1,1),(2,2);
drop table if exists t1 cascade;
create table t1(f1 int, f2 int references t_pri1(f2), f3 bool) with(storage_type = ustore);
insert into t1 values(1, 1, true), (2, 2, false);
alter table t1 modify f2 int first;
\d+ t1
select * from t1;
alter table t1 modify f2 int after f3;
\d+ t1
select * from t1;

-- partition table
drop table if exists t1 cascade;
create table t1
(f1 int, f2 int, f3 int) with(storage_type = ustore)
partition by range(f1, f2)
(
        partition t1_p0 values less than (10, 0),
        partition t1_p1 values less than (20, 0),
        partition t1_p2 values less than (30, 0)
);
select relname, parttype, partkey from pg_partition where parentid=(select oid from pg_class where relname='t1') order by relname;

alter table t1 modify f1 int after f2, modify f3 int first, modify f2 int first;
select relname, parttype, partkey from pg_partition where parentid=(select oid from pg_class where relname='t1') order by relname;

alter table t1 modify f1 int after f2;
select relname, parttype, partkey from pg_partition where parentid=(select oid from pg_class where relname='t1') order by relname;

-- modify operation before add
alter table t1 add f4 int after f2, modify f1 int after f2;
select relname, parttype, partkey from pg_partition where parentid=(select oid from pg_class where relname='t1');

drop table if exists t1 cascade;
create table t1
(f1 int, f2 int, f3 int) with(storage_type = ustore)
partition by range(f1, f2)
(
        partition t1_p0 values less than (10, 0),
        partition t1_p1 values less than (20, 0),
        partition t1_p2 values less than (30, 0)
);
insert into t1 values(9, -1, 1), (19, -1, 2), (29, -1, 3);
\d+ t1
select relname, parttype, partkey from pg_partition where parentid=(select oid from pg_class where relname='t1');
select * from t1 partition (t1_p0);
select * from t1 partition (t1_p1);
select * from t1 partition (t1_p2);

alter table t1 modify f1 int after f2, modify f3 int first, modify f2 int first;
\d+ t1
select relname, parttype, partkey from pg_partition where parentid=(select oid from pg_class where relname='t1');
select * from t1 partition (t1_p0);
select * from t1 partition (t1_p1);
select * from t1 partition (t1_p2);

alter table t1 modify f1 int after f2;
\d+ t1
select relname, parttype, partkey from pg_partition where parentid=(select oid from pg_class where relname='t1');
select * from t1 partition (t1_p0);
select * from t1 partition (t1_p1);
select * from t1 partition (t1_p2);

alter table t1 add f4 int after f2, modify f1 int after f2;
\d+ t1
select relname, parttype, partkey from pg_partition where parentid=(select oid from pg_class where relname='t1');
select * from t1 partition (t1_p0);
select * from t1 partition (t1_p1);
select * from t1 partition (t1_p2);

-- subpartition table
drop table if exists range_range cascade;
create table range_range(id int, gender varchar not null, birthday date not null) with(storage_type = ustore)
partition by range (id) subpartition by range (birthday)
(
        partition p_1 values less than(100)
        (
                subpartition p_1_a values less than('2022-01-01'),
                subpartition p_1_b values less than(MAXVALUE)
        ),
        partition p_2 values less than(200)
        (
                subpartition p_2_a values less than('2022-01-01'),
                subpartition p_2_b values less than(MAXVALUE)
        ),
        partition p_3 values less than(MAXVALUE)
        (
                subpartition p_3_a values less than('2022-01-01'),
                subpartition p_3_b values less than(MAXVALUE)
        )
);
select relname, parttype, partkey from pg_partition where parentid=(select oid from pg_class where relname='range_range') order by relname;

alter table range_range modify birthday date first, modify id int after gender;
\d+ range_range
select relname, parttype, partkey from pg_partition where parentid=(select oid from pg_class where relname='range_range') order by relname;


drop table if exists range_range cascade;
create table range_range(id int, gender varchar not null, birthday date not null) with(storage_type = ustore)
partition by range (id) subpartition by range (birthday)
(
        partition p_1 values less than(100)
        (
                subpartition p_1_a values less than('2022-01-01'),
                subpartition p_1_b values less than(MAXVALUE)
        ),
        partition p_2 values less than(200)
        (
                subpartition p_2_a values less than('2022-01-01'),
                subpartition p_2_b values less than(MAXVALUE)
        ),
        partition p_3 values less than(MAXVALUE)
        (
                subpartition p_3_a values less than('2022-01-01'),
                subpartition p_3_b values less than(MAXVALUE)
        )
);

insert into range_range values(198,'boy','2010-02-15'),(33,'boy','2003-08-11'),(78,'girl','2014-06-24');
insert into range_range values(233,'girl','2010-01-01'),(360,'boy','2007-05-14'),(146,'girl','2005-03-08');
insert into range_range values(111,'girl','2013-11-19'),(15,'girl','2009-01-12'),(156,'boy','2011-05-21');

select relname, parttype, partkey from pg_partition where parentid=(select oid from pg_class where relname='range_range') order by relname;

alter table range_range modify birthday date first, modify id int after gender;
\d+ range_range
select relname, parttype, partkey from pg_partition where parentid=(select oid from pg_class where relname='range_range') order by relname;

select * from range_range;

-- orientation = column not support
drop table if exists t1 cascade;
create table t1(f1 int, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool) with (orientation = column);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
-- error
alter table t1 modify f1 int after f3;
-- error
alter table t1 modify f3 timestamp first;

-- pg_constraint test
set enable_default_ustore_table = on;
drop table if exists t_pri cascade;
drop table if exists t1 cascade;
create table t_pri(f1 int, f2 int, f3 int, primary key(f2, f3));
create table t1
(
        f1 int, f2 int, f3 varchar(20), f4 int, f5 int, f6 int, f7 int,
        foreign key(f1, f2) references t_pri(f2, f3),
        unique((lower(f3)), (abs(f4))),
        check(f5 = 10),
        unique(f4, f5) include(f6, f7)
);
\d+ t_pri
\d+ t1
select conname, contype, conkey, confkey, conbin, consrc, conincluding from pg_constraint 
        where conrelid = (select oid from pg_class where relname = 't_pri') order by conname;
select conname, contype, conkey, confkey, conbin, consrc, conincluding from pg_constraint 
        where conrelid = (select oid from pg_class where relname = 't1') order by conname;

alter table t_pri modify f3 int first;
alter table t1 modify f2 int first, modify f4 int after f1, modify f5 int after f7;
\d+ t_pri
\d+ t1
select conname, contype, conkey, confkey, conbin, consrc, conincluding from pg_constraint 
        where conrelid = (select oid from pg_class where relname = 't_pri') order by conname;
select conname, contype, conkey, confkey, conbin, consrc, conincluding from pg_constraint 
        where conrelid = (select oid from pg_class where relname = 't1') order by conname;

set enable_default_ustore_table = off;

-- pg_index test
drop table if exists t1 cascade;
create table t1
(
        f1 int, f2 int, f3 varchar(20), f4 int, f5 int, f6 int, f7 int,
        primary key(f1, f2),
        unique((lower(f3)), (abs(f4))),
        check(f5 = 10)
);
create unique index partial_t1_idx on t1(f5, abs(f6)) where f5 + f6 - abs(f7) > 0;

\d+ t1
select indkey, indexprs, indpred from pg_index where indrelid = (select oid from pg_class where relname = 't1');

alter table t1 modify f1 int after f2, modify f4 int after f6, modify f5 int first;
\d+ t1
select indkey, indexprs, indpred from pg_index where indrelid = (select oid from pg_class where relname = 't1');

-- pg_attribute test
drop table if exists t1 cascade;
create table t1(f1 int, f2 int, f3 int);
\d+ t1
select attname, attnum, atthasdef, attisdropped from pg_attribute where attrelid = (select oid from pg_class where relname = 't1') and attnum > 0 order by attnum;

alter table t1 modify f3 int first, modify f1 int after f2;
\d+ t1
select attname, attnum, atthasdef, attisdropped from pg_attribute where attrelid = (select oid from pg_class where relname = 't1') and attnum > 0 order by attnum;

-- pg_attrdef test
drop table if exists t1 cascade;
create table t1(f1 int primary key auto_increment, f2 int, f3 int default 3, f4 int generated always as (f2 + f3) stored);
\d+ t1
select adnum, adsrc, adgencol from pg_attrdef where adrelid = (select oid from pg_class where relname = 't1') order by adnum;

alter table t1 modify f3 int first, modify f1 int after f4, modify f4 int first;
\d+ t1
select adnum, adsrc, adgencol from pg_attrdef where adrelid = (select oid from pg_class where relname = 't1') order by adnum;

-- pg_depend test
drop table if exists t1 cascade;
create table t1(f1 int default 10, f2 int primary key, f3 int generated always as (f1 + f2) stored, f4 int, unique ((abs(f4))));
\d+ t1
select classid, objsubid, refclassid, refobjsubid, deptype from pg_depend
        where refobjid = (select oid from pg_class where relname='t1') or objid = (select oid from pg_class where relname='t1') order by 1, 2, 3, 4, 5;

alter table t1 modify f4 int first, modify f3 int after f1, modify f1 int after f2;
\d+ t1
select classid, objsubid, refclassid, refobjsubid, deptype from pg_depend
        where refobjid = (select oid from pg_class where relname='t1') or objid = (select oid from pg_class where relname='t1') order by 1, 2, 3, 4, 5;

-- pg_partition test
drop table if exists range_range cascade;
create table range_range(id int, gender varchar not null, birthday date not null)
partition by range (id) subpartition by range (birthday)
(
        partition p_1 values less than(100)
        (
                subpartition p_1_a values less than('2022-01-01'),
                subpartition p_1_b values less than(MAXVALUE)
        ),
        partition p_2 values less than(200)
        (
                subpartition p_2_a values less than('2022-01-01'),
                subpartition p_2_b values less than(MAXVALUE)
        ),
        partition p_3 values less than(MAXVALUE)
        (
                subpartition p_3_a values less than('2022-01-01'),
                subpartition p_3_b values less than(MAXVALUE)
        )
);
insert into range_range values(198,'boy','2010-02-15'),(33,'boy','2003-08-11'),(78,'girl','2014-06-24');
insert into range_range values(233,'girl','2010-01-01'),(360,'boy','2007-05-14'),(146,'girl','2005-03-08');
insert into range_range values(111,'girl','2013-11-19'),(15,'girl','2009-01-12'),(156,'boy','2011-05-21');

\d+ range_range
select relname, parttype, partkey from pg_partition where parentid=(select oid from pg_class where relname='range_range') order by relname;

alter table range_range modify gender varchar after birthday;
\d+ range_range
select relname, parttype, partkey from pg_partition where parentid=(select oid from pg_class where relname='range_range') order by relname;

alter table range_range modify birthday date first, modify id int after gender;
\d+ range_range
select relname, parttype, partkey from pg_partition where parentid=(select oid from pg_class where relname='range_range') order by relname;


-- pg_rewrite test
drop table if exists t1 cascade;
create table t1(f1 int, f2 int, f3 int, f4 int);
insert into t1 values(1, 2, 3, 4), (11, 22, 33, 44);
create view t1_view1 as select * from t1;
create view t1_view2 as select f1, f4 from t1;
\d+ t1_view1
\d+ t1_view2
\d+ t1
select pg_get_viewdef('t1_view1');
select pg_get_viewdef('t1_view2');
select * from t1_view1;
select * from t1_view2;
select * from t1;
alter table t1 modify f2 int first, modify f1 int after f4, add f5 int after f4;
\d+ t1_view1
\d+ t1_view2
\d+ t1
select pg_get_viewdef('t1_view1');
select pg_get_viewdef('t1_view2');
select * from t1_view1;
select * from t1_view2;
select * from t1;

-- pg_trigger test
drop table if exists t1 cascade;
create table t1(f1 boolean not null, f2 text, f3 int, f4 date);
alter table t1 add primary key(f1);
create or replace function dummy_update_func() returns trigger as $$
begin
        raise notice 'dummy_update_func(%) called: action = %, oid = %, new = %', TG_ARGV[0], TG_OP, OLD, NEW;
        return new;
end;
$$ language plpgsql;

drop trigger if exists f1_trig_update on t1;
drop trigger if exists f1_trig_insert on t1;

create trigger f1_trig_update after update of f1 on t1 for each row
        when (not old.f1 and new.f1) execute procedure dummy_update_func('update');
create trigger f1_trig_insert after insert on t1 for each row
        when (not new.f1) execute procedure dummy_update_func('insert');

\d+ t1
select tgname, tgattr, tgqual from pg_trigger where tgrelid = (select oid from pg_class where relname='t1') order by tgname;

alter table t1 modify f3 int first, modify f1 boolean after f4;
\d+ t1
select tgname, tgattr, tgqual from pg_trigger where tgrelid = (select oid from pg_class where relname='t1') order by tgname;

-- pg_rlspolicy test
drop table if exists t1 cascade;
drop role if exists test_rlspolicy3;
create role test_rlspolicy3 nologin password 'Gauss_234';
create table t1 (f1 int, f2 int, f3 text) partition by range (f1)
(
        partition t1_p0 values less than(10),
        partition t1_p1 values less than(50),
        partition t1_p2 values less than(100),
        partition t1_p3 values less than(MAXVALUE)
);

INSERT INTO t1 VALUES (generate_series(1, 150) % 24, generate_series(1, 150), 'huawei');
grant select on t1 to public;

create row level security policy t1_rls1 on t1 as permissive to public using (f2 <= 20);
create row level security policy t1_rls2 on t1 as restrictive to test_rlspolicy3 using (f1 < 30);

\d+ t1
select * from t1 limit 10;
select polname, polqual from pg_rlspolicy where polrelid = (select oid from pg_class where relname='t1');

alter table t1 modify f2 int first, modify f1 int after f3;

\d+ t1
select * from t1 limit 10;
select polname, polqual from pg_rlspolicy where polrelid = (select oid from pg_class where relname='t1');


-- expression test
drop table if exists t1 cascade;
create table t1(f1 int, f2 int, f3 int, f4 bool, f5 text, f6 text);
insert into t1 values(1, 2, 3, true, 'nanjin', 'huawei');
-- T_FuncExpr
create index t1_idx1 on t1(abs(f1), f2);
-- T_OpExpr
create index t1_idx2 on t1((f1 + f2), (f1 - f3));
-- T_BooleanTest
create index t1_idx3 on t1((f4 is true));
-- T_CaseExpr and T_CaseWhen
create index t1_idx4 on t1((case f1 when f2 then 'yes' when f3 then 'no' else 'unknow' end));
-- T_ArrayExpr
create index t1_idx5 on t1((array[f1, f2, f3]));
-- T_TypeCast
create index t1_idx6 on t1(((f1 + f2 + 1) :: text));
-- T_BoolExpr
create index t1_idx7 on t1((f1 and f2), (f2 or f3));
-- T_ArrayRef
create index t1_idx8 on t1((f1 = (array[f1, f2, 3])[1]));
-- T_ScalarArrayOpExpr
create index t1_idx9 on t1((f1 = ANY(ARRAY[f2, 1, f1 + 10])));
-- T_RowCompareExpr
create index t1_idx10 on t1((row(f1, f5) < row(f2, f6)));
-- T_MinMaxExpr
create index t1_idx11 on t1(greatest(f1, f2, f3), least(f1, f2, f3));
-- T_RowExpr
drop table if exists mytable cascade;
create table mytable(f1 int, f2 int, f3 text);
create function getf1(mytable) returns int as 'select $1.f1' language sql;
create index t1_idx12 on t1(getf1(row(f1, 2, 'a')));
-- T_CoalesceExpr
create index t1_idx13 on t1(nvl(f1, f2));
-- T_NullTest
create index t1_idx14 on t1((f1 is null));
-- T_ScalarArrayOpExpr
create index t1_idx16 on t1((f1 in (1,2,3)));
-- T_NullIfExpr
create index t1_idx17 on t1(nullif(f5,f6));
-- T_RelabelType
alter table t1 add f7 oid;
create index t1_idx18 on t1((f7::int4));
-- T_CoerceViaIO
alter table t1 add f8 json;
create index t1_idx19 on t1((f8::jsonb));
-- T_ArrayCoerceExpr
alter table t1 add f9 float[];
create index t1_idx20 on t1((f9::int[]));

\d+ t1
select * from t1;

alter table t1 modify f8 json first, modify f2 int after f6, modify f7 oid after f3;

\d+ t1
select * from t1;

drop table if exists t1;
create table t1(f1 int, f2 int);
insert into t1 values(1,2);
alter table t1 add f3 int default 3, add f4 int default 4 after f3, add f5 int default 5, add f6 int default 6 after f3;
select * from t1;

drop table if exists t1;
create table t1(f1 int, f2 int);
insert into t1 values(1,2);
alter table t1 add f3 int default 3, add f4 int default 4 after f1, add f5 int default 5, add f6 int default 6 after f5;
select * from t1;

drop table if exists t1;
create table t1(f1 int, f2 int);
insert into t1 values(1,2);
alter table t1 add f3 int, add f4 int after f3, add f5 int, add f6 int first;
select * from t1;

drop table if exists t1;
create table t1(f1 int, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);

alter table t1 drop f5,
        add f6 int default 6 , add f7 int first, add f8 int default 8 after f3,
        modify f3 timestamp first, modify f6 int after f2, modify f1 text, modify f2 text after f4;

drop table if exists t1 cascade;
create table t1(f1 int, f2 int, f3 int, primary key(f1, f3));
insert into t1 values(1, 2, 3), (11, 22, 33);
\d+ t1
select * from t1;
alter table t1 modify f3 int first, modify f1 int after f2;
\d+ t1
select * from  t1;

drop table if exists t1 cascade;
create table t1(f1 int, f2 int, f3 int);
insert into t1 values(1, 2, 3), (11, 12, 13), (21, 22, 23);
select * from t1;
alter table t1 add f4 int generated always as (f1 + 100) stored after f1, add f5 int generated always as (f2 * 10) stored first;
select * from t1;

drop table if exists t1 cascade;
create table t1(f1 int, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool, f6 int generated always as (f1 * 10) stored, primary key(f1, f2));
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
select * from t1;

alter table t1 drop f4,
        add f7 int default 7 , add f8 int first, add f9 int default 9 after f3,
        modify f3 timestamp first, modify f6 int after f2, modify f5 int, modify f2 text after f5,
        add f10 timestamp generated always as (f3) stored after f3,
        add f11 int generated always as (f1 * 100) stored first;

\d+ t1
select * from t1;

drop table if exists t1 cascade;
create table t1(f1 int, f2 varchar(20), f3 int, primary key(f1, f3));
insert into t1 values(1, 'a', 1), (2, 'b', 2);
\d+ t1
select * from t1;

alter table t1 modify f1 text after f3, add f10 int default 10 after f2;
\d+ t1
select * from t1;

-- unlogged table
drop table if exists t1 cascade;
create unlogged table t1(f1 int, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool, f6 int generated always as (f1 * 10) stored, primary key(f1, f2));
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
\d+ t1
select * from t1;

alter table t1 drop f4,
        add f7 int default 7 , add f8 int first, add f9 int default 9 after f3,
        modify f3 timestamp first, modify f6 int after f2, modify f5 int, modify f2 text after f5,
        add f10 timestamp generated always as (f3) stored after f3,
        add f11 int generated always as (f1 * 100) stored first;

\d+ t1
select * from t1;

-- temp table
drop table if exists t1 cascade;
create temp table t1(f1 int, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool, f6 int generated always as (f1 * 10) stored, primary key(f1, f2));
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
select * from t1;

alter table t1 drop f4,
        add f7 int default 7 , add f8 int first, add f9 int default 9 after f3,
        modify f3 timestamp first, modify f6 int after f2, modify f5 int, modify f2 text after f5,
        add f10 timestamp generated always as (f3) stored after f3,
        add f11 int generated always as (f1 * 100) stored first;
select * from t1;

drop table if exists t1 cascade;
create table t1(f1 int, f2 SET('beijing','shanghai','nanjing','wuhan'));
insert into t1 values(1, 'shanghai,beijing'), (2, 'wuhan');
\d+ t1
select * from t1;
alter table t1 add f3 int default 3 first, add f4 int default 4 after f3,
        add f5 SET('beijing','shanghai','nanjing','wuhan') default 'nanjing' first;
\d+ t1
select * from t1;

drop table if exists t1 cascade;
create table t1(f1 int, f2 SET('beijing','shanghai','nanjing','wuhan'));
-- error
alter table t1 modify f2 SET('beijing','shanghai','nanjing','wuhan') first;
alter table t1 modify f2 SET('beijing','shanghai','nanjing','wuhan') after f1;

drop table if exists t1 cascade;

--DTS
drop table if exists unit cascade;
CREATE TABLE unit
(
    f11  INTEGER CHECK (f11 >=2),
    f12  bool,
    f13  text,
	f14  varchar(20),
	primary key (f11,f12)
);

insert into unit values(2,3,4,5);
insert into unit values(3,4,5,6);
ALTER TABLE unit  ADD f1 int CHECK (f1 >=10) FIRST;
insert into unit values (10,6,1,1,1);
insert into unit values (11,7,1,1,1);
ALTER TABLE unit  ADD f2 int CHECK (f2 >=10) after f11;
select * from unit;
ALTER TABLE unit MODIFY f12 int FIRST;
select * from unit;
drop table if exists unit cascade;

-- dts for set
drop table if exists test_s1 cascade;
create table test_s1 (c1 int,c2 SET('aaa','bbb','ccc'), c3 bool, primary key(c1));
insert into test_s1 values(1,2,1), (2,'aaa',3), (3,4,4), (4,5,5), (5,1,6), (6,3,7);
alter table test_s1 add f1 text after c1;
alter table test_s1 modify c2 int first;
select * from test_s1;
drop table if exists test_s1 cascade;

drop table if exists test_s2 cascade;
create table test_s2 (c1 int,c2 SET('aaa','bbb','ccc'), c3 bool, primary key(c1));
insert into test_s2 values(1,2,1), (2,'aaa',3), (3,4,4), (4,5,5), (5,1,6), (6,3,7);
alter table test_s2 add f1 text check(f1 >= 2) after c1;
alter table test_s2 add f2 SET('w','ww','www','wwww') first;
alter table test_s2 modify f2 text after c1;
alter table test_s2 modify c2 int first;
select * from test_s2;
drop table if exists test_s2 cascade;

drop table if exists t1 cascade;
create table t1(f1 set('aaa','bbb','ccc'), f2 set('1','2','3'), f3 set('beijing','shannghai','nanjing'),
        f4 set('aaa','bbb','ccc') generated always as(f1+f2+f3) stored,
        f5 set('1','2','3') generated always as(f1+f2+f3) stored,
        f6 set('beijing','shannghai','nanjing') generated always as(f1+f2+f3) stored);
\d+ t1
alter table t1 modify f1 int after f6;
\d+ t1
alter table t1 drop f1;
\d+ t1
drop table if exists t1 cascade;

drop table t1 cascade;
create table t1(f1 int, f2 text, f3 int, f4 bool, f5 int generated always as (f1 + f3) stored); 
insert into t1 values(1, 'aaa', 3, true);
insert into t1 values(11, 'bbb', 33, false);
insert into t1 values(111, 'ccc', 333, true);
insert into t1 values(1111, 'ddd', 3333, true);
 
create view t1_view1 as select * from t1;
select * from t1_view1;
alter table t1 modify f1 int after f2, modify f3 int first;
drop view t1_view1;
create view t1_view1 as select * from t1;
alter table t1 modify f1 int after f2, modify f3 int first;
drop table t1 cascade;

create table t1(f1 int, f2 text, f3 int, f4 bigint, f5 int generated always as (f1 + f3) stored); 
insert into t1 values(1, 'aaa', 3, 1);
insert into t1 values(11, 'bbb', 33, 2);
insert into t1 values(111, 'ccc', 333, 3);
insert into t1 values(1111, 'ddd', 3333, 4);

create view t1_view1 as select * from t1;
select * from t1_view1;
alter table t1 add f6 int first, add f7 int after f4, modify f1 int after f2, modify f3 int first;
select * from t1_view1;
drop view t1_view1;

create view t1_view2 as select f1, f3, f5 from t1 where f2='aaa';
select * from t1_view2;
alter table t1 add f8 int first, add f9 int after f4, modify f1 int after f2, modify f3 int first, modify f2 varchar(20) first;
select * from t1_view2;
drop view t1_view2;

create view t1_view3 as select * from (select f1+f3, f5 from t1);
select * from t1_view3;
alter table t1 add f10 int first, add f11 int after f4, modify f1 int after f2, modify f3 int first, modify f2 varchar(20) first;
select * from t1_view3;
drop view t1_view3;

create view t1_view4 as select * from (select abs(f1+f3) as col1, abs(f5) as col2 from t1);
select * from t1_view4;
alter table t1 add f12 int first, add f13 int after f4, modify f1 int after f2, modify f3 int first, modify f2 varchar(20) first;
select * from t1_view4;
drop view t1_view4;

create view t1_view5 as select * from (select * from t1);
select * from t1_view5;
alter table t1 add f14 int first, add f15 int after f4, modify f1 int after f2, modify f3 int first;
select * from t1_view5;
drop view t1_view5;

create view t1_view6 as select f1, f3, f5 from t1 where f2='aaa';
select * from t1_view6;
alter table t1 modify f1 int after f2, modify f3 int first, modify f2 varchar(20) first;
select * from t1_view6;
drop view t1_view6;
drop table t1 cascade;

-- dts for add
drop table if exists test_d;
create table test_d (f2 int primary key, f3 bool, f5 text);
insert into test_d values(1,2,3), (2,3,4), (3,4,5);
select * from test_d;
alter table test_d add f1 int default 1,add f11 text check (f11 >=2) first;
select * from test_d;
 
drop table if exists test_d;
create table test_d (f2 int primary key, f3 bool, f5 text);
insert into test_d values(1,2,3), (2,3,4), (3,4,5);
select * from test_d;
alter table test_d add f1 int default 1;
alter table test_d add f11 text check (f11 >=2) first;
select * from test_d;
drop table if exists test_d;
 
drop table if exists t1 cascade;
create table t1(f1 int, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
select * from t1;
alter table t1 add f6 int generated always as (f1 * 10) stored, add f7 text default '777' first,
        add f8 int default 8, add f9 int primary key auto_increment after f6;
select * from t1;
 
drop table if exists t1 cascade;
create table t1(f1 int, f2 varchar(20), f3 timestamp, f4 bit(8), f5 bool);
insert into t1 values(1, 'a', '2022-11-08 19:56:10.158564', x'41', true), (2, 'b', '2022-11-09 19:56:10.158564', x'42', false);
select * from t1;
alter table t1 add f6 int generated always as (f1 * 10) stored, add f7 text default '7' first,
        add f8 int default 8, add f9 int primary key auto_increment after f1,
        add f10 bool default true, add f11 timestamp after f2,
        add f12 text after f3, add f14 int default '14', add f15 int default 15 check(f15 = 15) after f9;
select * from t1;
drop table if exists t1 cascade;

drop table if exists t1 cascade;
create table t1(f1 int comment 'f1 is int', f2 varchar(20), f3 timestamp comment 'f3 is timestamp', f4 bit(8), f5 bool comment 'f5 is boolean');
SELECT pg_get_tabledef('t1');
alter table t1 add f6 int generated always as (f1 * 10) stored, add f7 text default '7' first, add f8 int primary key auto_increment after f2;
SELECT pg_get_tabledef('t1');
alter table t1 modify f1 int after f3, modify f5 bool first, modify f3 timestamp after f4;
SELECT pg_get_tabledef('t1');
drop table if exists t1 cascade;

-- test about setting schema by RenameStmt
CREATE SCHEMA test1;
CREATE SCHEMA test2;
CREATE TABLE test1.test(a int primary key, b int not null);
CREATE INDEX ON test1.test using btree(b);
INSERT INTO test1.test VALUES (1, 1);
\d+ test1.test
SELECT n.nspname, c.relname from pg_class c, pg_namespace n where n.oid = c.relnamespace and c.relname in ('test', 'test_pkey', 'test_b_idx', 'tttt') order by c.relname;
SELECT * FROM test1.test;
-- check about type
SELECT n.nspname, t.typname FROM pg_type t, pg_namespace n where t.typnamespace = n.oid and t.typname in ('test','tttt');
ALTER TABLE test1.test RENAME TO test2.tttt;
\d+ test1.test
\d+ test2.tttt
SELECT n.nspname, c.relname from pg_class c, pg_namespace n where n.oid = c.relnamespace and c.relname in ('test', 'test_pkey', 'test_b_idx', 'tttt') order by c.relname;
INSERT INTO test2.tttt VALUES (2, 2);
SELECT * FROM test1.test;
SELECT * FROM test2.tttt;
-- check about type
SELECT n.nspname, t.typname FROM pg_type t, pg_namespace n where t.typnamespace = n.oid and t.typname in ('test','tttt');

-- just rename
ALTER TABLE test2.tttt RENAME TO test2.ttt;
SELECT * FROM test2.tttt;
SELECT * FROM test2.ttt;
-- check about type
SELECT n.nspname, t.typname FROM pg_type t, pg_namespace n where t.typnamespace = n.oid and t.typname = 'ttt';

-- just move to other schema
CREATE TABLE test1.t1 (a int);
\d+ test1.t1
ALTER TABLE test1.t1 RENAME TO test2.t1;
\d+ test1.t1
\d+ test2.t1
-- check about type
SELECT n.nspname, t.typname FROM pg_type t, pg_namespace n where t.typnamespace = n.oid and t.typname = 't1';

-- test about partition table
CREATE TABLE test1.sales_table1
(
    order_no INTEGER NOT NULL,
    goods_name CHAR(20) NOT NULL,
    sales_date DATE NOT NULL,
    sales_volume INTEGER,
    sales_store CHAR(20)
)
PARTITION BY RANGE(sales_date)
(
    PARTITION sales_table1_season1 VALUES LESS THAN('2021-04-01 00:00:00'),
    PARTITION sales_table1_season2 VALUES LESS THAN('2021-07-01 00:00:00'),
    PARTITION sales_table1_season3 VALUES LESS THAN('2021-10-01 00:00:00'),
    PARTITION sales_table1_season4 VALUES LESS THAN(MAXVALUE)
);

CREATE TABLE test1.sales_table2
(
    order_no INTEGER NOT NULL,
    goods_name CHAR(20) NOT NULL,
    sales_date DATE NOT NULL,
    sales_volume INTEGER,
    sales_store CHAR(20)
)
PARTITION BY RANGE(sales_date)
(
    PARTITION sales_table2_season1 VALUES LESS THAN('2021-04-01 00:00:00'),
    PARTITION sales_table2_season2 VALUES LESS THAN('2021-07-01 00:00:00'),
    PARTITION sales_table2_season3 VALUES LESS THAN('2021-10-01 00:00:00'),
    PARTITION sales_table2_season4 VALUES LESS THAN(MAXVALUE)
);

CREATE TABLE test1.sales_table3
(
    order_no INTEGER NOT NULL,
    goods_name CHAR(20) NOT NULL,
    sales_date DATE NOT NULL,
    sales_volume INTEGER,
    sales_store CHAR(20)
)
PARTITION BY RANGE(sales_date)
(
    PARTITION sales_table3_season1 VALUES LESS THAN('2021-04-01 00:00:00'),
    PARTITION sales_table3_season2 VALUES LESS THAN('2021-07-01 00:00:00'),
    PARTITION sales_table3_season3 VALUES LESS THAN('2021-10-01 00:00:00'),
    PARTITION sales_table3_season4 VALUES LESS THAN(MAXVALUE)
);
SELECT n.nspname, c.relname, p.relname AS partition_name
FROM pg_class c, pg_namespace n, pg_partition p 
WHERE n.oid = c.relnamespace and c.oid = p.parentid and c.relname like '%sales_table%' ORDER BY 1, 2, 3;
ALTER TABLE test1.sales_table1 RENAME TO test2.sales_table1;
ALTER TABLE test1.sales_table2 RENAME TO test2.sales_table3;
ALTER TABLE test1.sales_table3 RENAME TO test1.sales_table4;
SELECT n.nspname, c.relname, p.relname AS partition_name
FROM pg_class c, pg_namespace n, pg_partition p 
WHERE n.oid = c.relnamespace and c.oid = p.parentid and c.relname like '%sales_table%' ORDER BY 1, 2, 3;

-- rename table with view or matview
CREATE TABLE test1.test_table_view1 (a int);
CREATE TABLE test1.test_table_view2 (a int);
CREATE TABLE test1.test_table_matview1 (a int);
CREATE TABLE test1.test_table_matview2 (a int);

CREATE VIEW test1.test_view1 AS SELECT * FROM test1.test_table_view1;
CREATE VIEW test1.test_view2 AS SELECT * FROM test1.test_table_view2;
CREATE MATERIALIZED VIEW test1.test_matview1 AS SELECT * FROM test1.test_table_matview1;
CREATE MATERIALIZED VIEW test1.test_matview2 AS SELECT * FROM test1.test_table_matview2;

ALTER TABLE test1.test_table_view1 RENAME TO test2.test_table_view1; -- just move to other schema
ALTER TABLE test1.test_table_view2 RENAME TO test2.test_table_view3; -- rename and move to other schema
SELECT * FROM test1.test_view1;
SELECT pg_get_viewdef('test1.test_view1');
SELECT * FROM test1.test_view2;
SELECT pg_get_viewdef('test1.test_view2');

ALTER TABLE test1.test_table_matview1 RENAME TO test2.test_table_matview1; -- just move to other schema
ALTER TABLE test1.test_table_matview2 RENAME TO test2.test_table_matview3; -- rename and move to other schema
SELECT * FROM test1.test_matview1;
SELECT pg_get_viewdef('test1.test_matview1');
SELECT * FROM test1.test_matview2;
SELECT pg_get_viewdef('test1.test_matview2');

-- rename to a existed table
CREATE TABLE test1.name1 (a int);
CREATE TABLE test2.name1 (a int);
CREATE TABLE test2.name2 (a int);
ALTER TABLE test1.name1 RENAME TO test2.name1;
ALTER TABLE test1.name1 RENAME TO test2.name2;
ALTER TABLE test2.name1 RENAME TO test2.name2;

CREATE TABLE t_after_first ( c4 INT , c5 INT ) ;
INSERT INTO t_after_first VALUES ( 1 , 2 ) , ( 3 , 4 ) ;
ALTER TABLE t_after_first ADD COLUMN c11 VARCHAR ( 2 ) , ADD COLUMN c22 VARCHAR ( 2 ) AFTER c11 , ADD COLUMN c57 INT FIRST;
select * from t_after_first;

drop table if exists test_dts;
create table test_dts(
        f1 int primary key,
        f2 varchar(8),
        f3 timestamp
);
insert into test_dts(f1, f2, f3) values(1, 'data1', '2023-07-31 12:34:56'), (2, 'date2', '2023-07-31 13:45:30'),
(3, 'data3', '2023-07-31 14:56:15'), (4, 'data1', '2023-07-31 12:34:56'), (5, 'date2', '2023-07-31 13:45:30'),
(6, 'data3', '2023-07-31 14:56:15');
analyze test_dts;
analyze test_dts((f1, f3));
select staattnum, stavalues1 from pg_statistic where starelid = 'test_dts'::regclass order by staattnum;
select stakey from pg_statistic_ext where starelid = 'test_dts'::regclass;
alter table test_dts add column f4 int default 0 after f1;
select staattnum, stavalues1 from pg_statistic where starelid = 'test_dts'::regclass order by staattnum;
select stakey from pg_statistic_ext where starelid = 'test_dts'::regclass;
alter table test_dts add column f5 varchar(20) default 'f5' first;
select staattnum, stavalues1 from pg_statistic where starelid = 'test_dts'::regclass order by staattnum;
select stakey from pg_statistic_ext where starelid = 'test_dts'::regclass;
alter table test_dts modify f5 varchar(20) after f4;
select staattnum, stavalues1 from pg_statistic where starelid = 'test_dts'::regclass order by staattnum;
select stakey from pg_statistic_ext where starelid = 'test_dts'::regclass;
alter table test_dts modify f1 int first;
select staattnum, stavalues1 from pg_statistic where starelid = 'test_dts'::regclass order by staattnum;
select stakey from pg_statistic_ext where starelid = 'test_dts'::regclass;
drop table if exists test_dts;

drop table if exists t_after_first;
create table t_after_first(c4 int, c5 int);
insert into t_after_first values(1, 2), (3, 4);
alter table t_after_first add column c11 varchar(2), add column c22 varchar(2) after c11, add column c57 int first;
select * from t_after_first;
drop table if exists t_after_first;

-- test for modify type
drop table if exists fat0;
create table fat0(c41 int, c17 int);
insert into fat0 values(-104 not in (58, -123, -109), -49), (64, -28);
alter table fat0 modify column c17 int first, modify column c41 text, add column c4 int after c17, add column c61 int after c41, add constraint cc0 unique fai0(c17);
insert into fat0 values(-51, 2, -20, default), (-34, 81, -24, default);
select * from fat0;
drop table if exists fat0;

create table fat0(c41 int, c17 text);
insert into fat0 values(-104 not in(58, -123, -109), -49), (64, -28);
alter table fat0 modify c17 int first, modify column c41 int, add column c4 int after c17;
select * from fat0;
drop table if exists fat0;

create table fat0(c41 int, c17 int, c21 int);
insert into fat0 values(-104 not in(58, -123, -109), -49, -12),(64, -28, 20);
alter table fat0 modify column c17 text, modify column c41 text, add column c21 int after c41;
insert into fat0 values(-51, 2, -20), (-34, 81, -24);
select * from fat0;
drop table if exists fat0;

create table fat0(c41 int, c17 int, c21 int);
insert into fat0 values(-104 not in (58, -123, -109), -49, -12), (64, -28, 20);
alter table fat0 modify column c17 text first, modify column c41 text, add column c21 int after c41;
insert into fat0 values(-51, 2, -20), (-34, 81, -24);
select * from fat0;
drop table if exists fat0;

-- test for modify type not has column
create table fat0(c41 int, c17 int);
insert into fat0 values(-104 not in(58, -123, -109), -49), (64, -28);
alter table fat0 modify c17 int first, modify c41 text, add c4 int after c17, add c61 int after c41, add constraint cc0 unique fai0(c17);
select * from fat0;
drop table if exists fat0;

create table fat0(c41 int, c17 int);
insert into fat0 values(-104 not in (58, -123, -109), -49), (64, -28);
alter table fat0 modify column c17 int first, modify column c41 text;
drop table if exists fat0;

create table fat0(c41 int, c17 int);
insert into fat0 values(-104 not in (58, -123, -109), -49), (64, -28);
alter table fat0 modify c17 int first, modify c41 text;
drop table if exists fat0;

drop table if exists t0;
create table t0(c32 int, c6 text default (substring (-108, true) is null));
insert into t0 values (55, -34), (-123, -49);
alter table t0 change column c32 c59 int after c6, modify column c59 bigint;
select * from t0;

drop table if exists t0;
create table t0(c32 int, c6 text default (substring (-108, true) is null));
insert into t0 values(55, -34), (-123, -49);
alter table t0 change column c32 c59 int after c6, modify column c59 bigint;
select * from t0;
alter table t0 change column c6 c int first, add x int default 11 first, modify column c text;
select * from t0;

drop table if exists t0;
create table t0(c32 int, c6 text default (substring (-108, true) is null));
insert into t0 values(55, -34), (-123, -49);
alter table t0 change column c32 c59 int after c6, modify c59 bigint;
select * from t0;
alter table t0 add x int default 11 first, change column c6 c int first, modify c text;
select * from t0;
drop table if exists t0;

drop table if exists t0;
create table t0(c32 int, c6 text default (substring(-108, true) is null));
insert into t0 values(55, -34), (-123, -49);
alter table t0 change column c32 c59 int after c6, modify c59 bigint;
select * from t0;
alter table t0 change column c6 c int first, add x int default 11 first, modify c text;
select * from t0;
drop table if exists t0;

drop table if exists t0;
create table t0(c32 int, c6 text default (substring (-108, true) is null));
insert into t0 change column c32 c59 int after c6, modify c59 bigint;
select * from t0;
alter table t0 add x int default 11 first;
select * from t0;
alter table t0 change column c6 c int first, modify c text after x, change column c59 c32 bigint first, modify column c32 text after c;
select * from t0;
drop table if exists t0;

\c postgres
drop database test_first_after_B;