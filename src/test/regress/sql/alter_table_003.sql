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
