--
--FOR BLACKLIST FEATURE: REFERENCES/INHERITS/WITH OIDS/RULE/CREATE TYPE/DOMAIN is not supported.
--
--
-- ALTER_TABLE
-- add attribute
--

CREATE TABLE tmp (initial int4);

COMMENT ON TABLE tmp_wrong IS 'table comment';
COMMENT ON TABLE tmp IS 'table comment';
COMMENT ON TABLE tmp IS NULL;

ALTER TABLE tmp ADD COLUMN xmin integer; -- fails

ALTER TABLE tmp ADD COLUMN a int4 default 3;

ALTER TABLE tmp ADD COLUMN b name;

ALTER TABLE tmp ADD COLUMN c text;

ALTER TABLE tmp ADD COLUMN d float8;

ALTER TABLE tmp ADD COLUMN e float4;

ALTER TABLE tmp ADD COLUMN f int2;

ALTER TABLE tmp ADD COLUMN g polygon;

ALTER TABLE tmp ADD COLUMN h abstime;

ALTER TABLE tmp ADD COLUMN i char;

ALTER TABLE tmp ADD COLUMN j abstime[];

ALTER TABLE tmp ADD COLUMN k int4;

ALTER TABLE tmp ADD COLUMN l tid;

ALTER TABLE tmp ADD COLUMN m xid;

ALTER TABLE tmp ADD COLUMN n oidvector;

--ALTER TABLE tmp ADD COLUMN o lock;
ALTER TABLE tmp ADD COLUMN p smgr;

ALTER TABLE tmp ADD COLUMN q point;

ALTER TABLE tmp ADD COLUMN r lseg;

ALTER TABLE tmp ADD COLUMN s path;

ALTER TABLE tmp ADD COLUMN t box;

ALTER TABLE tmp ADD COLUMN u tinterval;

ALTER TABLE tmp ADD COLUMN v timestamp;

ALTER TABLE tmp ADD COLUMN w interval;

ALTER TABLE tmp ADD COLUMN x float8[];

ALTER TABLE tmp ADD COLUMN y float4[];

ALTER TABLE tmp ADD COLUMN z int2[];

INSERT INTO tmp (a, b, c, d, e, f, g, h, i, j, k, l, m, n, p, q, r, s, t, u,
	v, w, x, y, z)
   VALUES (4, 'name', 'text', 4.1, 4.1, 2, '(4.1,4.1,3.1,3.1)',
        'Mon May  1 00:30:30 1995', 'c', '{Mon May  1 00:30:30 1995, Monday Aug 24 14:43:07 1992, epoch}',
	314159, '(1,1)', '512',
	'1 2 3 4 5 6 7 8', 'magnetic disk', '(1.1,1.1)', '(4.1,4.1,3.1,3.1)',
	'(0,2,4.1,4.1,3.1,3.1)', '(4.1,4.1,3.1,3.1)', '["epoch" "infinity"]',
	'epoch', '01:00:10', '{1.0,2.0,3.0,4.0}', '{1.0,2.0,3.0,4.0}', '{1,2,3,4}');

SELECT * FROM tmp;

DROP TABLE tmp;

-- the wolf bug - schema mods caused inconsistent row descriptors
CREATE TABLE tmp (
	initial 	int4
);

ALTER TABLE tmp ADD COLUMN a int4;

ALTER TABLE tmp ADD COLUMN b name;

ALTER TABLE tmp ADD COLUMN c text;

ALTER TABLE tmp ADD COLUMN d float8;

ALTER TABLE tmp ADD COLUMN e float4;

ALTER TABLE tmp ADD COLUMN f int2;

ALTER TABLE tmp ADD COLUMN g polygon;

ALTER TABLE tmp ADD COLUMN h abstime;

ALTER TABLE tmp ADD COLUMN i char;

ALTER TABLE tmp ADD COLUMN j abstime[];

ALTER TABLE tmp ADD COLUMN k int4;

ALTER TABLE tmp ADD COLUMN l tid;

ALTER TABLE tmp ADD COLUMN m xid;

ALTER TABLE tmp ADD COLUMN n oidvector;

--ALTER TABLE tmp ADD COLUMN o lock;
ALTER TABLE tmp ADD COLUMN p smgr;

ALTER TABLE tmp ADD COLUMN q point;

ALTER TABLE tmp ADD COLUMN r lseg;

ALTER TABLE tmp ADD COLUMN s path;

ALTER TABLE tmp ADD COLUMN t box;

ALTER TABLE tmp ADD COLUMN u tinterval;

ALTER TABLE tmp ADD COLUMN v timestamp;

ALTER TABLE tmp ADD COLUMN w interval;

ALTER TABLE tmp ADD COLUMN x float8[];

ALTER TABLE tmp ADD COLUMN y float4[];

ALTER TABLE tmp ADD COLUMN z int2[];

INSERT INTO tmp (a, b, c, d, e, f, g, h, i, j, k, l, m, n, p, q, r, s, t, u,
	v, w, x, y, z)
   VALUES (4, 'name', 'text', 4.1, 4.1, 2, '(4.1,4.1,3.1,3.1)',
        'Mon May  1 00:30:30 1995', 'c', '{Mon May  1 00:30:30 1995, Monday Aug 24 14:43:07 1992, epoch}',
	314159, '(1,1)', '512',
	'1 2 3 4 5 6 7 8', 'magnetic disk', '(1.1,1.1)', '(4.1,4.1,3.1,3.1)',
	'(0,2,4.1,4.1,3.1,3.1)', '(4.1,4.1,3.1,3.1)', '["epoch" "infinity"]',
	'epoch', '01:00:10', '{1.0,2.0,3.0,4.0}', '{1.0,2.0,3.0,4.0}', '{1,2,3,4}');

SELECT * FROM tmp;

DROP TABLE tmp;


--
-- rename - check on both non-temp and temp tables
--
CREATE TABLE tmp (regtable int);
-- Enforce use of COMMIT instead of 2PC for temporary objects
\set VERBOSITY verbose
-- CREATE TEMP TABLE tmp (tmptable int);

ALTER TABLE tmp RENAME TO tmp_new;

-- SELECT * FROM tmp;
-- SELECT * FROM tmp_new;

-- ALTER TABLE tmp RENAME TO tmp_new2;

SELECT * FROM tmp;		-- should fail
SELECT *, FROM tmp;		-- should fail
SELECT * FROM tmp_new;
-- SELECT * FROM tmp_new2;

DROP TABLE tmp_new;
-- DROP TABLE tmp_new2;
CREATE TABLE tmp (ch1 character(1));
insert into tmp values ('asdv');
DROP TABLE tmp;
\set VERBOSITY default
-- ALTER TABLE ... RENAME on non-table relations
-- renaming indexes (FIXME: this should probably test the index's functionality)
ALTER INDEX IF EXISTS __onek_unique1 RENAME TO tmp_onek_unique1;
ALTER INDEX IF EXISTS __tmp_onek_unique1 RENAME TO onek_unique1;

ALTER INDEX onek_unique1 RENAME TO tmp_onek_unique1;
ALTER INDEX tmp_onek_unique1 RENAME TO onek_unique1;
-- renaming views
CREATE VIEW tmp_view (unique1) AS SELECT unique1 FROM tenk1;
ALTER TABLE tmp_view RENAME TO tmp_view_new;

-- hack to ensure we get an indexscan here
ANALYZE tenk1;
set enable_seqscan to off;
set enable_bitmapscan to off;
-- 5 values, sorted 
SELECT unique1 FROM tenk1 WHERE unique1 < 5 ORDER BY unique1;
reset enable_seqscan;
reset enable_bitmapscan;

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
CREATE TABLE constraint_rename_test2 (a int CONSTRAINT con1 CHECK (a > 0), d int) INHERITS (constraint_rename_test);
\d constraint_rename_test2
ALTER TABLE constraint_rename_test2 RENAME CONSTRAINT con1 TO con1foo; -- fail
ALTER TABLE ONLY constraint_rename_test RENAME CONSTRAINT con1 TO con1foo; -- fail
ALTER TABLE constraint_rename_test RENAME CONSTRAINT con1 TO con1foo; -- ok
\d constraint_rename_test
\d constraint_rename_test2
ALTER TABLE constraint_rename_test ADD CONSTRAINT con2 CHECK (b > 0) NO INHERIT;
ALTER TABLE ONLY constraint_rename_test RENAME CONSTRAINT con2 TO con2foo; -- ok
ALTER TABLE constraint_rename_test RENAME CONSTRAINT con2foo TO con2bar; -- ok
\d constraint_rename_test
\d constraint_rename_test2
ALTER TABLE constraint_rename_test ADD CONSTRAINT con3 PRIMARY KEY (a);
ALTER TABLE constraint_rename_test RENAME CONSTRAINT con3 TO con3foo; -- ok
\d constraint_rename_test
\d constraint_rename_test2
DROP TABLE constraint_rename_test2;
DROP TABLE constraint_rename_test;
ALTER TABLE IF EXISTS constraint_rename_test ADD CONSTRAINT con4 UNIQUE (a);

-- FOREIGN KEY CONSTRAINT adding TEST

CREATE TABLE tmp2 (a int primary key);

CREATE TABLE tmp3 (a int, b int);

CREATE TABLE tmp4 (a int, b int, unique(a,b));

CREATE TABLE tmp5 (a int, b int);

-- Insert rows into tmp2 (pktable)
INSERT INTO tmp2 values (1);
INSERT INTO tmp2 values (2);
INSERT INTO tmp2 values (3);
INSERT INTO tmp2 values (4);

-- Insert rows into tmp3
INSERT INTO tmp3 values (1,10);
INSERT INTO tmp3 values (1,20);
INSERT INTO tmp3 values (5,50);

-- Try (and fail) to add constraint due to invalid source columns
ALTER TABLE tmp3 add constraint tmpconstr foreign key(c) references tmp2 match full;

-- Try (and fail) to add constraint due to invalide destination columns explicitly given
ALTER TABLE tmp3 add constraint tmpconstr foreign key(a) references tmp2(b) match full;

-- Try (and fail) to add constraint due to invalid data
ALTER TABLE tmp3 add constraint tmpconstr foreign key (a) references tmp2 match full;

-- Delete failing row
DELETE FROM tmp3 where a=5;

-- Try (and succeed)
ALTER TABLE tmp3 add constraint tmpconstr foreign key (a) references tmp2 match full;
ALTER TABLE tmp3 drop constraint tmpconstr;

INSERT INTO tmp3 values (5,50);

-- Try NOT VALID and then VALIDATE CONSTRAINT, but fails. Delete failure then re-validate
ALTER TABLE tmp3 add constraint tmpconstr foreign key (a) references tmp2 match full NOT VALID;
ALTER TABLE tmp3 validate constraint tmpconstr;

-- Delete failing row
DELETE FROM tmp3 where a=5;

-- Try (and succeed) and repeat to show it works on already valid constraint
ALTER TABLE tmp3 validate constraint tmpconstr;
ALTER TABLE tmp3 validate constraint tmpconstr;

-- Try a non-verified CHECK constraint
ALTER TABLE tmp3 ADD CONSTRAINT b_greater_than_ten CHECK (b > 10); -- fail
ALTER TABLE tmp3 ADD CONSTRAINT b_greater_than_ten CHECK (b > 10) NOT VALID; -- succeeds
ALTER TABLE tmp3 VALIDATE CONSTRAINT b_greater_than_ten; -- fails
DELETE FROM tmp3 WHERE NOT b > 10;
ALTER TABLE tmp3 VALIDATE CONSTRAINT b_greater_than_ten; -- succeeds
ALTER TABLE tmp3 VALIDATE CONSTRAINT b_greater_than_ten; -- succeeds

-- Test inherited NOT VALID CHECK constraints
select * from tmp3;
CREATE TABLE tmp6 () INHERITS (tmp3);
CREATE TABLE tmp7 () INHERITS (tmp3);

INSERT INTO tmp6 VALUES (6, 30), (7, 16);
ALTER TABLE tmp3 ADD CONSTRAINT b_le_20 CHECK (b <= 20) NOT VALID;
ALTER TABLE tmp3 VALIDATE CONSTRAINT b_le_20;	-- fails
DELETE FROM tmp6 WHERE b > 20;
ALTER TABLE tmp3 VALIDATE CONSTRAINT b_le_20;	-- succeeds

-- An already validated constraint must not be revalidated
CREATE FUNCTION boo(int) RETURNS int IMMUTABLE STRICT LANGUAGE plpgsql AS $$ BEGIN RAISE NOTICE 'boo: %', $1; RETURN $1; END; $$;
INSERT INTO tmp7 VALUES (8, 18);
set client_min_messages=WARNING;
ALTER TABLE tmp7 ADD CONSTRAINT identity CHECK (b = boo(b));
reset client_min_messages;
ALTER TABLE tmp3 ADD CONSTRAINT IDENTITY check (b = boo(b)) NOT VALID;
set client_min_messages=WARNING;
ALTER TABLE tmp3 VALIDATE CONSTRAINT identity;
reset client_min_messages;

-- Try (and fail) to create constraint from tmp5(a) to tmp4(a) - unique constraint on
-- tmp4 is a,b

ALTER TABLE tmp5 add constraint tmpconstr foreign key(a) references tmp4(a) match full;

DROP TABLE tmp7;

DROP TABLE tmp6;

DROP TABLE tmp5;

DROP TABLE tmp4;

DROP TABLE tmp3;

DROP TABLE tmp2;

-- NOT VALID with plan invalidation -- ensure we don't use a constraint for
-- exclusion until validated
set constraint_exclusion TO 'partition';
create table nv_parent (d date);
create table nv_child_2010 () inherits (nv_parent);
create table nv_child_2011 () inherits (nv_parent);
alter table nv_child_2010 add check (d between '2010-01-01'::date and '2010-12-31'::date) not valid;
alter table nv_child_2011 add check (d between '2011-01-01'::date and '2011-12-31'::date) not valid;
explain (costs off) select * from nv_parent where d between '2011-08-01' and '2011-08-31';
create table nv_child_2009 (check (d between '2009-01-01'::date and '2009-12-31'::date)) inherits (nv_parent);
explain (costs off) select * from nv_parent where d between '2011-08-01'::date and '2011-08-31'::date;
explain (costs off) select * from nv_parent where d between '2009-08-01'::date and '2009-08-31'::date;
-- after validation, the constraint should be used
alter table nv_child_2011 VALIDATE CONSTRAINT nv_child_2011_d_check;
explain (costs off) select * from nv_parent where d between '2009-08-01'::date and '2009-08-31'::date;


-- Foreign key adding test with mixed types

-- Note: these tables are TEMP to avoid name conflicts when this test
-- is run in parallel with foreign_key.sql.

CREATE TABLE PKTABLE (ptest1 int PRIMARY KEY);
INSERT INTO PKTABLE VALUES(42);
CREATE TABLE FKTABLE (ftest1 inet);
-- This next should fail, because int=inet does not exist
ALTER TABLE FKTABLE ADD FOREIGN KEY(ftest1) references pktable;
-- This should also fail for the same reason, but here we
-- give the column name
ALTER TABLE FKTABLE ADD FOREIGN KEY(ftest1) references pktable(ptest1);
DROP TABLE FKTABLE;
-- This should succeed, even though they are different types,
-- because int=int8 exists and is a member of the integer opfamily
CREATE TABLE FKTABLE (ftest1 int8);
ALTER TABLE FKTABLE ADD FOREIGN KEY(ftest1) references pktable;
-- Check it actually works
INSERT INTO FKTABLE VALUES(42);		-- should succeed
INSERT INTO FKTABLE VALUES(43);		-- should fail
DROP TABLE FKTABLE;
-- This should fail, because we'd have to cast numeric to int which is
-- not an implicit coercion (or use numeric=numeric, but that's not part
-- of the integer opfamily)
CREATE TABLE FKTABLE (ftest1 numeric);
ALTER TABLE FKTABLE ADD FOREIGN KEY(ftest1) references pktable;
DROP TABLE FKTABLE;
DROP TABLE PKTABLE;
-- On the other hand, this should work because int implicitly promotes to
-- numeric, and we allow promotion on the FK side
CREATE TABLE PKTABLE (ptest1 numeric PRIMARY KEY);
INSERT INTO PKTABLE VALUES(42);
CREATE TABLE FKTABLE (ftest1 int);
ALTER TABLE FKTABLE ADD FOREIGN KEY(ftest1) references pktable;
-- Check it actually works
INSERT INTO FKTABLE VALUES(42);		-- should succeed
INSERT INTO FKTABLE VALUES(43);		-- should fail
DROP TABLE FKTABLE;
DROP TABLE PKTABLE;

CREATE TABLE PKTABLE (ptest1 int, ptest2 inet,
                           PRIMARY KEY(ptest1, ptest2));
-- This should fail, because we just chose really odd types
CREATE TABLE FKTABLE (ftest1 cidr, ftest2 timestamp);
ALTER TABLE FKTABLE ADD FOREIGN KEY(ftest1, ftest2) references pktable;
DROP TABLE FKTABLE;
-- Again, so should this...
CREATE TABLE FKTABLE (ftest1 cidr, ftest2 timestamp);
ALTER TABLE FKTABLE ADD FOREIGN KEY(ftest1, ftest2)
     references pktable(ptest1, ptest2);
DROP TABLE FKTABLE;
-- This fails because we mixed up the column ordering
CREATE TABLE FKTABLE (ftest1 int, ftest2 inet);
ALTER TABLE FKTABLE ADD FOREIGN KEY(ftest1, ftest2)
     references pktable(ptest2, ptest1);
-- As does this...
ALTER TABLE FKTABLE ADD FOREIGN KEY(ftest2, ftest1)
     references pktable(ptest1, ptest2);

-- temp tables should go away by themselves, need not drop them.
