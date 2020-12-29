--
-- CREATE_INDEX
-- Create ancillary data structures (i.e. indices)
--

--
-- BTREE
--
CREATE INDEX onek_unique1 ON onek USING btree(unique1 int4_ops);

CREATE INDEX onek_unique2 ON onek USING btree(unique2 int4_ops);

CREATE INDEX onek_hundred ON onek USING btree(hundred int4_ops);

CREATE INDEX onek_stringu1 ON onek USING btree(stringu1 name_ops);

CREATE INDEX tenk1_unique1 ON tenk1 USING btree(unique1 int4_ops);

CREATE INDEX tenk1_unique2 ON tenk1 USING btree(unique2 int4_ops);

CREATE INDEX tenk1_hundred ON tenk1 USING btree(hundred int4_ops);

CREATE INDEX tenk1_thous_tenthous ON tenk1 (thousand, tenthous);

CREATE INDEX tenk2_unique1 ON tenk2 USING btree(unique1 int4_ops);

CREATE INDEX tenk2_unique2 ON tenk2 USING btree(unique2 int4_ops);

CREATE INDEX tenk2_hundred ON tenk2 USING btree(hundred int4_ops);

CREATE INDEX rix ON road USING btree (name text_ops);

CREATE INDEX iix ON ihighway USING btree (name text_ops);

CREATE INDEX six ON shighway USING btree (name text_ops);

-- test comments
COMMENT ON INDEX six_wrong IS 'bad index';
COMMENT ON INDEX six IS 'good index';
COMMENT ON INDEX six IS NULL;

--
-- BTREE ascending/descending cases
--
-- we load int4/text from pure descending data (each key is a new
-- low key) and name/f8 from pure ascending data (each key is a new
-- high key).  we had a bug where new low keys would sometimes be
-- "lost".
--
CREATE INDEX bt_i4_index ON bt_i4_heap USING btree (seqno int4_ops);

CREATE INDEX bt_name_index ON bt_name_heap USING btree (seqno name_ops);

CREATE INDEX bt_txt_index ON bt_txt_heap USING btree (seqno text_ops);

CREATE INDEX bt_f8_index ON bt_f8_heap USING btree (seqno float8_ops);

--
-- BTREE partial indices
--
CREATE INDEX onek2_u1_prtl ON onek2 USING btree(unique1 int4_ops)
	where unique1 < 20 or unique1 > 980;

CREATE INDEX onek2_u2_prtl ON onek2 USING btree(unique2 int4_ops)
	where stringu1 < 'B';

CREATE INDEX onek2_stu1_prtl ON onek2 USING btree(stringu1 name_ops)
	where onek2.stringu1 >= 'J' and onek2.stringu1 < 'K';

--
-- GIN over int[] and text[]
--
-- Note: GIN currently supports only bitmap scans, not plain indexscans
--

SET enable_seqscan = OFF;
SET enable_indexscan = OFF;
SET enable_bitmapscan = ON;

CREATE INDEX intarrayidx ON array_index_op_test USING gin (i);

explain (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT * FROM array_index_op_test WHERE i @> '{32}' ORDER BY seqno;

SELECT * FROM array_index_op_test WHERE i @> '{32}' ORDER BY seqno;
SELECT * FROM array_index_op_test WHERE i && '{32}' ORDER BY seqno;
SELECT * FROM array_index_op_test WHERE i @> '{17}' ORDER BY seqno;
SELECT * FROM array_index_op_test WHERE i && '{17}' ORDER BY seqno;
SELECT * FROM array_index_op_test WHERE i @> '{32,17}' ORDER BY seqno;
SELECT * FROM array_index_op_test WHERE i && '{32,17}' ORDER BY seqno;
SELECT * FROM array_index_op_test WHERE i <@ '{38,34,32,89}' ORDER BY seqno;
SELECT * FROM array_index_op_test WHERE i = '{47,77}' ORDER BY seqno;
SELECT * FROM array_index_op_test WHERE i = '{}' ORDER BY seqno;
SELECT * FROM array_index_op_test WHERE i @> '{}' ORDER BY seqno;
SELECT * FROM array_index_op_test WHERE i && '{}' ORDER BY seqno;
SELECT * FROM array_index_op_test WHERE i <@ '{}' ORDER BY seqno;
SELECT * FROM array_op_test WHERE i = '{NULL}' ORDER BY seqno;
SELECT * FROM array_op_test WHERE i @> '{NULL}' ORDER BY seqno;
SELECT * FROM array_op_test WHERE i && '{NULL}' ORDER BY seqno;
SELECT * FROM array_op_test WHERE i <@ '{NULL}' ORDER BY seqno;

CREATE INDEX textarrayidx ON array_index_op_test USING gin (t);

explain (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT * FROM array_index_op_test WHERE t @> '{AAAAAAAA72908}' ORDER BY seqno;

SELECT * FROM array_index_op_test WHERE t @> '{AAAAAAAA72908}' ORDER BY seqno;
SELECT * FROM array_index_op_test WHERE t && '{AAAAAAAA72908}' ORDER BY seqno;
SELECT * FROM array_index_op_test WHERE t @> '{AAAAAAAAAA646}' ORDER BY seqno;
SELECT * FROM array_index_op_test WHERE t && '{AAAAAAAAAA646}' ORDER BY seqno;
SELECT * FROM array_index_op_test WHERE t @> '{AAAAAAAA72908,AAAAAAAAAA646}' ORDER BY seqno;
SELECT * FROM array_index_op_test WHERE t && '{AAAAAAAA72908,AAAAAAAAAA646}' ORDER BY seqno;
SELECT * FROM array_index_op_test WHERE t <@ '{AAAAAAAA72908,AAAAAAAAAAAAAAAAAAA17075,AA88409,AAAAAAAAAAAAAAAAAA36842,AAAAAAA48038,AAAAAAAAAAAAAA10611}' ORDER BY seqno;
SELECT * FROM array_index_op_test WHERE t = '{AAAAAAAAAA646,A87088}' ORDER BY seqno;
SELECT * FROM array_index_op_test WHERE t = '{}' ORDER BY seqno;
SELECT * FROM array_index_op_test WHERE t @> '{}' ORDER BY seqno;
SELECT * FROM array_index_op_test WHERE t && '{}' ORDER BY seqno;
SELECT * FROM array_index_op_test WHERE t <@ '{}' ORDER BY seqno;

-- And try it with a multicolumn GIN index

DROP INDEX intarrayidx, textarrayidx;

CREATE INDEX botharrayidx ON array_index_op_test USING gin (i, t);

SELECT * FROM array_index_op_test WHERE i @> '{32}' ORDER BY seqno;
SELECT * FROM array_index_op_test WHERE i && '{32}' ORDER BY seqno;
SELECT * FROM array_index_op_test WHERE t @> '{AAAAAAA80240}' ORDER BY seqno;
SELECT * FROM array_index_op_test WHERE t && '{AAAAAAA80240}' ORDER BY seqno;
SELECT * FROM array_index_op_test WHERE i @> '{32}' AND t && '{AAAAAAA80240}' ORDER BY seqno;
SELECT * FROM array_index_op_test WHERE i && '{32}' AND t @> '{AAAAAAA80240}' ORDER BY seqno;
SELECT * FROM array_index_op_test WHERE t = '{}' ORDER BY seqno;
SELECT * FROM array_op_test WHERE i = '{NULL}' ORDER BY seqno;
SELECT * FROM array_op_test WHERE i <@ '{NULL}' ORDER BY seqno;

RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_bitmapscan;

--
-- Test GIN index's reloptions
--
CREATE INDEX gin_relopts_test ON array_index_op_test USING gin (i)
  WITH (FASTUPDATE=on, GIN_PENDING_LIST_LIMIT=128);
\d gin_relopts_test

--
-- HASH
--
CREATE INDEX hash_i4_index ON hash_i4_heap USING hash (random int4_ops);

CREATE INDEX hash_name_index ON hash_name_heap USING hash (random name_ops);

CREATE INDEX hash_txt_index ON hash_txt_heap USING hash (random text_ops);

CREATE INDEX hash_f8_index ON hash_f8_heap USING hash (random float8_ops);

-- CREATE INDEX hash_ovfl_index ON hash_ovfl_heap USING hash (x int4_ops);


--
-- Test functional index
--
-- PGXC: Here replication is used to ensure correct index creation
-- when a non-shippable expression is used.
-- PGXCTODO: this should be removed once global constraints are supported
CREATE TABLE func_index_heap (f1 text, f2 text) DISTRIBUTE BY REPLICATION;
CREATE UNIQUE INDEX func_index_index on func_index_heap (textcat(f1,f2));

INSERT INTO func_index_heap VALUES('ABC','DEF');
INSERT INTO func_index_heap VALUES('AB','CDEFG');
INSERT INTO func_index_heap VALUES('QWE','RTY');
-- this should fail because of unique index:
INSERT INTO func_index_heap VALUES('ABCD', 'EF');
-- but this shouldn't:
INSERT INTO func_index_heap VALUES('QWERTY');


--
-- Same test, expressional index
--
DROP TABLE func_index_heap;
-- PGXC: Here replication is used to ensure correct index creation
-- when a non-shippable expression is used.
-- PGXCTODO: this should be removed once global constraints are supported
CREATE TABLE func_index_heap (f1 text, f2 text) DISTRIBUTE BY REPLICATION;
CREATE UNIQUE INDEX func_index_index on func_index_heap ((f1 || f2) text_ops);

INSERT INTO func_index_heap VALUES('ABC','DEF');
INSERT INTO func_index_heap VALUES('AB','CDEFG');
INSERT INTO func_index_heap VALUES('QWE','RTY');
-- this should fail because of unique index:
INSERT INTO func_index_heap VALUES('ABCD', 'EF');
-- but this shouldn't:
INSERT INTO func_index_heap VALUES('QWERTY');

--
-- Also try building functional, expressional, and partial indexes on
-- tables that already contain data.
--
create unique index hash_f8_index_1 on hash_f8_heap(abs(random));
create unique index hash_f8_index_2 on hash_f8_heap((seqno + 1), random);
create unique index hash_f8_index_3 on hash_f8_heap(random) where seqno > 1000;

--
-- Try some concurrent index builds
--
-- Unfortunately this only tests about half the code paths because there are
-- no concurrent updates happening to the table at the same time.

CREATE TABLE concur_heap (f1 text, f2 text);
-- empty table
CREATE INDEX CONCURRENTLY concur_index1 ON concur_heap(f2,f1);
INSERT INTO concur_heap VALUES  ('a','b');
INSERT INTO concur_heap VALUES  ('b','b');
-- unique index
CREATE UNIQUE INDEX CONCURRENTLY concur_index2 ON concur_heap(f1);
-- check if constraint is set up properly to be enforced
INSERT INTO concur_heap VALUES ('b','x');
-- check if constraint is enforced properly at build time
CREATE UNIQUE INDEX CONCURRENTLY concur_index3 ON concur_heap(f2);
-- test that expression indexes and partial indexes work concurrently
CREATE INDEX CONCURRENTLY concur_index4 on concur_heap(f2) WHERE f1='a';
CREATE INDEX CONCURRENTLY concur_index5 on concur_heap(f2) WHERE f1='x';
-- here we also check that you can default the index name
CREATE INDEX CONCURRENTLY on concur_heap((f2||f1));

-- You can't do a concurrent index build in a transaction
START TRANSACTION;
CREATE INDEX CONCURRENTLY concur_index7 ON concur_heap(f1);
COMMIT;

-- But you can do a regular index build in a transaction
START TRANSACTION;
CREATE INDEX std_index on concur_heap(f2);
COMMIT;

-- check to make sure that the failed indexes were cleaned up properly and the
-- successful indexes are created properly. Notably that they do NOT have the
-- "invalid" flag set.

\d concur_heap

--
-- Try some concurrent index drops
--
DROP INDEX CONCURRENTLY "concur_index2";				-- works
DROP INDEX CONCURRENTLY IF EXISTS "concur_index2";		-- notice

-- failures
DROP INDEX CONCURRENTLY "concur_index2", "concur_index3";
START TRANSACTION;
DROP INDEX CONCURRENTLY "concur_index5";
ROLLBACK;

-- successes
DROP INDEX CONCURRENTLY IF EXISTS "concur_index3";
DROP INDEX CONCURRENTLY "concur_index4";
DROP INDEX CONCURRENTLY "concur_index5";
DROP INDEX CONCURRENTLY "concur_index1";
DROP INDEX CONCURRENTLY "concur_heap_expr_idx";

\d concur_heap

DROP TABLE concur_heap;

--
-- Test ADD CONSTRAINT USING INDEX
--

CREATE TABLE cwi_test( a int , b varchar(10), c char);

-- add some data so that all tests have something to work with.

INSERT INTO cwi_test VALUES(1, 2), (3, 4), (5, 6);

CREATE UNIQUE INDEX cwi_uniq_idx ON cwi_test(a , b);
ALTER TABLE cwi_test ADD primary key USING INDEX cwi_uniq_idx;

\d cwi_test

CREATE UNIQUE INDEX cwi_uniq2_idx ON cwi_test(b , a);
ALTER TABLE cwi_test DROP CONSTRAINT cwi_uniq_idx,
	ADD CONSTRAINT cwi_replaced_pkey PRIMARY KEY
		USING INDEX cwi_uniq2_idx;

\d cwi_test

DROP INDEX cwi_replaced_pkey;	-- Should fail; a constraint depends on it

DROP TABLE cwi_test;

--
-- Tests for IS NULL/IS NOT NULL with b-tree indexes
--

SELECT unique1, unique2 INTO onek_with_null FROM onek;
INSERT INTO onek_with_null (unique1,unique2) VALUES (NULL, -1), (NULL, NULL);
CREATE UNIQUE INDEX onek_nulltest ON onek_with_null (unique2,unique1);

SET enable_seqscan = OFF;
SET enable_indexscan = ON;
SET enable_bitmapscan = ON;

SELECT count(*) FROM onek_with_null WHERE unique1 IS NULL;
SELECT count(*) FROM onek_with_null WHERE unique1 IS NULL AND unique2 IS NULL;
SELECT count(*) FROM onek_with_null WHERE unique1 IS NOT NULL;
SELECT count(*) FROM onek_with_null WHERE unique1 IS NULL AND unique2 IS NOT NULL;
SELECT count(*) FROM onek_with_null WHERE unique1 IS NOT NULL AND unique1 > 500;
SELECT count(*) FROM onek_with_null WHERE unique1 IS NULL AND unique1 > 500;

DROP INDEX onek_nulltest;

CREATE UNIQUE INDEX onek_nulltest ON onek_with_null (unique2 desc,unique1);

SELECT count(*) FROM onek_with_null WHERE unique1 IS NULL;
SELECT count(*) FROM onek_with_null WHERE unique1 IS NULL AND unique2 IS NULL;
SELECT count(*) FROM onek_with_null WHERE unique1 IS NOT NULL;
SELECT count(*) FROM onek_with_null WHERE unique1 IS NULL AND unique2 IS NOT NULL;
SELECT count(*) FROM onek_with_null WHERE unique1 IS NOT NULL AND unique1 > 500;
SELECT count(*) FROM onek_with_null WHERE unique1 IS NULL AND unique1 > 500;

DROP INDEX onek_nulltest;

CREATE UNIQUE INDEX onek_nulltest ON onek_with_null (unique2 desc nulls last,unique1);

SELECT count(*) FROM onek_with_null WHERE unique1 IS NULL;
SELECT count(*) FROM onek_with_null WHERE unique1 IS NULL AND unique2 IS NULL;
SELECT count(*) FROM onek_with_null WHERE unique1 IS NOT NULL;
SELECT count(*) FROM onek_with_null WHERE unique1 IS NULL AND unique2 IS NOT NULL;
SELECT count(*) FROM onek_with_null WHERE unique1 IS NOT NULL AND unique1 > 500;
SELECT count(*) FROM onek_with_null WHERE unique1 IS NULL AND unique1 > 500;

DROP INDEX onek_nulltest;

CREATE UNIQUE INDEX onek_nulltest ON onek_with_null (unique2  nulls first,unique1);

SELECT count(*) FROM onek_with_null WHERE unique1 IS NULL;
SELECT count(*) FROM onek_with_null WHERE unique1 IS NULL AND unique2 IS NULL;
SELECT count(*) FROM onek_with_null WHERE unique1 IS NOT NULL;
SELECT count(*) FROM onek_with_null WHERE unique1 IS NULL AND unique2 IS NOT NULL;
SELECT count(*) FROM onek_with_null WHERE unique1 IS NOT NULL AND unique1 > 500;
SELECT count(*) FROM onek_with_null WHERE unique1 IS NULL AND unique1 > 500;

DROP INDEX onek_nulltest;

-- Check initial-positioning logic too

CREATE UNIQUE INDEX onek_nulltest ON onek_with_null (unique2);

SET enable_seqscan = OFF;
SET enable_indexscan = ON;
SET enable_bitmapscan = OFF;

SELECT unique1, unique2 FROM onek_with_null
  ORDER BY unique2 LIMIT 2;
SELECT unique1, unique2 FROM onek_with_null WHERE unique2 >= -1
  ORDER BY unique2 LIMIT 2;
SELECT unique1, unique2 FROM onek_with_null WHERE unique2 >= 0
  ORDER BY unique2 LIMIT 2;

SELECT unique1, unique2 FROM onek_with_null
  ORDER BY unique2 DESC LIMIT 2;
SELECT unique1, unique2 FROM onek_with_null WHERE unique2 >= -1
  ORDER BY unique2 DESC LIMIT 2;
SELECT unique1, unique2 FROM onek_with_null WHERE unique2 < 999
  ORDER BY unique2 DESC LIMIT 2;

RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_bitmapscan;

DROP TABLE onek_with_null;

--
-- Check bitmap index path planning
--

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT * FROM tenk1
  WHERE thousand = 42 AND (tenthous = 1 OR tenthous = 3 OR tenthous = 42);
SELECT * FROM tenk1
  WHERE thousand = 42 AND (tenthous = 1 OR tenthous = 3 OR tenthous = 42);

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM tenk1
  WHERE hundred = 42 AND (thousand = 42 OR thousand = 99);
SELECT count(*) FROM tenk1
  WHERE hundred = 42 AND (thousand = 42 OR thousand = 99);

--
-- Check behavior with duplicate index column contents
--

CREATE TABLE dupindexcols AS
  SELECT unique1 as id, stringu2::text as f1 FROM tenk1;
CREATE INDEX dupindexcols_i ON dupindexcols (f1, id, f1 text_pattern_ops);
ANALYZE dupindexcols;

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
  SELECT count(*) FROM dupindexcols
    WHERE f1 > 'WA' and id < 1000 and f1 ~<~ 'YX';
SELECT count(*) FROM dupindexcols
  WHERE f1 > 'WA' and id < 1000 and f1 ~<~ 'YX';

--
-- Check ordering of =ANY indexqual results (bug in 9.2.0)
--

vacuum analyze tenk1;		-- ensure we get consistent plans here

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT unique1 FROM tenk1
WHERE unique1 IN (1,42,7)
ORDER BY unique1;

SELECT unique1 FROM tenk1
WHERE unique1 IN (1,42,7)
ORDER BY unique1;

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT thousand, tenthous FROM tenk1
WHERE thousand < 2 AND tenthous IN (1001,3000)
ORDER BY thousand;

SELECT thousand, tenthous FROM tenk1
WHERE thousand < 2 AND tenthous IN (1001,3000)
ORDER BY thousand;

--test for relpages and reltuples of pg_class
create table test_table(a int, b int);
create table test_table_col(a int, b int) with (orientation=column);
insert into test_table select generate_series(1,100),generate_series(1,100);
insert into test_table_col select * from test_table;
analyze test_table;
analyze test_table_col;

select relpages>0 as relpages,reltuples>0 as reltuples from pg_class where relname='test_table';
create index test_table_idx1 on test_table(a,b);
select relpages>0 as relpages,reltuples>0 as reltuples from pg_class where relname='test_table';
analyze test_table;
select relpages>0 as relpages,reltuples>0 as reltuples from pg_class where relname='test_table_idx1';

select relpages>0 as relpages,reltuples>0 as reltuples from pg_class where relname='test_table_col';
create index test_table_col_idx1 on test_table_col(a,b);
select relpages>0 as relpages,reltuples>0 as reltuples from pg_class where relname='test_table_col';
analyze test_table_col;
select relpages>0 as relpages,reltuples>0 as reltuples from pg_class where relname='test_table_col_idx1';

--test create index schema_name.index_name on...
CREATE SCHEMA testindexschema;
CREATE SCHEMA testindexschema2;
set current_schema = testindexschema;
--Create index on the table
CREATE TABLE test1(a int, b int, c varchar(30));
CREATE INDEX ON test1(a);
\di+ test1_a_idx
CREATE INDEX indextest1 ON test1(a);
\di+ indextest1
CREATE INDEX indextest2 ON test1(a, b);
\di+ indextest2
CREATE INDEX indextest3 ON test1(a) WHERE a>1;
\di+ indextest3
CREATE INDEX indextest4 ON test1(lower(c));
\di+ indextest4
CREATE UNIQUE index indextest5 ON test1(a);
\di+ indextest5
CREATE INDEX testindexschema.indextest6 ON test1(a);
\di+ testindexschema.indextest6
CREATE INDEX testindexschema.indextest7 ON test1(a, b);
\di+ testindexschema.indextest7
CREATE INDEX testindexschema.indextest8 ON test1(a) WHERE a>1;
\di+ testindexschema.indextest8
CREATE INDEX testindexschema.indextest9 ON test1(lower(c));
\di+ testindexschema.indextest9
CREATE UNIQUE index testindexschema.indextest10 ON test1(a);
\di+ testindexschema.indextest10
CREATE UNIQUE index testindexschema2.indextest11 ON test1(a);
DROP TABLE test1;
--Create index on the partition table
CREATE TABLE partition_test1
(
	c1 int,
	c2 int,
	c3 varchar(30)
)
PARTITION BY RANGE (c1)
(
	PARTITION partition_p1 VALUES LESS THAN (50),
	PARTITION partition_p2 VALUES LESS THAN (100),
	PARTITION partition_p3 VALUES LESS THAN (150)
);
CREATE INDEX ON partition_test1(c1) local;
\di+ partition_test1_c1_idx
CREATE INDEX partition_idxtest1 ON partition_test1(c1) local;
\di+ partition_idxtest1
CREATE INDEX partition_idxtest2 ON partition_test1(c1, c2) local;
\di+ partition_idxtest2
CREATE INDEX partition_idxtest3 ON partition_test1(c1) WHERE c1>1 local;
\di+ partition_idxtest3
CREATE INDEX partition_idxtest4 ON partition_test1(lower(c3)) local;
\di+ partition_idxtest4
CREATE UNIQUE INDEX partition_idxtest5 ON partition_test1(c1) local;
\di+ partition_idxtest5
CREATE INDEX partition_idxtest6 ON partition_test1(c1) local
(
	PARTITION p1_index TABLESPACE PG_DEFAULT,
	PARTITION p2_index TABLESPACE PG_DEFAULT,
	PARTITION p3_index TABLESPACE PG_DEFAULT
);
\di+ partition_idxtest6
CREATE INDEX testindexschema.partition_idxtest7 ON partition_test1(c1) local;
\di+ testindexschema.partition_idxtest1
CREATE INDEX testindexschema.partition_idxtest8 ON partition_test1(c1, c2) local;
\di+ testindexschema.partition_idxtest2
CREATE INDEX testindexschema.partition_idxtest9 ON partition_test1(c1) WHERE c1>1 local;
\di+ testindexschema.partition_idxtest3
CREATE INDEX testindexschema.partition_idxtest10 ON partition_test1(lower(c3)) local;
\di+ testindexschema.partition_idxtest4
CREATE UNIQUE INDEX testindexschema.partition_idxtest11 ON partition_test1(c1) local;
\di+ testindexschema.partition_idxtest5
CREATE INDEX testindexschema.partition_idxtest12 ON partition_test1(c1) local
(
	PARTITION p1_index TABLESPACE PG_DEFAULT,
	PARTITION p2_index TABLESPACE PG_DEFAULT,
	PARTITION p3_index TABLESPACE PG_DEFAULT
);
\di+ testindexschema.partition_idxtest6
CREATE INDEX testindexschema2.partition_idxtest13 ON partition_test1(c1) local;
DROP TABLE partition_test1;
DROP SCHEMA testindexschema2 CASCADE;
--Add UNIQUE, PRIMARY KEY constraint, create implicit index
--UNIQUE
CREATE TABLE unique_table1
(
	a int UNIQUE, 
	b text
);
\di+ unique_table1_a_key
CREATE TABLE unique_table2
(
	a int CONSTRAINT unique_con UNIQUE, 
	b text
);
\di+ unique_con
CREATE TABLE partition_unique_table1
(
	c1 int UNIQUE,
	c2 int
)
PARTITION BY RANGE (c1)
(
	PARTITION test_unique_p1 VALUES LESS THAN (50),
	PARTITION test_unique_p2 VALUES LESS THAN (100),
	PARTITION test_unique_p3 VALUES LESS THAN (150)
);
\di+ partition_unique_table1_c1_key
CREATE TABLE partition_unique_table2
(
	c1 int CONSTRAINT partition_unique_con UNIQUE,
	c2 int
)
PARTITION BY RANGE (c1)
(
	PARTITION test_unique_p1 VALUES LESS THAN (50),
	PARTITION test_unique_p2 VALUES LESS THAN (100),
	PARTITION test_unique_p3 VALUES LESS THAN (150)
);
\di+ partition_unique_con
DROP TABLE unique_table1;
DROP TABLE unique_table2;
DROP TABLE partition_unique_table1;
DROP TABLE partition_unique_table2;
reset current_schema;
DROP SCHEMA testindexschema CASCADE;

drop index test_table_idx1;
drop index test_table_col_idx1;
drop table test_table;
drop table test_table_col;

SET enable_seqscan=off;
reset enable_indexscan;
reset enable_bitmapscan;

DROP TABLE IF EXISTS ddl_index_visibility;
CREATE TABLE ddl_index_visibility(id INT, first_name text, last_name text);
--insert data
INSERT INTO ddl_index_visibility SELECT id, md5(random()::text), md5(random()::text) FROM (SELECT * FROM generate_series(1,2000) AS id) AS x;
--this update will produce a HOT, then the next created index need to check xmin when using it(get_relation_info)
update ddl_index_visibility set first_name='+dw', last_name='dw_rt' where id = 698;
--create index, indcheckxmin will be true
create index ddl_index_visibility_idx on ddl_index_visibility USING btree(id);
--explain, should be BitmapHeapScan
explain(costs off) select * from ddl_index_visibility where id=698;
select * from ddl_index_visibility where id=698;

reset enable_seqscan;
DROP TABLE ddl_index_visibility;
