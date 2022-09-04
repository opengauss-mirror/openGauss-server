--
-- REINDEX CONCURRENTLY
--
CREATE TABLE concur_reindex_tab (c1 int);
-- REINDEX
REINDEX TABLE concur_reindex_tab; -- notice
REINDEX TABLE CONCURRENTLY concur_reindex_tab; -- notice
ALTER TABLE concur_reindex_tab ADD COLUMN c2 text; -- add toast index
-- Normal index with integer column
CREATE UNIQUE INDEX concur_reindex_ind1 ON concur_reindex_tab(c1);
-- Normal index with text column
CREATE INDEX concur_reindex_ind2 ON concur_reindex_tab(c2);
-- UNION INDEX index with expression
CREATE UNIQUE INDEX concur_reindex_ind3 ON concur_reindex_tab(abs(c1));
-- Duplicates column names error
CREATE INDEX concur_reindex_ind4 ON concur_reindex_tab(c1, c1, c2);
-- Create table for check on foreign key dependence switch with indexes swapped
ALTER TABLE concur_reindex_tab ADD PRIMARY KEY USING INDEX concur_reindex_ind1;
CREATE TABLE concur_reindex_tab2 (c1 int REFERENCES concur_reindex_tab);
INSERT INTO concur_reindex_tab VALUES (1, 'a');
INSERT INTO concur_reindex_tab VALUES (2, 'a');
-- Check materialized views
CREATE MATERIALIZED VIEW concur_reindex_matview AS SELECT * FROM concur_reindex_tab;
-- Dependency lookup before and after the follow-up REINDEX commands.
-- These should remain consistent.
SELECT pg_describe_object(classid, objid, objsubid) as obj,
       pg_describe_object(refclassid,refobjid,refobjsubid) as objref,
       deptype
FROM pg_depend
WHERE classid = 'pg_class'::regclass AND
    objid in ('concur_reindex_tab'::regclass,
              'concur_reindex_ind1'::regclass,
              'concur_reindex_ind2'::regclass,
              'concur_reindex_ind3'::regclass,
              'concur_reindex_matview'::regclass)
    ORDER BY 1, 2;
REINDEX INDEX CONCURRENTLY concur_reindex_ind1;
REINDEX TABLE CONCURRENTLY concur_reindex_tab;
REINDEX TABLE CONCURRENTLY concur_reindex_matview;
SELECT pg_describe_object(classid, objid, objsubid) as obj,
       pg_describe_object(refclassid,refobjid,refobjsubid) as objref,
       deptype
FROM pg_depend
WHERE classid = 'pg_class'::regclass AND
    objid in ('concur_reindex_tab'::regclass,
              'concur_reindex_ind1'::regclass,
              'concur_reindex_ind2'::regclass,
              'concur_reindex_ind3'::regclass,
              'concur_reindex_matview'::regclass)
    ORDER BY 1, 2;
-- Check views 
CREATE VIEW concur_reindex_view AS SELECT * FROM concur_reindex_tab;
REINDEX TABLE CONCURRENTLY concur_reindex_view; -- Error

-- Check column store table
CREATE TABLE test_cstore (t1 int, t2 int) with (orientation = column);
CREATE INDEX ind_cstore ON test_cstore(t1);
REINDEX INDEX CONCURRENTLY ind_cstore; -- Error
REINDEX TABLE CONCURRENTLY test_cstore; -- Error

-- Check ustore table
CREATE TABLE test_ustore (t1 int, t2 int) with (storage_type = ustore);
CREATE INDEX ind_ustore ON test_ustore(t1);
REINDEX INDEX CONCURRENTLY ind_ustore; -- Error
REINDEX TABLE CONCURRENTLY test_ustore; -- Error

-- Check temp table
CREATE TEMP TABLE test_temp (t1 int, t2 int);
CREATE INDEX ind_temp ON test_temp(t1);
REINDEX INDEX CONCURRENTLY ind_temp; -- Error
REINDEX TABLE CONCURRENTLY test_temp; -- Error

-- Check global temp table
CREATE GLOBAL TEMP TABLE test_global (t1 int, t2 int);
CREATE INDEX ind_global ON test_global(t1);
REINDEX INDEX CONCURRENTLY ind_global; -- Error
REINDEX TABLE CONCURRENTLY test_global; -- Error

-- Check that comments are preserved
CREATE TABLE testcomment (i int);
CREATE INDEX testcomment_idx1 ON testcomment(i);
COMMENT ON INDEX testcomment_idx1 IS 'test comment';
SELECT obj_description('testcomment_idx1'::regclass, 'pg_class');
REINDEX TABLE testcomment;
SELECT obj_description('testcomment_idx1'::regclass, 'pg_class');
REINDEX TABLE CONCURRENTLY testcomment;
SELECT obj_description('testcomment_idx1'::regclass, 'pg_class');
DROP TABLE testcomment;
-- Check that indisclustered updates are preserved
CREATE TABLE concur_clustered(i int);
CREATE INDEX concur_clustered_i_idx ON concur_clustered(i);
ALTER TABLE concur_clustered CLUSTER ON concur_clustered_i_idx;
REINDEX TABLE CONCURRENTLY concur_clustered;
SELECT indexrelid::regclass, indisclustered FROM pg_index
  WHERE indrelid = 'concur_clustered'::regclass;
DROP TABLE concur_clustered;

-- Check error
-- Cannot run inside a transaction block
BEGIN;
REINDEX TABLE CONCURRENTLY concur_reindex_tab;
COMMIT;
REINDEX TABLE CONCURRENTLY pg_database; -- no shared relation
REINDEX TABLE CONCURRENTLY pg_class; -- no catalog relations
REINDEX INDEX CONCURRENTLY pg_class_oid_index; -- no catalog index
REINDEX SYSTEM CONCURRENTLY postgres; -- not allowed for SYSTEM

-- Check the relation status, there should not be invalid indexe
\d concur_reindex_tab
DROP TABLE test_temp, test_global;
DROP TABLE test_cstore, test_ustore;
DROP VIEW concur_reindex_view;
DROP MATERIALIZED VIEW concur_reindex_matview;
DROP TABLE concur_reindex_tab, concur_reindex_tab2;

-- Check handling of invalid indexes
CREATE TABLE concur_reindex_tab4 (c1 int);
INSERT INTO concur_reindex_tab4 VALUES (1), (1), (2);
-- This trick creates an invalid index.
CREATE UNIQUE INDEX CONCURRENTLY concur_reindex_ind5 ON concur_reindex_tab4 (c1);
-- Reindexing concurrently this index fails with the same failure.
-- The extra index created is itself invalid, and can be dropped.
REINDEX INDEX CONCURRENTLY concur_reindex_ind5;
\d concur_reindex_tab4
DROP INDEX concur_reindex_ind5_ccnew;
-- This makes the previous failure go away, so the index can become valid.
DELETE FROM concur_reindex_tab4 WHERE c1 = 1;
-- The invalid index is not processed when running REINDEX TABLE.
REINDEX TABLE CONCURRENTLY concur_reindex_tab4;
\d concur_reindex_tab4
-- But it is fixed with REINDEX INDEX.
REINDEX INDEX CONCURRENTLY concur_reindex_ind5;
\d concur_reindex_tab4
DROP TABLE concur_reindex_tab4;

-- Check handling of unusable indexes
CREATE TABLE concur_reindex_tab5 (c1 int);
CREATE INDEX concur_reindex_ind6 ON concur_reindex_tab5(c1);
-- Set concur_reindex_ind6 unusable
ALTER INDEX concur_reindex_ind6 UNUSABLE;
\d concur_reindex_tab5
-- The unusable index is not processed when running REINDEX TABLE.
REINDEX TABLE CONCURRENTLY concur_reindex_tab5;
\d concur_reindex_tab5
-- But it is fixes with REINDEX INDEX
REINDEX INDEX CONCURRENTLY concur_reindex_ind6;
\d concur_reindex_tab5
DROP TABLE concur_reindex_tab5;

-- Check handling of indexes with expressions and predicates.  The
-- definitions of the rebuilt indexes should match the original
-- definitions.
CREATE TABLE concur_exprs_tab (c1 int , c2 boolean);
INSERT INTO concur_exprs_tab (c1, c2) VALUES (1369652450, FALSE),
   (414515746, TRUE),
   (897778963, FALSE);
CREATE UNIQUE INDEX concur_exprs_index_expr
   ON concur_exprs_tab ((c1::text COLLATE "C"));
CREATE UNIQUE INDEX concur_exprs_index_pred ON concur_exprs_tab (c1)
   WHERE (c1::text > 500000000::text COLLATE "C");
CREATE UNIQUE INDEX concur_exprs_index_pred_2
   ON concur_exprs_tab ((1 / c1))
   WHERE ('-H') >= (c2::TEXT) COLLATE "C";
ANALYZE concur_exprs_tab;
SELECT starelid::regclass, count(*) FROM pg_statistic WHERE starelid IN (
  'concur_exprs_index_expr'::regclass,
  'concur_exprs_index_pred'::regclass,
  'concur_exprs_index_pred_2'::regclass)
  GROUP BY starelid ORDER BY starelid::regclass::text;
SELECT pg_get_indexdef('concur_exprs_index_expr'::regclass);
SELECT pg_get_indexdef('concur_exprs_index_pred'::regclass);
SELECT pg_get_indexdef('concur_exprs_index_pred_2'::regclass);
REINDEX TABLE CONCURRENTLY concur_exprs_tab;
SELECT pg_get_indexdef('concur_exprs_index_expr'::regclass);
SELECT pg_get_indexdef('concur_exprs_index_pred'::regclass);
SELECT pg_get_indexdef('concur_exprs_index_pred_2'::regclass);
-- ALTER TABLE recreates the indexes, which should keep their collations.
ALTER TABLE concur_exprs_tab ALTER c2 TYPE TEXT;
SELECT pg_get_indexdef('concur_exprs_index_expr'::regclass);
SELECT pg_get_indexdef('concur_exprs_index_pred'::regclass);
SELECT pg_get_indexdef('concur_exprs_index_pred_2'::regclass);
-- Statistics should remain intact
SELECT starelid::regclass, count(*) FROM pg_statistic WHERE starelid IN (
  'concur_exprs_index_expr'::regclass,
  'concur_exprs_index_pred'::regclass,
  'concur_exprs_index_pred_2'::regclass)
  GROUP BY starelid ORDER BY starelid::regclass::text;
DROP TABLE concur_exprs_tab;