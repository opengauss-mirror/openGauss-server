--
-- Functional and plan checks for heap multi-insert and partition OID caching.
--
-- Two optimizations are observable through the EXPLAIN node label, which is
-- produced by GetPlanNodePlainText() in src/gausskernel/optimizer/util/optcommon.cpp:
--   * batchInsert    -> node label contains "batch mode"
--   * partKeyIsConst -> node label contains "const partition key"
-- These are planner flags; execution also requires the heap multi-insert
-- switch and astore storage. Check both plans and actual partition contents.
--
CREATE SCHEMA iud_heap_multi_insert;
SET search_path = iud_heap_multi_insert;
SET enable_heap_multi_insert_for_insert_select = on;
SET enable_partition_oid_caching = on;
SET batch_insert_min_rows = 1000;
SET enable_default_ustore_table = off;
SET enable_opfusion = off;
SET enable_codegen = off;
SET query_dop = 1;

DROP TABLE IF EXISTS prep202407_hmi;
DROP TABLE IF EXISTS dest202407_hmi;

CREATE TABLE prep202407_hmi (i int);
CREATE TABLE dest202407_hmi (i int);

SHOW enable_heap_multi_insert_for_insert_select;
INSERT INTO prep202407_hmi SELECT generate_series(1,10000);
INSERT INTO dest202407_hmi SELECT * FROM prep202407_hmi;
SELECT count(*) FROM dest202407_hmi;

DROP TABLE prep202407_hmi;
DROP TABLE dest202407_hmi;

-- ---------------------------------------------------------------------
-- helper: run EXPLAIN (COSTS OFF) on a query and check the node label
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION check_plan(testname text, query text, keyword text, should_match boolean)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    plan_text text := '';
    r record;
BEGIN
    FOR r IN EXECUTE 'EXPLAIN (COSTS OFF) ' || query LOOP
        IF r."QUERY PLAN" IS NOT NULL THEN
            plan_text := plan_text || r."QUERY PLAN" || E'\n';
        END IF;
    END LOOP;
    IF (strpos(plan_text, keyword) > 0) IS DISTINCT FROM should_match THEN
        RAISE NOTICE 'FAIL: % (keyword=[%], should_match=[%]) plan=%', testname, keyword, should_match, plan_text;
    ELSE
        RAISE NOTICE 'PASS: %', testname;
    END IF;
END $$;

-- ---------------------------------------------------------------------
-- batch insert (batchInsert):
--   * positive table: regular permanent table, no trigger/autoinc,
--     plan_rows >= 1000
--   * negative tables: before-row trigger / serial nextval default,
--     or queries with source plan_rows < 1000 / RETURNING / volatile fn
-- ---------------------------------------------------------------------
CREATE TABLE batch_src (i int);
INSERT INTO batch_src SELECT generate_series(1, 2000);
ANALYZE batch_src;

CREATE TABLE batch_dest (i int);
ANALYZE batch_dest;

CREATE TABLE batch_dest_tri (i int);
ANALYZE batch_dest_tri;
CREATE FUNCTION hmi_trg_fn() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NEW; END';
CREATE TRIGGER hmi_br BEFORE INSERT ON batch_dest_tri FOR EACH ROW EXECUTE PROCEDURE hmi_trg_fn();

CREATE TABLE batch_dest_auto (id serial, i int);
ANALYZE batch_dest_auto;

CREATE TABLE batch_src_small (i int);
INSERT INTO batch_src_small VALUES (1);
ANALYZE batch_src_small;

CREATE TABLE batch_dest_small (i int);
ANALYZE batch_dest_small;

DO $$
BEGIN
    PERFORM check_plan('batch_insert_positive',
        'INSERT INTO batch_dest SELECT i FROM batch_src', 'batch mode', true);
    PERFORM check_plan('batch_insert_neg_small_rows',
        'INSERT INTO batch_dest_small SELECT i FROM batch_src_small', 'batch mode', false);
    PERFORM check_plan('batch_insert_neg_before_trigger',
        'INSERT INTO batch_dest_tri SELECT i FROM batch_src', 'batch mode', false);
    PERFORM check_plan('batch_insert_neg_serial_default',
        'INSERT INTO batch_dest_auto(i) SELECT i FROM batch_src', 'batch mode', false);
    PERFORM check_plan('batch_insert_neg_returning',
        'INSERT INTO batch_dest SELECT i FROM batch_src RETURNING i', 'batch mode', false);
    PERFORM check_plan('batch_insert_neg_volatile',
        'INSERT INTO batch_dest SELECT (random() * 1000)::int FROM batch_src', 'batch mode', false);
END $$;

-- ---------------------------------------------------------------------
-- partition key const (partKeyIsConst):
--   * positive: INSERT ... SELECT with a constant partition key
--   * negative: partition key is a variable / a non-const expression
-- ---------------------------------------------------------------------
CREATE TABLE part_t (pk int, v int) PARTITION BY RANGE(pk) (
    PARTITION p0 VALUES LESS THAN (10),
    PARTITION p1 VALUES LESS THAN (20),
    PARTITION p2 VALUES LESS THAN (MAXVALUE));
ANALYZE part_t;

DO $$
BEGIN
    PERFORM check_plan('part_batch_positive',
        'INSERT INTO part_t(pk, v) SELECT 1, i FROM batch_src', 'batch mode', true);
    PERFORM check_plan('part_const_positive',
        'INSERT INTO part_t(pk, v) SELECT 1, i FROM batch_src', 'const partition key', true);
    PERFORM check_plan('part_batch_variable',
        'INSERT INTO part_t(pk, v) SELECT i, i FROM batch_src', 'batch mode', true);
    PERFORM check_plan('part_const_neg_variable',
        'INSERT INTO part_t(pk, v) SELECT i, i FROM batch_src', 'const partition key', false);
    PERFORM check_plan('part_const_neg_expr',
        'INSERT INTO part_t(pk, v) SELECT i + 0, i FROM batch_src', 'const partition key', false);

    -- Fixed key: every source row must reach p0 with its original value.
    INSERT INTO part_t(pk, v) SELECT 1, i FROM batch_src;
    IF (SELECT count(*) FROM part_t PARTITION(p0)) <> 2000 OR EXISTS (
        (SELECT 1 AS pk, i AS v FROM batch_src EXCEPT ALL SELECT pk, v FROM part_t)
        UNION ALL
        (SELECT pk, v FROM part_t EXCEPT ALL SELECT 1 AS pk, i AS v FROM batch_src)
    ) THEN
        RAISE NOTICE 'FAIL: fixed partition key contents or routing';
    ELSE
        RAISE NOTICE 'PASS: fixed partition key contents and routing';
    END IF;

    -- Varying keys: compare the full multiset and each physical partition.
    TRUNCATE part_t;
    INSERT INTO part_t(pk, v) SELECT i, i FROM batch_src;
    IF (SELECT count(*) = 9 AND bool_and(pk < 10) FROM part_t PARTITION(p0)) IS DISTINCT FROM true OR
       (SELECT count(*) = 10 AND bool_and(pk >= 10 AND pk < 20) FROM part_t PARTITION(p1)) IS DISTINCT FROM true OR
       (SELECT count(*) = 1981 AND bool_and(pk >= 20) FROM part_t PARTITION(p2)) IS DISTINCT FROM true OR EXISTS (
        (SELECT i AS pk, i AS v FROM batch_src EXCEPT ALL SELECT pk, v FROM part_t)
        UNION ALL
        (SELECT pk, v FROM part_t EXCEPT ALL SELECT i AS pk, i AS v FROM batch_src)
    ) THEN
        RAISE NOTICE 'FAIL: varying partition key contents or routing';
    ELSE
        RAISE NOTICE 'PASS: varying partition key contents and routing';
    END IF;
END $$;

-- ---------------------------------------------------------------------
-- cleanup
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS batch_src;
DROP TABLE IF EXISTS batch_dest;
DROP TABLE IF EXISTS batch_dest_tri;
DROP TABLE IF EXISTS batch_dest_auto;
DROP TABLE IF EXISTS batch_src_small;
DROP TABLE IF EXISTS batch_dest_small;
DROP TABLE IF EXISTS part_t;

DROP FUNCTION hmi_trg_fn();
DROP FUNCTION check_plan(text, text, text, boolean);
DROP SCHEMA iud_heap_multi_insert;
RESET search_path;
RESET enable_heap_multi_insert_for_insert_select;
RESET enable_partition_oid_caching;
RESET batch_insert_min_rows;
RESET enable_default_ustore_table;
RESET enable_opfusion;
RESET enable_codegen;
RESET query_dop;

-- Partition insert optimization: functional regression checks.
-- Assertions use NOTICE and expected-output comparison.

-- CAST expression identity
-- Partition OID caching must compare complete expressions, including casts.
BEGIN;
CREATE SCHEMA partition_oid_cache_cast;
SET LOCAL search_path = partition_oid_cache_cast;
SET LOCAL enable_opfusion = off;
SET LOCAL enable_codegen = off;
SET LOCAL query_dop = 1;
SET LOCAL enable_heap_multi_insert_for_insert_select = on;
SET LOCAL enable_partition_oid_caching = on;
SET LOCAL batch_insert_min_rows = -1;

CREATE TABLE src(k double precision) WITH (storage_type=astore);
INSERT INTO src VALUES (1.00000001), (1.00000002);
CREATE TABLE dst(k double precision) WITH (storage_type=astore)
PARTITION BY RANGE(k) (
    PARTITION p1 VALUES LESS THAN (1.000000015),
    PARTITION p2 VALUES LESS THAN (MAXVALUE)
);
CREATE TABLE dst_numeric(k numeric) WITH (storage_type=astore)
PARTITION BY RANGE(k) (
    PARTITION p1 VALUES LESS THAN (1.000000015),
    PARTITION p2 VALUES LESS THAN (MAXVALUE)
);
CREATE TABLE dst_real(k real) WITH (storage_type=astore)
PARTITION BY RANGE(k) (
    PARTITION p1 VALUES LESS THAN (1.5),
    PARTITION p2 VALUES LESS THAN (MAXVALUE)
);

-- Check the plan label without depending on the source scan or join plan.
CREATE OR REPLACE FUNCTION check_cache(query_text text, expected boolean) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE r record; cached boolean := false;
BEGIN
    FOR r IN EXECUTE 'EXPLAIN (COSTS OFF) ' || query_text LOOP
        cached := cached OR strpos(r."QUERY PLAN", 'const partition key') > 0;
    END LOOP;
    IF cached IS DISTINCT FROM expected THEN
        RAISE NOTICE 'FAIL: partition cache: expected %, got %', expected, cached;
    END IF;
END $$;

-- 1. Row insertion: k::real is constant, but the inserted double k varies.
SELECT check_cache('INSERT INTO dst SELECT k FROM src WHERE k::real = 1::real', false);
INSERT INTO dst SELECT k FROM src WHERE k::real = 1::real;
SELECT (SELECT count(*) FROM dst PARTITION(p1)) AS p1_rows,
       (SELECT count(*) FROM dst PARTITION(p2)) AS p2_rows,
       (SELECT count(*) FROM dst WHERE k > 1.000000015::double precision) AS pruned_rows;

-- 2. The same wrong-routing regression must also be fixed in batch insertion.
TRUNCATE dst;
SET LOCAL batch_insert_min_rows = 0;
SELECT check_cache('INSERT INTO dst SELECT k FROM src WHERE k::real = 1::real', false);
INSERT INTO dst SELECT k FROM src WHERE k::real = 1::real;
SELECT (SELECT count(*) FROM dst PARTITION(p1)) AS p1_rows,
       (SELECT count(*) FROM dst PARTITION(p2)) AS p2_rows,
       (SELECT count(*) FROM dst WHERE k > 1.000000015::double precision) AS pruned_rows;

-- 3. Different casts of the same Var must not be treated as the same expression.
SELECT check_cache('INSERT INTO dst_numeric SELECT k::numeric FROM src WHERE k::real = 1::real', false);
INSERT INTO dst_numeric SELECT k::numeric FROM src WHERE k::real = 1::real;
SELECT (SELECT count(*) FROM dst_numeric PARTITION(p1)) AS p1_rows,
       (SELECT count(*) FROM dst_numeric PARTITION(p2)) AS p2_rows,
       (SELECT count(*) FROM dst_numeric WHERE k > 1.000000015::numeric) AS pruned_rows;

-- 4. The complete cast expression itself is constant and may still be cached.
SELECT check_cache('INSERT INTO dst_real SELECT k::real FROM src WHERE k::real = 1::real', true);
INSERT INTO dst_real SELECT k::real FROM src WHERE k::real = 1::real;
SELECT count(*) AS rows, min(k) AS min_key, max(k) AS max_key FROM dst_real PARTITION(p1);

-- 5. Preserve the ordinary Var = Const optimization.
TRUNCATE dst;
SELECT check_cache('INSERT INTO dst SELECT k FROM src WHERE k = 1.00000001::double precision', true);
INSERT INTO dst SELECT k FROM src WHERE k = 1.00000001::double precision;
SELECT (SELECT count(*) FROM dst PARTITION(p1)) AS p1_rows,
       (SELECT count(*) FROM dst PARTITION(p2)) AS p2_rows;

-- 6. Preserve constant propagation through an equality join.
TRUNCATE dst;
SELECT check_cache('INSERT INTO dst SELECT s.k FROM src s JOIN src j ON s.k = j.k WHERE j.k = 1.00000001::double precision', true);
INSERT INTO dst SELECT s.k FROM src s JOIN src j ON s.k = j.k WHERE j.k = 1.00000001::double precision;
SELECT (SELECT count(*) FROM dst PARTITION(p1)) AS p1_rows,
       (SELECT count(*) FROM dst PARTITION(p2)) AS p2_rows;
ROLLBACK;

-- Constant DEFAULT and target expression
-- Cache the expression actually assigned to the partition key.
BEGIN;
CREATE SCHEMA partition_oid_cache_defaults;
SET LOCAL search_path = partition_oid_cache_defaults;
SET LOCAL enable_opfusion = off;
SET LOCAL enable_codegen = off;
SET LOCAL query_dop = 1;
SET LOCAL enable_heap_multi_insert_for_insert_select = on;
SET LOCAL enable_partition_oid_caching = on;
SET LOCAL batch_insert_min_rows = 0;

CREATE TABLE src(id int, order_day date, amount int) WITH (storage_type=astore);
INSERT INTO src VALUES (1, DATE '2026-11-15', 10),
                      (2, DATE '2026-11-15', 20),
                      (3, DATE '2026-12-15', 30);
CREATE TABLE orders(id int, order_day date, amount int) WITH (storage_type=astore)
PARTITION BY RANGE(order_day) (
    PARTITION p_nov VALUES LESS THAN ('2026-12-01'),
    PARTITION p_dec VALUES LESS THAN (MAXVALUE)
);
CREATE OR REPLACE FUNCTION check_cache(q text, expected boolean) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE r record; cached boolean := false;
BEGIN
    FOR r IN EXECUTE 'EXPLAIN (COSTS OFF) ' || q LOOP
        cached := cached OR strpos(r."QUERY PLAN", 'const partition key') > 0;
    END LOOP;
    IF cached IS DISTINCT FROM expected THEN
        RAISE NOTICE 'FAIL: partition cache: expected %, got %', expected, cached;
    END IF;
END $$;

-- 1. Preserve the existing Var = Const path.
SELECT check_cache('INSERT INTO orders SELECT * FROM src WHERE order_day=DATE ''2026-11-15''', true);
INSERT INTO orders SELECT * FROM src WHERE order_day=DATE '2026-11-15';
SELECT (SELECT count(*) FROM orders PARTITION(p_nov)) AS nov,
       (SELECT count(*) FROM orders PARTITION(p_dec)) AS dec;

-- 2. A literal needs no constant EC.
TRUNCATE orders;
SELECT check_cache('INSERT INTO orders SELECT id,DATE ''2026-11-15'',amount FROM src', true);
INSERT INTO orders SELECT id,DATE '2026-11-15',amount FROM src;
SELECT (SELECT count(*) FROM orders PARTITION(p_nov)) AS nov,
       (SELECT count(*) FROM orders PARTITION(p_dec)) AS dec;

-- 3. An explicitly assigned key does not use its column default.
ALTER TABLE orders ALTER COLUMN order_day SET DEFAULT DATE '2026-12-15';
TRUNCATE orders;
SELECT check_cache('INSERT INTO orders SELECT * FROM src WHERE order_day=DATE ''2026-11-15''', true);
INSERT INTO orders SELECT * FROM src WHERE order_day=DATE '2026-11-15';
SELECT (SELECT count(*) FROM orders PARTITION(p_nov)) AS nov,
       (SELECT count(*) FROM orders PARTITION(p_dec)) AS dec;

-- 4. Omitted key: rewritten DEFAULT, with reordered target columns.
TRUNCATE orders;
SELECT check_cache('INSERT INTO orders(amount,id) SELECT amount,id FROM src', true);
INSERT INTO orders(amount,id) SELECT amount,id FROM src;
SELECT (SELECT count(*) FROM orders PARTITION(p_nov)) AS nov,
       (SELECT count(*) FROM orders PARTITION(p_dec)) AS dec,
       sum(id) AS ids, sum(amount) AS amounts FROM orders;

-- 5. A constant default does not make explicitly varying keys constant.
TRUNCATE orders;
SELECT check_cache('INSERT INTO orders SELECT * FROM src', false);
INSERT INTO orders SELECT * FROM src;
SELECT (SELECT count(*) FROM orders PARTITION(p_nov)) AS nov,
       (SELECT count(*) FROM orders PARTITION(p_dec)) AS dec;

-- 6. A used nextval default must remain row-dependent.
CREATE SEQUENCE key_seq START 1;
CREATE TABLE seq_dst(id int, k int DEFAULT nextval('key_seq')) WITH (storage_type=astore)
PARTITION BY RANGE(k) (
    PARTITION p_low VALUES LESS THAN (3),
    PARTITION p_high VALUES LESS THAN (MAXVALUE)
);
SELECT check_cache('INSERT INTO seq_dst(id) SELECT id FROM src', false);
INSERT INTO seq_dst(id) SELECT id FROM src;
SELECT (SELECT count(*) FROM seq_dst PARTITION(p_low)) AS low,
       (SELECT count(*) FROM seq_dst PARTITION(p_high)) AS high, sum(k) AS keys FROM seq_dst;

-- 7. An unused volatile default must not prevent caching or be evaluated.
TRUNCATE seq_dst;
SELECT check_cache('INSERT INTO seq_dst SELECT id,1 FROM src', true);
INSERT INTO seq_dst SELECT id,1 FROM src;
SELECT (SELECT count(*) FROM seq_dst PARTITION(p_low)) AS low,
       (SELECT count(*) FROM seq_dst PARTITION(p_high)) AS high;
SELECT last_value FROM key_seq;

-- 8. A BEFORE trigger may replace a literal key: keep the existing exclusion.
TRUNCATE orders;
CREATE OR REPLACE FUNCTION change_day() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.id=3 THEN NEW.order_day:=DATE '2026-12-15'; END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER change_day BEFORE INSERT ON orders FOR EACH ROW EXECUTE PROCEDURE change_day();
SELECT check_cache('INSERT INTO orders SELECT id,DATE ''2026-11-15'',amount FROM src', false);
INSERT INTO orders SELECT id,DATE '2026-11-15',amount FROM src;
SELECT (SELECT count(*) FROM orders PARTITION(p_nov)) AS nov,
       (SELECT count(*) FROM orders PARTITION(p_dec)) AS dec;

-- 9. A NULL Const can route to a LIST default partition.
CREATE TABLE null_dst(k int) WITH (storage_type=astore)
PARTITION BY LIST(k) (PARTITION p_null VALUES(DEFAULT), PARTITION p_one VALUES(1));
SELECT check_cache('INSERT INTO null_dst SELECT NULL::int FROM src', true);
INSERT INTO null_dst SELECT NULL::int FROM src;
SELECT (SELECT count(*) FROM null_dst PARTITION(p_null)) AS null_rows,
       (SELECT count(*) FROM null_dst PARTITION(p_one)) AS one_rows;
ROLLBACK;

-- Expression partition keys in batch insertion
-- Expression keys must be evaluated before tuples enter a partition bulk.
BEGIN;
CREATE SCHEMA partition_expr_batch;
SET LOCAL search_path = partition_expr_batch;
SET LOCAL enable_opfusion = off;
SET LOCAL enable_codegen = off;
SET LOCAL query_dop = 1;
SET LOCAL enable_partition_oid_caching = on;
SET LOCAL enable_heap_multi_insert_for_insert_select = on;
SET LOCAL batch_insert_min_rows = 0;

CREATE OR REPLACE FUNCTION check_batch(q text) RETURNS void LANGUAGE plpgsql AS $$
DECLARE r record; batched boolean := false; cached boolean := false;
BEGIN
    FOR r IN EXECUTE 'EXPLAIN (COSTS OFF) ' || q LOOP
        batched := batched OR strpos(r."QUERY PLAN", 'batch mode') > 0;
        cached := cached OR strpos(r."QUERY PLAN", 'const partition key') > 0;
    END LOOP;
    IF NOT batched OR cached THEN
        RAISE NOTICE 'FAIL: expected batch without OID caching, got batch=%, cache=%', batched, cached;
    END IF;
END $$;

-- 1. Two integer keys route differently after abs(), in row and batch insertion.
CREATE TABLE src(id int) WITH (storage_type=astore);
INSERT INTO src VALUES (-1), (-11);
CREATE TABLE orders(id int) WITH (storage_type=astore)
PARTITION BY RANGE(abs(id)) (
    PARTITION p_small VALUES LESS THAN (10),
    PARTITION p_large VALUES LESS THAN (MAXVALUE)
);
SET LOCAL enable_heap_multi_insert_for_insert_select = off;
SET LOCAL batch_insert_min_rows = -1;
INSERT INTO orders SELECT id FROM src;
SELECT 'row' AS mode,
       (SELECT count(*) FROM orders PARTITION(p_small)) AS small_rows,
       (SELECT count(*) FROM orders PARTITION(p_large)) AS large_rows, sum(id) AS ids FROM orders;

TRUNCATE orders;
SET LOCAL enable_heap_multi_insert_for_insert_select = on;
SET LOCAL batch_insert_min_rows = 0;
SELECT check_batch('INSERT INTO orders SELECT id FROM src');
INSERT INTO orders SELECT id FROM src;
SELECT 'batch' AS mode,
       (SELECT count(*) FROM orders PARTITION(p_small)) AS small_rows,
       (SELECT count(*) FROM orders PARTITION(p_large)) AS large_rows, sum(id) AS ids FROM orders;
DO $$ BEGIN
    IF (SELECT count(*) FROM orders PARTITION(p_small) WHERE id=-1) <> 1 OR
       (SELECT count(*) FROM orders PARTITION(p_large) WHERE id=-11) <> 1 THEN
        RAISE NOTICE 'FAIL: abs(id) batch routing wrote a tuple to the wrong partition';
    END IF;
END $$;

-- 2. Pass-by-reference expression results and a NULL key.
CREATE TABLE numeric_src(k numeric) WITH (storage_type=astore);
INSERT INTO numeric_src VALUES (0.5), (11.5), (NULL);
CREATE TABLE numeric_dst(k numeric) WITH (storage_type=astore)
PARTITION BY LIST(k + 1) (
    PARTITION p_small VALUES (1.5),
    PARTITION p_other VALUES (DEFAULT)
);
SELECT check_batch('INSERT INTO numeric_dst SELECT k FROM numeric_src');
INSERT INTO numeric_dst SELECT k FROM numeric_src;
SELECT (SELECT count(*) FROM numeric_dst PARTITION(p_small)) AS small_rows,
       (SELECT count(*) FROM numeric_dst PARTITION(p_other)) AS other_rows,
       (SELECT count(*) FROM numeric_dst WHERE k IS NULL) AS null_rows;
DO $$ BEGIN
    IF (SELECT count(*) FROM numeric_dst PARTITION(p_small) WHERE k=0.5) <> 1 OR
       (SELECT count(*) FROM numeric_dst PARTITION(p_other) WHERE k=11.5) <> 1 OR
       (SELECT count(*) FROM numeric_dst PARTITION(p_other) WHERE k IS NULL) <> 1 THEN
        RAISE NOTICE 'FAIL: numeric or NULL expression key was routed incorrectly';
    END IF;
END $$;

-- 3. Alternate partitions and exceed the byte threshold before the final flush.
CREATE TABLE many_src(id int) WITH (storage_type=astore);
INSERT INTO many_src SELECT CASE WHEN i%2=0 THEN -1 ELSE -11 END FROM generate_series(1,4000) AS g(i);
TRUNCATE orders;
SELECT check_batch('INSERT INTO orders SELECT id FROM many_src');
INSERT INTO orders SELECT id FROM many_src;
SELECT (SELECT count(*) FROM orders PARTITION(p_small)) AS small_rows,
       (SELECT count(*) FROM orders PARTITION(p_large)) AS large_rows, sum(id) AS ids FROM orders;
DO $$ BEGIN
    IF (SELECT count(*) FROM orders PARTITION(p_small) WHERE id=-1) <> 2000 OR
       (SELECT count(*) FROM orders PARTITION(p_large) WHERE id=-11) <> 2000 THEN
        RAISE NOTICE 'FAIL: expression routing failed across batch flushes';
    END IF;
END $$;
ROLLBACK;

-- Explicit partition and subpartition validation
BEGIN;
CREATE SCHEMA specified_partition_fix;
SET LOCAL search_path = specified_partition_fix;
SET LOCAL enable_opfusion = off;
SET LOCAL enable_codegen = off;
SET LOCAL query_dop = 1;
SET LOCAL enable_heap_multi_insert_for_insert_select = on;
SET LOCAL enable_partition_oid_caching = on;
SET LOCAL batch_insert_min_rows = 0;

CREATE TABLE src(id int, order_day date, amount int, region_id int) WITH (storage_type=astore);
INSERT INTO src VALUES (1, DATE '2026-11-15', 10, 1), (2, DATE '2026-11-15', 20, 1),
                       (3, DATE '2026-11-15', 30, 11), (4, DATE '2026-12-15', 40, 1);
CREATE TABLE orders(id int, order_day date, amount int) WITH (storage_type=astore)
PARTITION BY RANGE(order_day) (
    PARTITION p_nov VALUES LESS THAN ('2026-12-01'),
    PARTITION p_dec VALUES LESS THAN ('2027-01-01')
);
CREATE TABLE orders_sub(id int, order_day date, amount int, region_id int) WITH (storage_type=astore)
PARTITION BY RANGE(order_day) SUBPARTITION BY RANGE(region_id) (
    PARTITION p_nov VALUES LESS THAN ('2026-12-01') (
        SUBPARTITION sp_nov_low VALUES LESS THAN (10),
        SUBPARTITION sp_nov_high VALUES LESS THAN (MAXVALUE)
    ),
    PARTITION p_dec VALUES LESS THAN ('2027-01-01') (
        SUBPARTITION sp_dec_low VALUES LESS THAN (10),
        SUBPARTITION sp_dec_high VALUES LESS THAN (MAXVALUE)
    )
);
CREATE OR REPLACE FUNCTION check_plan(q text, expected_cache boolean) RETURNS void LANGUAGE plpgsql AS $$
DECLARE r record; batched boolean := false; cached boolean := false;
BEGIN
    FOR r IN EXECUTE 'EXPLAIN (COSTS OFF) ' || q LOOP
        batched := batched OR strpos(r."QUERY PLAN", 'batch mode') > 0;
        cached := cached OR strpos(r."QUERY PLAN", 'const partition key') > 0;
    END LOOP;
    IF NOT batched OR cached IS DISTINCT FROM expected_cache THEN
        RAISE NOTICE 'FAIL: expected batch with cache=%, got batch=% cache=%', expected_cache, batched, cached;
    END IF;
END $$;
CREATE OR REPLACE FUNCTION expect_partition_error(q text, expected_cache boolean) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    PERFORM check_plan(q, expected_cache);
    BEGIN
        EXECUTE q;
    EXCEPTION WHEN SQLSTATE 'P0002' THEN
        RETURN;
    END;
    RAISE NOTICE 'FAIL: expected a partition mismatch error: %', q;
END $$;

-- 1. Reject destinations outside each kind of explicit partition scope.
SELECT expect_partition_error($q$
    INSERT INTO orders PARTITION(p_nov)
    SELECT id,order_day,amount FROM src WHERE order_day=DATE '2026-12-15'
$q$, true);
SELECT expect_partition_error($q$
    INSERT INTO orders_sub PARTITION(p_nov)
    SELECT * FROM src WHERE order_day=DATE '2026-12-15' AND region_id=1
$q$, true);
SELECT expect_partition_error($q$
    INSERT INTO orders_sub SUBPARTITION(sp_nov_low)
    SELECT * FROM src WHERE order_day=DATE '2026-11-15' AND region_id=11
$q$, true);
DO $$ BEGIN
    IF EXISTS(SELECT 1 FROM orders) OR EXISTS(SELECT 1 FROM orders_sub) THEN
        RAISE NOTICE 'FAIL: rejected statements left rows behind';
    END IF;
END $$;
SELECT 'PASS: all three mismatches rejected';

-- 2. A valid fixed destination remains eligible for batch insertion and OID caching.
SELECT check_plan($q$
    INSERT INTO orders PARTITION(p_nov)
    SELECT id,order_day,amount FROM src WHERE order_day=DATE '2026-11-15'
$q$, true);
INSERT INTO orders PARTITION(p_nov)
SELECT id,order_day,amount FROM src WHERE order_day=DATE '2026-11-15';
DO $$ BEGIN
    IF (SELECT count(*) FROM orders PARTITION(p_nov)) <> 3 OR
       (SELECT count(*) FROM orders PARTITION(p_dec)) <> 0 THEN
        RAISE NOTICE 'FAIL: valid cached partition insert failed';
    END IF;
END $$;
SELECT 'PASS: valid cached partition';

-- 3. A specified parent permits multiple child partitions.
SELECT check_plan($q$
    INSERT INTO orders_sub PARTITION(p_nov)
    SELECT * FROM src WHERE order_day=DATE '2026-11-15'
$q$, false);
INSERT INTO orders_sub PARTITION(p_nov)
SELECT * FROM src WHERE order_day=DATE '2026-11-15';
DO $$ BEGIN
    IF (SELECT count(*) FROM orders_sub SUBPARTITION(sp_nov_low)) <> 2 OR
       (SELECT count(*) FROM orders_sub SUBPARTITION(sp_nov_high)) <> 1 OR
       (SELECT count(*) FROM orders_sub) <> 3 THEN
        RAISE NOTICE 'FAIL: valid parent scope rejected or misrouted a child';
    END IF;
END $$;
SELECT 'PASS: parent scope allows both children';

-- 4. Compare a leaf OID to the requested subpartition and its actual parent.
TRUNCATE orders_sub;
SELECT check_plan($q$
    INSERT INTO orders_sub SUBPARTITION(sp_nov_low)
    SELECT * FROM src WHERE order_day=DATE '2026-11-15' AND region_id=1
$q$, true);
INSERT INTO orders_sub SUBPARTITION(sp_nov_low)
SELECT * FROM src WHERE order_day=DATE '2026-11-15' AND region_id=1;
DO $$ BEGIN
    IF (SELECT count(*) FROM orders_sub SUBPARTITION(sp_nov_low)) <> 2 OR
       (SELECT count(*) FROM orders_sub) <> 2 THEN
        RAISE NOTICE 'FAIL: valid cached subpartition insert failed';
    END IF;
END $$;
SELECT 'PASS: valid cached subpartition';

-- 5. The bad final row must roll back even batches already flushed by earlier rows.
TRUNCATE orders;
CREATE TABLE many_src(id int, order_day date, amount int) WITH (storage_type=astore);
INSERT INTO many_src
SELECT i, CASE WHEN i<=2000 THEN DATE '2026-11-15' ELSE DATE '2026-12-15' END, i
FROM generate_series(1,2001) AS g(i);
SELECT expect_partition_error($q$
    INSERT INTO orders PARTITION(p_nov) SELECT * FROM many_src ORDER BY id
$q$, false);
DO $$ BEGIN
    IF EXISTS(SELECT 1 FROM orders) THEN
        RAISE NOTICE 'FAIL: partition error did not roll back earlier batch flushes';
    END IF;
END $$;
SELECT 'PASS: later mismatch rolls back earlier batches';

ROLLBACK;

-- Planner and executor batch eligibility
-- Check planner eligibility without adding per-row executor conditions.
CREATE SCHEMA planner_batch_gate;
SET search_path = planner_batch_gate;
SET enable_opfusion = off;
SET enable_codegen = off;
SET query_dop = 1;
SET batch_insert_min_rows = 0;
SET enable_partition_oid_caching = on;

CREATE TABLE src(id int, order_day date, amount int) WITH(storage_type=astore);
INSERT INTO src
SELECT i, CASE WHEN i <= 4 THEN DATE '2026-11-15' ELSE DATE '2026-12-15' END, i * 10
FROM generate_series(1,8) AS g(i);
CREATE TABLE orders(id int, order_day date, amount int) WITH(storage_type=astore)
PARTITION BY RANGE(order_day) (
    PARTITION p_nov VALUES LESS THAN('2026-12-01'),
    PARTITION p_dec VALUES LESS THAN('2027-01-01')
);
CREATE TABLE orders_u(id int, order_day date, amount int) WITH(storage_type=ustore)
PARTITION BY RANGE(order_day) (
    PARTITION p_nov VALUES LESS THAN('2026-12-01'),
    PARTITION p_dec VALUES LESS THAN('2027-01-01')
);
CREATE OR REPLACE FUNCTION check_plan(label text, q text, expected_batch boolean, expected_cache boolean)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE r record; batched boolean := false; cached boolean := false;
BEGIN
    FOR r IN EXECUTE 'EXPLAIN (COSTS OFF) ' || q LOOP
        batched := batched OR strpos(r."QUERY PLAN", 'batch mode') > 0;
        cached := cached OR strpos(r."QUERY PLAN", 'const partition key') > 0;
    END LOOP;
    IF batched IS DISTINCT FROM expected_batch OR cached IS DISTINCT FROM expected_cache THEN
        RAISE NOTICE 'FAIL: % batch=% cache=%', label, batched, cached;
    ELSE
        RAISE NOTICE 'PASS: % batch=% cache=%', label, batched, cached;
    END IF;
END $$;

-- Eligible astore insertion still uses both optimizations.
SET enable_heap_multi_insert_for_insert_select = on;
SELECT check_plan('astore_on',
    'INSERT INTO orders SELECT * FROM src WHERE order_day=DATE ''2026-11-15''', true, true);
INSERT INTO orders SELECT * FROM src WHERE order_day=DATE '2026-11-15';
SELECT 'DATA|astore_on|' || count(*) || '|' || sum(id) || '|' || sum(amount) FROM orders;
SELECT 'PARTS|astore_on|' || (SELECT count(*) FROM orders PARTITION(p_nov)) || '|' ||
    (SELECT count(*) FROM orders PARTITION(p_dec));

-- The master switch disables batching but leaves the OID cache independent.
TRUNCATE orders;
SET enable_heap_multi_insert_for_insert_select = off;
SELECT check_plan('astore_off_cache_on',
    'INSERT INTO orders SELECT * FROM src WHERE order_day=DATE ''2026-11-15''', false, true);
INSERT INTO orders SELECT * FROM src WHERE order_day=DATE '2026-11-15';
SELECT 'DATA|astore_off_cache_on|' || count(*) || '|' || sum(id) || '|' || sum(amount) FROM orders;
SELECT 'PARTS|astore_off_cache_on|' || (SELECT count(*) FROM orders PARTITION(p_nov)) || '|' ||
    (SELECT count(*) FROM orders PARTITION(p_dec));

-- Ustore is excluded even with the master switch enabled.
SET enable_heap_multi_insert_for_insert_select = on;
SELECT check_plan('ustore_on', 'INSERT INTO orders_u SELECT * FROM src', false, false);
INSERT INTO orders_u SELECT * FROM src;
SELECT 'DATA|ustore_on|' || count(*) || '|' || sum(id) || '|' || sum(amount) FROM orders_u;
SELECT 'PARTS|ustore_on|' || (SELECT count(*) FROM orders_u PARTITION(p_nov)) || '|' ||
    (SELECT count(*) FROM orders_u PARTITION(p_dec));

-- Do not TRUNCATE between EXECUTEs: relcache invalidation could force replanning.
TRUNCATE orders;
SET plan_cache_mode = force_generic_plan;
PREPARE prepared_on AS INSERT INTO orders SELECT * FROM src;
SELECT 'PREPARED|on_initial';
EXPLAIN (COSTS OFF) EXECUTE prepared_on;
EXECUTE prepared_on;
SET enable_heap_multi_insert_for_insert_select = off;
SELECT 'PREPARED|on_then_off';
EXPLAIN (COSTS OFF) EXECUTE prepared_on;
EXECUTE prepared_on;
SET enable_heap_multi_insert_for_insert_select = on;
SELECT 'PREPARED|on_off_on';
EXPLAIN (COSTS OFF) EXECUTE prepared_on;
EXECUTE prepared_on;
SELECT 'DATA|prepared_on_off_on|' || count(*) || '|' || sum(id) || '|' || sum(amount) FROM orders;
DEALLOCATE prepared_on;

TRUNCATE orders;
SET enable_heap_multi_insert_for_insert_select = off;
PREPARE prepared_off AS INSERT INTO orders SELECT * FROM src;
SELECT 'PREPARED|off_initial';
EXPLAIN (COSTS OFF) EXECUTE prepared_off;
EXECUTE prepared_off;
SET enable_heap_multi_insert_for_insert_select = on;
SELECT 'PREPARED|off_then_on';
EXPLAIN (COSTS OFF) EXECUTE prepared_off;
EXECUTE prepared_off;
SELECT 'DATA|prepared_off_on|' || count(*) || '|' || sum(id) || '|' || sum(amount) FROM orders;
DEALLOCATE prepared_off;
PREPARE prepared_new AS INSERT INTO orders SELECT * FROM src;
SELECT 'PREPARED|new_on';
EXPLAIN (COSTS OFF) EXECUTE prepared_new;
EXECUTE prepared_new;
SELECT 'DATA|prepared_new_on|' || count(*) || '|' || sum(id) || '|' || sum(amount) FROM orders;
DEALLOCATE prepared_new;

DROP TABLE orders_u, orders, src;
DROP FUNCTION check_plan(text, text, boolean, boolean);
DROP SCHEMA planner_batch_gate;
RESET search_path;
RESET enable_heap_multi_insert_for_insert_select;
RESET enable_partition_oid_caching;
RESET batch_insert_min_rows;
RESET enable_opfusion;
RESET enable_codegen;
RESET query_dop;
RESET plan_cache_mode;

-- B compatibility: IGNORE and COPY routing
CREATE DATABASE partition_insert_ignore_b DBCOMPATIBILITY 'B';
\c partition_insert_ignore_b
BEGIN;
CREATE SCHEMA review_ignore_routing;
SET LOCAL search_path = review_ignore_routing;
SET LOCAL enable_opfusion = off;
SET LOCAL enable_codegen = off;
SET LOCAL query_dop = 1;
SET LOCAL enable_heap_multi_insert_for_insert_select = on;
SET LOCAL enable_partition_oid_caching = off;

CREATE TABLE src(id int, order_day date, k int, region_id int) WITH(storage_type=astore);
INSERT INTO src VALUES
    (1, DATE '2026-11-15', 1, 1),
    (2, DATE '2026-12-15', -11, 11),
    (3, DATE '2026-11-15', 1, 1);
CREATE TABLE orders(id int, order_day date) WITH(storage_type=astore)
PARTITION BY RANGE(order_day) (PARTITION p_nov VALUES LESS THAN('2026-12-01'));
CREATE TABLE orders_expr(id int, k int) WITH(storage_type=astore)
PARTITION BY RANGE(abs(k)) (PARTITION p_small VALUES LESS THAN(10));
CREATE TABLE orders_sub(id int, order_day date, region_id int) WITH(storage_type=astore)
PARTITION BY RANGE(order_day) SUBPARTITION BY RANGE(region_id) (
    PARTITION p_nov VALUES LESS THAN('2026-12-01') (
        SUBPARTITION sp_small VALUES LESS THAN(10)
    )
);

-- Three valid-routing branches; the two-level helper has two failure positions.
CREATE TEMP TABLE cases(name text, dst_rel regclass, query text);
INSERT INTO cases VALUES
('plain', 'orders', $q$
    INSERT /*+ IGNORE_ERROR */ INTO orders
    SELECT id,order_day FROM src ORDER BY id
$q$),
('expression', 'orders_expr', $q$
    INSERT /*+ IGNORE_ERROR */ INTO orders_expr
    SELECT id,k FROM src ORDER BY id
$q$),
('missing_parent', 'orders_sub', $q$
    INSERT /*+ IGNORE_ERROR */ INTO orders_sub
    SELECT id,order_day,region_id FROM src ORDER BY id
$q$),
('missing_child', 'orders_sub', $q$
    INSERT /*+ IGNORE_ERROR */ INTO orders_sub
    SELECT id,DATE '2026-11-15',region_id FROM src ORDER BY id
$q$);

CREATE OR REPLACE FUNCTION probe(label text, dst_rel regclass, q text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
    r record; batched boolean := false; status text;
    n bigint; ids bigint;
BEGIN
    EXECUTE 'TRUNCATE ' || dst_rel::text;
    FOR r IN EXECUTE 'EXPLAIN (COSTS OFF) ' || q LOOP
        batched := batched OR strpos(r."QUERY PLAN", 'batch mode') > 0;
    END LOOP;
    BEGIN
        EXECUTE q;
        GET DIAGNOSTICS n = ROW_COUNT;
        status := 'INSERTED ' || n;
    EXCEPTION WHEN OTHERS THEN
        status := SQLSTATE || ': ' || SQLERRM;
    END;
    EXECUTE 'SELECT count(*),coalesce(sum(id),0) FROM ' || dst_rel::text INTO n,ids;
    RETURN label || '|batch=' || batched || '|' || status || '|rows=' || n || '|ids=' || ids;
END $$;

SET LOCAL batch_insert_min_rows = -1;
SELECT probe('row/' || name, dst_rel, query) FROM cases ORDER BY name;
SET LOCAL batch_insert_min_rows = 0;
SELECT probe('batch/' || name, dst_rel, query) FROM cases ORDER BY name;

-- Without IGNORE, routing errors must still roll back the whole INSERT.
SELECT probe('strict/' || name, dst_rel, replace(query, '/*+ IGNORE_ERROR */', ''))
FROM cases ORDER BY name;

-- Repeated unroutable constants must not be stored in the OID cache.
SET LOCAL enable_partition_oid_caching = on;
SELECT probe('constant/plain', 'orders', $q$
    INSERT /*+ IGNORE_ERROR */ INTO orders
    SELECT id, DATE '2026-12-15' FROM src ORDER BY id
$q$);
SELECT probe('constant/child', 'orders_sub', $q$
    INSERT /*+ IGNORE_ERROR */ INTO orders_sub
    SELECT id, DATE '2026-11-15', 11 FROM src ORDER BY id
$q$);

-- INTERVAL still creates partitions for valid dates and skips NULL under IGNORE.
CREATE TABLE interval_src(id int, order_day date) WITH(storage_type=astore);
INSERT INTO interval_src VALUES (1, DATE '2026-11-15'), (2, DATE '2026-12-15'), (3, NULL);
CREATE TABLE orders_interval(id int, order_day date) WITH(storage_type=astore)
PARTITION BY RANGE(order_day) INTERVAL('1 month') (
    PARTITION p_base VALUES LESS THAN('2026-12-01')
);
SELECT probe('interval/create_and_null', 'orders_interval', $q$
    INSERT /*+ IGNORE_ERROR */ INTO orders_interval
    SELECT id, order_day FROM interval_src ORDER BY id
$q$);

-- More than one batch: a bad row after valid rows must not lose later rows.
CREATE TABLE many_src(id int, order_day date, region_id int) WITH(storage_type=astore);
INSERT INTO many_src SELECT i, DATE '2026-11-15', CASE WHEN i = 2001 THEN 11 ELSE 1 END
FROM generate_series(1, 2003) AS g(i);
SELECT probe('flush/child', 'orders_sub', $q$
    INSERT /*+ IGNORE_ERROR */ INTO orders_sub
    SELECT id, order_day, region_id FROM many_src ORDER BY id
$q$);

ROLLBACK;

-- COPY shares the helper but must still reject missing children atomically.
CREATE TABLE partition_ignore_copy_probe(p int,k int) WITH(storage_type=astore)
PARTITION BY RANGE(p) SUBPARTITION BY RANGE(k) (
 PARTITION p1 VALUES LESS THAN(10) (SUBPARTITION s1 VALUES LESS THAN(10)));
COPY partition_ignore_copy_probe FROM STDIN WITH(FORMAT csv);
1,1
1,11
\.
SELECT count(*) AS copy_rows_after_error FROM partition_ignore_copy_probe;
DROP TABLE partition_ignore_copy_probe;

BEGIN;
CREATE SCHEMA specified_partition_ignore;
SET LOCAL search_path = specified_partition_ignore;
SET LOCAL enable_opfusion = off;
SET LOCAL query_dop = 1;
SET LOCAL enable_heap_multi_insert_for_insert_select = on;
SET LOCAL enable_partition_oid_caching = off;
SET LOCAL batch_insert_min_rows = 0;
CREATE TABLE src(id int, k int) WITH (storage_type=astore);
INSERT INTO src VALUES(1,1),(2,2),(3,1);
CREATE TABLE orders(id int, k int) WITH (storage_type=astore)
PARTITION BY RANGE(k) (
    PARTITION p_one VALUES LESS THAN(2),
    PARTITION p_other VALUES LESS THAN(MAXVALUE)
);
EXPLAIN (COSTS OFF)
INSERT /*+ IGNORE_ERROR */ INTO orders PARTITION(p_one) SELECT * FROM src ORDER BY id;
INSERT /*+ IGNORE_ERROR */ INTO orders PARTITION(p_one) SELECT * FROM src ORDER BY id;
DO $$ BEGIN
    IF (SELECT count(*) FROM orders PARTITION(p_one)) <> 2 OR
       (SELECT count(*) FROM orders PARTITION(p_other)) <> 0 OR
       (SELECT sum(id) FROM orders) <> 4 THEN
        RAISE NOTICE 'FAIL: IGNORE did not skip exactly the out-of-scope row';
    END IF;
END $$;
SELECT 'PASS: IGNORE skips mismatch and continues';
ROLLBACK;

\c regression
DROP DATABASE partition_insert_ignore_b;
