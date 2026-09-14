-- DiskANN ORDER BY distance reuse: execution-lifetime and safety boundaries.

SET client_min_messages = warning;
DROP SCHEMA IF EXISTS ann_orderby_reuse_safety_test CASCADE;
CREATE SCHEMA ann_orderby_reuse_safety_test;
SET search_path = ann_orderby_reuse_safety_test, public;

CREATE FUNCTION ann_safety_capture(query_text text) RETURNS SETOF record AS $$
BEGIN
    RETURN QUERY EXECUTE query_text;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE ann_safety_t
(
    id integer PRIMARY KEY,
    title text,
    embedding vector(4)
);

INSERT INTO ann_safety_t
SELECT g,
       'row-' || g,
       ('[' || (g + 1) || ',' || (g * 2 + 3) || ',' ||
        (g * 3 + 5) || ',' || (g * 4 + 7) || ']')::vector
FROM generate_series(1, 40) g;

CREATE INDEX ann_safety_l2_idx ON ann_safety_t
USING diskann (embedding vector_l2_ops) WITH (index_size = 16);
CREATE INDEX ann_safety_cos_idx ON ann_safety_t
USING diskann (embedding vector_cosine_ops) WITH (index_size = 16);
ANALYZE ann_safety_t;

SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_expr_fusion = off;

-- Inspect actual prepared-statement values, not just its saved plan shape.
CREATE FUNCTION ann_cached_cosine_mismatches() RETURNS integer AS $$
DECLARE
    r record;
    expected_distance float8;
    mismatches integer := 0;
BEGIN
    FOR r IN EXECUTE 'EXECUTE ann_cached_cosine' LOOP
        SELECT cosine_distance(embedding, '[11,23,35,47]'::vector)
        INTO expected_distance FROM ann_safety_t WHERE id = r.id;
        IF float8send(r.distance) IS DISTINCT FROM float8send(expected_distance) THEN
            mismatches := mismatches + 1;
        END IF;
    END LOOP;
    RETURN mismatches;
END;
$$ LANGUAGE plpgsql;

-- A saved all-mode plan must choose one policy before returning its first
-- row.  Switching to l2 makes the entire later execution evaluate
-- the original SQL expression; it must not mix exact and approximate rows.
SET ann_orderby_distance_reuse = 'all';
PREPARE ann_cached_cosine AS
SELECT /*+ indexscan(ann_safety_t ann_safety_cos_idx) */
       id, embedding <=> '[11,23,35,47]'::vector AS distance
FROM ann_safety_t
ORDER BY embedding <=> '[11,23,35,47]'::vector
LIMIT 3;

EXPLAIN (ANALYZE, VERBOSE ON, COSTS OFF, TIMING OFF)
EXECUTE ann_cached_cosine;
SELECT ann_cached_cosine_mismatches() > 0 AS cached_plan_uses_approximation;

SET ann_orderby_distance_reuse = l2;
EXPLAIN (ANALYZE, VERBOSE ON, COSTS OFF, TIMING OFF)
EXECUTE ann_cached_cosine;
SELECT ann_cached_cosine_mismatches() AS cached_plan_fallback_mismatches;
DEALLOCATE ann_cached_cosine;

-- The flattened expression engine must make the same whole-scan choice.
SET enable_expr_fusion = on;
SET ann_orderby_distance_reuse = 'all';
PREPARE ann_cached_cosine AS
SELECT /*+ indexscan(ann_safety_t ann_safety_cos_idx) */
       id, embedding <=> '[11,23,35,47]'::vector AS distance
FROM ann_safety_t
ORDER BY embedding <=> '[11,23,35,47]'::vector
LIMIT 3;
SELECT ann_cached_cosine_mismatches() > 0 AS fused_plan_uses_approximation;
SET ann_orderby_distance_reuse = l2;
SELECT ann_cached_cosine_mismatches() AS fused_plan_fallback_mismatches;
DEALLOCATE ann_cached_cosine;
SET enable_expr_fusion = off;

-- Once a cursor has begun its scan, a GUC change between FETCH commands
-- must not change that active scan's distance policy.
SET ann_orderby_distance_reuse = cosine;
START TRANSACTION;
DECLARE ann_distance_cursor NO SCROLL CURSOR FOR
SELECT /*+ indexscan(ann_safety_t ann_safety_cos_idx) */
       id, embedding <=> '[11,23,35,47]'::vector AS distance
FROM ann_safety_t
ORDER BY embedding <=> '[11,23,35,47]'::vector
LIMIT 4;
FETCH 1 FROM ann_distance_cursor;
SET ann_orderby_distance_reuse = off;
FETCH 3 FROM ann_distance_cursor;
CLOSE ann_distance_cursor;
COMMIT;

-- A correlated subplan drives a real executor rescan with a different
-- query vector on each outer row.  Epoch/token state must be reset.
CREATE TABLE ann_query_t (qid integer PRIMARY KEY, q vector(4));
INSERT INTO ann_query_t VALUES
    (1, '[6,13,20,27]'),
    (2, '[31,63,95,127]');

SET ann_orderby_distance_reuse = l2;
EXPLAIN (ANALYZE, VERBOSE ON, COSTS OFF, TIMING OFF)
SELECT q.qid,
       (SELECT /*+ indexscan(t ann_safety_l2_idx) */ t.id
        FROM ann_safety_t t
        ORDER BY t.embedding <-> q.q
        LIMIT 1) AS nearest_id
FROM ann_query_t q
ORDER BY q.qid;

SELECT q.qid,
       (SELECT /*+ indexscan(t ann_safety_l2_idx) */ t.id
        FROM ann_safety_t t
        ORDER BY t.embedding <-> q.q
        LIMIT 1) AS nearest_id
FROM ann_query_t q
ORDER BY q.qid;

-- Updating a non-indexed column creates the ordinary HOT-visible-row shape.
-- The published root TID may differ in offset, but cannot cross heap blocks.
UPDATE ann_safety_t SET title = 'hot-updated' WHERE id = 10;
EXPLAIN (ANALYZE, VERBOSE ON, COSTS OFF, TIMING OFF)
SELECT /*+ indexscan(ann_safety_t ann_safety_l2_idx) */
       id, embedding <-> '[11,23,35,47]'::vector AS distance
FROM ann_safety_t
ORDER BY embedding <-> '[11,23,35,47]'::vector
LIMIT 1;

-- One graph node may own several heap TIDs. Only the original vector's
-- TID is proven exact; other merged TIDs require an explicit L2 recheck.
CREATE TABLE ann_duplicate_t
(
    id integer PRIMARY KEY,
    embedding vector(4)
);
INSERT INTO ann_duplicate_t
SELECT g, '[10,20,30,40]'::vector FROM generate_series(1, 8) g;
INSERT INTO ann_duplicate_t VALUES (9, '[100,200,300,400]');
CREATE INDEX ann_duplicate_idx ON ann_duplicate_t
USING diskann (embedding vector_l2_ops) WITH (index_size = 16);
ANALYZE ann_duplicate_t;

EXPLAIN (ANALYZE, VERBOSE ON, COSTS OFF, TIMING OFF)
SELECT /*+ indexscan(ann_duplicate_t ann_duplicate_idx) */
       id, embedding <-> '[10,20,30,40]'::vector AS distance
FROM ann_duplicate_t
ORDER BY embedding <-> '[10,20,30,40]'::vector
LIMIT 8;

SELECT count(*) AS duplicate_rows,
       count(DISTINCT distance) AS duplicate_distances
FROM (
    SELECT /*+ indexscan(ann_duplicate_t ann_duplicate_idx) */
           id, embedding <-> '[10,20,30,40]'::vector AS distance
    FROM ann_duplicate_t
    ORDER BY embedding <-> '[10,20,30,40]'::vector
    LIMIT 8
) s;

-- Distance-threshold deduplication can merge unequal vectors. Large norms
-- expose cancellation even for integer coordinates, and tiny differences
-- exercise the threshold itself. Both engines must match each heap row,
-- including two references reading the same rechecked value.
CREATE TABLE ann_merged_l2_t (id integer, title text, embedding vector(4));
INSERT INTO ann_merged_l2_t VALUES
    (1, 'a', '[10000,20000,30000,40000]'),
    (2, 'b', '[10001,20000,30000,40000]'),
    (3, 'c', '[10002,20000,30000,40000]'),
    (4, 'd', '[10000,20001,30000,40000]'),
    (5, 'e', '[100000,200000,300000,400000]'),
    (6, 'f', '[10,20,30,40]'),
    (7, 'g', '[10.00001,20,30,40]'),
    (8, 'h', '[10.00002,20,30,40]'),
    (9, 'i', '[10,20.00001,30,40]');
CREATE INDEX ann_merged_l2_idx ON ann_merged_l2_t
USING diskann (embedding vector_l2_ops) WITH (index_size = 16);
ANALYZE ann_merged_l2_t;

CREATE TEMP TABLE merged_recursive AS
SELECT * FROM ann_safety_capture($q$
    SELECT /*+ indexscan(ann_merged_l2_t ann_merged_l2_idx) */ id,
           embedding <-> '[10000,20000,30000,40000]'::vector AS d1,
           embedding <-> '[10000,20000,30000,40000]'::vector AS d2,
           l2_distance(embedding, '[10000,20000,30000,40000]'::vector) AS heap_distance
    FROM ann_merged_l2_t
    ORDER BY embedding <-> '[10000,20000,30000,40000]'::vector LIMIT 10
$q$) AS r(id integer, d1 float8, d2 float8, heap_distance float8);
SELECT count(*) AS merged_recursive_rows FROM merged_recursive;
SELECT count(*) AS merged_recursive_mismatches FROM merged_recursive
WHERE float8send(d1) IS DISTINCT FROM float8send(heap_distance)
   OR float8send(d2) IS DISTINCT FROM float8send(heap_distance);

SET enable_expr_fusion = on;
CREATE TEMP TABLE merged_fused AS
SELECT * FROM ann_safety_capture($q$
    SELECT /*+ indexscan(ann_merged_l2_t ann_merged_l2_idx) */ id,
           embedding <-> '[10,20,30,40]'::vector AS d1,
           embedding <-> '[10,20,30,40]'::vector AS d2,
           l2_distance(embedding, '[10,20,30,40]'::vector) AS heap_distance
    FROM ann_merged_l2_t
    ORDER BY embedding <-> '[10,20,30,40]'::vector LIMIT 10
$q$) AS r(id integer, d1 float8, d2 float8, heap_distance float8);
SELECT count(*) AS merged_fused_rows FROM merged_fused;
SELECT count(*) AS merged_fused_mismatches FROM merged_fused
WHERE float8send(d1) IS DISTINCT FROM float8send(heap_distance)
   OR float8send(d2) IS DISTINCT FROM float8send(heap_distance);
SET enable_expr_fusion = off;

-- A HOT update must retain the original vector identity, but not leak a
-- previous row's cached recheck. No index rebuild is involved.
UPDATE ann_merged_l2_t SET title = 'hot-' || title;
CREATE TEMP TABLE merged_hot AS
SELECT * FROM ann_safety_capture($q$
    SELECT /*+ indexscan(ann_merged_l2_t ann_merged_l2_idx) */ id,
           embedding <-> '[10000,20000,30000,40000]'::vector AS distance,
           l2_distance(embedding, '[10000,20000,30000,40000]'::vector) AS heap_distance
    FROM ann_merged_l2_t
    ORDER BY embedding <-> '[10000,20000,30000,40000]'::vector LIMIT 10
$q$) AS r(id integer, distance float8, heap_distance float8);
SELECT count(*) AS merged_hot_rows FROM merged_hot;
SELECT count(*) AS merged_hot_mismatches FROM merged_hot
WHERE float8send(distance) IS DISTINCT FROM float8send(heap_distance);

-- A new query vector on each rescan must invalidate both the AM scalar
-- and any cached heap recheck from the previous outer row.
SELECT q.qid,
       float8send((SELECT /*+ indexscan(t ann_merged_l2_idx) */ t.embedding <-> q.q
                   FROM ann_merged_l2_t t WHERE id = 3
                   ORDER BY t.embedding <-> q.q LIMIT 1)) =
       float8send(l2_distance('[10002,20000,30000,40000]'::vector, q.q))
       AS rescanned_l2_matches
FROM ann_query_t q ORDER BY q.qid;
SET enable_expr_fusion = on;
SELECT q.qid,
       float8send((SELECT /*+ indexscan(t ann_merged_l2_idx) */ t.embedding <-> q.q
                   FROM ann_merged_l2_t t WHERE id = 3
                   ORDER BY t.embedding <-> q.q LIMIT 1)) =
       float8send(l2_distance('[10002,20000,30000,40000]'::vector, q.q))
       AS fused_rescanned_l2_matches
FROM ann_query_t q ORDER BY q.qid;
SET enable_expr_fusion = off;

-- AnnIndexScan is shared by multiple ANN AMs.  HNSW must never receive the
-- DiskANN-only reference display.
CREATE INDEX ann_safety_hnsw_idx ON ann_safety_t
USING hnsw (embedding vector_l2_ops) WITH (m = 4, ef_construction = 10);

EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT /*+ indexscan(ann_safety_t ann_safety_hnsw_idx) */
       id, embedding <-> '[11,23,35,47]'::vector AS distance
FROM ann_safety_t
ORDER BY embedding <-> '[11,23,35,47]'::vector
LIMIT 3;

-- Approximate cosine is legal for projection/sort/limit, but not when the
-- projected value acquires DISTINCT or GROUP BY semantics.  Before the
-- consumer-chain guard this fixture collapsed 40 exact groups to 27.
SET ann_orderby_distance_reuse = cosine;
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT DISTINCT distance
FROM (
    SELECT /*+ indexscan(ann_safety_t ann_safety_cos_idx) */
           embedding <=> '[11,23,35,47]'::vector AS distance
    FROM ann_safety_t
    ORDER BY embedding <=> '[11,23,35,47]'::vector
    LIMIT 40
) s
ORDER BY distance;

SELECT count(*) AS distinct_distance_groups
FROM (
    SELECT DISTINCT distance
    FROM (
        SELECT /*+ indexscan(ann_safety_t ann_safety_cos_idx) */
               embedding <=> '[11,23,35,47]'::vector AS distance
        FROM ann_safety_t
        ORDER BY embedding <=> '[11,23,35,47]'::vector
        LIMIT 40
    ) ordered_rows
) distinct_rows;

EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT distance, count(*)
FROM (
    SELECT /*+ indexscan(ann_safety_t ann_safety_cos_idx) */
           embedding <=> '[11,23,35,47]'::vector AS distance
    FROM ann_safety_t
    ORDER BY embedding <=> '[11,23,35,47]'::vector
    LIMIT 40
) s
GROUP BY distance
ORDER BY distance;

SELECT count(*) AS grouped_distances, max(group_size) AS max_group_size
FROM (
    SELECT distance, count(*) AS group_size
    FROM (
        SELECT /*+ indexscan(ann_safety_t ann_safety_cos_idx) */
               embedding <=> '[11,23,35,47]'::vector AS distance
        FROM ann_safety_t
        ORDER BY embedding <=> '[11,23,35,47]'::vector
        LIMIT 40
    ) ordered_rows
    GROUP BY distance
) groups;

-- A LIMIT boundary prevents pull-up: an outer WHERE consumes the projected
-- distance.  The exact value is just above this threshold; AM rounding is
-- just below it.  Do not hide the unsafe shape under an aggregate in the
-- tested statement, since that would exercise the older aggregate guard.
CREATE TABLE ann_threshold_t (id integer, embedding vector(4));
INSERT INTO ann_threshold_t VALUES (1, '[2,4,6,8]');
INSERT INTO ann_threshold_t
SELECT g, ('[' || (-g) || ',' || g || ',0,1]')::vector
FROM generate_series(2, 40) g;
CREATE INDEX ann_threshold_cos_idx ON ann_threshold_t
USING diskann (embedding vector_cosine_ops) WITH (index_size = 16);
ANALYZE ann_threshold_t;

SET ann_orderby_distance_reuse = off;
SELECT id, distance FROM (
    SELECT /*+ indexscan(ann_threshold_t ann_threshold_cos_idx) */
           id, embedding <=> '[11,23,35,47]'::vector AS distance
    FROM ann_threshold_t ORDER BY distance LIMIT 10
) s WHERE distance < 0.00008161::float8;


SET ann_orderby_distance_reuse = cosine;
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT id, distance FROM (
    SELECT /*+ indexscan(ann_threshold_t ann_threshold_cos_idx) */
           id, embedding <=> '[11,23,35,47]'::vector AS distance
    FROM ann_threshold_t ORDER BY distance LIMIT 10
) s WHERE distance < 0.00008161::float8;
SELECT id, distance FROM (
    SELECT /*+ indexscan(ann_threshold_t ann_threshold_cos_idx) */
           id, embedding <=> '[11,23,35,47]'::vector AS distance
    FROM ann_threshold_t ORDER BY distance LIMIT 10
) s WHERE distance < 0.00008161::float8;

-- Projection expressions also consume a child's value; passing a distance
-- through a Var is safe, using it in a comparison is not.
SELECT id, distance < 0.00008161::float8 AS admitted FROM (
    SELECT /*+ indexscan(ann_threshold_t ann_threshold_cos_idx) */
           id, embedding <=> '[11,23,35,47]'::vector AS distance
    FROM ann_threshold_t ORDER BY distance LIMIT 1
) s;

-- An uncorrelated scalar subquery travels through PARAM_EXEC/InitPlan;
-- rejecting only T_SubPlan misses this path in plan_tree_walker.
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT (SELECT /*+ indexscan(ann_threshold_t ann_threshold_cos_idx) */
               embedding <=> '[11,23,35,47]'::vector
        FROM ann_threshold_t
        ORDER BY embedding <=> '[11,23,35,47]'::vector LIMIT 1)
       < 0.00008161::float8 AS initplan_admitted;
SELECT (SELECT /*+ indexscan(ann_threshold_t ann_threshold_cos_idx) */
               embedding <=> '[11,23,35,47]'::vector
        FROM ann_threshold_t
        ORDER BY embedding <=> '[11,23,35,47]'::vector LIMIT 1)
       < 0.00008161::float8 AS initplan_admitted;

RESET ann_orderby_distance_reuse;
RESET enable_expr_fusion;
RESET enable_seqscan;
RESET enable_bitmapscan;
DROP SCHEMA ann_orderby_reuse_safety_test CASCADE;
