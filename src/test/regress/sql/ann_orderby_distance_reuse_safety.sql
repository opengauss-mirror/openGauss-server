-- DiskANN ORDER BY distance reuse: execution-lifetime and safety boundaries.

SET client_min_messages = warning;
DROP SCHEMA IF EXISTS ann_orderby_reuse_safety_test CASCADE;
CREATE SCHEMA ann_orderby_reuse_safety_test;
SET search_path = ann_orderby_reuse_safety_test, public;

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

SET ann_orderby_distance_reuse = l2;
EXPLAIN (ANALYZE, VERBOSE ON, COSTS OFF, TIMING OFF)
EXECUTE ann_cached_cosine;
DEALLOCATE ann_cached_cosine;

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

-- One graph node may own several heap TIDs.  It performs one scalar
-- conversion and publishes the same value separately for all eight rows.
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

RESET ann_orderby_distance_reuse;
RESET enable_expr_fusion;
RESET enable_seqscan;
RESET enable_bitmapscan;
DROP SCHEMA ann_orderby_reuse_safety_test CASCADE;
