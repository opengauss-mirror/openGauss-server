-- DiskANN ORDER BY distance result reuse: plan, executor and correctness guards.

SET client_min_messages = warning;
DROP SCHEMA IF EXISTS ann_orderby_reuse_test CASCADE;
CREATE SCHEMA ann_orderby_reuse_test;
SET search_path = ann_orderby_reuse_test, public;

CREATE TABLE ann_l2_t
(
    id integer PRIMARY KEY,
    title text,
    embedding vector(4)
);

INSERT INTO ann_l2_t
SELECT g,
       'row-' || g,
       ('[' || (g + 1) || ',' || (g * 2 + 3) || ',' ||
        (g * 3 + 5) || ',' || (g * 4 + 7) || ']')::vector
FROM generate_series(1, 40) g;

-- A non-key update/filter value exercises heap filtering without changing
-- the query's exact zero-distance row.
UPDATE ann_l2_t SET title = NULL WHERE id = 20;

CREATE INDEX ann_l2_idx ON ann_l2_t
USING diskann (embedding vector_l2_ops) WITH (index_size = 16);
ANALYZE ann_l2_t;

SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_expr_fusion = off;

SHOW ann_orderby_distance_reuse;

-- Default/off: no reference is planned; EXPLAIN remains free of
-- distance-source diagnostics in every mode.
SET ann_orderby_distance_reuse = off;
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT /*+ indexscan(ann_l2_t ann_l2_idx) */
       id, embedding <-> '[11,23,35,47]'::vector AS distance
FROM ann_l2_t
WHERE title IS NOT NULL
ORDER BY embedding <-> '[11,23,35,47]'::vector
LIMIT 10;

CREATE TEMP TABLE l2_off AS
SELECT /*+ indexscan(ann_l2_t ann_l2_idx) */
       id, embedding <-> '[11,23,35,47]'::vector AS distance
FROM ann_l2_t
WHERE title IS NOT NULL
ORDER BY embedding <-> '[11,23,35,47]'::vector
LIMIT 10;

-- l2: AM d^2 is converted once with sqrt and projection reads it.
SET ann_orderby_distance_reuse = l2;
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT /*+ indexscan(ann_l2_t ann_l2_idx) */
       id, embedding <-> '[11,23,35,47]'::vector AS distance
FROM ann_l2_t
WHERE title IS NOT NULL
ORDER BY embedding <-> '[11,23,35,47]'::vector
LIMIT 10;

EXPLAIN (ANALYZE, VERBOSE ON, COSTS OFF, TIMING OFF)
SELECT /*+ indexscan(ann_l2_t ann_l2_idx) */
       id, embedding <-> '[11,23,35,47]'::vector AS distance
FROM ann_l2_t
WHERE title IS NOT NULL
ORDER BY embedding <-> '[11,23,35,47]'::vector
LIMIT 10;

CREATE TEMP TABLE l2_exact AS
SELECT /*+ indexscan(ann_l2_t ann_l2_idx) */
       id, embedding <-> '[11,23,35,47]'::vector AS distance
FROM ann_l2_t
WHERE title IS NOT NULL
ORDER BY embedding <-> '[11,23,35,47]'::vector
LIMIT 10;

SELECT count(*) AS l2_binary_mismatches
FROM (
    (SELECT id, float8send(distance) AS bits FROM l2_off
     EXCEPT ALL
     SELECT id, float8send(distance) AS bits FROM l2_exact)
    UNION ALL
    (SELECT id, float8send(distance) AS bits FROM l2_exact
     EXCEPT ALL
     SELECT id, float8send(distance) AS bits FROM l2_off)
) mismatch;

-- Comparing on/off is insufficient if both paths are wrong in the same
-- way.  Recompute the SQL operator against the heap row for every returned
-- ID and require bit-for-bit float8 equality.
SET ann_orderby_distance_reuse = off;
SELECT count(*) AS l2_heap_value_mismatches
FROM l2_exact r
JOIN ann_l2_t t USING (id)
WHERE float8send(r.distance) IS DISTINCT FROM
      float8send(t.embedding <-> '[11,23,35,47]'::vector);
SET ann_orderby_distance_reuse = l2;

-- Hidden ORDER BY output: the user requests only id, but the scan still
-- consumes the reference as a resjunk ordering value.
EXPLAIN (ANALYZE, VERBOSE ON, COSTS OFF, TIMING OFF)
SELECT /*+ indexscan(ann_l2_t ann_l2_idx) */ id
FROM ann_l2_t
WHERE title IS NOT NULL
ORDER BY embedding <-> '[11,23,35,47]'::vector
LIMIT 3;

-- Two equal outputs may read the same publication twice; a different
-- query vector must remain an ordinary expression and never receive the
-- ORDER BY value by mistake.
EXPLAIN (ANALYZE, VERBOSE ON, COSTS OFF, TIMING OFF)
SELECT /*+ indexscan(ann_l2_t ann_l2_idx) */
       id,
       embedding <-> '[11,23,35,47]'::vector AS distance_1,
       embedding <-> '[11,23,35,47]'::vector AS distance_2,
       embedding <-> '[6,13,20,27]'::vector AS other_distance
FROM ann_l2_t
WHERE title IS NOT NULL
ORDER BY embedding <-> '[11,23,35,47]'::vector
LIMIT 3;

-- A never-entered branch must report zero work, not a fabricated fallback.
EXPLAIN (ANALYZE, VERBOSE ON, COSTS OFF, TIMING OFF)
SELECT /*+ indexscan(ann_l2_t ann_l2_idx) */
       id, embedding <-> '[11,23,35,47]'::vector AS distance
FROM ann_l2_t
WHERE title IS NOT NULL
ORDER BY embedding <-> '[11,23,35,47]'::vector
LIMIT 0;

-- The flattened expression engine must consume the same reference.
SET enable_expr_fusion = on;
EXPLAIN (ANALYZE, VERBOSE ON, COSTS OFF, TIMING OFF)
SELECT /*+ indexscan(ann_l2_t ann_l2_idx) */
       id, embedding <-> '[11,23,35,47]'::vector AS distance
FROM ann_l2_t
WHERE title IS NOT NULL
ORDER BY embedding <-> '[11,23,35,47]'::vector
LIMIT 3;
SET enable_expr_fusion = off;

-- l2 must reject cosine; cosine may request its normalized value.
CREATE INDEX ann_cos_idx ON ann_l2_t
USING diskann (embedding vector_cosine_ops) WITH (index_size = 16);

SET ann_orderby_distance_reuse = l2;
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT /*+ indexscan(ann_l2_t ann_cos_idx) */
       id, embedding <=> '[11,23,35,47]'::vector AS distance
FROM ann_l2_t
WHERE title IS NOT NULL
ORDER BY embedding <=> '[11,23,35,47]'::vector
LIMIT 3;

SET ann_orderby_distance_reuse = cosine;

-- cosine is independent from l2 and must not reuse an L2 index result.
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT /*+ indexscan(ann_l2_t ann_l2_idx) */
       id, embedding <-> '[11,23,35,47]'::vector AS distance
FROM ann_l2_t
WHERE title IS NOT NULL
ORDER BY embedding <-> '[11,23,35,47]'::vector
LIMIT 3;

EXPLAIN (ANALYZE, VERBOSE ON, COSTS OFF, TIMING OFF)
SELECT /*+ indexscan(ann_l2_t ann_cos_idx) */
       id, embedding <=> '[11,23,35,47]'::vector AS distance
FROM ann_l2_t
WHERE title IS NOT NULL
ORDER BY embedding <=> '[11,23,35,47]'::vector
LIMIT 3;

CREATE TEMP TABLE cosine_approx AS
SELECT /*+ indexscan(ann_l2_t ann_cos_idx) */
       id, embedding <=> '[11,23,35,47]'::vector AS distance
FROM ann_l2_t
WHERE title IS NOT NULL
ORDER BY embedding <=> '[11,23,35,47]'::vector
LIMIT 10;

-- Validate the documented AM conversion itself, then separately record
-- its bounded difference from the exact SQL cosine operator.
SET ann_orderby_distance_reuse = off;
SELECT count(*) AS cosine_conversion_mismatches
FROM cosine_approx r
JOIN ann_l2_t t USING (id)
WHERE float8send(r.distance) IS DISTINCT FROM float8send(
          least(2.0::float8, greatest(0.0::float8,
              1.0::float8 + vector_negative_inner_product(
                  l2_normalize(t.embedding),
                  l2_normalize('[11,23,35,47]'::vector)))));

SELECT count(*) AS cosine_values_different_from_exact
FROM cosine_approx r
JOIN ann_l2_t t USING (id)
WHERE float8send(r.distance) IS DISTINCT FROM
      float8send(t.embedding <=> '[11,23,35,47]'::vector);

SELECT bool_and(r.distance >= 0.0 AND r.distance <= 2.0) AS cosine_in_range,
       bool_and(abs(r.distance -
                    (t.embedding <=> '[11,23,35,47]'::vector)) <=
                0.00001::float8) AS cosine_fixture_error_ok
FROM cosine_approx r
JOIN ann_l2_t t USING (id);
-- all combines the independently tested l2 and cosine capabilities.
SET ann_orderby_distance_reuse = 'all';

-- The WHERE distance remains the exact SQL expression even when the
-- projected/order value uses the explicitly permitted approximate source.
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT /*+ indexscan(ann_l2_t ann_cos_idx) */
       id, embedding <=> '[11,23,35,47]'::vector AS distance
FROM ann_l2_t
WHERE title IS NOT NULL
  AND embedding <=> '[11,23,35,47]'::vector < 0.00001::float8
ORDER BY embedding <=> '[11,23,35,47]'::vector
LIMIT 3;

-- Typical delta UNION ALL (snapshot anti-join delete) Top-10 structure.
CREATE TABLE delta_t (row_id bigint PRIMARY KEY, title text, embedding vector(4));
CREATE TABLE snapshot_t (row_id bigint PRIMARY KEY, title text, embedding vector(4));
CREATE TABLE delete_t (row_id bigint PRIMARY KEY, reason text);

INSERT INTO delta_t
SELECT 1000 + g, 'delta-' || g,
       ('[' || (g + 2) || ',' || (g * 2 + 5) || ',' ||
        (g * 3 + 8) || ',' || (g * 4 + 11) || ']')::vector
FROM generate_series(1, 24) g;
INSERT INTO snapshot_t
SELECT g, 'snapshot-' || g,
       ('[' || (g + 1) || ',' || (g * 2 + 4) || ',' ||
        (g * 3 + 7) || ',' || (g * 4 + 10) || ']')::vector
FROM generate_series(1, 24) g;
INSERT INTO delete_t VALUES (3, 'deleted'), (9, 'deleted'), (17, 'deleted');

CREATE INDEX delta_embedding_diskann ON delta_t
USING diskann (embedding vector_l2_ops) WITH (index_size = 16);
ANALYZE delta_t;
ANALYZE snapshot_t;
ANALYZE delete_t;

CREATE VIEW live_union AS
SELECT row_id, title, embedding
FROM delta_t
UNION ALL
SELECT s.row_id, s.title, s.embedding
FROM snapshot_t s
WHERE NOT EXISTS (SELECT 1 FROM delete_t d WHERE d.row_id = s.row_id);

SET enable_union_all_diskann_topk_pushdown = on;
SET enable_union_all_required_columns_pushdown = on;
SET ann_orderby_distance_reuse = 'all';

EXPLAIN (ANALYZE, VERBOSE ON, COSTS OFF, TIMING OFF)
SELECT row_id
FROM live_union
WHERE title IS NOT NULL
ORDER BY embedding <-> '[11,23,35,47]'::vector
LIMIT 10;

CREATE TEMP TABLE union_reuse_on AS
SELECT row_id
FROM live_union
WHERE title IS NOT NULL
ORDER BY embedding <-> '[11,23,35,47]'::vector
LIMIT 10;

SET ann_orderby_distance_reuse = off;
CREATE TEMP TABLE union_reuse_off AS
SELECT row_id
FROM live_union
WHERE title IS NOT NULL
ORDER BY embedding <-> '[11,23,35,47]'::vector
LIMIT 10;

SELECT count(*) AS union_result_mismatches
FROM (
    (SELECT row_id FROM union_reuse_on
     EXCEPT ALL
     SELECT row_id FROM union_reuse_off)
    UNION ALL
    (SELECT row_id FROM union_reuse_off
     EXCEPT ALL
     SELECT row_id FROM union_reuse_on)
) mismatch;

SET ann_orderby_distance_reuse = 'all';

SELECT row_id
FROM live_union
WHERE title IS NOT NULL
ORDER BY embedding <-> '[11,23,35,47]'::vector
LIMIT 10;

RESET ann_orderby_distance_reuse;
RESET enable_union_all_diskann_topk_pushdown;
RESET enable_union_all_required_columns_pushdown;
RESET enable_expr_fusion;
RESET enable_seqscan;
RESET enable_bitmapscan;
DROP SCHEMA ann_orderby_reuse_test CASCADE;
