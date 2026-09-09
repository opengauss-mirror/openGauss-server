-- UNION ALL required-column pushdown: plan-shape and correctness coverage.

SET client_min_messages = warning;
DROP SCHEMA IF EXISTS union_all_required_columns_test CASCADE;
CREATE SCHEMA union_all_required_columns_test;
SET search_path = union_all_required_columns_test, public;

CREATE TABLE delta_t
(
    row_id bigint PRIMARY KEY,
    title text,
    embedding vector(4)
);

CREATE TABLE snapshot_t
(
    row_id bigint PRIMARY KEY,
    title text,
    embedding vector(4)
);

CREATE TABLE delete_t
(
    row_id bigint PRIMARY KEY,
    reason text,
    unused_delete text
);

-- row_id=3 occurs in both live branches to guard UNION ALL duplicates.
INSERT INTO delta_t VALUES
    (3,   'delta-3',   '[3.25,0,0,0]'),
    (101, 'delta-101', '[1.5,0,0,0]'),
    (102, 'delta-102', '[2.5,0,0,0]'),
    (103, 'delta-103', '[3.5,0,0,0]'),
    (104, 'delta-104', '[4.5,0,0,0]'),
    (105, 'delta-105', '[5.5,0,0,0]'),
    (106, 'delta-106', '[6.5,0,0,0]');

INSERT INTO snapshot_t VALUES
    (1, 'snapshot-1', '[1,0,0,0]'),
    (2, 'snapshot-2', '[2,0,0,0]'),
    (3, 'snapshot-3', '[3,0,0,0]'),
    (4, 'snapshot-4', '[4,0,0,0]'),
    (5, 'snapshot-5', '[5,0,0,0]'),
    (6, 'snapshot-6', '[6,0,0,0]');

INSERT INTO delete_t VALUES
    (2, 'deleted', 'must-not-escape'),
    (5, 'deleted', 'must-not-escape'),
    (99, 'other', 'must-not-escape');

CREATE INDEX delta_embedding_diskann ON delta_t
    USING diskann (embedding vector_l2_ops);
CREATE INDEX snapshot_embedding_diskann ON snapshot_t
    USING diskann (embedding vector_l2_ops);

CREATE VIEW union_view AS
SELECT row_id, title, embedding
FROM delta_t
UNION ALL
SELECT s.row_id, s.title, s.embedding
FROM snapshot_t s
WHERE NOT EXISTS
(
    SELECT 1
    FROM delete_t d
    WHERE d.row_id = s.row_id
);

-- Keep the representative path above key-only so its anti-join can use both
-- primary-key indexes.  Exercise a delete-side filter in a separate view.
CREATE VIEW union_view_delete_filter AS
SELECT row_id, title, embedding
FROM delta_t
UNION ALL
SELECT s.row_id, s.title, s.embedding
FROM snapshot_t s
WHERE NOT EXISTS
(
    SELECT 1
    FROM delete_t d
    WHERE d.row_id = s.row_id
      AND d.reason = 'deleted'
);

ANALYZE delta_t;
ANALYZE snapshot_t;
ANALYZE delete_t;

-- Keep plan guards deterministic on the intentionally tiny data set.
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_hashjoin = off;
SET enable_nestloop = off;
SET enable_union_all_diskann_topk_pushdown = on;
SET enable_union_all_required_columns_pushdown = on;

-- -------------------------------------------------------------------------
-- Plan guards: the four target SQL combinations.
-- -------------------------------------------------------------------------

-- WHERE only: all three sources expose row_id only.  The outer recheck and
-- unused title/embedding columns must not reappear.
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT row_id
FROM union_view
WHERE row_id > 2;

-- WHERE + ORDER BY: each branch emits row_id plus a scalar distance.  The
-- vector is consumed inside the branch and is not a UNION boundary column.
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT row_id
FROM union_view
WHERE row_id > 2
ORDER BY embedding <-> '[0,0,0,0]';

-- WHERE + LIMIT: retain one global Limit; never synthesize branch Limits.
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT row_id
FROM union_view
WHERE row_id > 2
LIMIT 4;

-- WHERE + ORDER BY + LIMIT: retain the global Limit and add one Limit per
-- branch.  Snapshot deletion filtering remains below its branch Limit.
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT row_id
FROM union_view
WHERE row_id > 2
ORDER BY embedding <-> '[0,0,0,0]'
LIMIT 4;

-- A non-indexed predicate gives the delta branch a representative DiskANN
-- plan.  title is legal in Filter but must not occur in any Output list.
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT row_id
FROM union_view
WHERE title IS NOT NULL
ORDER BY embedding <-> '[0,0,0,0]'
LIMIT 4;

-- A delete-side filter column ends at its scan Filter.  Only d.row_id reaches
-- the Anti Join; unused_delete never enters a plan Output list.
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT row_id
FROM union_view_delete_filter
WHERE title <> 'never';

-- Required-column pruning remains active without branch Top-K.  There is one
-- global Limit and the UNION still transports a scalar distance.
SET enable_union_all_diskann_topk_pushdown = off;
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT row_id
FROM union_view
WHERE row_id > 2
ORDER BY embedding <-> '[0,0,0,0]'
LIMIT 4;
SET enable_union_all_diskann_topk_pushdown = on;

-- Disabling the new GUC is a guarded compatibility path: original wide leaf
-- outputs and Subquery Scan boundaries remain visible.
SET enable_union_all_required_columns_pushdown = off;
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT row_id
FROM union_view
WHERE row_id > 2
ORDER BY embedding <-> '[0,0,0,0]'
LIMIT 4;
SET enable_union_all_required_columns_pushdown = on;

-- A nested four-leaf UNION ALL guards recursive SetOperationStmt metadata
-- compaction and non-contiguous column renumbering.
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT row_id
FROM
(
    SELECT row_id, title, embedding FROM delta_t WHERE row_id < 101
    UNION ALL
    SELECT row_id, title, embedding FROM delta_t WHERE row_id >= 101
    UNION ALL
    SELECT row_id, title, embedding FROM snapshot_t WHERE row_id < 3
    UNION ALL
    SELECT row_id, title, embedding FROM snapshot_t WHERE row_id >= 3
) u
WHERE row_id > 0;

-- -------------------------------------------------------------------------
-- Correctness guards.
-- -------------------------------------------------------------------------

-- Explicit expected rows guard delete filtering and UNION ALL duplicates.
SELECT row_id, title
FROM union_view
WHERE row_id > 2
ORDER BY row_id, title;

-- Explicit ordered Top-K result guards scalar-key materialization.
SELECT row_id
FROM union_view
WHERE row_id > 2
ORDER BY embedding <-> '[0,0,0,0]', row_id
LIMIT 4;

-- Duplicate-sensitive GUC off/on comparison for all four target forms.
SET enable_union_all_required_columns_pushdown = off;
CREATE TEMP TABLE off_q1 AS
SELECT row_id FROM union_view WHERE row_id > 2;
CREATE TEMP TABLE off_q2 AS
SELECT row_id FROM union_view WHERE row_id > 2
ORDER BY embedding <-> '[0,0,0,0]';
CREATE TEMP TABLE off_q3 AS
SELECT row_id FROM union_view WHERE row_id > 2 LIMIT 4;
CREATE TEMP TABLE off_q4 AS
SELECT row_id FROM union_view WHERE row_id > 2
ORDER BY embedding <-> '[0,0,0,0]' LIMIT 4;

SET enable_union_all_required_columns_pushdown = on;
CREATE TEMP TABLE on_q1 AS
SELECT row_id FROM union_view WHERE row_id > 2;
CREATE TEMP TABLE on_q2 AS
SELECT row_id FROM union_view WHERE row_id > 2
ORDER BY embedding <-> '[0,0,0,0]';
CREATE TEMP TABLE on_q3 AS
SELECT row_id FROM union_view WHERE row_id > 2 LIMIT 4;
CREATE TEMP TABLE on_q4 AS
SELECT row_id FROM union_view WHERE row_id > 2
ORDER BY embedding <-> '[0,0,0,0]' LIMIT 4;

SELECT 'q1' AS case_name,
       (SELECT count(*) FROM off_q1) AS off_rows,
       (SELECT count(*) FROM on_q1) AS on_rows,
       (SELECT count(*) FROM
        ((SELECT * FROM off_q1 EXCEPT ALL SELECT * FROM on_q1)
         UNION ALL
         (SELECT * FROM on_q1 EXCEPT ALL SELECT * FROM off_q1)) d) AS diff_rows
UNION ALL
SELECT 'q2', (SELECT count(*) FROM off_q2), (SELECT count(*) FROM on_q2),
       (SELECT count(*) FROM
        ((SELECT * FROM off_q2 EXCEPT ALL SELECT * FROM on_q2)
         UNION ALL
         (SELECT * FROM on_q2 EXCEPT ALL SELECT * FROM off_q2)) d)
UNION ALL
SELECT 'q3', (SELECT count(*) FROM off_q3), (SELECT count(*) FROM on_q3),
       (SELECT count(*) FROM
        ((SELECT * FROM off_q3 EXCEPT ALL SELECT * FROM on_q3)
         UNION ALL
         (SELECT * FROM on_q3 EXCEPT ALL SELECT * FROM off_q3)) d)
UNION ALL
SELECT 'q4', (SELECT count(*) FROM off_q4), (SELECT count(*) FROM on_q4),
       (SELECT count(*) FROM
        ((SELECT * FROM off_q4 EXCEPT ALL SELECT * FROM on_q4)
         UNION ALL
         (SELECT * FROM on_q4 EXCEPT ALL SELECT * FROM off_q4)) d)
ORDER BY 1;

-- Compare ordered positions, not merely row sets.
SET enable_union_all_required_columns_pushdown = off;
CREATE TEMP TABLE off_order AS
SELECT row_number() OVER () AS seq, row_id
FROM
(
    SELECT row_id FROM union_view WHERE row_id > 2
    ORDER BY embedding <-> '[0,0,0,0]', row_id LIMIT 8
) q;
SET enable_union_all_required_columns_pushdown = on;
CREATE TEMP TABLE on_order AS
SELECT row_number() OVER () AS seq, row_id
FROM
(
    SELECT row_id FROM union_view WHERE row_id > 2
    ORDER BY embedding <-> '[0,0,0,0]', row_id LIMIT 8
) q;
SELECT count(*) AS positional_differences
FROM off_order o FULL JOIN on_order n USING (seq)
WHERE o.row_id IS DISTINCT FROM n.row_id;

-- Projection order and computed expressions remain correct after attno remap.
SELECT length(title) AS title_length, row_id
FROM union_view
WHERE row_id IN (3, 4)
ORDER BY row_id, title;

-- Volatile unused outputs cannot be pruned because evaluation is observable.
CREATE SEQUENCE side_effect_seq START 1;
CREATE VIEW volatile_view AS
SELECT row_id, title, nextval('side_effect_seq') AS side_effect FROM delta_t
UNION ALL
SELECT row_id, title, nextval('side_effect_seq') AS side_effect FROM snapshot_t;

SELECT setval('side_effect_seq', 1, false);
SET enable_union_all_required_columns_pushdown = off;
SELECT count(*) FROM (SELECT row_id FROM volatile_view WHERE row_id > 0) q;
SELECT last_value AS off_last_value FROM side_effect_seq;
SELECT setval('side_effect_seq', 1, false);
SET enable_union_all_required_columns_pushdown = on;
SELECT count(*) FROM (SELECT row_id FROM volatile_view WHERE row_id > 0) q;
SELECT last_value AS on_last_value FROM side_effect_seq;

-- Unsupported shapes use their original planning path and retain semantics.
CREATE VIEW distinct_view AS
SELECT row_id, title, embedding FROM delta_t
UNION
SELECT row_id, title, embedding FROM snapshot_t;

CREATE VIEW leaf_limit_view AS
(SELECT row_id, title, embedding FROM delta_t ORDER BY row_id LIMIT 2)
UNION ALL
(SELECT row_id, title, embedding FROM snapshot_t ORDER BY row_id LIMIT 2);

CREATE VIEW security_barrier_view WITH (security_barrier=true) AS
SELECT * FROM union_view;

EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT row_id FROM distinct_view WHERE row_id > 0;

EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT row_id FROM leaf_limit_view WHERE row_id > 0;

EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT row_id FROM security_barrier_view WHERE row_id > 0;

SELECT count(*) AS distinct_rows FROM distinct_view WHERE row_id > 0;
SELECT array_agg(row_id ORDER BY row_id) AS leaf_limit_rows
FROM leaf_limit_view WHERE row_id > 0;
SELECT count(*) AS barrier_rows FROM security_barrier_view WHERE row_id > 0;

-- Parameterized predicate, vector, and LIMIT exercise prepared-plan mapping.
PREPARE required_cols_plan(bigint, vector, bigint) AS
SELECT row_id FROM union_view WHERE row_id > $1
ORDER BY embedding <-> $2, row_id LIMIT $3;
EXECUTE required_cols_plan(2, '[0,0,0,0]', 4);
DEALLOCATE required_cols_plan;

RESET enable_union_all_required_columns_pushdown;
RESET enable_union_all_diskann_topk_pushdown;
RESET enable_nestloop;
RESET enable_hashjoin;
RESET enable_bitmapscan;
RESET enable_seqscan;
RESET search_path;
DROP SCHEMA union_all_required_columns_test CASCADE;
RESET client_min_messages;
