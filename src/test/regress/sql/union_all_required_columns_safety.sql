-- Semantic boundaries for UNION ALL column pruning and DiskANN Top-K.
SET client_min_messages = warning;
-- In the legacy expression framework hasTargetSRFs may be unset.  Safety
-- checks must inspect targetlist expressions, not rely solely on this flag.
SET enable_expr_fusion = off;
CREATE SCHEMA union_all_required_columns_safety;
SET search_path = union_all_required_columns_safety, public;

-- Five rows suffice for every wrong-result reproducer below.
CREATE TABLE branch_a (row_id int, title text, embedding vector(4));
CREATE TABLE branch_b (LIKE branch_a);
INSERT INTO branch_a VALUES
    (0, 'zero', '[0,0,0,0]'),
    (1, 'one', '[10,0,0,0]'),
    (3, 'three', '[1,0,0,0]');
INSERT INTO branch_b VALUES
    (2, 'two', '[20,0,0,0]'),
    (4, 'four', '[2,0,0,0]');
ANALYZE branch_a;
ANALYZE branch_b;

CREATE VIEW all_rows AS
SELECT * FROM branch_a UNION ALL SELECT * FROM branch_b;

-- Volatility prevents the generic planner from moving this predicate below
-- a volatile-output leaf.  Its rejection of zero must precede division.
CREATE FUNCTION nonzero(int) RETURNS bool LANGUAGE plpgsql VOLATILE AS $$
BEGIN
    RETURN $1 <> 0;
END
$$;
CREATE VIEW volatile_rows AS
SELECT row_id, title, embedding, random() AS side_effect FROM branch_a
UNION ALL
SELECT row_id, title, embedding, random() AS side_effect FROM branch_b;

SET enable_union_all_diskann_topk_pushdown = off;
SET enable_union_all_required_columns_pushdown = off;
SELECT row_id FROM volatile_rows WHERE nonzero(row_id) ORDER BY 100 / row_id;
SET enable_union_all_required_columns_pushdown = on;
-- The division stays above the residual Filter; dead title/vector still go.
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT row_id FROM volatile_rows WHERE nonzero(row_id) ORDER BY 100 / row_id;
SELECT row_id FROM volatile_rows WHERE nonzero(row_id) ORDER BY 100 / row_id;

-- A descendant can inherit narrow-scan policy without passing this Query's
-- column-rewrite eligibility.  Moving sum(n) into its leaves is invalid.
CREATE VIEW aggregate_sublink AS
SELECT 1 AS id,
       (SELECT sum(n) FROM (SELECT 1 AS n UNION ALL SELECT 2 AS n) q
        ORDER BY sum(n) LIMIT 1) AS total,
       'unused'::text AS title
UNION ALL
SELECT 2, 0::bigint, 'unused'::text;
SET enable_union_all_required_columns_pushdown = off;
SELECT id, total FROM aggregate_sublink WHERE id > 0 ORDER BY id;
SET enable_union_all_required_columns_pushdown = on;
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT id, total FROM aggregate_sublink WHERE id > 0 ORDER BY id;
SELECT id, total FROM aggregate_sublink WHERE id > 0 ORDER BY id;

-- ROWNUM labels are assigned before the outer filter.  In branch_a, row 1
-- must retain label 2 and row 3 label 3, even after row 0 is filtered out.
CREATE VIEW numbered_rows AS
SELECT rownum AS rn, row_id, title, embedding FROM branch_a
UNION ALL
SELECT rownum AS rn, row_id, title, embedding FROM branch_b;
SET enable_union_all_required_columns_pushdown = off;
CREATE TEMP TABLE numbered_off AS
SELECT rn, row_id FROM numbered_rows WHERE row_id > 0 ORDER BY row_id;
SELECT rn, row_id FROM numbered_rows WHERE row_id > 0 ORDER BY row_id;
SET enable_union_all_required_columns_pushdown = on;
CREATE TEMP TABLE numbered_on AS
SELECT rn, row_id FROM numbered_rows WHERE row_id > 0 ORDER BY row_id;
SELECT rn, row_id FROM numbered_rows WHERE row_id > 0 ORDER BY row_id;
SELECT count(*) AS rownum_differences FROM
((SELECT * FROM numbered_off EXCEPT ALL SELECT * FROM numbered_on)
 UNION ALL
 (SELECT * FROM numbered_on EXCEPT ALL SELECT * FROM numbered_off)) d;

-- Top-K must not cross the UNION container's own ORDER/LIMIT boundary.
-- The container picks IDs 1,2; the nearest of those is 1, not global ID 3.
CREATE VIEW limited_rows AS
SELECT * FROM branch_a WHERE row_id > 0
UNION ALL
SELECT * FROM branch_b
ORDER BY row_id LIMIT 2;

-- An SRF can emit zero rows.  The nearest input of each branch emits none,
-- so local input-row Top-K cannot implement the outer output-row LIMIT.
-- WITH TIES must retain all five tied rows, not one row from each leaf.
CREATE VIEW tied_rows AS
SELECT row_id, title, '[1,0,0,0]'::vector AS embedding FROM branch_a
UNION ALL
SELECT row_id, title, '[1,0,0,0]'::vector AS embedding FROM branch_b;

-- Baseline: both GUCs off.
SET enable_union_all_required_columns_pushdown = off;
SET enable_union_all_diskann_topk_pushdown = off;
CREATE TEMP TABLE limited_off AS
SELECT row_id FROM limited_rows WHERE row_id > 0
ORDER BY embedding <-> '[0,0,0,0]' LIMIT 1;
SELECT * FROM limited_off;
CREATE TEMP TABLE srf_off AS
SELECT row_id, generate_series(1, CASE WHEN row_id < 3 THEN 1 ELSE 0 END) AS n
FROM all_rows WHERE row_id > 0
ORDER BY embedding <-> '[0,0,0,0]' LIMIT 1;
SELECT * FROM srf_off;
CREATE TEMP TABLE ties_off AS
SELECT row_id FROM tied_rows WHERE row_id >= 0
ORDER BY embedding <-> '[0,0,0,0]' FETCH FIRST 1 ROWS WITH TIES;
SELECT row_id FROM ties_off ORDER BY row_id;

-- Top-K alone must reject these shapes as well.
SET enable_union_all_diskann_topk_pushdown = on;
SELECT row_id FROM limited_rows WHERE row_id > 0
ORDER BY embedding <-> '[0,0,0,0]' LIMIT 1;
SELECT row_id, generate_series(1, CASE WHEN row_id < 3 THEN 1 ELSE 0 END) AS n
FROM all_rows WHERE row_id > 0
ORDER BY embedding <-> '[0,0,0,0]' LIMIT 1;
SELECT row_id FROM
(SELECT row_id FROM tied_rows WHERE row_id >= 0
 ORDER BY embedding <-> '[0,0,0,0]' FETCH FIRST 1 ROWS WITH TIES) q
ORDER BY row_id;

-- Column pruning alone also falls back safely for the special shapes.
SET enable_union_all_required_columns_pushdown = on;
SET enable_union_all_diskann_topk_pushdown = off;
SELECT row_id FROM limited_rows WHERE row_id > 0
ORDER BY embedding <-> '[0,0,0,0]' LIMIT 1;
SELECT row_id, generate_series(1, CASE WHEN row_id < 3 THEN 1 ELSE 0 END) AS n
FROM all_rows WHERE row_id > 0
ORDER BY embedding <-> '[0,0,0,0]' LIMIT 1;
SELECT row_id FROM
(SELECT row_id FROM tied_rows WHERE row_id >= 0
 ORDER BY embedding <-> '[0,0,0,0]' FETCH FIRST 1 ROWS WITH TIES) q
ORDER BY row_id;

-- Both on: compare duplicate-sensitive results as well as exact expected IDs.
SET enable_union_all_diskann_topk_pushdown = on;
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT row_id FROM limited_rows WHERE row_id > 0
ORDER BY embedding <-> '[0,0,0,0]' LIMIT 1;
CREATE TEMP TABLE limited_on AS
SELECT row_id FROM limited_rows WHERE row_id > 0
ORDER BY embedding <-> '[0,0,0,0]' LIMIT 1;
CREATE TEMP TABLE srf_on AS
SELECT row_id, generate_series(1, CASE WHEN row_id < 3 THEN 1 ELSE 0 END) AS n
FROM all_rows WHERE row_id > 0
ORDER BY embedding <-> '[0,0,0,0]' LIMIT 1;
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT row_id FROM tied_rows WHERE row_id >= 0
ORDER BY embedding <-> '[0,0,0,0]' FETCH FIRST 1 ROWS WITH TIES;
CREATE TEMP TABLE ties_on AS
SELECT row_id FROM tied_rows WHERE row_id >= 0
ORDER BY embedding <-> '[0,0,0,0]' FETCH FIRST 1 ROWS WITH TIES;
SELECT 'limited' AS case_name, count(*) AS differences FROM
((SELECT * FROM limited_off EXCEPT ALL SELECT * FROM limited_on)
 UNION ALL
 (SELECT * FROM limited_on EXCEPT ALL SELECT * FROM limited_off)) d
UNION ALL
SELECT 'srf', count(*) FROM
((SELECT * FROM srf_off EXCEPT ALL SELECT * FROM srf_on)
 UNION ALL
 (SELECT * FROM srf_on EXCEPT ALL SELECT * FROM srf_off)) d
UNION ALL
SELECT 'ties', count(*) FROM
((SELECT * FROM ties_off EXCEPT ALL SELECT * FROM ties_on)
 UNION ALL
 (SELECT * FROM ties_on EXCEPT ALL SELECT * FROM ties_off)) d
ORDER BY 1;

-- A derived order key may still be materialized after a safe WHERE; retain
-- an explicit success case beside the rejection tests.
SELECT row_id FROM all_rows WHERE row_id > 0
ORDER BY embedding <-> '[0,0,0,0]', row_id LIMIT 2;

-- Differential matrix: reordered/constant projections, duplicate branches,
-- explicit aliases, expressions, multiple sort keys, coercions and offsets.
-- Every row has a deterministic full result or a uniquely ordered LIMIT.
CREATE TABLE differential_cases (sql_text text);
INSERT INTO differential_cases VALUES
('SELECT row_id FROM all_rows WHERE row_id >= 0'),
('SELECT title, row_id FROM all_rows WHERE row_id >= 0'),
('SELECT embedding FROM all_rows WHERE title IS NOT NULL'),
('SELECT 42 AS answer FROM all_rows WHERE row_id >= 0'),
('SELECT row_id + 10 AS id, length(title) AS len FROM all_rows WHERE title IS NOT NULL'),
('SELECT row_id FROM all_rows WHERE row_id >= 0 ORDER BY embedding <-> ''[0,0,0,0]'', row_id LIMIT 3'),
('SELECT row_id FROM all_rows WHERE row_id >= 0 ORDER BY embedding <-> ''[0,0,0,0]'' DESC NULLS FIRST, row_id LIMIT 3'),
('SELECT row_id FROM all_rows WHERE row_id >= 0 ORDER BY length(title), -row_id LIMIT 3'),
('SELECT embedding <-> ''[0,0,0,0]'' AS distance FROM all_rows WHERE row_id >= 0 ORDER BY distance LIMIT 3'),
('SELECT row_id FROM all_rows WHERE row_id >= 0 ORDER BY embedding <-> ''[0,0,0,0]'', row_id LIMIT 2 OFFSET 1'),
('SELECT u.id FROM (SELECT * FROM branch_a UNION ALL SELECT * FROM branch_b) u(id, name, vec) WHERE u.id >= 0 ORDER BY u.vec <-> ''[0,0,0,0]'' LIMIT 3'),
('SELECT id FROM (SELECT row_id AS id, title FROM branch_a UNION ALL SELECT row_id, title FROM branch_a UNION ALL SELECT row_id, title FROM branch_b) u WHERE id >= 0 ORDER BY length(title), id'),
('SELECT title FROM (SELECT row_id, title::varchar(5) AS title FROM branch_a UNION ALL SELECT row_id, title::varchar(12) FROM branch_b) u WHERE row_id >= 0 ORDER BY length(title), title'),
('SELECT row_id FROM all_rows WHERE row_id >= 0 AND row_id < 0 ORDER BY embedding <-> ''[0,0,0,0]'' LIMIT 3');

CREATE TABLE differential_summary (checked int, differences int);
DO $$
DECLARE
    test_query text;
    result_sql text;
    baseline text[];
    optimized text[];
    mode int;
    checked int := 0;
    differences int := 0;
BEGIN
    FOR test_query IN SELECT sql_text FROM differential_cases LOOP
        -- Dynamic EXECUTE forces planning under each GUC combination.
        result_sql := 'SELECT array_agg(q::text ORDER BY q::text) FROM (' || test_query || ') q';
        PERFORM set_config('enable_union_all_required_columns_pushdown', 'off', true);
        PERFORM set_config('enable_union_all_diskann_topk_pushdown', 'off', true);
        EXECUTE result_sql INTO baseline;
        FOR mode IN 1..3 LOOP
            PERFORM set_config('enable_union_all_required_columns_pushdown',
                               CASE WHEN mode IN (1, 3) THEN 'on' ELSE 'off' END, true);
            PERFORM set_config('enable_union_all_diskann_topk_pushdown',
                               CASE WHEN mode IN (2, 3) THEN 'on' ELSE 'off' END, true);
            EXECUTE result_sql INTO optimized;
            checked := checked + 1;
            IF baseline IS DISTINCT FROM optimized THEN
                differences := differences + 1;
            END IF;
        END LOOP;
    END LOOP;
    INSERT INTO differential_summary VALUES (checked, differences);
END
$$;
SELECT * FROM differential_summary;

-- Sort nodes share their child's targetlist.  With no row_id indexes, a
-- forced Merge Anti Join builds Sort -> Seq Scan on its preserved side.  The
-- source-projection rewrite must update that shared TargetEntry only once.
CREATE TABLE shared_tlist_delta (row_id int, title text, embedding vector(4));
CREATE TABLE shared_tlist_snapshot (row_id int, title text, embedding vector(4));
CREATE TABLE shared_tlist_delete (row_id int);
INSERT INTO shared_tlist_delta VALUES
    (3, 'delta-3', '[3.25,0,0,0]'),
    (10, 'delta-10', '[1.5,0,0,0]');
INSERT INTO shared_tlist_snapshot VALUES
    (1, 'snapshot-1', '[1,0,0,0]'),
    (3, 'snapshot-3', '[3,0,0,0]'),
    (4, 'snapshot-4', '[4,0,0,0]');
INSERT INTO shared_tlist_delete VALUES (1);
ANALYZE shared_tlist_delta;
ANALYZE shared_tlist_snapshot;
ANALYZE shared_tlist_delete;
CREATE VIEW shared_tlist_rows AS
SELECT row_id, title, embedding FROM shared_tlist_delta
UNION ALL
SELECT s.row_id, s.title, s.embedding FROM shared_tlist_snapshot s
WHERE NOT EXISTS
    (SELECT 1 FROM shared_tlist_delete d WHERE d.row_id = s.row_id);
SET enable_hashjoin = off;
SET enable_nestloop = off;
SET enable_mergejoin = on;
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT row_id FROM shared_tlist_rows WHERE row_id > 2
ORDER BY embedding <-> '[0,0,0,0]', row_id LIMIT 4;
SELECT row_id FROM shared_tlist_rows WHERE row_id > 2
ORDER BY embedding <-> '[0,0,0,0]', row_id LIMIT 4;
RESET enable_mergejoin;
RESET enable_nestloop;
RESET enable_hashjoin;

-- Source projection must fail closed when early distance evaluation can
-- change errors.  Row 1 has the wrong dimension but is deleted before the
-- distance expression.  An unconstrained vector column therefore remains a
-- raw scan output until after the Anti Join.
CREATE TABLE loose_delta (row_id int, title text, embedding vector);
CREATE TABLE loose_snapshot (row_id int, title text, embedding vector);
CREATE TABLE loose_delete (row_id int);
INSERT INTO loose_delta VALUES (10, 'keep', '[10,20,30,40]');
INSERT INTO loose_snapshot VALUES
    (1, 'deleted-bad-dimension', '[1,2,3]'),
    (2, 'keep', '[2,3,4,5]');
INSERT INTO loose_delete VALUES (1);
CREATE VIEW loose_rows AS
SELECT row_id, title, embedding FROM loose_delta
UNION ALL
SELECT s.row_id, s.title, s.embedding FROM loose_snapshot s
WHERE NOT EXISTS (SELECT 1 FROM loose_delete d WHERE d.row_id = s.row_id);
SET enable_union_all_required_columns_pushdown = on;
SET enable_union_all_diskann_topk_pushdown = on;
SELECT row_id FROM loose_rows WHERE title IS NOT NULL
ORDER BY embedding <-> '[1,2,3,4]'::vector, row_id LIMIT 10;

-- Exercise a genuinely generic Param plan, not only first-execution custom
-- planning where bind values have already become constants.
SET plan_cache_mode = force_generic_plan;
PREPARE generic_safe(int, vector, bigint) AS
SELECT row_id FROM all_rows WHERE row_id > $1
ORDER BY embedding <-> $2, row_id LIMIT $3;
EXECUTE generic_safe(0, '[0,0,0,0]', 2);
EXECUTE generic_safe(1, '[0,0,0,0]', 1);
EXECUTE generic_safe(0, '[0,0,0,0]', 0);
DEALLOCATE generic_safe;
RESET plan_cache_mode;

-- Also guard the modern expression framework and an SRF in the UNION leaf.
SET enable_expr_fusion = on;
SELECT row_id, generate_series(1, CASE WHEN row_id < 3 THEN 1 ELSE 0 END) AS n
FROM all_rows WHERE row_id > 0
ORDER BY embedding <-> '[0,0,0,0]' LIMIT 1;
SELECT row_id, n FROM
(SELECT row_id, generate_series(1, row_id) AS n FROM branch_a
 UNION ALL
 SELECT row_id, generate_series(1, row_id) AS n FROM branch_b) u
WHERE n > 2 ORDER BY row_id, n;
SET enable_expr_fusion = off;
SELECT row_id, n FROM
(SELECT row_id, generate_series(1, row_id) AS n FROM branch_a
 UNION ALL
 SELECT row_id, generate_series(1, row_id) AS n FROM branch_b) u
WHERE n > 2 ORDER BY row_id, n;

DROP TABLE numbered_off, numbered_on, limited_off, limited_on,
           srf_off, srf_on, ties_off, ties_on;
RESET enable_union_all_required_columns_pushdown;
RESET enable_union_all_diskann_topk_pushdown;
RESET enable_expr_fusion;
RESET search_path;
DROP SCHEMA union_all_required_columns_safety CASCADE;
RESET client_min_messages;
