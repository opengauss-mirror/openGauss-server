-- Required-column pruning now accepts an absent outer WHERE.
-- Keep the complete UNION/Anti Join safety checks: only the syntactic WHERE
-- requirement is relaxed. Repeated user projections must keep seven columns.

SET client_min_messages = warning;
DROP SCHEMA IF EXISTS union_all_no_where_test CASCADE;
CREATE SCHEMA union_all_no_where_test;
SET search_path = union_all_no_where_test, public;

CREATE TABLE delta_t
(
    id bigint, row_id bigint, embedding vector(4),
    n1 integer, n2 integer, n3 integer, n4 integer, unused_text text
);
CREATE TABLE snapshot_t
(
    id bigint, row_id bigint, embedding vector(4),
    n1 integer, n2 integer, n3 integer, n4 integer, unused_text text
);
CREATE TABLE delete_t(row_id bigint);

-- Small deterministic data: both distances increase strictly with the first
-- coordinate. No snapshot primary key; only snapshot has vector indexes.
INSERT INTO delta_t
SELECT 100+g, 100+g, ('[' || (12+g) || ',2,2,2]')::vector,
       g, g+1, g+2, g+3, 'unused-delta'
FROM generate_series(1,4) g;
INSERT INTO snapshot_t
SELECT g, g, ('[' || (2+g) || ',2,2,2]')::vector,
       g, g+1, g+2, g+3, 'unused-snapshot'
FROM generate_series(1,6) g;
INSERT INTO delete_t VALUES (2), (5);

CREATE INDEX delete_row_id_idx ON delete_t(row_id);
CREATE INDEX snapshot_l2_idx ON snapshot_t
    USING diskann(embedding vector_l2_ops);
CREATE INDEX snapshot_cosine_idx ON snapshot_t
    USING diskann(embedding vector_cosine_ops);

CREATE VIEW union_view AS
SELECT id, embedding, n1, n2, n3, n4, unused_text FROM delta_t
UNION ALL
SELECT s.id, s.embedding, s.n1, s.n2, s.n3, s.n4, s.unused_text
FROM snapshot_t s
WHERE NOT EXISTS (SELECT 1 FROM delete_t d WHERE d.row_id=s.row_id);

ANALYZE delta_t;
ANALYZE snapshot_t;
ANALYZE delete_t;

SET query_dop = 1;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET enable_nestloop = on;
SET enable_union_all_diskann_topk_pushdown = on;

-- L2: local Top-K alone does not export the branch resjunk key through UNION.
-- Pruning must materialize that key, then setrefs can build transparent Vars.
SET ann_orderby_distance_reuse = l2;
SET enable_union_all_required_columns_pushdown = off;
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT id,id,id,n1,n2,n3,n4 FROM union_view
ORDER BY embedding <-> '[2,2,2,2]'::vector LIMIT 100;

SET enable_union_all_required_columns_pushdown = on;
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT id,id,id,n1,n2,n3,n4 FROM union_view
ORDER BY embedding <-> '[2,2,2,2]'::vector LIMIT 100;
SELECT id,id,id,n1,n2,n3,n4 FROM union_view
ORDER BY embedding <-> '[2,2,2,2]'::vector LIMIT 100;

-- Cosine exercises the approximate-consumer gate. Do not relax the Result
-- whitelist to compensate for a missing branch-local scalar projection.
SET ann_orderby_distance_reuse = cosine;
SET enable_union_all_required_columns_pushdown = off;
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT id,id,id,n1,n2,n3,n4 FROM union_view
ORDER BY embedding <=> '[2,2,2,2]'::vector LIMIT 100;

SET enable_union_all_required_columns_pushdown = on;
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT id,id,id,n1,n2,n3,n4 FROM union_view
ORDER BY embedding <=> '[2,2,2,2]'::vector LIMIT 100;
SELECT id,id,id,n1,n2,n3,n4 FROM union_view
ORDER BY embedding <=> '[2,2,2,2]'::vector LIMIT 100;

-- Each dynamic SELECT is planned independently, so the assertion aggregate
-- cannot disable pruning/reuse inside the query being checked. Sorting only
-- the captured IDs avoids depending on an unordered query's emission order.
CREATE FUNCTION capture_ids(query_text text) RETURNS SETOF bigint AS $$
BEGIN
    RETURN QUERY EXECUTE query_text;
END;
$$ LANGUAGE plpgsql;

SET ann_orderby_distance_reuse = l2;
SET enable_union_all_required_columns_pushdown = off;
SELECT 'plain' AS scenario, array_agg(id ORDER BY id) AS ids
FROM capture_ids('SELECT id FROM union_view') AS captured(id);
SELECT 'limit_only' AS scenario, array_agg(id ORDER BY id) AS ids
FROM capture_ids('SELECT id FROM union_view LIMIT 100') AS captured(id);
SELECT 'order_only' AS scenario, array_agg(id ORDER BY id) AS ids
FROM capture_ids(
    'SELECT id FROM union_view ORDER BY embedding <-> ''[2,2,2,2]''::vector') AS captured(id);

SET enable_union_all_required_columns_pushdown = on;
SELECT 'plain' AS scenario, array_agg(id ORDER BY id) AS ids
FROM capture_ids('SELECT id FROM union_view') AS captured(id);
SELECT 'limit_only' AS scenario, array_agg(id ORDER BY id) AS ids
FROM capture_ids('SELECT id FROM union_view LIMIT 100') AS captured(id);
SELECT 'order_only' AS scenario, array_agg(id ORDER BY id) AS ids
FROM capture_ids(
    'SELECT id FROM union_view ORDER BY embedding <-> ''[2,2,2,2]''::vector') AS captured(id);

RESET ann_orderby_distance_reuse;
RESET enable_union_all_required_columns_pushdown;
RESET enable_union_all_diskann_topk_pushdown;
RESET enable_nestloop;
RESET enable_mergejoin;
RESET enable_hashjoin;
RESET enable_bitmapscan;
RESET enable_seqscan;
RESET query_dop;
RESET search_path;
DROP SCHEMA union_all_no_where_test CASCADE;
RESET client_min_messages;
