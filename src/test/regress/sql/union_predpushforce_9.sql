DROP SCHEMA IF EXISTS union_predpushforce_9 CASCADE;
CREATE SCHEMA union_predpushforce_9;
SET search_path = union_predpushforce_9;
SET rewrite_rule = 'none';

CREATE TABLE src_a (id integer, k integer);
CREATE TABLE src_b (id integer, k integer);
CREATE TABLE fact_a (col1 integer, col2 integer, amount integer);
CREATE TABLE fact_b (col1 integer, col2 integer, amount integer);
INSERT INTO src_a VALUES (1, 2), (2, 3);
INSERT INTO src_b VALUES (1, 4), (2, 5);
INSERT INTO fact_a
SELECT g % 100 + 1, (g / 100) % 10 + 1, 1
FROM generate_series(0, 4999) g;
INSERT INTO fact_b SELECT * FROM fact_a;
ANALYZE src_a;
ANALYZE src_b;
ANALYZE fact_a;
ANALYZE fact_b;

CREATE VIEW v_all AS
WITH keep_native_union AS (SELECT 1 AS marker)
SELECT col1, col2, amount FROM fact_a
UNION ALL
SELECT col1, col2, amount FROM fact_b
ORDER BY col1, col2, amount;

CREATE VIEW v_distinct AS
SELECT col1, col2, amount FROM fact_a
UNION
SELECT col1, col2, amount FROM fact_b
ORDER BY col1, col2, amount;

CREATE VIEW v_all_fixed AS
WITH keep_native_union AS (SELECT 1 AS marker)
SELECT col1, col2, amount FROM fact_a WHERE col1 = 7
UNION ALL
SELECT col1, col2, amount FROM fact_b WHERE col1 = 7
ORDER BY col1, col2, amount;

CREATE TEMP TABLE baseline_all AS
SELECT a.id AS a_id, b.id AS b_id, v.col1, v.col2, v.amount
FROM src_a a CROSS JOIN src_b b
LEFT JOIN v_all v ON v.col1 = a.k AND v.col2 = b.k;

CREATE TEMP TABLE baseline_distinct AS
SELECT a.id AS a_id, b.id AS b_id, v.col1, v.col2, v.amount
FROM src_a a CROSS JOIN src_b b
LEFT JOIN v_distinct v ON v.col1 = a.k AND v.col2 = b.k;

CREATE INDEX fact_a_prefix ON fact_a (col1, col2);

SELECT 'CASE 01_all_first_branch_head_then_tail';
-- Both a and b contribute to fact_a's prefix; fact_b has no index.
EXPLAIN (COSTS OFF)
SELECT /*+ set(rewrite_rule predpushforce) */ a.id, b.id, v.col1, v.col2, v.amount
FROM src_a a CROSS JOIN src_b b
LEFT JOIN v_all v ON v.col1 = a.k AND v.col2 = b.k;

SELECT 'CASE 02_all_first_branch_tail_then_head';
-- a alone supplies only col2. Do not discard it before b supplies col1.
EXPLAIN (COSTS OFF)
SELECT /*+ set(rewrite_rule predpushforce) */ a.id, b.id, v.col1, v.col2, v.amount
FROM src_a a CROSS JOIN src_b b
LEFT JOIN v_all v ON v.col2 = a.k AND v.col1 = b.k;

DROP INDEX fact_a_prefix;
CREATE INDEX fact_b_prefix ON fact_b (col1, col2);

SELECT 'CASE 03_all_only_second_branch_matches';
-- Empty matches in the first branch must not reject the second branch's match.
EXPLAIN (COSTS OFF)
SELECT /*+ set(rewrite_rule predpushforce) */ a.id, b.id, v.col1, v.col2, v.amount
FROM src_a a CROSS JOIN src_b b
LEFT JOIN v_all v ON v.col1 = a.k AND v.col2 = b.k;

CREATE TEMP TABLE actual_all AS
SELECT /*+ set(rewrite_rule predpushforce) */ a.id AS a_id, b.id AS b_id, v.col1, v.col2, v.amount
FROM src_a a CROSS JOIN src_b b
LEFT JOIN v_all v ON v.col1 = a.k AND v.col2 = b.k;

SELECT 'CASE 04_only_second_key_rejected';
-- The only proposed condition addresses col2; no candidate fills col1.
EXPLAIN (COSTS OFF)
SELECT /*+ set(rewrite_rule predpushforce) */ a.id, v.col1, v.col2, v.amount
FROM src_a a
LEFT JOIN v_all v ON v.col2 = a.k;

SELECT 'CASE 05_fixed_first_key_does_not_complete_candidate_prefix';
-- Existing col1=7 may still use the index as a STATIC scan condition.
-- It must not admit the new col2=a.k predicate under the candidate-only rule.
EXPLAIN (COSTS OFF)
SELECT /*+ set(rewrite_rule predpushforce) */ a.id, v.col1, v.col2, v.amount
FROM src_a a
LEFT JOIN v_all_fixed v ON v.col2 = a.k;

SELECT 'CASE 06_union_distinct_only_second_branch_matches';
EXPLAIN (COSTS OFF)
SELECT /*+ set(rewrite_rule predpushforce) */ a.id, b.id, v.col1, v.col2, v.amount
FROM src_a a CROSS JOIN src_b b
LEFT JOIN v_distinct v ON v.col1 = a.k AND v.col2 = b.k;

CREATE TEMP TABLE actual_distinct AS
SELECT /*+ set(rewrite_rule predpushforce) */ a.id AS a_id, b.id AS b_id, v.col1, v.col2, v.amount
FROM src_a a CROSS JOIN src_b b
LEFT JOIN v_distinct v ON v.col1 = a.k AND v.col2 = b.k;

-- Also execute SELECT directly, so the result checks are not limited to CTAS.
SELECT /*+ set(rewrite_rule predpushforce) */ 'direct_all' AS diagnostic,
       COUNT(*) AS result_rows, SUM(v.amount) AS total
FROM src_a a CROSS JOIN src_b b
LEFT JOIN v_all v ON v.col1 = a.k AND v.col2 = b.k;

SELECT /*+ set(rewrite_rule predpushforce) */ 'direct_distinct' AS diagnostic,
       COUNT(*) AS result_rows, SUM(v.amount) AS total
FROM src_a a CROSS JOIN src_b b
LEFT JOIN v_distinct v ON v.col1 = a.k AND v.col2 = b.k;

DROP INDEX fact_b_prefix;

SELECT 'CASE 07_no_index_auto_force';
EXPLAIN (COSTS OFF)
SELECT /*+ set(rewrite_rule predpushforce) */ a.id, b.id, v.col1, v.col2, v.amount
FROM src_a a CROSS JOIN src_b b
LEFT JOIN v_all v ON v.col1 = a.k AND v.col2 = b.k;

SELECT 'CASE 08_no_index_rewrite_none';
EXPLAIN (COSTS OFF)
SELECT /*+ set(rewrite_rule none) */ a.id, b.id, v.col1, v.col2, v.amount
FROM src_a a CROSS JOIN src_b b
LEFT JOIN v_all v ON v.col1 = a.k AND v.col2 = b.k;

SET rewrite_rule = 'predpush';
SELECT 'CASE 09_no_index_nonforce_predpush';
EXPLAIN (COSTS OFF)
SELECT a.id, b.id, v.col1, v.col2, v.amount
FROM src_a a CROSS JOIN src_b b
LEFT JOIN v_all v ON v.col1 = a.k AND v.col2 = b.k;

SET rewrite_rule = 'none';
SELECT 'CASE 10_no_index_explicit_force_hint';
EXPLAIN (COSTS OFF)
SELECT /*+ set(rewrite_rule predpushforce) predpush(a b) */ a.id, b.id, v.col1, v.col2, v.amount
FROM src_a a CROSS JOIN src_b b
LEFT JOIN v_all v ON v.col1 = a.k AND v.col2 = b.k;

RESET rewrite_rule;
RESET search_path;
DROP SCHEMA union_predpushforce_9 CASCADE;
