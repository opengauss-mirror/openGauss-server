-- UNION ALL Fast-Start Reorder coverage test
--
-- Coverage target: >=90% of unionall_faststart.cpp lines.
-- Determinism measures:
--   1. EXPLAIN (COSTS OFF): hides cost/rows numbers, keeps only plan shape.
--   2. No indexes on shared test tables: Seq Scan only, no scan-method
--      ambiguity. The index-ordered branch case uses a dedicated table.
--   3. Every qsort-path scenario uses branches with DISTINCT startup costs
--      (sort branches or OFFSET branches), because make_sorted_list tie-breaks
--      by pointer address, which is not portable across environments.
--   4. Enumeration paths (2-4 branches) are deterministic by construction:
--      strict cost comparison keeps the first-seen best order on ties.
--   5. Leaf branches carrying ORDER BY/LIMIT/FETCH are parenthesized: the
--      grammar only allows these clauses at the end of a whole set operation.
--   6. PERCENT / WITH TIES use FETCH FIRST standard syntax; LIMIT n PERCENT
--      is not supported by this version.
-- Data: created once, reused across all scenarios.
-- ----------------------------------------------------------------------
-- Setup: tables used by all scenarios
-- ----------------------------------------------------------------------

CREATE SCHEMA union_all_faststart_cov;
SET search_path TO union_all_faststart_cov;

CREATE TABLE union_all_big(id int, sort_key int);
CREATE TABLE union_all_mid(id int, sort_key int);
CREATE TABLE union_all_small(id int);
CREATE TABLE union_all_tiny(id int);
CREATE TABLE union_all_join1(id int, val int);
CREATE TABLE union_all_join2(id int, val int);
CREATE TABLE union_all_empty(id int);

INSERT INTO union_all_big SELECT n, (n * 791) % 10000 FROM generate_series(1, 10000) n;
INSERT INTO union_all_mid SELECT n, (n * 7919) % 10000 FROM generate_series(1, 1000) n;
INSERT INTO union_all_small VALUES (1);
INSERT INTO union_all_tiny VALUES (10), (20);
INSERT INTO union_all_join1 SELECT n, n * 2 FROM generate_series(1, 500) n;
INSERT INTO union_all_join2 SELECT n, n * 3 FROM generate_series(1, 500) n;
-- union_all_empty stays empty

ANALYZE union_all_big;
ANALYZE union_all_mid;
ANALYZE union_all_small;
ANALYZE union_all_tiny;
ANALYZE union_all_join1;
ANALYZE union_all_join2;
ANALYZE union_all_empty;

-- ----------------------------------------------------------------------
-- Baseline: 2 branches, none vs faststart (proof feature is active)
-- A 1-row branch versus a filtered scan of 10,000 rows is sufficient.
-- ----------------------------------------------------------------------

SET rewrite_rule = 'none';
EXPLAIN (COSTS OFF) SELECT id FROM union_all_big WHERE id = 10000 UNION ALL SELECT id FROM union_all_small LIMIT 1;

SET rewrite_rule = 'union_all_faststart';
EXPLAIN (COSTS OFF) SELECT id FROM union_all_big WHERE id = 10000 UNION ALL SELECT id FROM union_all_small LIMIT 1;

-- The remaining cases use five rows to retain the multi-branch LIMIT
-- boundaries and the result checks below.
INSERT INTO union_all_small VALUES (2), (3), (4), (5);
ANALYZE union_all_small;

-- From here on, only faststart is used for coverage.
SET rewrite_rule = 'union_all_faststart';

-- ----------------------------------------------------------------------
-- 2 branches: Path entry with Sort branch, reorder happens
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT * FROM ((SELECT id FROM union_all_big ORDER BY sort_key LIMIT 100) UNION ALL SELECT id FROM union_all_small) t LIMIT 5;

-- ----------------------------------------------------------------------
-- 2 branches: Path entry, needrows exceeds first branch rows
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT * FROM (SELECT id FROM union_all_tiny UNION ALL SELECT id FROM union_all_small) t LIMIT 4;

-- ----------------------------------------------------------------------
-- 2 branches: Path entry, original order already optimal (no swap)
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT * FROM (SELECT id FROM union_all_small UNION ALL (SELECT id FROM union_all_big ORDER BY sort_key LIMIT 100)) t LIMIT 5;

-- ----------------------------------------------------------------------
-- 2 branches: OFFSET + LIMIT (needrows = OFFSET + LIMIT)
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT id FROM union_all_big WHERE id < 50 UNION ALL SELECT id FROM union_all_small OFFSET 20 LIMIT 5;

-- ----------------------------------------------------------------------
-- 2 branches: Plan entry, original order already optimal (no swap)
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT id FROM union_all_small UNION ALL SELECT id FROM union_all_big WHERE id < 50 LIMIT 5;

-- ----------------------------------------------------------------------
-- 2 branches: Plan entry, needrows exceeds first branch rows
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT id FROM union_all_tiny UNION ALL SELECT id FROM union_all_small LIMIT 4;

-- ----------------------------------------------------------------------
-- 3 branches: enumerate 6 orders, prefix satisfied, suffix sorted
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT * FROM ((SELECT id FROM union_all_big ORDER BY sort_key LIMIT 10) UNION ALL SELECT id FROM union_all_mid UNION ALL SELECT id FROM union_all_small) t LIMIT 5;

-- ----------------------------------------------------------------------
-- 3 branches: needrows >= total rows, direct startup_cost sort
-- (distinct sort startup costs keep qsort order deterministic)
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT * FROM ((SELECT id FROM union_all_big ORDER BY sort_key LIMIT 10) UNION ALL (SELECT id FROM union_all_mid ORDER BY sort_key LIMIT 10) UNION ALL (SELECT id FROM union_all_small ORDER BY id LIMIT 10)) t LIMIT 2000;

-- ----------------------------------------------------------------------
-- 3 branches: empty branch (rows clamped to 1), partial cost guard
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT * FROM ((SELECT id FROM union_all_big ORDER BY sort_key LIMIT 10) UNION ALL SELECT id FROM union_all_empty UNION ALL SELECT id FROM union_all_small) t LIMIT 5;

-- ----------------------------------------------------------------------
-- 3 branches: Path entry, needrows exceeds first two branch rows
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT * FROM (SELECT id FROM union_all_tiny UNION ALL SELECT id FROM union_all_small UNION ALL SELECT id FROM union_all_big WHERE id < 10) t LIMIT 8;

-- ----------------------------------------------------------------------
-- 3 branches: Plan entry, needrows exceeds first two branch rows
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT id FROM union_all_tiny UNION ALL SELECT id FROM union_all_small UNION ALL SELECT id FROM union_all_big WHERE id < 10 LIMIT 8;

-- ----------------------------------------------------------------------
-- 3 branches: Plan entry, first branch covers needrows, suffix swap
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT id FROM union_all_mid UNION ALL (SELECT id FROM union_all_big ORDER BY sort_key LIMIT 10) UNION ALL SELECT id FROM union_all_small LIMIT 5;

-- ----------------------------------------------------------------------
-- 4 branches: enumerate 24 orders, prefix satisfied, suffix sorted
-- (OFFSET branch gives suffix elements distinct startup costs for qsort)
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT * FROM ((SELECT id FROM union_all_big ORDER BY sort_key LIMIT 10) UNION ALL (SELECT id FROM union_all_mid ORDER BY sort_key LIMIT 10) UNION ALL SELECT id FROM union_all_small UNION ALL (SELECT id FROM union_all_tiny OFFSET 1)) t LIMIT 5;

-- ----------------------------------------------------------------------
-- 4 branches: needrows >= total rows, direct startup_cost sort
-- (distinct startup costs; OFFSET 9000 leaves 1000 rows, so the four
--  branches return at most 1025 rows, below the outer LIMIT 2000)
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT * FROM ((SELECT id FROM union_all_big OFFSET 9000) UNION ALL (SELECT id FROM union_all_big ORDER BY sort_key LIMIT 10) UNION ALL (SELECT id FROM union_all_mid ORDER BY sort_key LIMIT 10) UNION ALL (SELECT id FROM union_all_small ORDER BY id LIMIT 10)) t LIMIT 2000;

-- ----------------------------------------------------------------------
-- 4 branches: Plan entry, needrows reaches 4th branch in enumeration
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT id FROM union_all_small UNION ALL SELECT id FROM union_all_tiny UNION ALL SELECT 999 UNION ALL SELECT id FROM union_all_big WHERE id < 10 LIMIT 9;

-- ----------------------------------------------------------------------
-- 5 branches: within threshold, sort by startup_cost
-- (distinct startup costs: bare scan < mid-sort < big OFFSET 4000
--  < big OFFSET 7000 < big-sort)
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT * FROM (
  (SELECT id FROM union_all_big ORDER BY sort_key LIMIT 10)
  UNION ALL (SELECT id FROM union_all_mid ORDER BY sort_key LIMIT 10)
  UNION ALL (SELECT id FROM union_all_big OFFSET 7000)
  UNION ALL (SELECT id FROM union_all_big OFFSET 4000)
  UNION ALL SELECT id FROM union_all_small
) t LIMIT 5;

-- ----------------------------------------------------------------------
-- 5 branches: needrows > threshold, no reorder
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT * FROM (
  (SELECT id FROM union_all_big ORDER BY sort_key LIMIT 10)
  UNION ALL (SELECT id FROM union_all_mid ORDER BY sort_key LIMIT 10)
  UNION ALL (SELECT id FROM union_all_big OFFSET 7000)
  UNION ALL (SELECT id FROM union_all_big OFFSET 4000)
  UNION ALL SELECT id FROM union_all_small
) t LIMIT 101;

-- ----------------------------------------------------------------------
-- 5 branches: needrows == threshold, reorder allowed
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT * FROM (
  (SELECT id FROM union_all_big ORDER BY sort_key LIMIT 10)
  UNION ALL (SELECT id FROM union_all_mid ORDER BY sort_key LIMIT 10)
  UNION ALL (SELECT id FROM union_all_big OFFSET 7000)
  UNION ALL (SELECT id FROM union_all_big OFFSET 4000)
  UNION ALL SELECT id FROM union_all_small
) t LIMIT 100;

-- ----------------------------------------------------------------------
-- 5 branches: Plan entry, sort by startup_cost
-- (distinct OFFSET startup costs: empty < big < mid < small < tiny)
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) (SELECT id FROM union_all_mid OFFSET 2) UNION ALL (SELECT id FROM union_all_small OFFSET 1) UNION ALL (SELECT id FROM union_all_tiny OFFSET 1) UNION ALL (SELECT id FROM union_all_big OFFSET 1) UNION ALL (SELECT id FROM union_all_empty) LIMIT 5;

-- ----------------------------------------------------------------------
-- 5 branches: Plan entry, needrows > threshold, no reorder
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) (SELECT id FROM union_all_mid OFFSET 2) UNION ALL (SELECT id FROM union_all_small OFFSET 1) UNION ALL (SELECT id FROM union_all_tiny OFFSET 1) UNION ALL (SELECT id FROM union_all_big OFFSET 1) UNION ALL (SELECT id FROM union_all_empty) LIMIT 101;

-- ----------------------------------------------------------------------
-- Threshold = 0: disable 5+ branch reorder
-- ----------------------------------------------------------------------
SET union_all_faststart_limit_threshold = 0;
EXPLAIN (COSTS OFF) SELECT * FROM (
  (SELECT id FROM union_all_big ORDER BY sort_key LIMIT 10)
  UNION ALL (SELECT id FROM union_all_mid ORDER BY sort_key LIMIT 10)
  UNION ALL (SELECT id FROM union_all_big OFFSET 7000)
  UNION ALL (SELECT id FROM union_all_big OFFSET 4000)
  UNION ALL SELECT id FROM union_all_small
) t LIMIT 5;
RESET union_all_faststart_limit_threshold;

-- ----------------------------------------------------------------------
-- Guard: top-level ORDER BY
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT id FROM union_all_big WHERE id < 10 UNION ALL SELECT id FROM union_all_small ORDER BY id LIMIT 5;

-- ----------------------------------------------------------------------
-- Guard: top-level PERCENT
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT id FROM union_all_big WHERE id < 10 UNION ALL SELECT id FROM union_all_small FETCH FIRST 50 PERCENT ROWS ONLY;

-- ----------------------------------------------------------------------
-- Guard: top-level WITH TIES + ORDER BY
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT id FROM union_all_big WHERE id < 10 UNION ALL SELECT id FROM union_all_small ORDER BY id FETCH FIRST 5 ROWS WITH TIES;

-- ----------------------------------------------------------------------
-- Guard: top-level aggregate
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT count(*) FROM (SELECT id FROM union_all_big WHERE id < 10 UNION ALL SELECT id FROM union_all_small) t LIMIT 1;

-- ----------------------------------------------------------------------
-- Guard: top-level DISTINCT (outer query op blocks reorder)
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT DISTINCT id FROM (SELECT id FROM union_all_big WHERE id < 10 UNION ALL SELECT id FROM union_all_small) t LIMIT 5;

-- ----------------------------------------------------------------------
-- Guard: top-level GROUP BY (outer query op blocks reorder)
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT id FROM (SELECT id FROM union_all_big WHERE id < 10 UNION ALL SELECT id FROM union_all_small) t GROUP BY id LIMIT 5;

-- ----------------------------------------------------------------------
-- Guard: top-level GROUPING SETS (same groupClause check as GROUP BY)
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT id, count(*) FROM (SELECT id FROM union_all_big WHERE id < 10 UNION ALL SELECT id FROM union_all_small) t GROUP BY GROUPING SETS ((id), ()) LIMIT 5;

-- ----------------------------------------------------------------------
-- Guard: top-level HAVING with aggregate (not pushable, hits pre-check)
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT id FROM (SELECT id FROM union_all_big WHERE id < 10 UNION ALL SELECT id FROM union_all_small) t GROUP BY id HAVING count(*) > 0 LIMIT 5;

-- ----------------------------------------------------------------------
-- Branch-local GROUP BY + aggregate: allowed to reorder
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT id, count(*) FROM union_all_big WHERE id < 10 GROUP BY id HAVING count(*) > 0 UNION ALL SELECT id, 1 FROM union_all_small LIMIT 5;

-- ----------------------------------------------------------------------
-- Guard: top-level SRF in target list
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT generate_series(1, 3) UNION ALL SELECT id FROM union_all_small LIMIT 2;

-- ----------------------------------------------------------------------
-- Guard: top-level window function (window consumes full input, no gain)
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT row_number() OVER (ORDER BY id) AS rn, id FROM (SELECT id FROM union_all_big WHERE id < 10 UNION ALL SELECT id FROM union_all_small) t LIMIT 5;

-- ----------------------------------------------------------------------
-- Guard: inheritance scan keeps original order
-- (Path entry requires the parent rte to be a flattened UNION ALL subquery;
--  inheritance parent is RTE_RELATION, so the appendrel is never reordered.
--  FOR UPDATE/SHARE with UNION ALL is rejected by parse analysis, so no
--  rowMarks guard exists in the feature code.)
-- ----------------------------------------------------------------------
CREATE TABLE union_all_inh_parent(id int);
CREATE TABLE union_all_inh_child() INHERITS (union_all_inh_parent);
INSERT INTO union_all_inh_parent VALUES (1), (2);
INSERT INTO union_all_inh_child VALUES (1), (2), (3), (4), (5);
ANALYZE union_all_inh_parent;
ANALYZE union_all_inh_child;
EXPLAIN (COSTS OFF) SELECT * FROM union_all_inh_parent LIMIT 5;
DROP TABLE union_all_inh_parent CASCADE;

-- ----------------------------------------------------------------------
-- Guard: outer JOIN (appendrel does not cover all base rels)
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT t.id FROM (SELECT id FROM union_all_big WHERE id < 10 UNION ALL SELECT id FROM union_all_small) t JOIN union_all_join1 j ON t.id = j.id LIMIT 5;

-- ----------------------------------------------------------------------
-- Inner LIMIT on UNION ALL feeding a JOIN: the union keeps its own
-- SubqueryScan (LIMIT blocks flattening), reorder happens in the union's
-- own query block; the outer JOIN has no LIMIT demand.
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT * FROM ((SELECT id FROM union_all_big ORDER BY sort_key LIMIT 100) UNION ALL SELECT id FROM union_all_small LIMIT 3) u JOIN union_all_tiny ON u.id = union_all_tiny.id;

-- ----------------------------------------------------------------------
-- JOIN inside branch: allowed to reorder
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT id FROM (SELECT j1.id FROM union_all_join1 j1 JOIN union_all_join2 j2 ON j1.id = j2.id UNION ALL SELECT id FROM union_all_small) t LIMIT 5;

-- ----------------------------------------------------------------------
-- Branch with EXISTS filter (customer EXISTS-subquery shape): InitPlan cost
-- makes the constant branch expensive, cheap branch moves first.
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT * FROM (SELECT 1 AS id WHERE EXISTS (SELECT 1 FROM union_all_big WHERE id = 1) UNION ALL SELECT id FROM union_all_small) t LIMIT 5;

-- ----------------------------------------------------------------------
-- Guard: UNION (not UNION ALL)
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT id FROM union_all_big WHERE id < 10 UNION SELECT id FROM union_all_small LIMIT 5;

-- ----------------------------------------------------------------------
-- Guard: INTERSECT above UNION ALL
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) (SELECT id FROM union_all_big WHERE id < 10 UNION ALL SELECT id FROM union_all_small) INTERSECT SELECT id FROM union_all_mid LIMIT 5;

-- ----------------------------------------------------------------------
-- Guard: EXCEPT above UNION ALL
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) (SELECT id FROM union_all_big WHERE id < 10 UNION ALL SELECT id FROM union_all_small) EXCEPT SELECT id FROM union_all_mid LIMIT 5;

-- ----------------------------------------------------------------------
-- Guard: branch root PERCENT Limit (Plan entry)
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT * FROM ((SELECT id FROM union_all_big WHERE id < 10 FETCH FIRST 50 PERCENT ROWS ONLY) UNION ALL SELECT id FROM union_all_small) t LIMIT 5;

-- ----------------------------------------------------------------------
-- Guard: branch root WITH TIES Limit (Plan entry)
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT * FROM ((SELECT id FROM union_all_big WHERE id < 10 ORDER BY sort_key FETCH FIRST 5 ROWS WITH TIES) UNION ALL SELECT id FROM union_all_small) t LIMIT 5;

-- ----------------------------------------------------------------------
-- Guard: branch root PERCENT Limit (Path entry via subquery)
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT * FROM (SELECT * FROM (SELECT id FROM union_all_big WHERE id < 10 FETCH FIRST 50 PERCENT ROWS ONLY) b UNION ALL SELECT id FROM union_all_small) t LIMIT 5;

-- ----------------------------------------------------------------------
-- Guard: branch root WITH TIES Limit (Path entry via subquery)
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT * FROM (SELECT * FROM (SELECT id FROM union_all_big WHERE id < 10 ORDER BY sort_key FETCH FIRST 5 ROWS WITH TIES) b UNION ALL SELECT id FROM union_all_small) t LIMIT 5;

-- ----------------------------------------------------------------------
-- Deep PERCENT (join above it, branch root is not Limit): allowed
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) (SELECT id FROM union_all_big WHERE id < 10 ORDER BY id LIMIT 5) UNION ALL SELECT a.id FROM (SELECT id FROM union_all_mid FETCH FIRST 50 PERCENT ROWS ONLY) a JOIN union_all_small s ON a.id = s.id LIMIT 5;

-- ----------------------------------------------------------------------
-- Deep PERCENT below aggregate, subquery form (set_subquery_path entry):
-- allowed. The GROUP BY leaf makes is_simple_union_all reject expansion even
-- with aligned types (1::bigint against count(*)), so the subplan is planned
-- as one SubqueryScan and the outer LIMIT reorders the Append inside it.
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT * FROM ((SELECT id, 1::bigint FROM union_all_big WHERE id < 10 ORDER BY id LIMIT 5) UNION ALL (SELECT id, count(*) FROM (SELECT id FROM union_all_mid FETCH FIRST 50 PERCENT ROWS ONLY) m GROUP BY id)) t LIMIT 5;

-- ----------------------------------------------------------------------
-- Guard: flat top-level with branch root PERCENT Limit (Plan entry): no
-- reorder. Setops leaves are wrapped in SubqueryScan, the guard checks the
-- leaf plan root through that wrapper.
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) (SELECT id FROM union_all_big WHERE id < 10 FETCH FIRST 50 PERCENT ROWS ONLY) UNION ALL SELECT id FROM union_all_small LIMIT 5;

-- ----------------------------------------------------------------------
-- Unflattened subquery (set_subquery_path entry): branch types int4 vs int8
-- need coercion, so is_simple_union_all rejects expansion. The cheap bare
-- scan second in text order must move first.
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT * FROM ((SELECT id, 1 FROM union_all_big ORDER BY sort_key LIMIT 10) UNION ALL (SELECT id, 1::bigint FROM union_all_small)) t LIMIT 5;

-- ----------------------------------------------------------------------
-- Guard: unflattened subquery with branch root PERCENT Limit
-- (set_subquery_path entry): no reorder.
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT * FROM ((SELECT id, 1 FROM union_all_big WHERE id < 10 FETCH FIRST 50 PERCENT ROWS ONLY) UNION ALL (SELECT id, 1::bigint FROM union_all_small)) t LIMIT 5;

-- ----------------------------------------------------------------------
-- Unflattened subquery (set_subquery_path entry) + outer JOIN:
-- outer join means rel->relids != root->all_baserels, guard rejects reorder.
-- coercion (int vs bigint) prevents flattening, so we enter the guard.
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT * FROM ((SELECT id, 1 FROM union_all_big ORDER BY sort_key LIMIT 10) UNION ALL (SELECT id, 1::bigint FROM union_all_small)) t JOIN union_all_tiny ON t.id = union_all_tiny.id LIMIT 5;

-- ----------------------------------------------------------------------
-- Unflattened subquery (set_subquery_path entry) + outer HAVING:
-- outer HAVING means havingQual != NULL, guard rejects reorder.
-- coercion (int vs bigint) prevents flattening, so we enter the guard.
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT id FROM ((SELECT id, 1 FROM union_all_big ORDER BY sort_key LIMIT 10) UNION ALL (SELECT id, 1::bigint FROM union_all_small)) t GROUP BY id HAVING count(*) > 0 LIMIT 5;

-- ----------------------------------------------------------------------
-- Unflattened subquery (set_subquery_path entry) + outer WHERE:
-- outer WHERE means rel->baserestrictinfo != NIL (the qual cannot be pushed
-- into the unflattened union, needrows would be underestimated), guard
-- rejects reorder. coercion (int vs bigint) prevents flattening, so we
-- enter the guard. join quals stay in joinqual, only a plain WHERE on the
-- subquery output reaches this branch.
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT * FROM ((SELECT id, 1 FROM union_all_big ORDER BY sort_key LIMIT 10) UNION ALL (SELECT id, 1::bigint FROM union_all_small)) t WHERE t.id > 2 LIMIT 5;

-- ----------------------------------------------------------------------
-- Unflattened UNION (dedup) subquery (set_subquery_path entry):
-- topop->all is false, guard rejects reorder before touching the plan.
-- coercion (int vs bigint) prevents flattening, so we enter the guard.
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT * FROM ((SELECT id, 1 FROM union_all_big ORDER BY sort_key LIMIT 10) UNION (SELECT id, 1::bigint FROM union_all_small)) t LIMIT 5;

-- ----------------------------------------------------------------------
-- 3 branches: Plan entry (unflattened via int/bigint coercion), enumerate
-- 6 orders; small bare scan wins first position, sorted branches follow.
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT * FROM (
  (SELECT id, 1 FROM union_all_big ORDER BY sort_key LIMIT 10)
  UNION ALL (SELECT id, 1::bigint FROM union_all_mid ORDER BY sort_key LIMIT 10)
  UNION ALL (SELECT id, 1 FROM union_all_small)) t LIMIT 5;

-- ----------------------------------------------------------------------
-- 4 branches: Plan entry (unflattened via int/bigint coercion), enumerate
-- 24 orders; small bare scan first, three Sort branches by startup after.
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT * FROM (
  (SELECT id, 1 FROM union_all_big ORDER BY sort_key LIMIT 10)
  UNION ALL (SELECT id, 1::bigint FROM union_all_mid ORDER BY sort_key LIMIT 10)
  UNION ALL (SELECT id, 1 FROM union_all_small)
  UNION ALL (SELECT id, 1::bigint FROM union_all_tiny)) t LIMIT 5;

-- ----------------------------------------------------------------------
-- 3 branches: Plan entry (unflattened via int/bigint coercion), needrows
-- >= total rows, full-demand sort by startup_cost (distinct Sort startups:
-- small bare 0 < mid-sort < big-sort keep qsort deterministic).
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT * FROM (
  (SELECT id, 1 FROM union_all_big ORDER BY sort_key LIMIT 10)
  UNION ALL (SELECT id, 1::bigint FROM union_all_mid ORDER BY sort_key LIMIT 10)
  UNION ALL (SELECT id, 1 FROM union_all_small)) t LIMIT 2000;

-- ----------------------------------------------------------------------
-- 5 branches: Plan entry (unflattened via int/bigint coercion), needrows
-- within threshold, sort by startup_cost (distinct OFFSET startups:
-- empty < big < mid < small < tiny).
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT * FROM (
  (SELECT id, 1 FROM union_all_mid OFFSET 2)
  UNION ALL (SELECT id, 1::bigint FROM union_all_small OFFSET 1)
  UNION ALL (SELECT id, 1 FROM union_all_tiny OFFSET 1)
  UNION ALL (SELECT id, 1::bigint FROM union_all_big OFFSET 1)
  UNION ALL (SELECT id, 1 FROM union_all_empty)) t LIMIT 5;

-- ----------------------------------------------------------------------
-- Boundary: single branch (same table twice, no reorder possible)
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT id FROM union_all_big WHERE id < 10 UNION ALL SELECT id FROM union_all_big WHERE id < 10 LIMIT 5;

-- ----------------------------------------------------------------------
-- Identical branches, full-demand startup sort (Plan and Path entry):
-- equal startup costs exercise the comparator tie-break; swapped output is
-- indistinguishable because both subtrees print identically
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT id FROM union_all_big WHERE id < 10 UNION ALL SELECT id FROM union_all_big WHERE id < 10 LIMIT 2147483648;
EXPLAIN (COSTS OFF) SELECT * FROM (SELECT id FROM union_all_big WHERE id < 10 UNION ALL SELECT id FROM union_all_big WHERE id < 10) t LIMIT 2147483648;

-- ----------------------------------------------------------------------
-- Boundary: LIMIT 0
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT id FROM union_all_big WHERE id < 10 UNION ALL SELECT id FROM union_all_small LIMIT 0;

-- ----------------------------------------------------------------------
-- Boundary: LIMIT NULL
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT id FROM union_all_big WHERE id < 10 UNION ALL SELECT id FROM union_all_small LIMIT NULL;

-- ----------------------------------------------------------------------
-- Boundary: no LIMIT
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT id FROM union_all_big WHERE id < 10 UNION ALL SELECT id FROM union_all_small;

-- ----------------------------------------------------------------------
-- Boundary: non-constant LIMIT
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT id FROM union_all_big WHERE id < 10 UNION ALL SELECT id FROM union_all_small LIMIT (SELECT 5);

-- ----------------------------------------------------------------------
-- Boundary: non-constant OFFSET
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT id FROM union_all_big WHERE id < 10 UNION ALL SELECT id FROM union_all_small OFFSET (SELECT 2) LIMIT 5;

-- ----------------------------------------------------------------------
-- Boundary: OFFSET 0
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT id FROM union_all_big WHERE id < 10 UNION ALL SELECT id FROM union_all_small OFFSET 0 LIMIT 5;

-- ----------------------------------------------------------------------
-- Boundary: OFFSET NULL
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT id FROM union_all_big WHERE id < 10 UNION ALL SELECT id FROM union_all_small OFFSET NULL LIMIT 5;

-- ----------------------------------------------------------------------
-- Boundary: int8 LIMIT, needrows covers total rows, startup sort
-- (OFFSET branch gives distinct startup costs for qsort)
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT id FROM union_all_big WHERE id < 10 UNION ALL (SELECT id FROM union_all_small OFFSET 1) LIMIT 2147483648;

-- ----------------------------------------------------------------------
-- Boundary: casted LIMIT (parser coerces to int8, reorder proceeds)
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT id FROM union_all_big WHERE id < 10 UNION ALL SELECT id FROM union_all_small LIMIT 5::int2;

-- ----------------------------------------------------------------------
-- Boundary: LIMIT + OFFSET overflow
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT id FROM union_all_big WHERE id < 10 UNION ALL SELECT id FROM union_all_small LIMIT 9223372036854775807 OFFSET 1;

-- ----------------------------------------------------------------------
-- Nested setops: UNION ALL inside UNION ALL
-- (the Sort leaf has a positive startup cost versus zero for the bare
--  filtered scan, keeping the suffix order independent of pointer address)
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT * FROM ((SELECT id FROM union_all_big WHERE id < 10 UNION ALL SELECT id FROM union_all_small) UNION ALL (SELECT id FROM union_all_big ORDER BY sort_key LIMIT 3)) t LIMIT 5;

-- ----------------------------------------------------------------------
-- Mixed setops: UNION ALL + UNION
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) (SELECT id FROM union_all_big WHERE id < 10 UNION ALL SELECT id FROM union_all_small) UNION SELECT id FROM union_all_tiny LIMIT 5;

-- ----------------------------------------------------------------------
-- WITH TIES without ORDER BY (treated as regular Limit)
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT id FROM union_all_big WHERE id < 10 UNION ALL SELECT id FROM union_all_small FETCH FIRST 5 ROWS WITH TIES;

-- ----------------------------------------------------------------------
-- 16 branches: no upper bound on branch count
-- (all branches read the exact-stats mid table with strictly increasing
--  OFFSET startup costs, so every qsort comparison is decided by cost,
--  never by pointer address)
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT * FROM (
  (SELECT id FROM union_all_mid OFFSET 15) UNION ALL (SELECT id FROM union_all_mid OFFSET 14)
  UNION ALL (SELECT id FROM union_all_mid OFFSET 13) UNION ALL (SELECT id FROM union_all_mid OFFSET 12)
  UNION ALL (SELECT id FROM union_all_mid OFFSET 11) UNION ALL (SELECT id FROM union_all_mid OFFSET 10)
  UNION ALL (SELECT id FROM union_all_mid OFFSET 9) UNION ALL (SELECT id FROM union_all_mid OFFSET 8)
  UNION ALL (SELECT id FROM union_all_mid OFFSET 7) UNION ALL (SELECT id FROM union_all_mid OFFSET 6)
  UNION ALL (SELECT id FROM union_all_mid OFFSET 5) UNION ALL (SELECT id FROM union_all_mid OFFSET 4)
  UNION ALL (SELECT id FROM union_all_mid OFFSET 3) UNION ALL (SELECT id FROM union_all_mid OFFSET 2)
  UNION ALL (SELECT id FROM union_all_mid OFFSET 1) UNION ALL (SELECT id FROM union_all_mid OFFSET 0)
) t LIMIT 5;

-- ----------------------------------------------------------------------
-- CTE branch: not expanded, root checked only
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) WITH cte AS (SELECT id FROM union_all_big WHERE id < 10) SELECT * FROM cte UNION ALL SELECT id FROM union_all_small LIMIT 5;

-- ----------------------------------------------------------------------
-- Index-ordered leaf branch: Index Scan (no Sort node) startup is near
-- zero, it must win over the Sort branch. Dedicated indexed table keeps
-- all shared tables index-free; dropped right after this case.
-- ----------------------------------------------------------------------
CREATE TABLE union_all_idx(id int);
INSERT INTO union_all_idx SELECT n FROM generate_series(1, 10000) n;
CREATE INDEX idx_union_all_idx ON union_all_idx(id);
ANALYZE union_all_idx;
EXPLAIN (COSTS OFF) SELECT * FROM ((SELECT id FROM union_all_big ORDER BY sort_key LIMIT 100) UNION ALL (SELECT id FROM union_all_idx ORDER BY id LIMIT 100)) t LIMIT 5;
DROP TABLE union_all_idx;

-- ----------------------------------------------------------------------
-- Result correctness: reorder must not lose or duplicate rows. Full
-- result verified by order-independent checksum with the rule off and
-- on; LIMIT row count stays exact after reorder.
-- ----------------------------------------------------------------------
SET rewrite_rule = 'none';
SELECT sum(id) AS s, count(*) AS c FROM (SELECT id FROM union_all_big WHERE id < 100 UNION ALL SELECT id FROM union_all_small) t;
SET rewrite_rule = 'union_all_faststart';
SELECT sum(id) AS s, count(*) AS c FROM (SELECT id FROM union_all_big WHERE id < 100 UNION ALL SELECT id FROM union_all_small) t;
SELECT count(*) AS c FROM (SELECT id FROM union_all_big WHERE id < 100 UNION ALL SELECT id FROM union_all_small LIMIT 5) s;

-- ----------------------------------------------------------------------
-- Empty table branch: participates in cost comparison
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT id FROM union_all_empty UNION ALL SELECT id FROM union_all_small LIMIT 5;

-- ----------------------------------------------------------------------
-- Cleanup
-- ----------------------------------------------------------------------

RESET rewrite_rule;
DROP TABLE union_all_big;
DROP TABLE union_all_mid;
DROP TABLE union_all_small;
DROP TABLE union_all_tiny;
DROP TABLE union_all_join1;
DROP TABLE union_all_join2;
DROP TABLE union_all_empty;
RESET search_path;
DROP SCHEMA union_all_faststart_cov;
