DROP SCHEMA IF EXISTS union_predpushforce_7 CASCADE;
CREATE SCHEMA union_predpushforce_7;
SET search_path = union_predpushforce_7;
SET rewrite_rule = 'predpushforce';

CREATE TABLE source_p1 (k integer);
CREATE TABLE source_p2 (k integer);
CREATE TABLE branch_a (k1 integer, k2 integer, amount integer);
CREATE TABLE branch_b (k1 integer, k2 integer, amount integer);

INSERT INTO source_p1 VALUES (1), (2), (2000);
INSERT INTO source_p2 VALUES (1), (2), (20);
INSERT INTO branch_a
SELECT (g - 1) / 10 + 1, (g - 1) % 10 + 1, 1
FROM generate_series(1, 1000) g;
INSERT INTO branch_b
SELECT (g - 1) / 10 + 1, (g - 1) % 10 + 1, 2
FROM generate_series(1, 1000) g;
ANALYZE source_p1;
ANALYZE source_p2;
ANALYZE branch_a;
ANALYZE branch_b;

-- P1 is v.k1 = p.k; P2 is v.k2 = q.k in all six cases.
-- Case 1: A contributes P1, B contributes P2. Their union retains both.
CREATE INDEX branch_a_k1 ON branch_a (k1);
CREATE INDEX branch_b_k2 ON branch_b (k2);
SELECT 'any_branch_keeps_both' AS case_name;
EXPLAIN (COSTS OFF)
SELECT p.k AS p1, q.k AS p2, COALESCE(SUM(v.amount), 0) AS total_amount
FROM source_p1 p CROSS JOIN source_p2 q
LEFT JOIN (
    SELECT k1, k2, amount FROM branch_a
    UNION
    SELECT k1, k2, amount FROM branch_b
) v ON v.k1 = p.k AND v.k2 = q.k
GROUP BY p.k, q.k
ORDER BY p.k, q.k;
SELECT p.k AS p1, q.k AS p2, COALESCE(SUM(v.amount), 0) AS total_amount
FROM source_p1 p CROSS JOIN source_p2 q
LEFT JOIN (
    SELECT k1, k2, amount FROM branch_a
    UNION
    SELECT k1, k2, amount FROM branch_b
) v ON v.k1 = p.k AND v.k2 = q.k
GROUP BY p.k, q.k
ORDER BY p.k, q.k;

-- Case 2: A contributes P1 and P2 via different indexes; B only P2.
-- Both predicates remain eligible because either branch may support them.
CREATE INDEX branch_a_k2 ON branch_a (k2);
SELECT 'one_branch_p1_both_p2' AS case_name;
EXPLAIN (COSTS OFF)
SELECT p.k AS p1, q.k AS p2, COALESCE(SUM(v.amount), 0) AS total_amount
FROM source_p1 p CROSS JOIN source_p2 q
LEFT JOIN (
    SELECT k1, k2, amount FROM branch_a
    UNION
    SELECT k1, k2, amount FROM branch_b
) v ON v.k1 = p.k AND v.k2 = q.k
GROUP BY p.k, q.k
ORDER BY p.k, q.k;
SELECT p.k AS p1, q.k AS p2, COALESCE(SUM(v.amount), 0) AS total_amount
FROM source_p1 p CROSS JOIN source_p2 q
LEFT JOIN (
    SELECT k1, k2, amount FROM branch_a
    UNION
    SELECT k1, k2, amount FROM branch_b
) v ON v.k1 = p.k AND v.k2 = q.k
GROUP BY p.k, q.k
ORDER BY p.k, q.k;

-- Case 3: Both branches contribute both predicates. Keep P1 and P2.
-- A physical scan may still choose one index and a residual filter by cost.
CREATE INDEX branch_b_k1 ON branch_b (k1);
SELECT 'both_branches_both_predicates' AS case_name;
EXPLAIN (COSTS OFF)
SELECT p.k AS p1, q.k AS p2, COALESCE(SUM(v.amount), 0) AS total_amount
FROM source_p1 p CROSS JOIN source_p2 q
LEFT JOIN (
    SELECT k1, k2, amount FROM branch_a
    UNION
    SELECT k1, k2, amount FROM branch_b
) v ON v.k1 = p.k AND v.k2 = q.k
GROUP BY p.k, q.k
ORDER BY p.k, q.k;
SELECT p.k AS p1, q.k AS p2, COALESCE(SUM(v.amount), 0) AS total_amount
FROM source_p1 p CROSS JOIN source_p2 q
LEFT JOIN (
    SELECT k1, k2, amount FROM branch_a
    UNION
    SELECT k1, k2, amount FROM branch_b
) v ON v.k1 = p.k AND v.k2 = q.k
GROUP BY p.k, q.k
ORDER BY p.k, q.k;

-- Case 4: A has (k1,k2), B has (k2). A supports the complete joint
-- prefix, so both predicates survive without a fixed-point recheck.
DROP INDEX branch_a_k1;
DROP INDEX branch_a_k2;
DROP INDEX branch_b_k1;
CREATE INDEX branch_a_k1_k2 ON branch_a (k1, k2);
SELECT 'one_branch_joint_prefix' AS case_name;
EXPLAIN (COSTS OFF)
SELECT p.k AS p1, q.k AS p2, COALESCE(SUM(v.amount), 0) AS total_amount
FROM source_p1 p CROSS JOIN source_p2 q
LEFT JOIN (
    SELECT k1, k2, amount FROM branch_a
    UNION
    SELECT k1, k2, amount FROM branch_b
) v ON v.k1 = p.k AND v.k2 = q.k
GROUP BY p.k, q.k
ORDER BY p.k, q.k;
SELECT p.k AS p1, q.k AS p2, COALESCE(SUM(v.amount), 0) AS total_amount
FROM source_p1 p CROSS JOIN source_p2 q
LEFT JOIN (
    SELECT k1, k2, amount FROM branch_a
    UNION
    SELECT k1, k2, amount FROM branch_b
) v ON v.k1 = p.k AND v.k2 = q.k
GROUP BY p.k, q.k
ORDER BY p.k, q.k;

-- Case 5: Same indexes as case 4, but A has a FIXED local k1 = 1.
-- Candidate P1 (not the fixed equality) supports A's prefix; keep both.
-- The local condition only belongs to A: source p1 = 2 still sees B's rows.
SELECT 'fixed_ignored_candidates_keep_both' AS case_name;
EXPLAIN (COSTS OFF)
SELECT p.k AS p1, q.k AS p2, COALESCE(SUM(v.amount), 0) AS total_amount
FROM source_p1 p CROSS JOIN source_p2 q
LEFT JOIN (
    SELECT k1, k2, amount FROM branch_a WHERE k1 = 1
    UNION
    SELECT k1, k2, amount FROM branch_b
) v ON v.k1 = p.k AND v.k2 = q.k
GROUP BY p.k, q.k
ORDER BY p.k, q.k;
SELECT p.k AS p1, q.k AS p2, COALESCE(SUM(v.amount), 0) AS total_amount
FROM source_p1 p CROSS JOIN source_p2 q
LEFT JOIN (
    SELECT k1, k2, amount FROM branch_a WHERE k1 = 1
    UNION
    SELECT k1, k2, amount FROM branch_b
) v ON v.k1 = p.k AND v.k2 = q.k
GROUP BY p.k, q.k
ORDER BY p.k, q.k;

-- Case 6: Both branches have (k1,k2), with no fixed leading-key condition.
-- Prepare P1 and P2 together: P1 supplies the first key and P2 the second.
-- Both original predicates contribute in both branches and must survive.
-- Inspect conditions below the UNION boundary; outer aggregate/join filters
-- alone do not show whether the individual branch predicates were pushed.
DROP INDEX branch_b_k2;
CREATE INDEX branch_b_k1_k2 ON branch_b (k1, k2);
SELECT 'joint_prefix_keeps_both' AS case_name;
EXPLAIN (COSTS OFF)
SELECT p.k AS p1, q.k AS p2, COALESCE(SUM(v.amount), 0) AS total_amount
FROM source_p1 p CROSS JOIN source_p2 q
LEFT JOIN (
    SELECT k1, k2, amount FROM branch_a
    UNION
    SELECT k1, k2, amount FROM branch_b
) v ON v.k1 = p.k AND v.k2 = q.k
GROUP BY p.k, q.k
ORDER BY p.k, q.k;
SELECT p.k AS p1, q.k AS p2, COALESCE(SUM(v.amount), 0) AS total_amount
FROM source_p1 p CROSS JOIN source_p2 q
LEFT JOIN (
    SELECT k1, k2, amount FROM branch_a
    UNION
    SELECT k1, k2, amount FROM branch_b
) v ON v.k1 = p.k AND v.k2 = q.k
GROUP BY p.k, q.k
ORDER BY p.k, q.k;

RESET rewrite_rule;
RESET search_path;
DROP SCHEMA union_predpushforce_7 CASCADE;
