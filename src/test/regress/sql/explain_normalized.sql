-- EXPLAIN (NORMALIZED, COSTS false) and EXPLAIN (GENERIC_PLAN, COSTS false) regression tests

-- Setup: create test tables
drop table if exists explain_norm_t1;
CREATE TABLE explain_norm_t1 (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    salary NUMERIC(10,2),
    dept_id INTEGER
);

drop table if exists explain_norm_t2;
CREATE TABLE explain_norm_t2 (
    dept_id INTEGER PRIMARY KEY,
    dept_name VARCHAR(100)
);

INSERT INTO explain_norm_t1 VALUES (1, 'Alice', 50000.00, 10), (2, 'Bob', 60000.00, 20),
    (3, 'Charlie', 70000.00, 10), (4, 'Diana', 55000.00, 30), (5, 'Eve', 65000.00, 20);
INSERT INTO explain_norm_t2 VALUES (10, 'Engineering'), (20, 'Sales'), (30, 'Marketing');

-- Create index for Index Scan tests
CREATE INDEX idx_explain_norm_dept ON explain_norm_t1(dept_id);
CREATE INDEX idx_explain_norm_salary ON explain_norm_t1(salary);

-- ============================================================
-- 1. Basic NORMALIZED syntax
-- ============================================================

-- 1a. Basic NORMALIZED with single constant
EXPLAIN (NORMALIZED, COSTS false) SELECT * FROM explain_norm_t1 WHERE dept_id = 10;

-- 1b. NORMALIZED with multiple constants
EXPLAIN (NORMALIZED, COSTS false) SELECT * FROM explain_norm_t1 WHERE dept_id = 10 AND salary > 50000;

-- 1c. NORMALIZED with string constant
EXPLAIN (NORMALIZED, COSTS false) SELECT * FROM explain_norm_t1 WHERE name = 'Alice';

-- 1d. Leading whitespace before EXPLAIN: the prefix must still be
--     stripped from the normalized display.
 EXPLAIN (NORMALIZED, COSTS false) SELECT 42;

-- 1e. Leading comment before EXPLAIN: the prefix must still be
--     stripped from the normalized display.
/* lead */ EXPLAIN (NORMALIZED, COSTS false) SELECT 42;

-- ============================================================
-- 2. NORMALIZED plan consistency with regular EXPLAIN
-- ============================================================

-- Regular EXPLAIN
EXPLAIN (COSTS false) SELECT * FROM explain_norm_t1 WHERE dept_id = 10;

-- NORMALIZED EXPLAIN (same query)
EXPLAIN (NORMALIZED, COSTS false) SELECT * FROM explain_norm_t1 WHERE dept_id = 10;

-- ============================================================
-- 3. NORMALIZED + ANALYZE combination
-- ============================================================

EXPLAIN (NORMALIZED, ANALYZE false, COSTS false, TIMING false) SELECT * FROM explain_norm_t1 WHERE dept_id = 10;

-- 3a. NORMALIZED + ANALYZE with string constant
EXPLAIN (NORMALIZED, ANALYZE false, COSTS false, TIMING false) SELECT * FROM explain_norm_t1 WHERE name = 'Alice';

-- ============================================================
-- 4. GENERIC_PLAN with original constants (auto-convert to Param)
-- ============================================================

EXPLAIN (GENERIC_PLAN, COSTS false) SELECT * FROM explain_norm_t1 WHERE dept_id = 10 AND salary > 50000;

-- ============================================================
-- 5. GENERIC_PLAN + ANALYZE should report error
-- ============================================================

EXPLAIN (GENERIC_PLAN, ANALYZE) SELECT * FROM explain_norm_t1 WHERE dept_id = 10;

-- ============================================================
-- 6. NORMALIZED + GENERIC_PLAN mutual exclusion
-- ============================================================

EXPLAIN (NORMALIZED, GENERIC_PLAN) SELECT * FROM explain_norm_t1 WHERE dept_id = 10;

-- ============================================================
-- 7. NORMALIZED with different scan types
-- ============================================================

-- 7a. Index Scan
SET enable_seqscan = off;
EXPLAIN (NORMALIZED, COSTS false) SELECT * FROM explain_norm_t1 WHERE dept_id = 10;
SET enable_seqscan = on;

-- 7b. Join query
EXPLAIN (NORMALIZED, COSTS false)
SELECT t1.name, t2.dept_name
FROM explain_norm_t1 t1
JOIN explain_norm_t2 t2 ON t1.dept_id = t2.dept_id
WHERE t1.salary > 50000;

-- ============================================================
-- 8. Regular EXPLAIN output NOT affected (regression check)
-- ============================================================

EXPLAIN (COSTS false) SELECT * FROM explain_norm_t1 WHERE dept_id = 10;

-- ============================================================
-- 8a. NORMALIZED with user-supplied $N — expected ERROR
--     $N is parsed as a Param node by the standard parser.  NORMALIZED
--     mode is display-only (not parameterized), so there is no bound
--     parameter list — the parser rejects an unbound $N at analysis time.
-- ============================================================

-- 8a1. $N in NORMALIZED context → ERROR
EXPLAIN (NORMALIZED, COSTS false) SELECT * FROM explain_norm_t1 WHERE dept_id = $1;

-- 8a2. Mixed $N + constants → ERROR (same reason)
EXPLAIN (NORMALIZED, COSTS false) SELECT * FROM explain_norm_t1 WHERE dept_id = $1 AND salary > 50000;

-- ============================================================
-- 8a3. ? placeholder in NORMALIZED — NORMALIZED does not enable '?' lexing; '?' stays an operator (parse error)
--      (? is not a valid token in the standard parser; NORMALIZED does
--       not activate the ? preprocessor.  Only GENERIC_PLAN does.)
EXPLAIN (NORMALIZED, COSTS false) SELECT * FROM explain_norm_t1 WHERE dept_id = ?;

-- ============================================================
-- 8b. GENERIC_PLAN with auto-Const-to-Param conversion
--     GENERIC_PLAN accepts constants and auto-converts them to Param
--     nodes via const_to_param_mutator before planning.  This gives
--     the user a better experience than PG 16 — no need to manually
--     rewrite constants as $N.
--     (Note: GENERIC_PLAN also accepts $N if bound parameters exist,
--      e.g. in PREPARE/EXECUTE context.  Standalone $N without bound
--      parameters is rejected by the parser.)
-- ============================================================

-- 8b1. Single constant — auto-converted to Param
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT * FROM explain_norm_t1 WHERE dept_id = 10;

-- 8b2. Multiple constants — all auto-converted
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT * FROM explain_norm_t1 WHERE dept_id = 10 AND salary > 50000;

-- 8b3. Same query with different constant — same generic plan
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT * FROM explain_norm_t1 WHERE dept_id = 20 AND salary > 60000;

-- 8b4. ? placeholder in GENERIC_PLAN — lexed as a parameter by scan.l
--      Every '?' becomes a placeholder inside GENERIC_PLAN, so the
--      '?'-named operators (?| ?& ?# ?? ...) are not usable there —
--      see section 21.
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT * FROM explain_norm_t1 WHERE dept_id = ?;

-- 8b5. Multiple ? placeholders in GENERIC_PLAN — each ? → $1, $2, ...
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT * FROM explain_norm_t1 WHERE dept_id = ? AND salary > ?;

-- ============================================================
-- 9. CTE with constants (tests Query-node traversal and
--    parameter ordering consistency)
-- ============================================================

-- 9a. Single CTE with constants in both inner and outer query
EXPLAIN (NORMALIZED, COSTS false)
WITH cte AS (SELECT * FROM explain_norm_t1 WHERE salary > 50000)
SELECT * FROM cte WHERE dept_id = 10;

-- 9b. NORMALIZED + ANALYZE with CTE (tests param order: inner
--     constant should appear before outer constant in text)
EXPLAIN (NORMALIZED, ANALYZE false, COSTS false, TIMING false)
WITH cte AS (SELECT * FROM explain_norm_t1 WHERE salary > 50000)
SELECT * FROM cte WHERE dept_id = 10;

-- 9c. GENERIC_PLAN with CTE (tests find_max_existing_paramid
--     handles Query nodes in CTE correctly)
EXPLAIN (GENERIC_PLAN, COSTS false)
WITH cte AS (SELECT * FROM explain_norm_t1 WHERE salary > 50000)
SELECT * FROM cte WHERE dept_id = 10;

-- 9d. CTE with GENERIC_PLAN: constants in both CTE and outer query
--     (tests that Query nodes inside CTE are properly mutated)
EXPLAIN (GENERIC_PLAN, COSTS false)
WITH cte AS (SELECT * FROM explain_norm_t1 WHERE salary > 70000)
SELECT * FROM cte WHERE dept_id = 10;

-- ============================================================
-- 10. Multi-level subqueries with constants
-- ============================================================

-- 10a. Two-level subquery
EXPLAIN (NORMALIZED, COSTS false)
SELECT * FROM (
    SELECT * FROM (
        SELECT * FROM explain_norm_t1 WHERE salary > 50000
    ) t1 WHERE dept_id = 10
) t2;

-- 10b. GENERIC_PLAN with multi-level subquery
EXPLAIN (GENERIC_PLAN, COSTS false)
SELECT * FROM (
    SELECT * FROM (
        SELECT * FROM explain_norm_t1 WHERE salary > 50000
    ) t1 WHERE dept_id = 10
) t2;

-- ============================================================
-- 11. Multiple NORMALIZED queries in one session (regression)
-- ============================================================

EXPLAIN (NORMALIZED, COSTS false) SELECT * FROM explain_norm_t1 WHERE id = 1;
EXPLAIN (NORMALIZED, COSTS false) SELECT * FROM explain_norm_t1 WHERE id = 2 AND name = 'Bob';

-- ============================================================
-- 12. Non-TEXT FORMAT + NORMALIZED (tests Fix 4: format guard)
-- ============================================================

-- 12a. FORMAT JSON
EXPLAIN (FORMAT JSON, NORMALIZED, COSTS false) SELECT * FROM explain_norm_t1 WHERE dept_id = 10;

-- 12b. FORMAT XML
EXPLAIN (FORMAT XML, NORMALIZED, COSTS false) SELECT * FROM explain_norm_t1 WHERE dept_id = 10;

-- 12c. FORMAT YAML
EXPLAIN (FORMAT YAML, NORMALIZED, COSTS false) SELECT * FROM explain_norm_t1 WHERE dept_id = 10;

-- ============================================================
-- 13. explain_perf_mode + NORMALIZED (tests Fix 3: perf mode output)
-- ============================================================

SET explain_perf_mode = pretty;
EXPLAIN (NORMALIZED, COSTS false) SELECT * FROM explain_norm_t1 WHERE dept_id = 10;
SET explain_perf_mode = normal;

-- ============================================================
-- 14. DML statements with NORMALIZED
-- ============================================================

-- 14a. UPDATE with constants
EXPLAIN (NORMALIZED, COSTS false) UPDATE explain_norm_t1 SET salary = 70000 WHERE dept_id = 10;

-- 14b. DELETE with constants
EXPLAIN (NORMALIZED, COSTS false) DELETE FROM explain_norm_t1 WHERE id = 5;

-- ============================================================
-- 15. Special constant types
-- ============================================================

-- 15a. NULL constant (explicit NULL::integer as Const node)
EXPLAIN (NORMALIZED, COSTS false) SELECT * FROM explain_norm_t1 WHERE dept_id = NULL::integer;

-- 15b. Negative numeric constant
EXPLAIN (NORMALIZED, COSTS false) SELECT * FROM explain_norm_t1 WHERE salary > -100;

-- 15c. Boolean constant
EXPLAIN (NORMALIZED, COSTS false) SELECT * FROM explain_norm_t1 WHERE (salary > 50000) = true;

-- 15d. Float constant
EXPLAIN (NORMALIZED, COSTS false) SELECT * FROM explain_norm_t1 WHERE salary > 50000.50;

-- ============================================================
-- 16. UNION / set operations with constants
-- ============================================================

EXPLAIN (NORMALIZED, COSTS false)
SELECT * FROM explain_norm_t1 WHERE dept_id = 10
UNION ALL
SELECT * FROM explain_norm_t1 WHERE dept_id = 20;

-- ============================================================
-- 17. GENERIC_PLAN consecutive execution (paramid collision check)
-- ============================================================

EXPLAIN (GENERIC_PLAN, COSTS false) SELECT * FROM explain_norm_t1 WHERE id = 1;
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT * FROM explain_norm_t1 WHERE id = 2 AND salary = 3;

-- ============================================================
-- 18. Unique SQL ID output across EXPLAIN calls
--     Each EXPLAIN call emits a numeric Unique SQL ID. The values are
--     environment-dependent and are matched independently by regex.
-- ============================================================

-- 18a. First EXPLAIN call
EXPLAIN (NORMALIZED, COSTS false) SELECT * FROM explain_norm_t1 WHERE dept_id = 10;

-- 18b. Second call — numeric ID is present
EXPLAIN (NORMALIZED, COSTS false) SELECT * FROM explain_norm_t1 WHERE dept_id = 10;

-- 18c. Different option — numeric ID is still present
EXPLAIN (NORMALIZED, ANALYZE false, COSTS false, TIMING false) SELECT * FROM explain_norm_t1 WHERE dept_id = 10;

-- ============================================================
-- 19. JSONB ? operator with string containing 'GENERIC_PLAN'
--     Regression test for Bug 4: '?' must NOT be lexed as a
--     placeholder when GENERIC_PLAN appears only inside a string
--     constant in the inner SQL (not as an EXPLAIN option).

-- 19a. Setup JSONB test table
CREATE TABLE bug4_jsonb (id int, data jsonb);
INSERT INTO bug4_jsonb VALUES (1, '{"key": "GENERIC_PLAN"}');

-- 19b. NORMALIZED with JSONB ? operator + string 'GENERIC_PLAN'
--      This previously failed with: ERROR: syntax error at or near "0"
EXPLAIN (NORMALIZED, COSTS false)
SELECT * FROM bug4_jsonb WHERE data ? 'GENERIC_PLAN' AND id = 10;

-- 19c. Plain EXPLAIN with JSONB ? operator (no NORMALIZED option)
--      Also previously failed due to the same preprocessor bug
EXPLAIN (COSTS false)
SELECT * FROM bug4_jsonb WHERE data ? 'GENERIC_PLAN' AND id = 10;

-- 19d. Cleanup JSONB test table
DROP TABLE bug4_jsonb;

-- ============================================================
-- 20. Regression tests for review fixes (C/D)
-- ============================================================

-- 20a. A lone '?' directly after the BETWEEN keyword is a placeholder:
--      both ? must be recognized as placeholders.
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT * FROM explain_norm_t1 WHERE dept_id BETWEEN ? AND ?;
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT * FROM explain_norm_t1 WHERE dept_id BETWEEN SYMMETRIC ? AND ?;
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT * FROM explain_norm_t1 WHERE dept_id BETWEEN ASYMMETRIC ? AND ?;

-- 20a1. Other expression-introducing keywords must also keep ? as a value.
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT * FROM explain_norm_t1 t1 JOIN explain_norm_t2 t2 ON ?;
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT CASE ? WHEN 1 THEN ? ELSE ? END FROM explain_norm_t1;

-- 20b. (Fix C) ? after THEN/WHEN/ELSE keywords in a CASE expression.
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT CASE WHEN dept_id = 10 THEN ? ELSE ? END FROM explain_norm_t1;

-- 20c. ? placeholder on a varchar column must keep string
--      comparison semantics.  Previously `name = ?` was rewritten to
--      the numeric literal 0, and the parser silently resolved the
--      comparison to (name)::bigint = $1, corrupting the plan.
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT * FROM explain_norm_t1 WHERE name = ?;

-- 20d. GENERIC_PLAN consistency: a literal and a ? placeholder
--      on a varchar column produce the same generic plan.
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT * FROM explain_norm_t1 WHERE name = 'Alice';
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT * FROM explain_norm_t1 WHERE name = ?;

-- ============================================================
-- 21. '?' parameters come from the lexer: scan.l reads '?' as a
--     placeholder only while the grammar parses an EXPLAIN whose
--     option list enables GENERIC_PLAN.  Inside such a statement
--     every '?' becomes a placeholder, and an operator run is
--     split at its first '?' (mirroring B-compatibility mode), so
--     the '?'-named operators (?, ?|, ?&, ??, ?~, ...) cannot be
--     used there.  Outside EXPLAIN (GENERIC_PLAN) they keep
--     working, as 21b shows.
-- ============================================================

-- 21a. Setup: JSONB table + user-defined operators "??" and "?~".
CREATE TABLE explain_norm_jsonb (id INTEGER, data JSONB);
INSERT INTO explain_norm_jsonb VALUES (1, '{"a": 1, "b": 2}');

CREATE FUNCTION explain_norm_qm_op(jsonb, text) RETURNS BOOLEAN
    AS $$ BEGIN RETURN $1 ? $2; END $$ LANGUAGE plpgsql;

CREATE OPERATOR ?? (
    LEFTARG = jsonb, RIGHTARG = text,
    PROCEDURE = explain_norm_qm_op
);

CREATE OPERATOR ?~ (
    LEFTARG = jsonb, RIGHTARG = text,
    PROCEDURE = explain_norm_qm_op
);

-- 21b. NORMALIZED does not switch the lexer on: the "??" operator is
--      kept verbatim and only the constants become $N.
EXPLAIN (NORMALIZED, COSTS false) SELECT * FROM explain_norm_jsonb WHERE data ?? 'a' AND id = 1;

-- 21c. Inside GENERIC_PLAN the run "??" is split at its first '?':
--      both '?'s become placeholders, so the statement no longer
--      parses.
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT * FROM explain_norm_jsonb WHERE data ?? ? AND id = ?;

-- 21d. The same for the '?~' operator.
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT * FROM explain_norm_jsonb WHERE data ?~ ? AND id = ?;

-- 21e. Placeholder arithmetic: an operator run adjacent to a '?' is
--      split at the '?', so ?-1, ?+1, ?||'suffix' and friends stay
--      placeholders with the rest of the run as ordinary operators.
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT * FROM explain_norm_t1 WHERE dept_id = ?-1;
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT * FROM explain_norm_t1 WHERE dept_id = ?+1;
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT * FROM explain_norm_t1 WHERE dept_id = ?-?;
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT * FROM explain_norm_t1 WHERE dept_id = ?-(?);
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT * FROM explain_norm_t1 WHERE dept_id = ?-/* comment */1;
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT * FROM explain_norm_t1 WHERE dept_id = ?-salary;
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT * FROM explain_norm_t1 WHERE name = ?||'suffix';
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT * FROM explain_norm_t1 WHERE id = ?&id;
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT * FROM explain_norm_t1 WHERE id = ?#id;
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT * FROM explain_norm_t1 WHERE dept_id = ?- - -1;
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT * FROM explain_norm_t1 WHERE dept_id = ?-(-(-1));
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT ?-1;
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT ?||'suffix';


--      A prefix '?-' or '?|' operator cannot be written inside
--      GENERIC_PLAN either: the '?' becomes a placeholder and the
--      remainder an operator, which no longer matches an operator
--      of that name.
CREATE TABLE explain_norm_geom (l lseg);
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT * FROM explain_norm_geom WHERE ?- l;
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT * FROM explain_norm_geom WHERE ?| l;
DROP TABLE explain_norm_geom;

-- 21f. Column-level '?' operators: the lone '?' of the jsonb
--      existence operator becomes a placeholder, and '?|' splits
--      into placeholder plus '|' -- syntax errors inside
--      GENERIC_PLAN.
CREATE TABLE explain_norm_qm_ident (
    id INTEGER,
    set JSONB,
    between JSONB
);
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT * FROM explain_norm_qm_ident WHERE set ?| array['a'] AND id = ?;
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT * FROM explain_norm_qm_ident WHERE between ? 'a' AND id = ?;
DROP TABLE explain_norm_qm_ident;

-- 21g. A user-defined binary '?' operator is not usable inside
--      GENERIC_PLAN either: '1. ? 2' becomes '1. $1 2'.
CREATE FUNCTION explain_norm_numeric_qm(numeric, numeric) RETURNS numeric
    AS $$ SELECT $1 + $2 $$ LANGUAGE SQL IMMUTABLE;
CREATE OPERATOR ? (
    LEFTARG = numeric, RIGHTARG = numeric,
    PROCEDURE = explain_norm_numeric_qm
);
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT 1. ? 2;
DROP OPERATOR ? (numeric, numeric);
DROP FUNCTION explain_norm_numeric_qm;

-- 21h. An unknown '?~~~' token: the '?' becomes a placeholder and the
--      remainder '~~~' reaches the parser -- a syntax error instead
--      of the previous "operator does not exist".
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT * FROM explain_norm_jsonb WHERE data ?~~~ 'a';

DROP OPERATOR ?? (jsonb, text);
DROP OPERATOR ?~ (jsonb, text);
DROP FUNCTION explain_norm_qm_op;
DROP TABLE explain_norm_jsonb;
-- ============================================================
-- 22. ? inside string literals / comments is data, not a placeholder
-- ============================================================

-- 22a. ? inside a string literal is data, not a placeholder.
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT * FROM explain_norm_t1 WHERE name = 'x ? y' AND dept_id = ?;

-- 22b. ? inside a block comment is not a placeholder.
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT /* ? */ * FROM explain_norm_t1 WHERE dept_id = ?;

-- 22c. ? inside a dollar-quoted string is not a placeholder.
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT * FROM explain_norm_t1 WHERE name = $$a ? b$$ AND dept_id = ?;

-- 22d. A standard string treats a trailing backslash as data, while an E string
--      uses backslash escapes.  In both cases the later placeholder is visible.
SET standard_conforming_strings = on;
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT * FROM explain_norm_t1 WHERE name = 'x\' AND dept_id = ?;
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT * FROM explain_norm_t1 WHERE name = E'x\\' AND dept_id = ?;

-- ============================================================
-- 23. B-compatibility database scenario
-- ============================================================

create database explain_norm_b_db dbcompatibility 'B';
\c explain_norm_b_db

CREATE TABLE explain_norm_b_t1 (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    dept_id INTEGER
);
INSERT INTO explain_norm_b_t1 VALUES (1, 'Alice', 10), (2, 'Bob', 20);

-- 23a. NORMALIZED in a B database: same normalization as A databases.
EXPLAIN (NORMALIZED, COSTS false) SELECT * FROM explain_norm_b_t1 WHERE dept_id = 10 AND name = 'Alice';

-- 23b. GENERIC_PLAN with ? placeholders in a B database.
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT * FROM explain_norm_b_t1 WHERE dept_id = ?;

-- 23c. ? inside a string literal in a B database must not be rewritten.
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT * FROM explain_norm_b_t1 WHERE name = 'x ? y' AND dept_id = ?;

-- 23d. GENERIC_PLAN with ? placeholder on a varchar column in a B database.
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT * FROM explain_norm_b_t1 WHERE name = ?;

DROP TABLE explain_norm_b_t1;

\c regression
drop database explain_norm_b_db;

-- ============================================================
-- 24. EXPLAIN option keywords remain valid unreserved identifiers
-- ============================================================

DROP TABLE IF EXISTS generic_plan;
CREATE TABLE generic_plan (
    normalized INTEGER
);
SELECT normalized FROM generic_plan;
DROP TABLE generic_plan;

-- ============================================================
-- 25. GENERIC_PLAN keeps '?' as a parameter through parse analysis
--     (the placeholder must not pass through the target type's
--     input function as a stand-in constant, so types whose input
--     would reject '0' — uuid, enum, ... — work like any other)
-- ============================================================

-- 25a. UUID column comparison and an explicit uuid cast.
CREATE TABLE explain_norm_uuid (id INTEGER, u UUID);
EXPLAIN (GENERIC_PLAN, COSTS false, FORMAT JSON) SELECT * FROM explain_norm_uuid WHERE u = ?;
EXPLAIN (GENERIC_PLAN, COSTS false, FORMAT JSON) SELECT ?::uuid;

-- 25b. Enum without a '0' label: a stand-in '0' would fail enum input.
CREATE TYPE explain_norm_mood AS ENUM ('sad', 'happy');
CREATE TABLE explain_norm_enum (id INTEGER, m explain_norm_mood);
EXPLAIN (GENERIC_PLAN, COSTS false, FORMAT JSON) SELECT * FROM explain_norm_enum WHERE m = ?;

-- 25c. Mixing explicit $N with '?' placeholders is rejected: a forward
--      scan cannot guarantee distinct numbers, so the lexer errors out
--      (same contract as B-compatibility mode).
EXPLAIN (GENERIC_PLAN, COSTS false, FORMAT JSON) SELECT * FROM explain_norm_t1 WHERE dept_id BETWEEN $1 AND ?;

-- 25d. A placeholder without context is coerced to text during parse
--      analysis, matching openGauss variable-parameter behavior.
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT ?;

-- 25e. GENERIC_PLAN false keeps '?' an operator (parse error), and with
--      duplicate options the last one wins.
EXPLAIN (GENERIC_PLAN false, COSTS false) SELECT * FROM explain_norm_t1 WHERE dept_id = ?;
EXPLAIN (GENERIC_PLAN, GENERIC_PLAN false, COSTS false) SELECT * FROM explain_norm_t1 WHERE dept_id = ?;
EXPLAIN (GENERIC_PLAN false, GENERIC_PLAN, COSTS false, FORMAT JSON) SELECT ?::uuid;

DROP TABLE explain_norm_enum;
DROP TYPE explain_norm_mood;
DROP TABLE explain_norm_uuid;

-- ============================================================
-- 26. Multi-statement batch: '?' placeholders are lexed in place, so
--     statement boundaries and snippets all refer to the original
--     message text.
-- ============================================================

-- 26a. Two statements with ? placeholders in the first one.
--      The escaped semicolon keeps both in one simple-query message.
EXPLAIN (GENERIC_PLAN, COSTS false) SELECT * FROM explain_norm_t1 WHERE dept_id = ? AND salary > ?\; SELECT 1;

-- 26b. Ten placeholders, each lexed as a parameter in text order.
--      Parse analysis coerces the placeholders to text and the batch
--      then runs SELECT 1.  Again, \; prevents psql from splitting the
--      batch into separate simple-query messages.
EXPLAIN (GENERIC_PLAN) SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?\; SELECT 1;

-- ============================================================
-- 27. $N numbering must come from PARAM_EXTERN nodes, not from a
--     text scan.  '$' is a legal identifier continuation character
--     (ident_cont in scan.l), so a bare token like "foo$9" is a single
--     identifier, NOT a $9 parameter reference.  Constants must still be
--     numbered from $1, matching the Parameter Values list, regardless of
--     $N-looking substrings inside identifiers, comments or string literals.
-- ============================================================
-- 27a. Identifier containing $9: the alias "foo$9" is one identifier token.
--      The constant 42 must become $1 (not $10), and Parameter Values must
--      agree.  Before the fix, a text scan counted $9 and renumbered to $10.
EXPLAIN (NORMALIZED, COSTS false) SELECT 42 AS foo$9;

-- 27b. $N inside a block comment: not a real placeholder.  The constant must
--      still be numbered from $1.
EXPLAIN (NORMALIZED, COSTS false) SELECT /* $7 */ 42 AS x;

-- 27c. $N inside a quoted identifier: not a real placeholder.
EXPLAIN (NORMALIZED, COSTS false) SELECT 42 AS "bar$8";

-- 27d. $N inside a string literal that survives as a value: the literal is
--      itself normalized to a placeholder, so any $N-looking text inside it
--      must not reserve a parameter number.
EXPLAIN (NORMALIZED, COSTS false) SELECT * FROM explain_norm_t1 WHERE name = 'a$5b';

-- 27e. Multiple constants with a $N-bearing identifier in between: numbering
--      stays contiguous from $1 and matches Parameter Values order.
EXPLAIN (NORMALIZED, COSTS false) SELECT * FROM explain_norm_t1 WHERE dept_id = 10 AND name = 'x' AND id = 1;

-- ============================================================
-- 28. Window frame offsets are expressions visited by the normalization
--     engine, but the shared query_tree_walker/query_tree_mutator omit
--     Query.windowClause.  EXPLAIN's local walkers/mutators must include
--     them so normalized text, Parameter Values and GENERIC_PLAN conversion
--     all cover the same constants.
-- ============================================================

-- 28a. NORMALIZED collects both frame offsets and the WHERE constant.
--      Parameter Values must contain all three values in source order.
EXPLAIN (NORMALIZED, COSTS false) SELECT sum(id) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING) FROM explain_norm_t1 WHERE id > 3;

-- 28b. GENERIC_PLAN converts frame-offset Const nodes as well as the WHERE
--      Const.  VERBOSE exposes the frame expressions in the WindowAgg output.
EXPLAIN (GENERIC_PLAN, COSTS false, VERBOSE true) SELECT sum(id) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING) FROM explain_norm_t1 WHERE id > 3;

-- 28c. Existing frame placeholders participate in the maximum PARAM_EXTERN
--      id, so the converted WHERE constant must not reuse $1.  (The Unique
--      SQL ID is not asserted equal to 28b's: a frame placeholder is typed
--      int8 directly while the literal form hashes an int4 Const under the
--      int4->int8 coercion FuncExpr, so the two jumbles differ.)
EXPLAIN (GENERIC_PLAN, COSTS false, VERBOSE true) SELECT sum(id) OVER (ORDER BY id ROWS BETWEEN ? PRECEDING AND ? FOLLOWING) FROM explain_norm_t1 WHERE id > 3;

-- 28d. Single-offset frame placeholder: the lone '?' after ROWS is
--      lexed as a parameter placeholder.
EXPLAIN (GENERIC_PLAN, COSTS false, VERBOSE true) SELECT sum(id) OVER (ORDER BY id ROWS ? PRECEDING) FROM explain_norm_t1 WHERE id > 3;

-- ============================================================
-- 29. EXPLAIN (NORMALIZED) as the second statement of one
--     simple-query message: the portal source text is the
--     per-statement snippet, so the normalization engine must be
--     told the snippet's offset within the original message
--     (constant locations are message-relative).  The constant
--     42 must still become $1.
-- ============================================================
SELECT 1\; EXPLAIN (NORMALIZED, COSTS false) SELECT 42;

-- Cleanup
DROP TABLE explain_norm_t1 CASCADE;
DROP TABLE explain_norm_t2 CASCADE;
