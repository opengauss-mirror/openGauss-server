-- A-mode regression: DESC defaults to NULLS FIRST.
-- Use an index-ordering prefix to bound an ordinary sort.
CREATE TEMP TABLE prefix_sort_test(id int, name text);
INSERT INTO prefix_sort_test VALUES
    (1, 'b'), (1, NULL), (1, 'a'),
    (2, 'c'), (2, NULL), (2, 'a');
INSERT INTO prefix_sort_test
SELECT id, lpad(id::text, 3, '0') FROM generate_series(3, 65) AS g(id);
INSERT INTO prefix_sort_test VALUES (28, NULL);
CREATE INDEX prefix_sort_test_idx ON prefix_sort_test(id, name DESC NULLS LAST);
ANALYZE prefix_sort_test;
SET enable_sort = true;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
\pset format unaligned
EXPLAIN (COSTS OFF)
SELECT * FROM prefix_sort_test ORDER BY id, name DESC LIMIT 5;
SELECT * FROM prefix_sort_test ORDER BY id, name DESC LIMIT 5;
SELECT * FROM prefix_sort_test ORDER BY id, name DESC OFFSET 2 LIMIT 3;
-- The LIMIT boundary must include the complete group, even with OFFSET.
EXPLAIN (COSTS OFF)
SELECT * FROM prefix_sort_test ORDER BY id, name DESC OFFSET 30 LIMIT 5;
SELECT * FROM prefix_sort_test ORDER BY id, name DESC OFFSET 30 LIMIT 5;
-- Volatile sort keys require the final projection and must use ordinary sort.
EXPLAIN (COSTS OFF)
SELECT id FROM prefix_sort_test ORDER BY id, random() LIMIT 3;
-- Row locks and set-returning projections prevent a runtime sort bound.
EXPLAIN (COSTS OFF)
SELECT * FROM prefix_sort_test ORDER BY id, name DESC LIMIT 5 FOR UPDATE;
EXPLAIN (COSTS OFF)
SELECT id, name, generate_series(1, 2) FROM prefix_sort_test ORDER BY id, name DESC LIMIT 5;
-- Separate volatile expressions must retain independent evaluations.
CREATE SEQUENCE prefix_sort_volatile_seq;
SELECT id, nextval('prefix_sort_volatile_seq') AS r,
    nextval('prefix_sort_volatile_seq') AS s
FROM prefix_sort_test ORDER BY id, r LIMIT 3;
SELECT last_value FROM prefix_sort_volatile_seq;
SELECT count(*) AS rows_seen,
    bool_and(r >= 0 AND r < 1 AND s >= 0 AND s < 1) AS valid_values
FROM (
    SELECT random() + 0.0 * nextval('prefix_sort_volatile_seq') AS r,
        random() + 0.0 * nextval('prefix_sort_volatile_seq') AS s
    FROM prefix_sort_test ORDER BY id, r
) AS q;
SELECT last_value FROM prefix_sort_volatile_seq;
DROP SEQUENCE prefix_sort_volatile_seq;
-- OFFSET without LIMIT must use ordinary sorting through input EOF.
INSERT INTO prefix_sort_test
SELECT id, lpad(id::text, 5, '0') FROM generate_series(66, 10000) AS g(id);
ANALYZE prefix_sort_test;
EXPLAIN (COSTS OFF)
SELECT * FROM prefix_sort_test ORDER BY id, name DESC OFFSET 10000;
SELECT * FROM prefix_sort_test ORDER BY id, name DESC OFFSET 10000;
-- A held cursor without LIMIT must materialize the complete result.
BEGIN;
DECLARE prefix_sort_hold CURSOR WITH HOLD FOR
SELECT * FROM prefix_sort_test ORDER BY id, name DESC;
MOVE FORWARD ALL FROM prefix_sort_hold;
COMMIT;
MOVE ABSOLUTE 0 FROM prefix_sort_hold;
FETCH 3 FROM prefix_sort_hold;
CLOSE prefix_sort_hold;
-- A bounded sort retains ordinary backward scan and rewind support.
BEGIN;
DECLARE prefix_sort_scroll CURSOR WITH HOLD FOR
SELECT * FROM prefix_sort_test ORDER BY id, name DESC LIMIT 5;
MOVE FORWARD ALL FROM prefix_sort_scroll;
COMMIT;
MOVE ABSOLUTE 0 FROM prefix_sort_scroll;
FETCH FORWARD 5 FROM prefix_sort_scroll;
FETCH BACKWARD 2 FROM prefix_sort_scroll;
CLOSE prefix_sort_scroll;
\pset format aligned
RESET enable_bitmapscan;
RESET enable_seqscan;
RESET enable_sort;
DROP TABLE prefix_sort_test;

-- Issue #8331: default costing must use the useful index prefix without
-- disabling sequential or bitmap scans, and LIMIT 20 must stop after the
-- one-row lookahead instead of consuming the whole 100,000-row table.
CREATE TEMP TABLE prefix_sort_issue8331(id int, name varchar);
CREATE INDEX prefix_sort_issue8331_idx
    ON prefix_sort_issue8331(id, name DESC NULLS LAST);
INSERT INTO prefix_sort_issue8331
SELECT id, 'name' || id FROM generate_series(1, 100000) AS g(id);
ANALYZE prefix_sort_issue8331;
\pset format unaligned
EXPLAIN (ANALYZE ON, COSTS OFF, TIMING OFF)
SELECT * FROM prefix_sort_issue8331 ORDER BY id, name DESC LIMIT 20;
SELECT * FROM prefix_sort_issue8331 ORDER BY id, name DESC LIMIT 20;
\pset format aligned
DROP TABLE prefix_sort_issue8331;

-- Default costing must not assume qualifying rows are uniform in index order.
-- The filtering index competes with an ordering-prefix index; matches are last.
CREATE TEMP TABLE prefix_sort_filter(a int, b int, c int);
INSERT INTO prefix_sort_filter
SELECT g, CASE WHEN g > 19000 THEN 1 ELSE 0 END, -g
FROM generate_series(1, 20000) g;
CREATE INDEX prefix_sort_filter_a ON prefix_sort_filter(a);
CREATE INDEX prefix_sort_filter_b ON prefix_sort_filter(b);
ANALYZE prefix_sort_filter;
\pset format unaligned
EXPLAIN (COSTS OFF)
SELECT * FROM prefix_sort_filter WHERE b = 1 ORDER BY a, c LIMIT 10;
SELECT * FROM prefix_sort_filter WHERE b = 1 ORDER BY a, c LIMIT 10;
-- A non-leading index qual does not bound the number of index entries scanned.
DROP INDEX prefix_sort_filter_a;
CREATE INDEX prefix_sort_filter_abc ON prefix_sort_filter(a, b, c);
EXPLAIN (COSTS OFF)
SELECT * FROM prefix_sort_filter WHERE b = 1 ORDER BY a, c DESC LIMIT 10;
-- Index-only scans with residual filters need the same conservative costing.
CREATE INDEX prefix_sort_filter_abs_b ON prefix_sort_filter(abs(b));
ANALYZE prefix_sort_filter;
EXPLAIN (COSTS OFF)
SELECT * FROM prefix_sort_filter WHERE abs(b) = 1 ORDER BY a, c DESC LIMIT 10;
-- A leading range qual can still benefit from sorting only the bounded prefix.
CREATE INDEX prefix_sort_filter_a ON prefix_sort_filter(a);
EXPLAIN (COSTS OFF)
SELECT * FROM prefix_sort_filter WHERE a > 19000 ORDER BY a, c DESC LIMIT 10;
SELECT * FROM prefix_sort_filter WHERE a > 19000 ORDER BY a, c DESC LIMIT 10;
\pset format aligned
DROP TABLE prefix_sort_filter;
