DROP TABLE IF EXISTS predpush_guard_a, predpush_guard_b, predpush_guard_r CASCADE;
CREATE TABLE predpush_guard_r(id int);
CREATE TABLE predpush_guard_a(id int, stateflag int);
CREATE TABLE predpush_guard_b(id int, stateflag int);
INSERT INTO predpush_guard_r SELECT generate_series(1, 3);
INSERT INTO predpush_guard_a SELECT i, 0 FROM generate_series(1, 2000) i;
INSERT INTO predpush_guard_b SELECT i, 0 FROM generate_series(1, 2000) i;
CREATE INDEX predpush_guard_a_idx ON predpush_guard_a(id, stateflag);
CREATE INDEX predpush_guard_b_idx ON predpush_guard_b(id, stateflag);
ANALYZE predpush_guard_a;
ANALYZE predpush_guard_b;
ANALYZE predpush_guard_r;
SET rewrite_rule = predpushforce;

-- Both branches have a matching index: parameterized Append is allowed.
EXPLAIN (COSTS OFF)
SELECT v.id
FROM (SELECT id, stateflag FROM predpush_guard_a
      UNION ALL
      SELECT id, stateflag FROM predpush_guard_b) v
JOIN predpush_guard_r r ON v.id = r.id
WHERE v.stateflag = 0;

-- One branch has no matching index: still parameterize both branches.
DROP INDEX predpush_guard_a_idx;
EXPLAIN (COSTS OFF)
SELECT v.id
FROM (SELECT id, stateflag FROM predpush_guard_a
      UNION ALL
      SELECT id, stateflag FROM predpush_guard_b) v
JOIN predpush_guard_r r ON v.id = r.id
WHERE v.stateflag = 0;

-- An index may still be chosen as a SeqScan by cost, but it remains eligible.
CREATE INDEX predpush_guard_a_idx ON predpush_guard_a(id, stateflag);
DROP INDEX predpush_guard_b_idx;
CREATE INDEX predpush_guard_b_idx ON predpush_guard_b(id);
ANALYZE predpush_guard_a;
ANALYZE predpush_guard_b;
EXPLAIN (COSTS OFF)
SELECT v.id
FROM (SELECT id, stateflag FROM predpush_guard_a
      UNION ALL
      SELECT id, stateflag FROM predpush_guard_b) v
JOIN predpush_guard_r r ON v.id = r.id
WHERE v.stateflag = 0;

-- Result cardinality remains unchanged after parameterizing the Append.
SELECT count(*)
FROM (SELECT id, stateflag FROM predpush_guard_a
      UNION ALL
      SELECT id, stateflag FROM predpush_guard_b) v
JOIN predpush_guard_r r ON v.id = r.id
WHERE v.stateflag = 0;

DROP TABLE IF EXISTS predpush_guard_a, predpush_guard_b, predpush_guard_r CASCADE;
