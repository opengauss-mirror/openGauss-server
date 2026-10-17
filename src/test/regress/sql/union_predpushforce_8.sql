DROP SCHEMA IF EXISTS union_predpushforce_8 CASCADE;
CREATE SCHEMA union_predpushforce_8;
SET search_path = union_predpushforce_8;
SET rewrite_rule = 'predpushforce';
CREATE TABLE t_index_r(int_id int, tx_id int);
CREATE TABLE t_index_a(int_id int, stateflag int, name text);
CREATE TABLE t_index_b(int_id int, stateflag int, name text);
INSERT INTO t_index_r VALUES (1,10),(2,10);
INSERT INTO t_index_a VALUES (1,0,'a1'),(2,0,'a2');
INSERT INTO t_index_b VALUES (1,0,'b1'),(2,0,'b2');
CREATE VIEW v_index_match AS
SELECT int_id,stateflag,name FROM t_index_a WHERE stateflag=0
UNION ALL
SELECT int_id,stateflag,name FROM t_index_b WHERE stateflag=0;
CREATE INDEX idx_index_a_state_int ON t_index_a(int_id,stateflag);
CREATE INDEX idx_index_b_state_int ON t_index_b(int_id,stateflag);
SELECT 'CASE both_matching';
EXPLAIN (COSTS OFF) SELECT * FROM v_index_match t WHERE int_id IN (SELECT int_id FROM t_index_r WHERE tx_id=10);
SELECT count(*) FROM v_index_match t WHERE int_id IN (SELECT int_id FROM t_index_r WHERE tx_id=10);
DROP INDEX idx_index_a_state_int;
SELECT 'CASE only_b_matching';
EXPLAIN (COSTS OFF) SELECT * FROM v_index_match t WHERE int_id IN (SELECT int_id FROM t_index_r WHERE tx_id=10);
SELECT count(*) FROM v_index_match t WHERE int_id IN (SELECT int_id FROM t_index_r WHERE tx_id=10);
CREATE INDEX idx_index_a_state_int ON t_index_a(int_id,stateflag);
DROP INDEX idx_index_b_state_int;
SELECT 'CASE only_a_matching';
EXPLAIN (COSTS OFF) SELECT * FROM v_index_match t WHERE int_id IN (SELECT int_id FROM t_index_r WHERE tx_id=10);
SELECT count(*) FROM v_index_match t WHERE int_id IN (SELECT int_id FROM t_index_r WHERE tx_id=10);
DROP INDEX idx_index_a_state_int;
SELECT 'CASE neither_matching';
EXPLAIN (COSTS OFF) SELECT * FROM v_index_match t WHERE int_id IN (SELECT int_id FROM t_index_r WHERE tx_id=10);
SELECT count(*) FROM v_index_match t WHERE int_id IN (SELECT int_id FROM t_index_r WHERE tx_id=10);
SELECT 'CASE neither_index_matching_for_pushed-down predicates';
CREATE INDEX idx_index_a_state_int ON t_index_a(stateflag,int_id);
CREATE INDEX idx_index_b_state_int ON t_index_b(stateflag);
EXPLAIN (COSTS OFF) SELECT * FROM v_index_match t WHERE int_id IN (SELECT int_id FROM t_index_r WHERE tx_id=10);
SELECT count(*) FROM v_index_match t WHERE int_id IN (SELECT int_id FROM t_index_r WHERE tx_id=10);
DROP INDEX idx_index_b_state_int;
CREATE INDEX idx_index_b_state_int ON t_index_b(stateflag,int_id);
SELECT 'CASE both_matching_explicit_hint';
EXPLAIN (COSTS OFF) SELECT /*+ predpush(r) */ * FROM v_index_match t WHERE int_id IN (SELECT int_id FROM t_index_r r WHERE tx_id=10);
SELECT /*+ predpush(r) */ count(*) FROM v_index_match t WHERE int_id IN (SELECT int_id FROM t_index_r r WHERE tx_id=10);
DROP INDEX idx_index_b_state_int;
SELECT 'CASE only_a_matching_explicit_hint';
EXPLAIN (COSTS OFF) SELECT /*+ predpush(r) */ * FROM v_index_match t WHERE int_id IN (SELECT int_id FROM t_index_r r WHERE tx_id=10);
SELECT /*+ predpush(r) */ count(*) FROM v_index_match t WHERE int_id IN (SELECT int_id FROM t_index_r r WHERE tx_id=10);
-- Explicit hints retain the original behavior even when a branch has no matching index.
CREATE INDEX idx_index_b_state_int ON t_index_b(stateflag,int_id);
DROP INDEX idx_index_a_state_int;
SELECT 'CASE only_b_matching_explicit_hint';
EXPLAIN (COSTS OFF) SELECT /*+ predpush(r) */ * FROM v_index_match t WHERE int_id IN (SELECT int_id FROM t_index_r r WHERE tx_id=10);
SELECT /*+ predpush(r) */ count(*) FROM v_index_match t WHERE int_id IN (SELECT int_id FROM t_index_r r WHERE tx_id=10);
CREATE INDEX idx_index_a_state_int ON t_index_a(stateflag,int_id);
DROP INDEX idx_index_b_state_int;
CREATE INDEX idx_index_b_state_int ON t_index_b(stateflag);
SELECT 'CASE b_local_filter_index_only_explicit_hint';
EXPLAIN (COSTS OFF) SELECT /*+ predpush(r) */ * FROM v_index_match t WHERE int_id IN (SELECT int_id FROM t_index_r r WHERE tx_id=10);
SELECT /*+ predpush(r) */ count(*) FROM v_index_match t WHERE int_id IN (SELECT int_id FROM t_index_r r WHERE tx_id=10);
DROP INDEX idx_index_a_state_int;
DROP INDEX idx_index_b_state_int;
SELECT 'CASE neither_matching_explicit_hint';
EXPLAIN (COSTS OFF) SELECT /*+ predpush(r) */ * FROM v_index_match t WHERE int_id IN (SELECT int_id FROM t_index_r r WHERE tx_id=10);
SELECT /*+ predpush(r) */ count(*) FROM v_index_match t WHERE int_id IN (SELECT int_id FROM t_index_r r WHERE tx_id=10);
-- Also exercise an Append whose branches can be pulled up as base relations.
CREATE INDEX idx_index_a_state_int ON t_index_a(int_id, stateflag);
CREATE VIEW v_index_plain AS
SELECT int_id,stateflag,name FROM t_index_a
UNION ALL
SELECT int_id,stateflag,name FROM t_index_b;
SELECT 'CASE plain_only_a_matching_auto';
EXPLAIN (COSTS OFF) SELECT * FROM v_index_plain t WHERE stateflag=0 AND int_id IN (SELECT int_id FROM t_index_r r WHERE tx_id=10);
SELECT count(*) FROM v_index_plain t WHERE stateflag=0 AND int_id IN (SELECT int_id FROM t_index_r r WHERE tx_id=10);
SELECT 'CASE plain_only_a_matching_explicit_hint';
EXPLAIN (COSTS OFF) SELECT /*+ predpush(r) */ * FROM v_index_plain t WHERE stateflag=0 AND int_id IN (SELECT int_id FROM t_index_r r WHERE tx_id=10);
SELECT /*+ predpush(r) */ count(*) FROM v_index_plain t WHERE stateflag=0 AND int_id IN (SELECT int_id FROM t_index_r r WHERE tx_id=10);
-- UNION retains duplicate elimination; its existing explicit-hint behavior is unchanged.
CREATE VIEW v_index_distinct AS
SELECT int_id,stateflag,name FROM t_index_a WHERE stateflag=0
UNION
SELECT int_id,stateflag,name FROM t_index_b WHERE stateflag=0;
SELECT 'CASE union_only_a_matching_auto';
EXPLAIN (COSTS OFF) SELECT * FROM v_index_distinct t WHERE int_id IN (SELECT int_id FROM t_index_r r WHERE tx_id=10);
SELECT count(*) FROM v_index_distinct t WHERE int_id IN (SELECT int_id FROM t_index_r r WHERE tx_id=10);
SELECT 'CASE union_only_a_matching_explicit_hint';
EXPLAIN (COSTS OFF) SELECT /*+ predpush(r) */ * FROM v_index_distinct t WHERE int_id IN (SELECT int_id FROM t_index_r r WHERE tx_id=10);
SELECT /*+ predpush(r) */ count(*) FROM v_index_distinct t WHERE int_id IN (SELECT int_id FROM t_index_r r WHERE tx_id=10);

DROP SCHEMA union_predpushforce_8 CASCADE;
