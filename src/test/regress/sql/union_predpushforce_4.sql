CREATE SCHEMA union_predpushforce_4;
SET search_path = union_predpushforce_4;
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

CREATE INDEX idx_index_a_state_int ON t_index_a(stateflag,int_id);
CREATE INDEX idx_index_b_state_int ON t_index_b(stateflag,int_id);
EXPLAIN (COSTS OFF) SELECT * FROM v_index_match t WHERE int_id IN (SELECT int_id FROM t_index_r WHERE tx_id=10);

DROP INDEX idx_index_a_state_int;
DROP INDEX idx_index_b_state_int;
CREATE INDEX idx_index_a_name ON t_index_a(name);
CREATE INDEX idx_index_b_name ON t_index_b(name);
EXPLAIN (COSTS OFF) SELECT * FROM v_index_match t WHERE int_id IN (SELECT int_id FROM t_index_r WHERE tx_id=10);

DROP INDEX idx_index_a_name;
DROP INDEX idx_index_b_name;
EXPLAIN (COSTS OFF) SELECT * FROM v_index_match t WHERE int_id IN (SELECT int_id FROM t_index_r WHERE tx_id=10);

CREATE INDEX idx_index_a_name ON t_index_a(int_id);
CREATE INDEX idx_index_b_name ON t_index_b(int_id);
EXPLAIN (COSTS OFF) SELECT * FROM v_index_match t WHERE int_id IN (SELECT int_id FROM t_index_r WHERE tx_id=10);

CREATE TABLE t_map_a(stateflag int, int_id int, name text);
CREATE TABLE t_map_b(name text, stateflag int, int_id int);
CREATE TABLE t_map_c(int_id int, name text, stateflag int);
INSERT INTO t_map_a VALUES (0,1,'a'),(0,2,'b');
INSERT INTO t_map_b VALUES ('c',0,1),('d',0,2);
INSERT INTO t_map_c VALUES (1,'e',0),(2,'f',0);
CREATE VIEW v_index_tree AS
SELECT int_id,stateflag,name FROM t_map_a WHERE stateflag=0
UNION
(SELECT int_id,stateflag,name FROM t_map_b WHERE stateflag=0
 UNION ALL SELECT int_id,stateflag,name FROM t_map_c WHERE stateflag=0);
CREATE INDEX idx_map_a ON t_map_a(int_id,stateflag);
CREATE INDEX idx_map_b ON t_map_b(int_id,stateflag);
CREATE INDEX idx_map_c ON t_map_c(int_id,stateflag);

EXPLAIN (COSTS OFF) SELECT v.* FROM v_index_tree v JOIN t_index_r r ON v.int_id=r.int_id WHERE r.tx_id=10;
SELECT count(*) AS matched_rows FROM v_index_tree v JOIN t_index_r r ON v.int_id=r.int_id WHERE r.tx_id=10;

DROP INDEX idx_map_c;
CREATE INDEX idx_map_c ON t_map_c(name);
EXPLAIN (COSTS OFF) SELECT v.* FROM v_index_tree v JOIN t_index_r r ON v.int_id=r.int_id WHERE r.tx_id=10;

EXPLAIN (COSTS OFF) SELECT /*+ predpush(r) */ v.* FROM v_index_tree v JOIN t_index_r r ON v.int_id=r.int_id WHERE r.tx_id=10;

DROP INDEX idx_map_a;
DROP INDEX idx_map_b;
DROP INDEX idx_map_c;
EXPLAIN (COSTS OFF) SELECT v.* FROM v_index_tree v JOIN t_index_r r ON v.int_id=r.int_id WHERE r.tx_id=10;

EXPLAIN (COSTS OFF) SELECT /*+ predpush(r) */ v.* FROM v_index_tree v JOIN t_index_r r ON v.int_id=r.int_id WHERE r.tx_id=10;

CREATE INDEX idx_map_a ON t_map_a(name,int_id);
CREATE INDEX idx_map_b ON t_map_b(name,int_id);
CREATE INDEX idx_map_c ON t_map_c(name,int_id);
EXPLAIN (COSTS OFF) SELECT v.* FROM v_index_tree v JOIN t_index_r r ON v.int_id=r.int_id WHERE r.tx_id=10;

DROP SCHEMA union_predpushforce_4 CASCADE;
