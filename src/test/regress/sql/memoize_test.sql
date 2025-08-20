DROP TABLE IF EXISTS strtest;
CREATE TABLE strtest (n name, t text);
INSERT INTO strtest VALUES('one','one'),('two','two'),('three',repeat(md5('three'),100));
INSERT INTO strtest SELECT * FROM strtest;
INSERT INTO strtest SELECT * FROM strtest;
INSERT INTO strtest SELECT * FROM strtest;
INSERT INTO strtest SELECT * FROM strtest;
INSERT INTO strtest SELECT * FROM strtest;
INSERT INTO strtest SELECT * FROM strtest;
INSERT INTO strtest SELECT * FROM strtest;
INSERT INTO strtest SELECT * FROM strtest;
INSERT INTO strtest SELECT * FROM strtest;
INSERT INTO strtest SELECT * FROM strtest;
INSERT INTO strtest SELECT * FROM strtest;
CREATE INDEX strtest_n_idx ON strtest (n);
CREATE INDEX strtest_t_idx ON strtest (t);
ANALYZE strtest;



--SET enable_bitmapscan TO off;
--SET enable_seqscan TO off;
--SET enable_material TO off;
SET enable_memoize TO on;
SHOW enable_memoize;
EXPLAIN (COSTS OFF) SELECT * FROM strtest s1 INNER JOIN strtest s2 ON s1.n < s2.n;
SET enable_memoize TO off;
SHOW enable_memoize;
EXPLAIN (COSTS OFF) SELECT * FROM strtest s1 INNER JOIN strtest s2 ON s1.n < s2.n;
DROP TABLE strtest;
