--
-- SP-GiST
--

CREATE TABLE quad_point_tbl AS
    SELECT point(unique1,unique2) AS p FROM tenk1;

INSERT INTO quad_point_tbl
    SELECT '(333.0,400.0)'::point FROM generate_series(1,1000);

INSERT INTO quad_point_tbl VALUES (NULL), (NULL), (NULL);

CREATE INDEX sp_quad_ind ON quad_point_tbl USING spgist (p);

CREATE TABLE kd_point_tbl AS SELECT * FROM quad_point_tbl;

CREATE INDEX sp_kd_ind ON kd_point_tbl USING spgist (p kd_point_ops);

CREATE TABLE suffix_text_tbl AS
    SELECT name AS t FROM road WHERE name !~ '^[0-9]';

INSERT INTO suffix_text_tbl
    SELECT 'P0123456789abcdef' FROM generate_series(1,1000);
INSERT INTO suffix_text_tbl VALUES ('P0123456789abcde');
INSERT INTO suffix_text_tbl VALUES ('P0123456789abcdefF');

CREATE INDEX sp_suff_ind ON suffix_text_tbl USING spgist (t);


--
-- Test SP-GiST indexes
--

-- get non-indexed results for comparison purposes

SET enable_seqscan = ON;
SET enable_indexscan = OFF;
SET enable_bitmapscan = OFF;

SELECT count(*) FROM quad_point_tbl WHERE p IS NULL;

SELECT count(*) FROM quad_point_tbl WHERE p IS NOT NULL;

SELECT count(*) FROM quad_point_tbl;

SELECT count(*) FROM quad_point_tbl WHERE p <@ box '(200,200,1000,1000)';

SELECT count(*) FROM quad_point_tbl WHERE box '(200,200,1000,1000)' @> p;

SELECT count(*) FROM quad_point_tbl WHERE p << '(5000, 4000)';

SELECT count(*) FROM quad_point_tbl WHERE p >> '(5000, 4000)';

SELECT count(*) FROM quad_point_tbl WHERE p <^ '(5000, 4000)';

SELECT count(*) FROM quad_point_tbl WHERE p >^ '(5000, 4000)';

SELECT count(*) FROM quad_point_tbl WHERE p ~= '(4585, 365)';

SELECT count(*) FROM suffix_text_tbl WHERE t = 'P0123456789abcdef';

SELECT count(*) FROM suffix_text_tbl WHERE t = 'P0123456789abcde';

SELECT count(*) FROM suffix_text_tbl WHERE t = 'P0123456789abcdefF';

SELECT count(*) FROM suffix_text_tbl WHERE t <    'Aztec                         Ct  ';

SELECT count(*) FROM suffix_text_tbl WHERE t ~<~  'Aztec                         Ct  ';

SELECT count(*) FROM suffix_text_tbl WHERE t <=   'Aztec                         Ct  ';

SELECT count(*) FROM suffix_text_tbl WHERE t ~<=~ 'Aztec                         Ct  ';

SELECT count(*) FROM suffix_text_tbl WHERE t =    'Aztec                         Ct  ';

SELECT count(*) FROM suffix_text_tbl WHERE t =    'Worth                         St  ';

SELECT count(*) FROM suffix_text_tbl WHERE t >=   'Worth                         St  ';

SELECT count(*) FROM suffix_text_tbl WHERE t ~>=~ 'Worth                         St  ';

SELECT count(*) FROM suffix_text_tbl WHERE t >    'Worth                         St  ';

SELECT count(*) FROM suffix_text_tbl WHERE t ~>~  'Worth                         St  ';

-- Now check the results from plain indexscan
SET enable_seqscan = OFF;
SET enable_indexscan = ON;
SET enable_bitmapscan = OFF;

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM quad_point_tbl WHERE p IS NULL;
SELECT count(*) FROM quad_point_tbl WHERE p IS NULL;

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM quad_point_tbl WHERE p IS NOT NULL;
SELECT count(*) FROM quad_point_tbl WHERE p IS NOT NULL;

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM quad_point_tbl;
SELECT count(*) FROM quad_point_tbl;

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM quad_point_tbl WHERE p <@ box '(200,200,1000,1000)';
SELECT count(*) FROM quad_point_tbl WHERE p <@ box '(200,200,1000,1000)';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM quad_point_tbl WHERE box '(200,200,1000,1000)' @> p;
SELECT count(*) FROM quad_point_tbl WHERE box '(200,200,1000,1000)' @> p;

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM quad_point_tbl WHERE p << '(5000, 4000)';
SELECT count(*) FROM quad_point_tbl WHERE p << '(5000, 4000)';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM quad_point_tbl WHERE p >> '(5000, 4000)';
SELECT count(*) FROM quad_point_tbl WHERE p >> '(5000, 4000)';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM quad_point_tbl WHERE p <^ '(5000, 4000)';
SELECT count(*) FROM quad_point_tbl WHERE p <^ '(5000, 4000)';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM quad_point_tbl WHERE p >^ '(5000, 4000)';
SELECT count(*) FROM quad_point_tbl WHERE p >^ '(5000, 4000)';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM quad_point_tbl WHERE p ~= '(4585, 365)';
SELECT count(*) FROM quad_point_tbl WHERE p ~= '(4585, 365)';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM kd_point_tbl WHERE p <@ box '(200,200,1000,1000)';
SELECT count(*) FROM kd_point_tbl WHERE p <@ box '(200,200,1000,1000)';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM kd_point_tbl WHERE box '(200,200,1000,1000)' @> p;
SELECT count(*) FROM kd_point_tbl WHERE box '(200,200,1000,1000)' @> p;

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM kd_point_tbl WHERE p << '(5000, 4000)';
SELECT count(*) FROM kd_point_tbl WHERE p << '(5000, 4000)';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM kd_point_tbl WHERE p >> '(5000, 4000)';
SELECT count(*) FROM kd_point_tbl WHERE p >> '(5000, 4000)';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM kd_point_tbl WHERE p <^ '(5000, 4000)';
SELECT count(*) FROM kd_point_tbl WHERE p <^ '(5000, 4000)';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM kd_point_tbl WHERE p >^ '(5000, 4000)';
SELECT count(*) FROM kd_point_tbl WHERE p >^ '(5000, 4000)';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM kd_point_tbl WHERE p ~= '(4585, 365)';
SELECT count(*) FROM kd_point_tbl WHERE p ~= '(4585, 365)';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM suffix_text_tbl WHERE t = 'P0123456789abcdef';
SELECT count(*) FROM suffix_text_tbl WHERE t = 'P0123456789abcdef';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM suffix_text_tbl WHERE t = 'P0123456789abcde';
SELECT count(*) FROM suffix_text_tbl WHERE t = 'P0123456789abcde';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM suffix_text_tbl WHERE t = 'P0123456789abcdefF';
SELECT count(*) FROM suffix_text_tbl WHERE t = 'P0123456789abcdefF';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM suffix_text_tbl WHERE t <    'Aztec                         Ct  ';
SELECT count(*) FROM suffix_text_tbl WHERE t <    'Aztec                         Ct  ';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM suffix_text_tbl WHERE t ~<~  'Aztec                         Ct  ';
SELECT count(*) FROM suffix_text_tbl WHERE t ~<~  'Aztec                         Ct  ';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM suffix_text_tbl WHERE t <=   'Aztec                         Ct  ';
SELECT count(*) FROM suffix_text_tbl WHERE t <=   'Aztec                         Ct  ';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM suffix_text_tbl WHERE t ~<=~ 'Aztec                         Ct  ';
SELECT count(*) FROM suffix_text_tbl WHERE t ~<=~ 'Aztec                         Ct  ';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM suffix_text_tbl WHERE t =    'Aztec                         Ct  ';
SELECT count(*) FROM suffix_text_tbl WHERE t =    'Aztec                         Ct  ';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM suffix_text_tbl WHERE t =    'Worth                         St  ';
SELECT count(*) FROM suffix_text_tbl WHERE t =    'Worth                         St  ';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM suffix_text_tbl WHERE t >=   'Worth                         St  ';
SELECT count(*) FROM suffix_text_tbl WHERE t >=   'Worth                         St  ';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM suffix_text_tbl WHERE t ~>=~ 'Worth                         St  ';
SELECT count(*) FROM suffix_text_tbl WHERE t ~>=~ 'Worth                         St  ';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM suffix_text_tbl WHERE t >    'Worth                         St  ';
SELECT count(*) FROM suffix_text_tbl WHERE t >    'Worth                         St  ';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM suffix_text_tbl WHERE t ~>~  'Worth                         St  ';
SELECT count(*) FROM suffix_text_tbl WHERE t ~>~  'Worth                         St  ';

-- Now check the results from bitmap indexscan
SET enable_seqscan = OFF;
SET enable_indexscan = OFF;
SET enable_bitmapscan = ON;

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM quad_point_tbl WHERE p IS NULL;
SELECT count(*) FROM quad_point_tbl WHERE p IS NULL;

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM quad_point_tbl WHERE p IS NOT NULL;
SELECT count(*) FROM quad_point_tbl WHERE p IS NOT NULL;

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM quad_point_tbl;
SELECT count(*) FROM quad_point_tbl;

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM quad_point_tbl WHERE p <@ box '(200,200,1000,1000)';
SELECT count(*) FROM quad_point_tbl WHERE p <@ box '(200,200,1000,1000)';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM quad_point_tbl WHERE box '(200,200,1000,1000)' @> p;
SELECT count(*) FROM quad_point_tbl WHERE box '(200,200,1000,1000)' @> p;

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM quad_point_tbl WHERE p << '(5000, 4000)';
SELECT count(*) FROM quad_point_tbl WHERE p << '(5000, 4000)';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM quad_point_tbl WHERE p >> '(5000, 4000)';
SELECT count(*) FROM quad_point_tbl WHERE p >> '(5000, 4000)';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM quad_point_tbl WHERE p <^ '(5000, 4000)';
SELECT count(*) FROM quad_point_tbl WHERE p <^ '(5000, 4000)';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM quad_point_tbl WHERE p >^ '(5000, 4000)';
SELECT count(*) FROM quad_point_tbl WHERE p >^ '(5000, 4000)';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM quad_point_tbl WHERE p ~= '(4585, 365)';
SELECT count(*) FROM quad_point_tbl WHERE p ~= '(4585, 365)';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM kd_point_tbl WHERE p <@ box '(200,200,1000,1000)';
SELECT count(*) FROM kd_point_tbl WHERE p <@ box '(200,200,1000,1000)';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM kd_point_tbl WHERE box '(200,200,1000,1000)' @> p;
SELECT count(*) FROM kd_point_tbl WHERE box '(200,200,1000,1000)' @> p;

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM kd_point_tbl WHERE p << '(5000, 4000)';
SELECT count(*) FROM kd_point_tbl WHERE p << '(5000, 4000)';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM kd_point_tbl WHERE p >> '(5000, 4000)';
SELECT count(*) FROM kd_point_tbl WHERE p >> '(5000, 4000)';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM kd_point_tbl WHERE p <^ '(5000, 4000)';
SELECT count(*) FROM kd_point_tbl WHERE p <^ '(5000, 4000)';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM kd_point_tbl WHERE p >^ '(5000, 4000)';
SELECT count(*) FROM kd_point_tbl WHERE p >^ '(5000, 4000)';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM kd_point_tbl WHERE p ~= '(4585, 365)';
SELECT count(*) FROM kd_point_tbl WHERE p ~= '(4585, 365)';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM suffix_text_tbl WHERE t = 'P0123456789abcdef';
SELECT count(*) FROM suffix_text_tbl WHERE t = 'P0123456789abcdef';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM suffix_text_tbl WHERE t = 'P0123456789abcde';
SELECT count(*) FROM suffix_text_tbl WHERE t = 'P0123456789abcde';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM suffix_text_tbl WHERE t = 'P0123456789abcdefF';
SELECT count(*) FROM suffix_text_tbl WHERE t = 'P0123456789abcdefF';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM suffix_text_tbl WHERE t <    'Aztec                         Ct  ';
SELECT count(*) FROM suffix_text_tbl WHERE t <    'Aztec                         Ct  ';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM suffix_text_tbl WHERE t ~<~  'Aztec                         Ct  ';
SELECT count(*) FROM suffix_text_tbl WHERE t ~<~  'Aztec                         Ct  ';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM suffix_text_tbl WHERE t <=   'Aztec                         Ct  ';
SELECT count(*) FROM suffix_text_tbl WHERE t <=   'Aztec                         Ct  ';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM suffix_text_tbl WHERE t ~<=~ 'Aztec                         Ct  ';
SELECT count(*) FROM suffix_text_tbl WHERE t ~<=~ 'Aztec                         Ct  ';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM suffix_text_tbl WHERE t =    'Aztec                         Ct  ';
SELECT count(*) FROM suffix_text_tbl WHERE t =    'Aztec                         Ct  ';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM suffix_text_tbl WHERE t =    'Worth                         St  ';
SELECT count(*) FROM suffix_text_tbl WHERE t =    'Worth                         St  ';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM suffix_text_tbl WHERE t >=   'Worth                         St  ';
SELECT count(*) FROM suffix_text_tbl WHERE t >=   'Worth                         St  ';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM suffix_text_tbl WHERE t ~>=~ 'Worth                         St  ';
SELECT count(*) FROM suffix_text_tbl WHERE t ~>=~ 'Worth                         St  ';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM suffix_text_tbl WHERE t >    'Worth                         St  ';
SELECT count(*) FROM suffix_text_tbl WHERE t >    'Worth                         St  ';

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT count(*) FROM suffix_text_tbl WHERE t ~>~  'Worth                         St  ';
SELECT count(*) FROM suffix_text_tbl WHERE t ~>~  'Worth                         St  ';

RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_bitmapscan;