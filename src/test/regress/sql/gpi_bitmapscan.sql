
DROP TABLE if exists gpi_bitmap_table1;
DROP TABLE if exists gpi_bitmap_table2;

SET enable_seqscan=off;
SET enable_indexscan=off;

CREATE TABLE gpi_bitmap_table1
(
    c1 int,
    c2 int,
    c3 int
)
partition by range (c1)
(
    partition p0_gpi_bitmap_table1 VALUES less than (10000),
    partition p1_gpi_bitmap_table1 VALUES less than (20000),
    partition p2_gpi_bitmap_table1 VALUES less than (30000),
    partition p3_gpi_bitmap_table1 VALUES less than (maxvalue)
);


INSERT INTO gpi_bitmap_table1
  SELECT (r%53), (r%59), (r%56)
  FROM generate_series(1,70000) r;
--ok

CREATE INDEX idx1_gpi_bitmap_table1 ON gpi_bitmap_table1 (c1) GLOBAL;
--ok
CREATE INDEX idx2_gpi_bitmap_table1 ON gpi_bitmap_table1 (c2) GLOBAL;
--ok

-- test bitmap-and
SELECT count(*) FROM gpi_bitmap_table1 WHERE c1 = 1 AND c2 = 1;
SELECT * FROM gpi_bitmap_table1 WHERE c1 = 1 AND c2 = 1;
SELECT * FROM gpi_bitmap_table1 WHERE c1 = 1 AND c2 = 1 ORDER BY c3;

-- test bitmap-or
SELECT count(*) FROM gpi_bitmap_table1 WHERE c1 = 1 OR c2 = 1;

CREATE TABLE gpi_bitmap_table2
(
    c1 int,
    c2 int,
    c3 int
)
partition by range (c1)
(
    partition p0_gpi_bitmap_table2 VALUES less than (10000),
    partition p1_gpi_bitmap_table2 VALUES less than (20000),
    partition p2_gpi_bitmap_table2 VALUES less than (30000),
    partition p3_gpi_bitmap_table2 VALUES less than (maxvalue)
);
--ok

INSERT INTO gpi_bitmap_table2
  SELECT r, (r+1), 500
  FROM generate_series(1,70000) r;
--ok

CREATE INDEX idx1_gpi_bitmap_table2 ON gpi_bitmap_table2 (c1) GLOBAL;
--ok
CREATE INDEX idx2_gpi_bitmap_table2 ON gpi_bitmap_table2 (c2) GLOBAL;
--ok
EXPLAIN (COSTS OFF) SELECT count(*) FROM gpi_bitmap_table2 WHERE c1 > 9990 AND c1 < 10010;
SELECT count(*) FROM gpi_bitmap_table2 WHERE c1 > 9990 AND c1 < 10010;
SELECT * FROM gpi_bitmap_table2 WHERE c1 > 9990 AND c1 < 10010;

EXPLAIN (COSTS OFF) SELECT * FROM gpi_bitmap_table2 WHERE c1 = 10000 AND c2 = 10000;
SELECT * FROM gpi_bitmap_table2 WHERE c1 = 10000 AND c2 = 10000;
EXPLAIN (COSTS OFF) SELECT * FROM gpi_bitmap_table2 WHERE c1 = 10000 OR c2 = 10000;
SELECT * FROM gpi_bitmap_table2 WHERE c1 = 10000 OR c2 = 10000;
SELECT * FROM gpi_bitmap_table2 WHERE c1 = 9999 OR c2 = 10001;
EXPLAIN (COSTS OFF) SELECT * FROM gpi_bitmap_table2 WHERE c1 = 10000 OR c2 = 30000;
SELECT * FROM gpi_bitmap_table2 WHERE c1 = 10000 OR c2 = 30000;


DROP TABLE idx1_gpi_bitmap_table1;
DROP TABLE idx1_gpi_bitmap_table2;
SET enable_seqscan=on;
SET enable_indexscan=on;