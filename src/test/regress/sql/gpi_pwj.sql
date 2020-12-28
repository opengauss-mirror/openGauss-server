
DROP TABLE if exists gpi_pwj_table1;
DROP TABLE if exists gpi_pwj_table2;
CREATE TABLE gpi_pwj_table1
(
    c1 int,
    c2 int,
    c3 int
)
partition by range (c1)
(
    partition p0_gpi_pwj_table1 VALUES less than (10000),
    partition p1_gpi_pwj_table1 VALUES less than (20000),
    partition p2_gpi_pwj_table1 VALUES less than (30000),
    partition p3_gpi_pwj_table1 VALUES less than (maxvalue)
);
CREATE TABLE gpi_pwj_table2
(
    c1 int,
    c2 int,
    c3 int
)
partition by range (c1)
(
    partition p0_gpi_pwj_table2 VALUES less than (10000),
    partition p1_gpi_pwj_table2 VALUES less than (20000),
    partition p2_gpi_pwj_table2 VALUES less than (30000),
    partition p3_gpi_pwj_table2 VALUES less than (maxvalue)
);

INSERT INTO gpi_pwj_table1 SELECT r, r, r FROM generate_series(0,40000) AS r;
INSERT INTO gpi_pwj_table2 SELECT r, r, r FROM generate_series(0,40000) AS r;

CREATE INDEX idx1_gpi_pwj_table1 ON gpi_pwj_table1 (c1) GLOBAL;
CREATE INDEX idx1_gpi_pwj_table2 ON gpi_pwj_table2 (c2) GLOBAL;

explain (costs off)
SELECT count(*) FROM gpi_pwj_table1, gpi_pwj_table2 WHERE gpi_pwj_table1.c1 = gpi_pwj_table2.c1 AND gpi_pwj_table1.c2 < 15000;
SELECT count(*) FROM gpi_pwj_table1, gpi_pwj_table2 WHERE gpi_pwj_table1.c1 = gpi_pwj_table2.c1 AND gpi_pwj_table1.c2 < 15000;
VACUUM analyze gpi_pwj_table1;
VACUUM analyze gpi_pwj_table2;
SET enable_partitionwise=ON;
explain (costs off)
SELECT count(*) FROM gpi_pwj_table1, gpi_pwj_table2 WHERE gpi_pwj_table1.c1 = gpi_pwj_table2.c1 AND gpi_pwj_table1.c2 < 15000;
SELECT count(*) FROM gpi_pwj_table1, gpi_pwj_table2 WHERE gpi_pwj_table1.c1 = gpi_pwj_table2.c1 AND gpi_pwj_table1.c2 < 15000;
SET enable_partitionwise=OFF;
