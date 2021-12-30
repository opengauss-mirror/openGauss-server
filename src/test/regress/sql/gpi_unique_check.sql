DROP TABLE if exists gpi_uniquecheck_table1;
CREATE TABLE gpi_uniquecheck_table1
(
    c1 int,
    c2 int,
    c3 int
)
partition by range (c1)
(
    partition p0_gpi_uniquecheck_table1 VALUES less than (10000),
    partition p1_gpi_uniquecheck_table1 VALUES less than (20000),
    partition p2_gpi_uniquecheck_table1 VALUES less than (30000),
    partition p3_gpi_uniquecheck_table1 VALUES less than (maxvalue)
);

INSERT INTO gpi_uniquecheck_table1 SELECT r, r, r FROM generate_series(0,40000) AS r;

CREATE UNIQUE INDEX idx1_gpi_uniquecheck_table1 ON gpi_uniquecheck_table1 (c2);

INSERT INTO gpi_uniquecheck_table1 VALUES (9999,1,1);
--error
INSERT INTO gpi_uniquecheck_table1 VALUES (10000,1,1);
--error
INSERT INTO gpi_uniquecheck_table1 VALUES (10001,1,1);
--error
INSERT INTO gpi_uniquecheck_table1 VALUES (10001,40001,1);
--ok
