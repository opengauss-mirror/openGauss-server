create schema gpi_bitmapscan_prefetch;
set search_path to gpi_bitmapscan_prefetch;

CREATE TABLE gpi_bitmap_table1
(
    c1 int,
    c2 int,
    c3 int,
    c4 text
)
partition by range (c1)
(
    partition p0_gpi_bitmap_table1 VALUES less than (100),
    partition p1_gpi_bitmap_table1 VALUES less than (200)
);

INSERT INTO gpi_bitmap_table1
  SELECT 50, 70, 50, md5(random()::text)
  FROM generate_series(1,1) r;

INSERT INTO gpi_bitmap_table1
  SELECT 50, 80, 50, md5(random()::text)
  FROM generate_series(1,200) r;

INSERT INTO gpi_bitmap_table1
  SELECT 50, 70, 50, md5(random()::text)
  FROM generate_series(1,1) r;

INSERT INTO gpi_bitmap_table1
  SELECT 150, 50, 150, 'explain analyze SELECT * FROM gpi_bitmap_table1 WHERE c2 = 70;explain analyze SELECT * FROM gpi_bitmap_table1 WHERE c2 = 70;explain analyze SELECT * FROM gpi_bitmap_table1 WHERE c2 = 70;explain analyze SELECT * FROM gpi_bitmap_table1 WHERE c2 = 70;explain analyze SELECT * FROM gpi_bitmap_table1 WHERE c2 = 70;explain analyze SELECT * FROM gpi_bitmap_table1 WHERE c2 = 70;explain analyze SELECT * FROM gpi_bitmap_table1 WHERE c2 = 70;explain analyze SELECT * FROM gpi_bitmap_table1 WHERE c2 = 70;explain analyze SELECT * FROM gpi_bitmap_table1 WHERE c2 = 70;explain analyze SELECT * FROM gpi_bitmap_table1 WHERE c2 = 70;explain analyze SELECT * FROM gpi_bitmap_table1 WHERE c2 = 70;explain analyze SELECT * FROM gpi_bitmap_table1 WHERE c2 = 70;explain analyze SELECT * FROM gpi_bitmap_table1 WHERE c2 = 70;explain analyze SELECT * FROM gpi_bitmap_table1 WHERE c2 = 70;explain analyze SELECT * FROM gpi_bitmap_table1 WHERE c2 = 70;explain analyze SELECT * FROM gpi_bitmap_table1 WHERE c2 = 70;explain analyze SELECT * FROM gpi_bitmap_table1 WHERE c2 = 70;explain analyze SELECT * FROM gpi_bitmap_table1 WHERE c2 = 70;explain analyze SELECT * FROM gpi_bitmap_table1 WHERE c2 = 70;explain analyze SELECT * FROM gpi_bitmap_table1 WHERE c2 = 70;explain analyze SELECT * FROM gpi_bitmap_table1 WHERE c2 = 70;explain analyze SELECT * FROM gpi_bitmap_table1 WHERE c2 = 70;'
  FROM generate_series(1,800000) r;

INSERT INTO gpi_bitmap_table1
  SELECT 150, 70, 150, md5(random()::text)
  FROM generate_series(1,1) r;

INSERT INTO gpi_bitmap_table1
  SELECT 150, 80, 150, md5(random()::text)
  FROM generate_series(2,100) r;

INSERT INTO gpi_bitmap_table1
  SELECT 150, 70, 150, md5(random()::text)
  FROM generate_series(1,1) r;

SET maintenance_work_mem = '1GB';
CREATE INDEX idx2_gpi_bitmap_table1 ON gpi_bitmap_table1 (c2) GLOBAL;
RESET maintenance_work_mem;

SET effective_io_concurrency = 200;
SET enable_seqscan = off;
SET enable_indexscan = off;
SELECT c2 FROM gpi_bitmap_table1 WHERE c2 = 70;
RESET effective_io_concurrency;
RESET enable_seqscan;
RESET enable_indexscan;
drop schema gpi_bitmapscan_prefetch cascade;
