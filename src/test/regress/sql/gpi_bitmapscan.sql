
DROP TABLE if exists gpi_bitmap_table1;
DROP TABLE if exists gpi_bitmap_table2;

SET enable_seqscan=off;
SET enable_indexscan=off;
SET force_bitmapand=on;
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
SET force_bitmapand=off;

drop table if exists test_part_bitmapand_ginst_btree;
CREATE TABLE test_part_bitmapand_ginst_btree (a int, txtkeyword TEXT, txtsample TEXT) partition by range(a) (partition p1 values less than (1001), partition p2 values  less than (2001), partition p3 values less than (3001));
insert into test_part_bitmapand_ginst_btree values (10, $$'New York'$$, 'new & york | big & apple | nyc');
insert into test_part_bitmapand_ginst_btree values (1010, 'Moscow',	'moskva | moscow');
insert into test_part_bitmapand_ginst_btree values (1020, $$'Sanct Peter'$$,	$$Peterburg | peter | 'Sanct Peterburg'$$);
insert into test_part_bitmapand_ginst_btree values (1030, $$'foo bar qq'$$,	'foo & (bar | qq) & city');

ALTER TABLE test_part_bitmapand_ginst_btree ADD COLUMN keyword tsquery;
UPDATE test_part_bitmapand_ginst_btree SET keyword = to_tsquery('english', txtkeyword);
ALTER TABLE test_part_bitmapand_ginst_btree ADD COLUMN sample tsquery;
UPDATE test_part_bitmapand_ginst_btree SET sample = to_tsquery('english', txtsample::text);

CREATE UNIQUE INDEX ON test_part_bitmapand_ginst_btree (a) local;
-- failed
CREATE INDEX qq ON test_part_bitmapand_ginst_btree USING gist (keyword tsquery_ops);
CREATE INDEX qq ON test_part_bitmapand_ginst_btree USING gist (keyword tsquery_ops) local;

CREATE INDEX ON test_part_bitmapand_ginst_btree USING gist (keyword tsquery_ops);

explain (costs off) SELECT keyword FROM test_part_bitmapand_ginst_btree WHERE keyword @> 'new' and a = 10;
SELECT keyword FROM test_part_bitmapand_ginst_btree WHERE keyword @> 'new' and a = 10;

set force_bitmapand = on;
set enable_seqscan = off;
set enable_indexscan = off;

--bitmapand scan
explain (costs off) SELECT keyword FROM test_part_bitmapand_ginst_btree WHERE keyword @> 'new' and a = 10;
SELECT keyword FROM test_part_bitmapand_ginst_btree WHERE keyword @> 'new' and a = 10;

drop index test_part_bitmapand_ginst_btree_a_idx;
CREATE UNIQUE INDEX ON test_part_bitmapand_ginst_btree (a);

--bitmapand scan
explain (costs off) SELECT keyword FROM test_part_bitmapand_ginst_btree WHERE keyword @> 'new' and a = 10;
SELECT keyword FROM test_part_bitmapand_ginst_btree WHERE keyword @> 'new' and a = 10;

drop table if exists test_part_bitmapand_gin_btree;
create table test_part_bitmapand_gin_btree (a int, ts tsvector) partition by range(a) (partition p1 values less than (1001), partition p2 values  less than (2001), partition p3 values less than (3001));
insert into test_part_bitmapand_gin_btree values (10, to_tsvector('Lore ipsam'));
insert into test_part_bitmapand_gin_btree values (1010, to_tsvector('Lore ipsum'));
create index test_part_bitmapand_gin_btree_idx on test_part_bitmapand_gin_btree using gin(ts) local;
create index test_part_bitmapand_gin_btree_idx_a on test_part_bitmapand_gin_btree using btree(a) local;

set force_bitmapand = on;
set enable_seqscan = off;
set enable_indexscan = off;
explain (costs off) select * from test_part_bitmapand_gin_btree where 'ipsu:*'::tsquery @@ ts and a = 10;
select * from test_part_bitmapand_gin_btree where 'ipsu:*'::tsquery @@ ts and a = 10;

explain (costs off) select * from test_part_bitmapand_gin_btree where 'ipsu:*'::tsquery @@ ts and a = 1010;
select * from test_part_bitmapand_gin_btree where 'ipsu:*'::tsquery @@ ts and a = 1010;

drop index test_part_bitmapand_gin_btree_idx_a;
create index test_part_bitmapand_gin_btree_idx_a on test_part_bitmapand_gin_btree using btree(a) global;
explain (costs off) select * from test_part_bitmapand_gin_btree where 'ipsu:*'::tsquery @@ ts and a = 1010;
select * from test_part_bitmapand_gin_btree where 'ipsu:*'::tsquery @@ ts and a = 1010;

reset force_bitmapand;
reset enable_seqscan;
reset enable_indexscan;

drop table test_part_bitmapand_gin_btree;
drop table test_part_bitmapand_ginst_btree;