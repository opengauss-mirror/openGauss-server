-- 1. column partitioned table
CREATE TABLE hw_cstore_alter1_t3 (a int, b int, c decimal(20, 0) ) with (orientation = column)
PARTITION BY RANGE(a)(
PARTITION p1 values less than (10),
PARTITION p2 values less than (20),
PARTITION p3 values less than (30)
);
INSERT INTO hw_cstore_alter1_t3 VALUES (1, 2, 20), (12, 22, 40), (24, 55, 100);
ALTER TABLE hw_cstore_alter1_t3 RENAME a TO a1; -- ok
SELECT a1 FROM hw_cstore_alter1_t3 ORDER BY 1;
ALTER TABLE hw_cstore_alter1_t3 RENAME COLUMN b TO b1; -- ok
SELECT b1 FROM hw_cstore_alter1_t3 ORDER BY 1;
ALTER TABLE hw_cstore_alter1_t3 RENAME c TO a1; -- failed, name conflicts
ALTER TABLE hw_cstore_alter1_t3 RENAME c TO xmin; -- failed, conflict with system column name
SELECT c FROM hw_cstore_alter1_t3 ORDER BY 1;
ALTER TABLE hw_cstore_alter1_t3 RENAME c TO abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghij1234; -- ok , but truncate
SELECT * FROM hw_cstore_alter1_t3 ORDER BY a1;
ALTER TABLE hw_cstore_alter1_t3 RENAME abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghij1234 TO c; -- ok
-- 1.1 column partitioned index
CREATE INDEX idx_hw_cstore_alter1_t3 ON hw_cstore_alter1_t3(b1) LOCAL;
ALTER INDEX idx_hw_cstore_alter1_t3 RENAME TO idx1_hw_cstore_alter1_t3; -- ok
select count(1) from pg_class where relname = 'idx1_hw_cstore_alter1_t3';
-- 2. column partition
ALTER TABLE hw_cstore_alter1_t3 RENAME PARTITION p1 TO p2; -- failed, name conflicts
ALTER TABLE IF EXISTS hw_cstore_alter1_t3 RENAME PARTITION p1 TO p2; -- failed, name conflicts
ALTER TABLE IF EXISTS hw_cstore_alter1_t3 RENAME PARTITION FOR (5) TO p2; -- failed, name conflicts
ALTER TABLE hw_cstore_alter1_t3 RENAME PARTITION FOR (5) TO p2; -- failed, name conflicts
ALTER TABLE hw_cstore_alter1_t3 RENAME PARTITION FOR (5) TO p4; -- ok
ALTER TABLE hw_cstore_alter1_t3 RENAME PARTITION p4 TO p5; -- ok
ALTER TABLE hw_cstore_alter1_t3 RENAME PARTITION p6 TO p7; -- failed, partition not exist
ALTER TABLE hw_cstore_alter1_t3 RENAME PARTITION FOR (100) TO p7; -- failed, partition not exist
ALTER TABLE hw_cstore_alter1_t3 RENAME PARTITION p2 TO pabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghij1234; -- ok but truncate
SELECT * FROM hw_cstore_alter1_t3 PARTITION (pabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghij12) ORDER BY 1; -- ok
SELECT * FROM hw_cstore_alter1_t3 PARTITION (pabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghij1234) ORDER BY 1; -- ok but truncate
ALTER TABLE hw_cstore_alter1_t3 RENAME PARTITION pabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghij1234
TO pabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghij1234; -- failed
-- 2.1 index partition
\d+ hw_cstore_alter1_t3
ALTER INDEX idx1_hw_cstore_alter1_t3 RENAME PARTITION p1_b1_idx TO p2_b1_idx; -- failed, name conflicts
ALTER INDEX IF EXISTS idx1_hw_cstore_alter1_t3 RENAME PARTITION p7_b1_idx TO p1_b1_idx; -- failed, not found
ALTER INDEX idx1_hw_cstore_alter1_t3 RENAME PARTITION p1_b1_idx TO pabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghij1234; -- ok, but truncate
ALTER INDEX idx1_hw_cstore_alter1_t3 RENAME PARTITION pabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghij1234 TO p1_b1_idx; -- ok
DROP TABLE hw_cstore_alter1_t3;
-- 3. update view on partition
create table hw_cstore_alter_t4(id int) partition by range(id)
(
partition p1 values less than(5),
partition p2 values less than(10)
);
INSERT INTO hw_cstore_alter_t4 values(1), (2), (3), (4);
-- 3.1 partiton name
create view v1_hw_cstore_alter_t4 as select id from hw_cstore_alter_t4 partition(p1);
\d+ v1_hw_cstore_alter_t4
alter table hw_cstore_alter_t4 rename partition p1 to pp1;
\d+ v1_hw_cstore_alter_t4
SELECT COUNT(*) FROM v1_hw_cstore_alter_t4;
-- 3.2 partition for (values list)
create view v2_hw_cstore_alter_t4 as select id from hw_cstore_alter_t4 partition FOR (8);
\d+ v2_hw_cstore_alter_t4
alter table hw_cstore_alter_t4 rename partition p2 TO pp2;
\d+ v2_hw_cstore_alter_t4
SELECT COUNT(*) FROM v2_hw_cstore_alter_t4;
DROP TABLE hw_cstore_alter_t4 cascade;
-- 3.10 column table
create table hw_cstore_alter_t5(id int) with (orientation = column)
partition by range(id)
(
partition p1 values less than(5),
partition p2 values less than(10)
);
INSERT INTO hw_cstore_alter_t5 values(1), (2), (3), (4);
-- 3.10.1 partition name
create view v1_hw_cstore_alter_t5 as select id from hw_cstore_alter_t5 partition(p1);
\d+ v1_hw_cstore_alter_t5
alter table hw_cstore_alter_t5 rename partition p1 to pp1;
\d+ v1_hw_cstore_alter_t5
SELECT COUNT(*) FROM v1_hw_cstore_alter_t5;
-- 3.10.2 partition for (values list)
create view v2_hw_cstore_alter_t5 as select id from hw_cstore_alter_t5 partition FOR (8);
\d+ v2_hw_cstore_alter_t5
alter table hw_cstore_alter_t5 rename partition p2 TO pp2;
\d+ v2_hw_cstore_alter_t5
SELECT COUNT(*) FROM v2_hw_cstore_alter_t5;
DROP TABLE hw_cstore_alter_t5 cascade;
-- 4. alter table column set/reset attribute_option
CREATE TABLE hw_cstore_alter_t6 (a int, b int, c int);
ALTER TABLE hw_cstore_alter_t6 ALTER COLUMN a SET (n_distinct_inherited = 7);
ALTER TABLE hw_cstore_alter_t6 ALTER COLUMN a RESET (n_distinct_inherited);
ALTER TABLE hw_cstore_alter_t6 ALTER COLUMN a SET (n_distinct = 7);
ALTER TABLE hw_cstore_alter_t6 ALTER COLUMN a RESET (n_distinct);
ALTER TABLE hw_cstore_alter_t6 ALTER COLUMN a RESET (n_distinct, n_distinct_inherited);
DROP TABLE hw_cstore_alter_t6;
CREATE TABLE hw_cstore_alter_t7 (a int, b int, c int) with (internal_mask = 1024); -- failed
CREATE TABLE hw_cstore_alter_t7 (a int, b int, c int) WITH (orientation = column);
ALTER TABLE hw_cstore_alter_t7 ALTER COLUMN a SET (n_distinct_inherited = 7); -- failed
ALTER TABLE hw_cstore_alter_t7 ALTER COLUMN a RESET (n_distinct_inherited); -- failed
ALTER TABLE hw_cstore_alter_t7 ALTER COLUMN a SET (n_distinct = 7);
ALTER TABLE hw_cstore_alter_t7 ALTER COLUMN a RESET (n_distinct);
ALTER TABLE hw_cstore_alter_t7 ALTER COLUMN a RESET (n_distinct, n_distinct_inherited); -- failed
\d+ hw_cstore_alter_t7
-- 4.1 change storage type
ALTER TABLE hw_cstore_alter_t7 ALTER COLUMN a SET STORAGE PLAIN; -- failed
ALTER TABLE hw_cstore_alter_t7 ALTER COLUMN a SET STORAGE EXTERNAL; -- failed
ALTER TABLE hw_cstore_alter_t7 ALTER COLUMN a SET STORAGE EXTENDED; -- failed
ALTER TABLE hw_cstore_alter_t7 ALTER COLUMN a SET STORAGE MAIN; -- failed
-- 4.2 set with/without oids
ALTER TABLE hw_cstore_alter_t7 SET WITH OIDS; -- failed
ALTER TABLE hw_cstore_alter_t7 SET WITHOUT OIDS; -- failed
-- 4.3 SET/RESET relation options
-- 4.3.1 column table
ALTER TABLE hw_cstore_alter_t7 SET (ORIENTATION = ROW); -- failed
ALTER TABLE hw_cstore_alter_t7 RESET (ORIENTATION); -- failed
ALTER TABLE hw_cstore_alter_t7 SET (internal_mask = 0); -- failed
ALTER TABLE hw_cstore_alter_t7 RESET (internal_mask); -- failed
ALTER TABLE hw_cstore_alter_t7 SET (FILLFACTOR = 70); -- failed
ALTER TABLE hw_cstore_alter_t7 RESET (FILLFACTOR); -- failed
ALTER TABLE hw_cstore_alter_t7 SET (autovacuum_enabled = true); -- ok
ALTER TABLE hw_cstore_alter_t7 RESET (autovacuum_enabled); -- ok
ALTER TABLE hw_cstore_alter_t7 SET (security_barrier); -- failed
ALTER TABLE hw_cstore_alter_t7 RESET (security_barrier); -- failed
\d+ hw_cstore_alter_t7
-- 4.3.2 psort table
CREATE INDEX idx1_hw_cstore_alter_t7 ON hw_cstore_alter_t7(b) with (compression = 'low'); -- failed
CREATE INDEX idx1_hw_cstore_alter_t7 ON hw_cstore_alter_t7(b) with (fillfactor = 100); -- failed
CREATE INDEX idx1_hw_cstore_alter_t7 ON hw_cstore_alter_t7(b) with (autovacuum_enabled = true); -- failed
CREATE INDEX idx1_hw_cstore_alter_t7 ON hw_cstore_alter_t7(b) with (security_barrier = on); -- failed
CREATE INDEX idx1_hw_cstore_alter_t7 ON hw_cstore_alter_t7(b) with (internal_mask = 0); -- failed
CREATE INDEX idx1_hw_cstore_alter_t7 ON hw_cstore_alter_t7(b) with (orientation = column); -- failed
CREATE INDEX idx1_hw_cstore_alter_t7 ON hw_cstore_alter_t7(b) with (max_batchrow = 60000, deltarow_threshold = 9999, partial_cluster_rows = 600000); -- ok
ALTER INDEX idx1_hw_cstore_alter_t7 SET (ORIENTATION = ROW); -- failed
ALTER INDEX idx1_hw_cstore_alter_t7 RESET (ORIENTATION); -- failed
ALTER INDEX idx1_hw_cstore_alter_t7 SET (compression = 'yes'); -- failed
ALTER INDEX idx1_hw_cstore_alter_t7 RESET (compression); -- failed
ALTER INDEX idx1_hw_cstore_alter_t7 SET (internal_mask = 0); -- failed
ALTER INDEX idx1_hw_cstore_alter_t7 RESET (internal_mask); -- failed
ALTER INDEX idx1_hw_cstore_alter_t7 SET (max_batchrow = 60000); -- ok
ALTER INDEX idx1_hw_cstore_alter_t7 RESET (max_batchrow); -- ok
ALTER INDEX idx1_hw_cstore_alter_t7 SET (deltarow_threshold = 5000); -- ok
ALTER INDEX idx1_hw_cstore_alter_t7 RESET (deltarow_threshold); -- ok
ALTER INDEX idx1_hw_cstore_alter_t7 SET (partial_cluster_rows = 700000); -- ok
ALTER INDEX idx1_hw_cstore_alter_t7 RESET (partial_cluster_rows); -- ok
ALTER INDEX idx1_hw_cstore_alter_t7 SET (security_barrier = false); -- failed
ALTER INDEX idx1_hw_cstore_alter_t7 RESET (security_barrier); -- failed
ALTER INDEX idx1_hw_cstore_alter_t7 SET (FILLFACTOR = 70); -- failed
ALTER INDEX idx1_hw_cstore_alter_t7 RESET (FILLFACTOR); -- failed
ALTER TABLE idx1_hw_cstore_alter_t7 SET (autovacuum_enabled = true); -- failed
ALTER INDEX idx1_hw_cstore_alter_t7 RESET (autovacuum_enabled); -- failed
DROP INDEX idx1_hw_cstore_alter_t7;
DROP TABLE hw_cstore_alter_t7;
-- 4.3.3 row table
CREATE TABLE hw_cstore_alter_t8(a int , b int , c int ) with (compression = 'high'); -- failed
CREATE TABLE hw_cstore_alter_t8(a int , b int , c int ) with (max_batchrow = 60000); -- failed
CREATE TABLE hw_cstore_alter_t8(a int , b int , c int ) with (deltarow_threshold = 9999); -- failed
CREATE TABLE hw_cstore_alter_t8(a int , b int , c int ) with (partial_cluster_rows = 600000); -- failed
CREATE TABLE hw_cstore_alter_t8(a int , b int , c int ) with (internal_mask = 1024); -- failed
CREATE TABLE hw_cstore_alter_t8(a int , b int , c int ) with (compression = 'no', fillfactor=70); -- ok
ALTER TABLE hw_cstore_alter_t8 SET (compression = 'yes'); -- ok
ALTER TABLE hw_cstore_alter_t8 SET (compression = 'no'); -- ok
ALTER TABLE hw_cstore_alter_t8 SET (compression = 'low'); -- failed
ALTER TABLE hw_cstore_alter_t8 SET (compression = 'middle'); -- failed
ALTER TABLE hw_cstore_alter_t8 SET (compression = 'high'); -- failed
\d+ hw_cstore_alter_t8
ALTER TABLE hw_cstore_alter_t8 RESET (compression); -- ok
\d+ hw_cstore_alter_t8
ALTER TABLE hw_cstore_alter_t8 SET (max_batchrow = 10000); -- failed
ALTER TABLE hw_cstore_alter_t8 RESET (max_batchrow); -- failed
ALTER TABLE hw_cstore_alter_t8 SET (deltarow_threshold = 9999); -- failed
ALTER TABLE hw_cstore_alter_t8 RESET (deltarow_threshold); -- failed
ALTER TABLE hw_cstore_alter_t8 SET (partial_cluster_rows = 600000); -- failed
ALTER TABLE hw_cstore_alter_t8 RESET (partial_cluster_rows); -- failed
ALTER TABLE hw_cstore_alter_t8 SET (ORIENTATION = COLUMN); -- failed
ALTER TABLE hw_cstore_alter_t8 SET (ORIENTATION = ROW); -- failed
ALTER TABLE hw_cstore_alter_t8 RESET (ORIENTATION); -- failed
ALTER TABLE hw_cstore_alter_t8 SET (internal_mask = 0); -- failed
ALTER TABLE hw_cstore_alter_t8 RESET (internal_mask); -- failed
DROP TABLE hw_cstore_alter_t8;
CREATE TABLE hw_cstore_alter_t9(a int , b int , c int ); -- ok
DROP TABLE hw_cstore_alter_t9;
CREATE TABLE hw_cstore_alter_t9(a int , b int , c int ) with ( orientation = row ); -- ok
-- 4.3.3.1 bree index
CREATE INDEX idx1_hw_cstore_alter_t9 ON hw_cstore_alter_t9(b); -- ok
ALTER INDEX idx1_hw_cstore_alter_t9 SET (fillfactor = 70); -- ok
ALTER INDEX idx1_hw_cstore_alter_t9 RESET (fillfactor); -- ok
ALTER INDEX idx1_hw_cstore_alter_t9 SET (compression = 'yes'); -- failed, unsupported
ALTER INDEX idx1_hw_cstore_alter_t9 RESET (compression); -- ok, ignore it
ALTER INDEX idx1_hw_cstore_alter_t9 SET (autovacuum_enabled = true); -- failed, unsupported
ALTER INDEX idx1_hw_cstore_alter_t9 RESET (autovacuum_enabled); -- ok, ignore it
ALTER INDEX idx1_hw_cstore_alter_t9 SET (security_barrier = true); -- failed, unsupported
ALTER INDEX idx1_hw_cstore_alter_t9 RESET (security_barrier); -- ok, ignore it
ALTER INDEX idx1_hw_cstore_alter_t9 SET (max_batchrow=10000); -- failed
ALTER INDEX idx1_hw_cstore_alter_t9 RESET (max_batchrow); -- ok, ignore it
ALTER INDEX idx1_hw_cstore_alter_t9 SET (orientation = column); -- failed
ALTER INDEX idx1_hw_cstore_alter_t9 RESET (orientation); -- ok, ignore it
ALTER INDEX idx1_hw_cstore_alter_t9 SET (internal_mask = 0); -- failed
ALTER INDEX idx1_hw_cstore_alter_t9 RESET (internal_mask); -- failed
ALTER INDEX idx1_hw_cstore_alter_t9 SET (partial_cluster_rows = 600000); -- failed
ALTER INDEX idx1_hw_cstore_alter_t9 RESET (partial_cluster_rows); -- ok, ignore it
ALTER INDEX idx1_hw_cstore_alter_t9 SET (deltarow_threshold = 5000); -- failed
ALTER INDEX idx1_hw_cstore_alter_t9 RESET (deltarow_threshold); -- ok, ignore it
\d+ idx1_hw_cstore_alter_t9
DROP INDEX idx1_hw_cstore_alter_t9;
drop TABLE hw_cstore_alter_t9;
