-- OWNER TO
CREATE TABLE hw_cstore_alter_t1(a int, b text) WITH(orientation=column);
CREATE INDEX ON hw_cstore_alter_t1(a);
CREATE ROLE cstore_role PASSWORD 'gauss@123';
ALTER TABLE hw_cstore_alter_t1 OWNER TO cstore_role;
DROP TABLE hw_cstore_alter_t1;
--  unsupported feature
CREATE TABLE hw_cstore_alter_t3  ( a int , b int ) with ( orientation = column ) ;
CREATE TABLE hw_cstore_alter_t4  ( c int, d int ) inherits ( hw_cstore_alter_t3 )  with ( orientation = column ) ; -- failed, not supported
DROP TABLE IF EXISTS hw_cstore_alter_t3;
DROP TABLE IF EXISTS hw_cstore_alter_t4;
-- rename column
-- 5. column ordinary table
CREATE TABLE hw_cstore_alter_t2(a int , b bigint, c char(10), d decimal(20,2) ) with (orientation = column);
INSERT INTO hw_cstore_alter_t2 VALUES(1, 2, 'text', 3);
ALTER TABLE hw_cstore_alter_t2 RENAME a TO a1; -- ok
SELECT a1 FROM hw_cstore_alter_t2;
ALTER TABLE hw_cstore_alter_t2 RENAME COLUMN b TO b1; -- ok
SELECT b1 FROM hw_cstore_alter_t2;
ALTER TABLE IF EXISTS hw_cstore_alter_t2 RENAME c TO c1; -- ok
SELECT c1 FROM hw_cstore_alter_t2;
ALTER TABLE hw_cstore_alter_t2 RENAME d TO a1; -- failed, name conflicts
SELECT d FROM hw_cstore_alter_t2;
ALTER TABLE hw_cstore_alter_t3 RENAME a TO a1; -- failed, table doesn't exist
ALTER TABLE hw_cstore_alter_t2 RENAME d TO xmin; -- failed, conflict with system column name
ALTER TABLE hw_cstore_alter_t2 RENAME d TO abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghij1234; -- ok , but truncate
SELECT abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghij1234 FROM hw_cstore_alter_t2; -- ok, attribute name will be truncated
SELECT abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghij123 FROM hw_cstore_alter_t2; -- ok
ALTER TABLE hw_cstore_alter_t2 RENAME abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghij1234 
TO abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghij123; -- failed, truncate but name conflicts
-- 5.1 column ordinary index
CREATE INDEX idx_hw_cstore_alter_t2 ON hw_cstore_alter_t2(c1);
ALTER INDEX idx_hw_cstore_alter_t2 RENAME TO idx1_hw_cstore_alter_t2; -- ok
select count(1) from pg_class where relname = 'idx1_hw_cstore_alter_t2';
ALTER INDEX IF EXISTS idx1_hw_cstore_alter_t2 RENAME TO idx2_hw_cstore_alter_t2; -- ok
select count(1) from pg_class where relname = 'idx2_hw_cstore_alter_t2';
ALTER INDEX idx2_hw_cstore_alter_t2 RENAME TO hw_cstore_alter_t2; -- failed, name conflicts
ALTER INDEX idx2_hw_cstore_alter_t2 RENAME TO idx3_abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghij1234; -- ok, but truncate
ALTER INDEX idx3_abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghij1234 RENAME TO idx2_hw_cstore_alter_t2; -- ok
select count(1) from pg_class where relname = 'idx2_hw_cstore_alter_t2';
DROP TABLE hw_cstore_alter_t2;
-- 6 temp table
-- 6.1 rename column
CREATE TEMP TABLE hw_cstore_alter_t10 (a int , b int , c int ) with (orientation = column);
ALTER TABLE hw_cstore_alter_t10 RENAME a to b; -- failed, name conflict
ALTER TABLE hw_cstore_alter_t10 RENAME a to xmin; -- failed, system column name
ALTER TABLE hw_cstore_alter_t10 RENAME a to a1; -- ok
SELECT a1 FROM hw_cstore_alter_t10; -- ok
ALTER TABLE hw_cstore_alter_t10 RENAME a1 TO abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghij1234;
SELECT abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghij1234 FROM hw_cstore_alter_t10;
SELECT abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghij123 FROM hw_cstore_alter_t10;
ALTER TABLE hw_cstore_alter_t10 RENAME abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghij1234 
TO abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghij123; -- failed, truncate but name conflicts
CREATE INDEX idx1_hw_cstore_alter_t10 ON hw_cstore_alter_t10(b);
\d+ idx1_hw_cstore_alter_t10
ALTER INDEX idx1_hw_cstore_alter_t10 RENAME TO idx2_hw_cstore_alter_t10; -- ok
DROP TABLE hw_cstore_alter_t10;
-- 6.2 rename partition
CREATE TEMP TABLE hw_cstore_alter_t11 (a int, b int, c decimal(20, 0) ) with (orientation = column)
PARTITION BY RANGE(a)(
PARTITION p1 values less than (10),
PARTITION p2 values less than (20),
PARTITION p3 values less than (30)
); -- failed, unsupported feature
-- 6.3 temp row table options
CREATE TEMP TABLE hw_cstore_alter_t12(a int , b int , c int ) with (compression = 'high'); -- failed
CREATE TEMP TABLE hw_cstore_alter_t12(a int , b int , c int ) with (max_batchrow = 60000); -- failed
CREATE TEMP TABLE hw_cstore_alter_t12(a int , b int , c int ) with (deltarow_threshold = 9999); -- failed
CREATE TEMP TABLE hw_cstore_alter_t12(a int , b int , c int ) with (partial_cluster_rows = 600000); -- failed
CREATE TEMP TABLE hw_cstore_alter_t12(a int , b int , c int ) with (internal_mask = 1024); -- failed
CREATE TEMP TABLE hw_cstore_alter_t12(a int , b int , c int ) with (compression = 'yes', fillfactor=70); -- ok
ALTER TABLE hw_cstore_alter_t12 SET (compression = 'yes'); -- ok
ALTER TABLE hw_cstore_alter_t12 SET (compression = 'no'); -- ok
ALTER TABLE hw_cstore_alter_t12 SET (compression = 'low'); -- failed
ALTER TABLE hw_cstore_alter_t12 SET (compression = 'middle'); -- failed
ALTER TABLE hw_cstore_alter_t12 SET (compression = 'high'); -- failed
-- \d+ hw_cstore_alter_t12
ALTER TABLE hw_cstore_alter_t12 RESET (compression); -- ok
-- \d+ hw_cstore_alter_t12
ALTER TABLE hw_cstore_alter_t12 SET (max_batchrow = 10000); -- failed
ALTER TABLE hw_cstore_alter_t12 RESET (max_batchrow); -- failed
ALTER TABLE hw_cstore_alter_t12 SET (deltarow_threshold = 9999); -- failed
ALTER TABLE hw_cstore_alter_t12 RESET (deltarow_threshold); -- failed
ALTER TABLE hw_cstore_alter_t12 SET (partial_cluster_rows = 600000); -- failed
ALTER TABLE hw_cstore_alter_t12 RESET (partial_cluster_rows); -- failed
ALTER TABLE hw_cstore_alter_t12 SET (ORIENTATION = COLUMN); -- failed
ALTER TABLE hw_cstore_alter_t12 SET (ORIENTATION = ROW); -- failed
ALTER TABLE hw_cstore_alter_t12 RESET (ORIENTATION); -- failed
ALTER TABLE hw_cstore_alter_t12 SET (internal_mask = 0); -- failed
ALTER TABLE hw_cstore_alter_t12 RESET (internal_mask); -- failed
DROP TABLE hw_cstore_alter_t12;
CREATE TEMP TABLE hw_cstore_alter_t13(a int , b int , c int ); -- ok
DROP TABLE hw_cstore_alter_t13;
CREATE TEMP TABLE hw_cstore_alter_t14(a int , b int , c int ) with ( orientation = row ); -- ok
-- 5.3.3.1 bree index
CREATE INDEX idx1_hw_cstore_alter_t14 ON hw_cstore_alter_t14(b); -- ok
ALTER INDEX idx1_hw_cstore_alter_t14 SET (fillfactor = 70); -- ok
ALTER INDEX idx1_hw_cstore_alter_t14 RESET (fillfactor); -- ok
ALTER INDEX idx1_hw_cstore_alter_t14 SET (compression = 'yes'); -- failed, unsupported
ALTER INDEX idx1_hw_cstore_alter_t14 RESET (compression); -- ok, ignore it
ALTER INDEX idx1_hw_cstore_alter_t14 SET (autovacuum_enabled = true); -- failed, unsupported
ALTER INDEX idx1_hw_cstore_alter_t14 RESET (autovacuum_enabled); -- ok, ignore it
ALTER INDEX idx1_hw_cstore_alter_t14 SET (security_barrier = true); -- failed, unsupported
ALTER INDEX idx1_hw_cstore_alter_t14 RESET (security_barrier); -- ok, ignore it
ALTER INDEX idx1_hw_cstore_alter_t14 SET (max_batchrow=10000); -- failed
ALTER INDEX idx1_hw_cstore_alter_t14 RESET (max_batchrow); -- ok, ignore it
ALTER INDEX idx1_hw_cstore_alter_t14 SET (orientation = column); -- failed
ALTER INDEX idx1_hw_cstore_alter_t14 RESET (orientation); -- ok, ignore it
ALTER INDEX idx1_hw_cstore_alter_t14 SET (internal_mask = 0); -- failed
ALTER INDEX idx1_hw_cstore_alter_t14 RESET (internal_mask); -- failed
ALTER INDEX idx1_hw_cstore_alter_t14 SET (partial_cluster_rows = 600000); -- failed
ALTER INDEX idx1_hw_cstore_alter_t14 RESET (partial_cluster_rows); -- ok, ignore it
ALTER INDEX idx1_hw_cstore_alter_t14 SET (deltarow_threshold = 5000); -- failed
ALTER INDEX idx1_hw_cstore_alter_t14 RESET (deltarow_threshold); -- ok, ignore it
-- \d+ idx1_hw_cstore_alter_t14
DROP INDEX idx1_hw_cstore_alter_t14;
drop TABLE hw_cstore_alter_t14;
-- 6.4 temp column table options
-- 6.4.1 alter table column set/reset attribute_option
CREATE TEMP TABLE hw_cstore_alter_t15 (a int, b int, c int);
ALTER TABLE hw_cstore_alter_t15 ALTER COLUMN a SET (n_distinct_inherited = 7); -- failed
ALTER TABLE hw_cstore_alter_t15 ALTER COLUMN a RESET (n_distinct_inherited); -- failed
ALTER TABLE hw_cstore_alter_t15 ALTER COLUMN a SET (n_distinct = 7);  -- ok
ALTER TABLE hw_cstore_alter_t15 ALTER COLUMN a RESET (n_distinct); -- ok
ALTER TABLE hw_cstore_alter_t15 ALTER COLUMN a RESET (n_distinct, n_distinct_inherited); -- failed
DROP TABLE hw_cstore_alter_t15;
CREATE TEMP TABLE hw_cstore_alter_t16 (a int, b int, c int) with (internal_mask = 1024); -- failed
CREATE TEMP TABLE hw_cstore_alter_t16 (a int, b int, c int) WITH (orientation = column);
ALTER TABLE hw_cstore_alter_t16 ALTER COLUMN a SET (n_distinct_inherited = 7); -- failed
ALTER TABLE hw_cstore_alter_t16 ALTER COLUMN a RESET (n_distinct_inherited); -- failed
ALTER TABLE hw_cstore_alter_t16 ALTER COLUMN a SET (n_distinct = 7);
ALTER TABLE hw_cstore_alter_t16 ALTER COLUMN a RESET (n_distinct);
ALTER TABLE hw_cstore_alter_t16 ALTER COLUMN a RESET (n_distinct, n_distinct_inherited); -- failed
-- \d+ hw_cstore_alter_t16
-- 6.4.2 change storage type
ALTER TABLE hw_cstore_alter_t16 ALTER COLUMN a SET STORAGE PLAIN; -- failed
ALTER TABLE hw_cstore_alter_t16 ALTER COLUMN a SET STORAGE EXTERNAL; -- failed
ALTER TABLE hw_cstore_alter_t16 ALTER COLUMN a SET STORAGE EXTENDED; -- failed
ALTER TABLE hw_cstore_alter_t16 ALTER COLUMN a SET STORAGE MAIN; -- failed
-- 6.4.3 set with/without oids
ALTER TABLE hw_cstore_alter_t16 SET WITH OIDS; -- failed
ALTER TABLE hw_cstore_alter_t16 SET WITHOUT OIDS; -- failed
-- 6.4.4 SET/RESET relation options
-- 6.4.4.1 column table
ALTER TABLE hw_cstore_alter_t16 SET (ORIENTATION = ROW); -- failed
ALTER TABLE hw_cstore_alter_t16 RESET (ORIENTATION); -- failed
ALTER TABLE hw_cstore_alter_t16 SET (internal_mask = 0); -- failed
ALTER TABLE hw_cstore_alter_t16 RESET (internal_mask); -- failed
ALTER TABLE hw_cstore_alter_t16 SET (FILLFACTOR = 70); -- failed
ALTER TABLE hw_cstore_alter_t16 RESET (FILLFACTOR); -- failed
ALTER TABLE hw_cstore_alter_t16 SET (autovacuum_enabled = true); -- ok
ALTER TABLE hw_cstore_alter_t16 RESET (autovacuum_enabled); -- ok
ALTER TABLE hw_cstore_alter_t16 SET (security_barrier); -- failed
ALTER TABLE hw_cstore_alter_t16 RESET (security_barrier); -- failed
\d+ hw_cstore_alter_t16
-- 6.4.4.1.2 psort table
CREATE INDEX idx1_hw_cstore_alter_t16 ON hw_cstore_alter_t16(b) with (compression = 'low'); -- failed
CREATE INDEX idx1_hw_cstore_alter_t16 ON hw_cstore_alter_t16(b) with (fillfactor = 100); -- failed
CREATE INDEX idx1_hw_cstore_alter_t16 ON hw_cstore_alter_t16(b) with (autovacuum_enabled = true); -- failed
CREATE INDEX idx1_hw_cstore_alter_t16 ON hw_cstore_alter_t16(b) with (security_barrier = on); -- failed
CREATE INDEX idx1_hw_cstore_alter_t16 ON hw_cstore_alter_t16(b) with (internal_mask = 0); -- failed
CREATE INDEX idx1_hw_cstore_alter_t16 ON hw_cstore_alter_t16(b) with (orientation = column); -- failed
CREATE INDEX idx1_hw_cstore_alter_t16 ON hw_cstore_alter_t16(b) with (max_batchrow = 60000, deltarow_threshold = 9999, partial_cluster_rows = 600000); -- ok
ALTER INDEX idx1_hw_cstore_alter_t16 SET (ORIENTATION = ROW); -- failed
ALTER INDEX idx1_hw_cstore_alter_t16 RESET (ORIENTATION); -- failed
ALTER INDEX idx1_hw_cstore_alter_t16 SET (compression = 'yes'); -- failed
ALTER INDEX idx1_hw_cstore_alter_t16 RESET (compression); -- failed
ALTER INDEX idx1_hw_cstore_alter_t16 SET (internal_mask = 0); -- failed
ALTER INDEX idx1_hw_cstore_alter_t16 RESET (internal_mask); -- failed
ALTER INDEX idx1_hw_cstore_alter_t16 SET (max_batchrow = 60000); -- ok
ALTER INDEX idx1_hw_cstore_alter_t16 RESET (max_batchrow); -- ok
ALTER INDEX idx1_hw_cstore_alter_t16 SET (deltarow_threshold = 5000); -- ok
ALTER INDEX idx1_hw_cstore_alter_t16 RESET (deltarow_threshold); -- ok
ALTER INDEX idx1_hw_cstore_alter_t16 SET (partial_cluster_rows = 700000); -- ok
ALTER INDEX idx1_hw_cstore_alter_t16 RESET (partial_cluster_rows); -- ok
ALTER INDEX idx1_hw_cstore_alter_t16 SET (security_barrier = false); -- failed
ALTER INDEX idx1_hw_cstore_alter_t16 RESET (security_barrier); -- failed
ALTER INDEX idx1_hw_cstore_alter_t16 SET (FILLFACTOR = 70); -- failed
ALTER INDEX idx1_hw_cstore_alter_t16 RESET (FILLFACTOR); -- failed
ALTER TABLE idx1_hw_cstore_alter_t16 SET (autovacuum_enabled = true); -- failed
ALTER INDEX idx1_hw_cstore_alter_t16 RESET (autovacuum_enabled); -- failed
DROP INDEX idx1_hw_cstore_alter_t16;
DROP TABLE hw_cstore_alter_t16;
-- 6.5 alter cstore partition table owner
CREATE USER hw_user_u1 password 'GAUSS@123';
CREATE TABLE hw_cstore_alter_t17 (a int, b int, c int) with (orientation = column) distribute by hash(a)
partition by range (a)
(
    partition p1  values less than (1000),
    partition p2  values less than (2000),
    partition p3  values less than (maxvalue)
);
CREATE INDEX idx1_hw_cstore_alter_t17 on hw_cstore_alter_t17 using PSORT (a) local;
ALTER TABLE hw_cstore_alter_t17 owner to hw_user_u1;
DROP TABLE hw_cstore_alter_t17;
DROP USER hw_user_u1;

CREATE USER hw_user_u2 password 'GAUSS@123';
CREATE TABLE hw_cstore_alter_t18 (a int, b int, c int) with (orientation = column) distribute by hash(a)
partition by range (a)
(
    partition p1  values less than (1000),
    partition p2  values less than (2000),
    partition p3  values less than (maxvalue)
);
CREATE INDEX idx1_hw_cstore_alter_t18 on hw_cstore_alter_t18 using PSORT (a) local;
ALTER TABLE hw_cstore_alter_t18 owner to hw_user_u2;
DROP USER hw_user_u2 cascade;
