-- predictability
SET synchronous_commit = on;
set enable_data_replicate = false;

SELECT 'init' FROM pg_create_logical_replication_slot('regression_slot', 'mppdb_decoding');
CREATE TABLE replication_example(id SERIAL PRIMARY KEY, somedata int, text varchar(120))with(storage_type = ustore);
INSERT INTO replication_example(somedata) VALUES (1);
SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1');

INSERT INTO replication_example(somedata) VALUES (2);
ALTER TABLE replication_example ADD COLUMN testcolumn1 int;
INSERT INTO replication_example(somedata, testcolumn1) VALUES (3,  1);

INSERT INTO replication_example(somedata) VALUES (3);
ALTER TABLE replication_example ADD COLUMN testcolumn2 int;
INSERT INTO replication_example(somedata, testcolumn1, testcolumn2) VALUES (4,  2, 1);

set xc_maintenance_mode = on;
VACUUM FULL pg_am;
VACUUM FULL pg_amop;
VACUUM FULL pg_proc;
VACUUM FULL pg_opclass;
VACUUM FULL pg_type;
VACUUM FULL pg_index;
VACUUM FULL pg_database;


-- repeated rewrites that succeed
CLUSTER pg_class USING pg_class_oid_index;
CLUSTER pg_class USING pg_class_oid_index;
CLUSTER pg_class USING pg_class_oid_index;

 -- repeated rewrites in different transactions
VACUUM FULL pg_class;
VACUUM FULL pg_class;
set xc_maintenance_mode = off;

INSERT INTO replication_example(somedata, testcolumn1) VALUES (5, 3);

INSERT INTO replication_example(somedata, testcolumn1) VALUES (6, 4);
ALTER TABLE replication_example ADD COLUMN testcolumn3 int;
INSERT INTO replication_example(somedata, testcolumn1, testcolumn3) VALUES (7, 5, 1);

-- make old files go away
CHECKPOINT;

SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1');
SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1');
SELECT pg_drop_replication_slot('regression_slot');

DROP TABLE IF EXISTS replication_example;
