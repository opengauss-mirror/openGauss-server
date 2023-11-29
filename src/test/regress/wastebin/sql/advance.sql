SET synchronous_commit = on;

 -- fail because of an already existing slot
execute direct on (datanode1)'SELECT ''init'' FROM pg_create_logical_replication_slot(''regression_slot'', ''test_decoding'');';
CREATE TABLE replication_example(id SERIAL PRIMARY KEY, somedata int, text varchar(120));
BEGIN;
INSERT INTO replication_example(somedata, text) VALUES (1, 1);
INSERT INTO replication_example(somedata, text) VALUES (1, 2);
COMMIT;
execute direct on (datanode1)'SELECT * FROM pg_replication_slot_advance(''regression_slot'', NULL);';
INSERT INTO replication_example(somedata, text) VALUES (1, 4);
INSERT INTO replication_example(somedata, text) VALUES (1, 5);
execute direct on (datanode1)'SELECT * FROM pg_logical_slot_get_changes(''regression_slot'', NULL, NULL);';
execute direct on (datanode1)'SELECT pg_drop_replication_slot(''regression_slot'');';

