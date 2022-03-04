START TRANSACTION;

DROP TABLE IF EXISTS test_astore;
CREATE TABLE test_astore (user_id serial PRIMARY KEY, time_clock VARCHAR ( 50 ));
CREATE INDEX test_astore_idx ON test_astore(user_id);
insert into test_astore select generate_series(1, 200), clock_timestamp();
update test_astore set time_clock = NULL where user_id = 150;
update test_astore set time_clock = 'time' where user_id = 150;
delete test_astore where user_id > 190;

DROP TABLE IF EXISTS test_ustore;
CREATE TABLE test_ustore (user_id serial PRIMARY KEY, time_clock VARCHAR ( 50 )) with(storage_type=ustore);
CREATE INDEX test_ustore_idx ON test_ustore(user_id);
insert into test_ustore select generate_series(1, 200), clock_timestamp();
update test_ustore set time_clock = NULL where user_id = 150;
update test_ustore set time_clock = 'time' where user_id = 150;
delete test_ustore where user_id > 190;

DROP TABLE IF EXISTS test_segment;
CREATE TABLE test_segment (a int, b int, c int) with(segment=on);
INSERT INTO test_segment values(generate_series(1,10),generate_series(1,10), generate_series(1,10));

CREATE OR REPLACE FUNCTION gs_parse_page_bypath_test(tablename in varchar2, block_num in int, relation in varchar2, read_mem in bool)
RETURNS table (output text)
LANGUAGE plpgsql
AS
$$
DECLARE
	param1  text;
BEGIN
	SELECT pg_relation_filepath(tablename) into param1;
	return query SELECT gs_parse_page_bypath(''|| param1 ||'', block_num, relation, read_mem);
END;
$$
;

SELECT gs_parse_page_bypath_test('test_astore', 0, 'heap', true);
SELECT gs_parse_page_bypath_test('test_segment', 0, 'segment', true);
CHECKPOINT;
SELECT gs_parse_page_bypath_test('test_astore', -1, 'heap', true);
SELECT gs_parse_page_bypath_test('test_astore_idx', 1, 'btree', false);
SELECT gs_parse_page_bypath_test('test_ustore', -1, 'uheap', false);
SELECT gs_parse_page_bypath_test('test_ustore_idx', 1, 'ubtree', false);
SELECT gs_parse_page_bypath_test('test_segment', 0, 'segment', false);

DROP INDEX IF EXISTS test_astore_idx;
DROP TABLE IF EXISTS test_astore;
DROP INDEX IF EXISTS test_ustore_idx;
DROP TABLE IF EXISTS test_ustore;
DROP TABLE IF EXISTS test_segment;

COMMIT;

