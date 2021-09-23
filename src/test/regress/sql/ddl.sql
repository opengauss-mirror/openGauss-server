-- predictability
SET synchronous_commit = on;

SELECT 'init' FROM pg_create_logical_replication_slot('regression_slot', 'test_decoding');
-- fail because of an already existing slot
SELECT 'init' FROM pg_create_logical_replication_slot('regression_slot', 'test_decoding');
-- fail because of an invalid name
SELECT 'init' FROM pg_create_logical_replication_slot('Invalid Name', 'test_decoding');

-- fail twice because of an invalid parameter values
SELECT 'init' FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-xids', 'frakbar');
SELECT 'init' FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'nonexistant-option', 'frakbar');
SELECT 'init' FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-xids', 'frakbar');

-- succeed once
SELECT pg_drop_replication_slot('regression_slot');
-- fail
SELECT pg_drop_replication_slot('regression_slot');


SELECT 'init' FROM pg_create_logical_replication_slot('regression_slot', 'test_decoding');

/*
 * Check that changes are handled correctly when interleaved with ddl
 */
CREATE TABLE replication_example(id SERIAL PRIMARY KEY, somedata int, text varchar(120));
START TRANSACTION;
INSERT INTO replication_example(somedata, text) VALUES (1, 1);
INSERT INTO replication_example(somedata, text) VALUES (1, 2);
COMMIT;
START TRANSACTION;
INSERT INTO replication_example(somedata, text) VALUES (3, 2);
INSERT INTO replication_example(somedata, text) VALUES (3, 3);
COMMIT;

-- collect all changes
SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1');

/*
 * check that disk spooling works
 */
/* display results, but hide most of the output */
SELECT count(*), min(data), max(data)
FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1')
GROUP BY substring(data, 1, 24)
ORDER BY 1,2;

/*
 * check whether we decode subtransactions correctly in relation with each
 * other
 */
CREATE TABLE tr_sub (id serial primary key, path text);

-- toplevel, subtxn, toplevel, subtxn, subtxn
START TRANSACTION;
INSERT INTO tr_sub(path) VALUES ('1-top-#1');

SAVEPOINT a;
INSERT INTO tr_sub(path) VALUES ('1-top-1-#1');
INSERT INTO tr_sub(path) VALUES ('1-top-1-#2');
RELEASE SAVEPOINT a;

SAVEPOINT b;
SAVEPOINT c;
INSERT INTO tr_sub(path) VALUES ('1-top-2-1-#1');
INSERT INTO tr_sub(path) VALUES ('1-top-2-1-#2');
RELEASE SAVEPOINT c;
INSERT INTO tr_sub(path) VALUES ('1-top-2-#1');
RELEASE SAVEPOINT b;
COMMIT;

SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1');

-- check that we handle xlog assignments correctly
START TRANSACTION;
-- nest 80 subtxns
SAVEPOINT subtop;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
-- assign xid by inserting
INSERT INTO tr_sub(path) VALUES ('2-top-1...--#1');
INSERT INTO tr_sub(path) VALUES ('2-top-1...--#2');
INSERT INTO tr_sub(path) VALUES ('2-top-1...--#3');
RELEASE SAVEPOINT subtop;
INSERT INTO tr_sub(path) VALUES ('2-top-#1');
COMMIT;

SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1');

-- make sure rollbacked subtransactions aren't decoded
START TRANSACTION;
INSERT INTO tr_sub(path) VALUES ('3-top-2-#1');
SAVEPOINT a;
INSERT INTO tr_sub(path) VALUES ('3-top-2-1-#1');
SAVEPOINT b;
INSERT INTO tr_sub(path) VALUES ('3-top-2-2-#1');
ROLLBACK TO SAVEPOINT b;
INSERT INTO tr_sub(path) VALUES ('3-top-2-#2');
COMMIT;

SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1');

-- test whether a known, but not yet logged toplevel xact, followed by a
-- subxact commit is handled correctly
START TRANSACTION;
SELECT txid_current() != 0; -- so no fixed xid apears in the outfile
SAVEPOINT a;
INSERT INTO tr_sub(path) VALUES ('4-top-1-#1');
RELEASE SAVEPOINT a;
COMMIT;

-- test whether a change in a subtransaction, in an unknown toplevel
-- xact is handled correctly.
START TRANSACTION;
SAVEPOINT a;
INSERT INTO tr_sub(path) VALUES ('5-top-1-#1');
COMMIT;

SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1');

/*
 * check whether we handle updates/deletes correct with & without a pkey
 */

/* we should handle the case without a key at all more gracefully */
CREATE TABLE table_without_key(id serial, data int);
INSERT INTO table_without_key(data) VALUES(1),(2);
DELETE FROM table_without_key WHERE data = 1;
-- won't log old keys
UPDATE table_without_key SET data = 3 WHERE data = 2;
UPDATE table_without_key SET id = -id;
UPDATE table_without_key SET id = -id;

-- check toast support
START TRANSACTION;
CREATE SEQUENCE toasttable_rand_seq START 79 INCREMENT 1499; -- portable "random"
CREATE TABLE toasttable(
       id serial primary key,
       toasted_col1 text,
       rand1 float8 DEFAULT nextval('toasttable_rand_seq'),
       toasted_col2 text,
       rand2 float8 DEFAULT nextval('toasttable_rand_seq')
       );
COMMIT;
-- uncompressed external toast data
INSERT INTO toasttable(toasted_col1) SELECT string_agg(g.i::text, '') FROM generate_series(1, 2000) g(i);


-- compressed external toast data
INSERT INTO toasttable(toasted_col2) SELECT repeat(string_agg(to_char(g.i, 'FM0000'), ''), 50) FROM generate_series(1, 500) g(i);

-- update of existing column
UPDATE toasttable
    SET toasted_col1 = (SELECT string_agg(g.i::text, '') FROM generate_series(1, 2000) g(i))
WHERE id = 1;

SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1');

INSERT INTO toasttable(toasted_col1) SELECT string_agg(g.i::text, '') FROM generate_series(1, 2000) g(i);

-- update of second column, first column unchanged
UPDATE toasttable
    SET toasted_col2 = (SELECT string_agg(g.i::text, '''') FROM generate_series(1, 2000) g(i))
WHERE id = 1;

-- make sure we decode correctly even if the toast table is gone
DROP TABLE toasttable;
create table bmsql_order_line (
  ol_w_id         integer   not null,
  ol_d_id         integer   not null,
  ol_o_id         integer   not null,
  ol_number       integer   not null,
  ol_i_id         integer   not null,
  ol_delivery_d   timestamp,
  ol_amount       decimal(6,2),
  ol_supply_w_id  integer,
  ol_quantity     integer,
  ol_dist_info    char(24)
)
partition by range(ol_d_id)
(
  partition p0 values less than (10),
  partition p1 values less than (100),
  partition p2 values less than (maxvalue)
);
alter table bmsql_order_line add constraint bmsql_order_line_pkey primary key (ol_w_id, ol_d_id, ol_o_id, ol_number);
insert into bmsql_order_line(ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_dist_info) values(1, 1, 1, 1, 1, '123');
update bmsql_order_line set ol_dist_info='ss' where ol_w_id =1;
delete from bmsql_order_line;


-- done, free logical replication slot
SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1');

SELECT pg_drop_replication_slot('regression_slot');

create table t1(id1 int, id2 int, data text, primary key(id1, id2)) distribute by hash(id2);
insert into t1 values (1, generate_series(1, 10000), 'gao');
SELECT 'init' FROM pg_create_logical_replication_slot('regression_slot', 'test_decoding');
update t1 set id1= id1+10000 where id2 <=10000;
SELECT data FROM pg_logical_slot_peek_changes('regression_slot', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1');
drop table t1;
SELECT pg_drop_replication_slot('regression_slot');
drop table replication_example;
drop table tr_sub;
drop table table_without_key;
drop table bmsql_order_line;
drop sequence toasttable_rand_seq;
