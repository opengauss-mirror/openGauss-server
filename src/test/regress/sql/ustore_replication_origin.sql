SELECT 'init' FROM pg_create_logical_replication_slot('regression_slot', 'mppdb_decoding');
create table a (a int, b int)with(storage_type = ustore);
insert into a values(generate_series(1, 10), 1);
update a set b = b+1 where b = 1;
delete a where b = 2;
SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL);

set reporiginid = 1;

insert into a values(generate_series(1, 10), 1);
update a set b = b+1 where b = 1;
delete a where b = 2;
SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL);

insert into a values(generate_series(1, 10), 1);
update a set b = b+1 where b = 1;
delete a where b = 2;

SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'only-local', '0');
insert into a values(generate_series(1, 10), 1);
update a set b = b+1 where b = 1;
delete a where b = 2;

SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'only-local', '1');
SELECT pg_drop_replication_slot('regression_slot');
drop table a;
