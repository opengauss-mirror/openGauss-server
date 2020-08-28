drop foreign table if exists mm_test;
create foreign table mm_test (id int not null, name varchar(1000)) server mot_server;
insert into mm_test values (1, 'aaa'), (2, 'bbb'), (3, 'ccc'), (4, 'ddd'), (5, 'eee'), (6, 'fff');
select pg_relation_size('mm_test') > 0;
create index mm_test_index on mm_test using btree(id);
select pg_relation_size('mm_test_index') > 0;
drop foreign table mm_test;
