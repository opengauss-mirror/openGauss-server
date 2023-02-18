create schema create_index_if_not_exists;
set current_schema = create_index_if_not_exists;

create table test(a int);
create index test_index on test(a);
create index test_index on test(a);
create index if not exists test_index on test(a);
create index if not exists test_index1 on test(a);

drop table test;
drop schema create_index_if_not_exists;
