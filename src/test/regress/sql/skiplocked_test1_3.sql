select pg_sleep(1);

set enable_opfusion = on;
select * from skiplocked_t1 order by id for update nowait;
select * from skiplocked_t1 order by id for update skip locked;
explain select * from skiplocked_t1 order by id for update skip locked;
select * from skiplocked_t1 order by id for update;

create view skiplocked_v1 as select * from skiplocked_t1;
create view skiplocked_v2 as select * from skiplocked_t2;
create view skiplocked_v3 as select * from skiplocked_t3;
select * from skiplocked_v1 order by 1 limit 1 for update skip locked;
select * from skiplocked_v2 order by 1 limit 1 for update skip locked;
select * from skiplocked_v3 order by 1 limit 1 for update skip locked;