select pg_sleep(1);

set enable_opfusion = on;
select * from skiplocked_t1 order by id for update nowait;
select * from skiplocked_t1 order by id for update skip locked;
explain select * from skiplocked_t1 order by id for update skip locked;
select * from skiplocked_t1 order by id for update;