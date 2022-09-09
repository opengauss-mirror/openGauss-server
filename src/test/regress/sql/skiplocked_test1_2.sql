select pg_sleep(1);
set enable_opfusion = off;
select * from skiplocked_t1 order by id for update nowait;

select * from skiplocked_t1 order by id for update skip locked;
explain select * from skiplocked_t1 order by id for update skip locked;
select * from skiplocked_t1 order by id for update;


select * from skiplocked_t2 order by id for update nowait;
select * from skiplocked_t2 order by id for update skip locked;
select * from skiplocked_t2 order by id for update;
