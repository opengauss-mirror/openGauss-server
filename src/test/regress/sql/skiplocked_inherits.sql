select pg_sleep(1);
select * from skiplocked_inherits_1 order by 1 desc limit 1 FOR UPDATE SKIP LOCKED;
