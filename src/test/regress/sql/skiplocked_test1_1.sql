begin;

update skiplocked_t1 set info = 'two2' where id = 2;
select * from skiplocked_inherits_1 order by 1 desc limit 1 FOR UPDATE SKIP LOCKED;
select pg_sleep(5);

end;
