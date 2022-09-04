begin;
update gtest1 set a=a||'thread1';
select pg_sleep(7);
commit;
