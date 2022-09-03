select pg_sleep(3);
update gtest1 set a=a||'thread2';
