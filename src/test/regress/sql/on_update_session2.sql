select pg_sleep(2);
\c mysql_test
update tb666 set c2 = 3;
select * from tb666;