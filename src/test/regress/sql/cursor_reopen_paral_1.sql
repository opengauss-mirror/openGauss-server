start transaction;
call proc1();
select pg_sleep(2);
commit;

start transaction;
call proc2();
select pg_sleep(1);
call proc1();
commit;