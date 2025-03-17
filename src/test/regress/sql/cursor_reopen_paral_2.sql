start transaction;
select pg_sleep(10);
call proc1();
select * from employees;
commit;

start transaction;
call proc2();
select * from employees2;
commit;
