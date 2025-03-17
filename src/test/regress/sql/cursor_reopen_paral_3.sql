start transaction;
select pg_sleep(6);
call proc1();
select * from employees;
commit;