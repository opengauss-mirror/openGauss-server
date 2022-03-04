create table hw_t1 (id1 int, id2 int, num int);
insert into hw_t1 values (1,11,11), (2,21,21), (3,31,31), (4,41,41), (5,51,51);

/*----------------test1 Locking succeeded. */
select current_time;
begin;
select * from hw_t1 where id1 = 3 for update;
select pg_sleep(2);
--time delay
end;
select current_time;
select pg_sleep(1);--wait session2

/*----------------test2 Locking failed. */
select current_time;
begin;
select * from hw_t1 where id1 = 3 for update;
select pg_sleep(4);
--time delay
end;
select current_time;
create table t1(val int, val2 int);
insert into t1 values(1,11),(2,11); insert into t1 values(1,11),(2,11);
insert into t1 values(3,11),(4,11); insert into t1 values(5,11),(6,11);

/*----------------test3 Locking succeeded. */
select current_time;
begin;
select * from (select * from t1 for update of t1 nowait) as foo;
--time delay
select pg_sleep(2);
--time delay
end;
select current_time;
select pg_sleep(1);--wait session2

/*----------------test4 Locking failed. */
select current_time;
begin;
select * from (select * from t1 for update of t1 nowait) as foo;
--time delay
select pg_sleep(4);
--time delay
end;
select current_time;
/*----------------test5 Locking update. */
select current_time;
begin;
update hw_t1 set num=666 where id1 = 2;
select pg_sleep(4);
--time 4
end;
select current_time;
/*----------------test5_1 Locking update. */
select current_time;
begin;
update hw_t1 set num=666;
select pg_sleep(4);
--time 4
end;
select current_time;
/*----------------test6 Locking delete. */
select current_time;
begin;
delete hw_t1 where id1 = 3;
select pg_sleep(4);
--time 4
end;
select current_time;
/*----------------test6_1 Locking delete. */
select current_time;
begin;
delete from hw_t1;
select pg_sleep(4);
--time 4
end;
select current_time;
drop table hw_t1;
drop table t1;