\timing on
select pg_sleep(1);
/*----------------test1 Locking succeeded. */
select current_time;
select * from hw_t1 where id1 = 3 for update WAIT 10;
select current_time;
--time delay
select pg_sleep(2);--wait session1
--time delay
/*----------------test2 Locking failed. */
select current_time;
select * from hw_t1 where id1 = 3 for update WAIT 2;
select current_time;
--time delay
select pg_sleep(2);--wait session1
--time delay
/*----------------test3 Locking succeeded. */
select current_time;
select * from (select * from t1 for update of t1 wait 10) as foo;
select current_time;
--time delay
select pg_sleep(2);
--time delay
/*----------------test4 Locking failed. */
select current_time;
select * from (select * from t1 for update of t1 wait 2) as foo;
select current_time;
--time delay
select pg_sleep(2);
/*----------------test5 Locking update. */
select current_time;
select * from hw_t1 where id1 = 2 for update wait 2;
select current_time;
select pg_sleep(2);
/*----------------test5_1 Locking update. */
select current_time;
select * from hw_t1 where id1 = 2 for update wait 2;
select current_time;
/*----------------test6 Locking delete. */
select pg_sleep(2);
select current_time;
select * from hw_t1 where id1 = 3 for update WAIT 2;
select current_time;
select pg_sleep(2);
/*----------------test6_1 Locking delete. */
select current_time;
select * from hw_t1 where id1 = 2 for update WAIT 2;
select current_time;
\timing off