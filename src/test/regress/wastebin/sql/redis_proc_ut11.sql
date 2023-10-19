/*
 *  UT11:  1)redis_proc get the access share lock
 *         2)Alter table truncate partition *can* cancel redis_proc
 */
START TRANSACTION;
	select pg_enable_redis_proc_cancelable();
	select current_timestamp;
	LOCK TABLE ptx IN ACCESS SHARE MODE;
	insert into data_redis.data_redis_tmp_12345 select * from ptx partition (p0); insert into data_redis.data_redis_tmp_12345 select * from ptx partition (p1);insert into data_redis.data_redis_tmp_12345 select * from ptx partition (p2); 
	select current_timestamp;
	select pg_sleep(10);
END;
select current_timestamp;
select pg_sleep(1);
select count(*) from ptx partition (p0);
