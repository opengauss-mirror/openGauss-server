/*
 *  UT12:  1)redis_proc get the access share lock of the partition
 *         2)IUD came
 *         3)AT truncate partition DOES NOT wait and cancels redis_proc directly but acquires lock until IUD finishes
 *           - cancel happens at pg_sleep time
 *           - IUD continues
 *           - truncate partition finishes after IUD finishes
 */
START TRANSACTION;
	select pg_enable_redis_proc_cancelable();
	select current_timestamp;
	LOCK TABLE ptx IN ACCESS SHARE MODE;
	insert into data_redis.data_redis_tmp_12345 select * from ptx partition (p0);insert into data_redis.data_redis_tmp_12345 select * from ptx partition (p1); insert into data_redis.data_redis_tmp_12345 select * from ptx partition (p2);
	select current_timestamp;
	select pg_sleep(10);
	select current_timestamp;
COMMIT;
select current_timestamp;
select count(*) from data_redis.data_redis_tmp_12345;
select count(*) from ptx partition (p0);
