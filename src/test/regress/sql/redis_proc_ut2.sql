/*
 *   UT2:  1)redis_proc get the access share lock
 *         2)IUD came
 *         3)Drop DOES NOT wait and cancels redis_proc directly but acquires lock until IUD finishes
 *           - cancel happens at pg_sleep time 
 *           - IUD continues
 *           - drop table finishes after IUD finishes
 */
START TRANSACTION;
        select pg_enable_redis_proc_cancelable();
	select current_timestamp;
	LOCK TABLE tx IN ACCESS SHARE MODE;
	select current_timestamp;
	select pg_sleep(10);
	select current_timestamp;
COMMIT;
select current_timestamp;
select count(*) from tx;
