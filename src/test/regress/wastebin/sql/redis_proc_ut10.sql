/*
 *   UT2:  1)IUD came
 *         2)redis_proc get the ACCESS EXCLUSIVE LOCK and it waits for IUD to finish
 *         3)truncate DOES NOT wait and cancels redis_proc directly while redis_proc waits and acquires lock until IUD finishes
 *           - cancel happens at pg_sleep time 
 *           - IUD continues
 *           - truncate table finishes after IUD finishes
 */
START TRANSACTION;
        select pg_sleep(1);
        select pg_enable_redis_proc_cancelable();
	select current_timestamp;
	LOCK TABLE tx IN SHARE MODE;
	select pg_sleep(10);
        select current_timestamp;
COMMIT;
select current_timestamp;
select count(*) from tx;
