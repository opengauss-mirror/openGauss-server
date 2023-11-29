/*
 *   UT2:  1)redis_proc get the access share lock
 *         2)IUD came
 *         3)truncate DO NOT wait and cancel redis_proc directly but DO NOT get lock yet until insert finishs
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
