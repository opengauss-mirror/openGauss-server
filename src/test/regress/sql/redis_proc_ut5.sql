/*
 *  UT1:  1)redis_proc get the access share lock
 *        2)Drop *can* cancel redis_proc
 */
START TRANSACTION;
        select pg_enable_redis_proc_cancelable();
	select current_timestamp;
	LOCK TABLE tx IN ACCESS SHARE MODE;
	select current_timestamp;
	select pg_sleep(10);
	select current_timestamp;
END;
select current_timestamp;
select count(*) from tx;
