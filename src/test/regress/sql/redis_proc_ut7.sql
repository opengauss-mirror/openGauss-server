/*
 *   UT3:  1)redis_proc get access exclusive lock (swap table phase)
 *         2)Drop wait until swap done
 */
start transaction;
	select current_timestamp;
	lock table tx in SHARE mode;
	select current_timestamp;
	select pg_sleep(5);
	select count(*) from tx;
	select current_timestamp;
END;
select current_timestamp;
select count(*) from tx;
