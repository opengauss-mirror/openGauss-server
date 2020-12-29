/*
 *  UT13:  1)redis_proc get access exclusive lock (swap table phase)
 *         2)AT Trancate partition wait until swap done
 */
start transaction;
	select current_timestamp;
	lock table ptx in ACCESS EXCLUSIVE mode;
	select current_timestamp;
	select pg_sleep(5);
	select count(*) from ptx;
	insert into cmpts values(1,current_timestamp);
END;
select current_timestamp;
select count(*) from ptx partition (p0);
