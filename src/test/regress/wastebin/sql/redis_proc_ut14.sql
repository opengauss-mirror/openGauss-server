/*   
 * UT14: 1)redis_proc get the access exclusive lock
 *       2)IUD wait until redis_proc finish
 *       3)truncate partition wait until IUD finish
 */
START TRANSACTION;
	select current_timestamp;
	LOCK TABLE ptx IN ACCESS EXCLUSIVE MODE;
	select current_timestamp;
	select pg_sleep(3);
	insert into cmpts values(1,current_timestamp);
COMMIT;
select current_timestamp;
select count(*) from ptx partition (p0);
