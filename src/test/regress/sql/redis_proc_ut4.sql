/*   
 * UT4:  1)redis_proc get the access exclusive lock
 *       2)IUD wait until redis_proc finish
 *       3)Drop wait until IUD finish
 */
START TRANSACTION;
	select current_timestamp;
	LOCK TABLE tx IN SHARE MODE;
	select current_timestamp;
	select pg_sleep(5);
	insert into cmpts values(1,current_timestamp);
	select current_timestamp;
COMMIT;
select pg_sleep(2);
select current_timestamp;
select count(*) from tx;
