START TRANSACTION;
	select pg_sleep(1);
	select current_timestamp;
	insert into ptx (select 100x,10x,x from generate_series(1,1000) as x);
	insert into cmpts values(1,current_timestamp);
	select current_timestamp;
COMMIT;
select count(*) from ptx partition(p0);
