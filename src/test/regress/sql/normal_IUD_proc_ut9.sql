START TRANSACTION;
	select current_timestamp;
	insert into tx (select 100x,10x,x from generate_series(1,1000) as x);
	insert into cmpts values(2,current_timestamp);
	select pg_sleep(10);
	select current_timestamp;
COMMIT;
select pg_sleep(2);
select count(*) from tx;
