START TRANSACTION;
	select pg_sleep(1);
	select current_timestamp;
	insert into tx (select 100x,10x,x from generate_series(1,10000) as x);
	insert into cmpts values(2,current_timestamp);
	select current_timestamp;
COMMIT;
select count(*) from tx;
