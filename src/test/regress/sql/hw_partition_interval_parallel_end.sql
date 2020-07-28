-- check results and clean
select count(*) from partition_interval_parallel;
select relname, parttype, partstrategy, boundaries, indisusable from pg_partition
	where parentid = (select oid from pg_class where relname = 'partition_interval_parallel')
	order by 1;
--cleanup
DROP TABLE partition_interval_parallel;
