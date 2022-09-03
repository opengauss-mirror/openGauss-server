-- prepare the parrallel modifed table;
DROP TABLE IF EXISTS partition_interval_parallel;
create table partition_interval_parallel
(
	c1 int,
	c2 int,
	c3 date not null
)
partition by range (c3)
INTERVAL ('1 month') 
(
	PARTITION partition_interval_parallel_p1 VALUES LESS THAN ('2020-05-01'),
	PARTITION partition_interval_parallel_p2 VALUES LESS THAN ('2020-06-01')
);
CREATE INDEX idx1_partition_interval_parallel on partition_interval_parallel(c3) local ;





