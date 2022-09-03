DROP SCHEMA row_partition_iterator_elimination CASCADE;
CREATE SCHEMA row_partition_iterator_elimination;
SET CURRENT_SCHEMA TO row_partition_iterator_elimination;

drop table test_hash_ht;
create table test_hash_ht ( a int,b int, c int, d int)
partition by hash(a)
(
	partition p1, 
	partition p2, 
	partition p3,
	partition p4,
	partition p5,
	partition p6,
	partition p7,
	partition p8,
	partition p9,
	partition p10,
	partition p11,
	partition p12,
	partition p13,
	partition p14,
	partition p15,
	partition p16,
	partition p17,
	partition p18,
	partition p19,
	partition p20
);

create index idx_hash_local on test_hash_ht(a) local;
insert into test_hash_ht values(generate_series(0,49), generate_series(1,100));

drop table test_range_pt;
create table test_range_pt (a int, b int, c int, d int)
partition by range(a)
(
	partition p1 values less than (20),
	partition p2 values less than (30),
	partition p3 values less than (40),
	partition p4 values less than (50)
)ENABLE ROW MOVEMENT;

create index idx_range_local on test_range_pt(a) local;
insert into test_range_pt values(generate_series(0,49), generate_series(1,100));

--base 验证数据结果
select * from test_hash_ht where a = 30 order by 1,2,3,4;
select count(a) from test_hash_ht where a = 30;
select count(b) from test_hash_ht where a = 30;
select * from test_range_pt where a = 30 order by 1,2,3,4;
select count(a) from test_range_pt where a = 30;
select count(b) from test_range_pt where a = 30;
select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 order by 1,2,3,4;
select count(t1.a) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5;
select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5 order by 1,2,3,4;
select count(t1.b) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5;

--设置参数
set partition_iterator_elimination = on;
set enable_material = off;

--seqscan + nestloop
set enable_seqscan = on;
set enable_indexscan = off;
set enable_bitmapscan = off;
set enable_nestloop = on;
set enable_hashjoin = off;
set enable_mergejoin = off;

explain(costs off, verbose on) select * from test_hash_ht where a = 30 order by 1,2,3,4;
explain(costs off, verbose on) select count(a) from test_hash_ht where a = 30;
explain(costs off, verbose on) select count(b) from test_hash_ht where a = 30;
explain(costs off, verbose on) select * from test_range_pt where a = 30 order by 1,2,3,4;
explain(costs off, verbose on) select count(a) from test_range_pt where a = 30;
explain(costs off, verbose on) select count(b) from test_range_pt where a = 30;
explain(costs off, verbose on) select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 order by 1,2,3,4;
explain(costs off, verbose on) select count(t1.a) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5;
explain(costs off, verbose on) select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5 order by 1,2,3,4;
explain(costs off, verbose on) select count(t1.b) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5;
select * from test_hash_ht where a = 30 order by 1,2,3,4;
select count(a) from test_hash_ht where a = 30;
select count(b) from test_hash_ht where a = 30;
select * from test_range_pt where a = 30 order by 1,2,3,4;
select count(a) from test_range_pt where a = 30;
select count(b) from test_range_pt where a = 30;
select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 order by 1,2,3,4;
select count(t1.a) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5;
select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5 order by 1,2,3,4;
select count(t1.b) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5;

--seqscan + hashjoin
set enable_seqscan = on;
set enable_indexscan = off;
set enable_bitmapscan = off;
set enable_nestloop = off;
set enable_hashjoin = on;
set enable_mergejoin = off;

explain(costs off, verbose on) select * from test_hash_ht where a = 30 order by 1,2,3,4;
explain(costs off, verbose on) select count(a) from test_hash_ht where a = 30;
explain(costs off, verbose on) select count(b) from test_hash_ht where a = 30;
explain(costs off, verbose on) select * from test_range_pt where a = 30 order by 1,2,3,4;
explain(costs off, verbose on) select count(a) from test_range_pt where a = 30;
explain(costs off, verbose on) select count(b) from test_range_pt where a = 30;
explain(costs off, verbose on) select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 order by 1,2,3,4;
explain(costs off, verbose on) select count(t1.a) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5;
explain(costs off, verbose on) select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5 order by 1,2,3,4;
explain(costs off, verbose on) select count(t1.b) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5;
select * from test_hash_ht where a = 30 order by 1,2,3,4;
select count(a) from test_hash_ht where a = 30;
select count(b) from test_hash_ht where a = 30;
select * from test_range_pt where a = 30 order by 1,2,3,4;
select count(a) from test_range_pt where a = 30;
select count(b) from test_range_pt where a = 30;
select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 order by 1,2,3,4;
select count(t1.a) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5;
select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5 order by 1,2,3,4;
select count(t1.b) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5;

--seqscan + mergejoin
set enable_seqscan = on;
set enable_indexscan = off;
set enable_bitmapscan = off;
set enable_nestloop = off;
set enable_hashjoin = off;
set enable_mergejoin = on;

explain(costs off, verbose on) select * from test_hash_ht where a = 30 order by 1,2,3,4;
explain(costs off, verbose on) select count(a) from test_hash_ht where a = 30;
explain(costs off, verbose on) select count(b) from test_hash_ht where a = 30;
explain(costs off, verbose on) select * from test_range_pt where a = 30 order by 1,2,3,4;
explain(costs off, verbose on) select count(a) from test_range_pt where a = 30;
explain(costs off, verbose on) select count(b) from test_range_pt where a = 30;
explain(costs off, verbose on) select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 order by 1,2,3,4;
explain(costs off, verbose on) select count(t1.a) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5;
explain(costs off, verbose on) select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5 order by 1,2,3,4;
explain(costs off, verbose on) select count(t1.b) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5;
select * from test_hash_ht where a = 30 order by 1,2,3,4;
select count(a) from test_hash_ht where a = 30;
select count(b) from test_hash_ht where a = 30;
select * from test_range_pt where a = 30 order by 1,2,3,4;
select count(a) from test_range_pt where a = 30;
select count(b) from test_range_pt where a = 30;
select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 order by 1,2,3,4;
select count(t1.a) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5;
select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5 order by 1,2,3,4;
select count(t1.b) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5;

--indexscan/indexonlyscan + nestloop
set enable_seqscan = off;
set enable_indexscan = on;
set enable_bitmapscan = off;
set enable_nestloop = on;
set enable_hashjoin = off;
set enable_mergejoin = off;

explain(costs off, verbose on) select * from test_hash_ht where a = 30 order by 1,2,3,4;
explain(costs off, verbose on) select count(a) from test_hash_ht where a = 30;
explain(costs off, verbose on) select count(b) from test_hash_ht where a = 30;
explain(costs off, verbose on) select * from test_range_pt where a = 30 order by 1,2,3,4;
explain(costs off, verbose on) select count(a) from test_range_pt where a = 30;
explain(costs off, verbose on) select count(b) from test_range_pt where a = 30;
explain(costs off, verbose on) select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 order by 1,2,3,4;
explain(costs off, verbose on) select count(t1.a) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5;
explain(costs off, verbose on) select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5 order by 1,2,3,4;
explain(costs off, verbose on) select count(t1.b) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5;
select * from test_hash_ht where a = 30 order by 1,2,3,4;
select count(a) from test_hash_ht where a = 30;
select count(b) from test_hash_ht where a = 30;
select * from test_range_pt where a = 30 order by 1,2,3,4;
select count(a) from test_range_pt where a = 30;
select count(b) from test_range_pt where a = 30;
select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 order by 1,2,3,4;
select count(t1.a) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5;
select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5 order by 1,2,3,4;
select count(t1.b) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5;

--indexscan/indexonlyscan + hashjoin
set enable_seqscan = off;
set enable_indexscan = on;
set enable_bitmapscan = off;
set enable_nestloop = off;
set enable_hashjoin = on;
set enable_mergejoin = off;

explain(costs off, verbose on) select * from test_hash_ht where a = 30 order by 1,2,3,4;
explain(costs off, verbose on) select count(a) from test_hash_ht where a = 30;
explain(costs off, verbose on) select count(b) from test_hash_ht where a = 30;
explain(costs off, verbose on) select * from test_range_pt where a = 30 order by 1,2,3,4;
explain(costs off, verbose on) select count(a) from test_range_pt where a = 30;
explain(costs off, verbose on) select count(b) from test_range_pt where a = 30;
explain(costs off, verbose on) select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 order by 1,2,3,4;
explain(costs off, verbose on) select count(t1.a) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5;
explain(costs off, verbose on) select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5 order by 1,2,3,4;
explain(costs off, verbose on) select count(t1.b) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5;
select * from test_hash_ht where a = 30 order by 1,2,3,4;
select count(a) from test_hash_ht where a = 30;
select count(b) from test_hash_ht where a = 30;
select * from test_range_pt where a = 30 order by 1,2,3,4;
select count(a) from test_range_pt where a = 30;
select count(b) from test_range_pt where a = 30;
select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 order by 1,2,3,4;
select count(t1.a) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5;
select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5 order by 1,2,3,4;
select count(t1.b) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5;

--indexscan/indexonlyscan + mergejoin
set enable_seqscan = off;
set enable_indexscan = on;
set enable_bitmapscan = off;
set enable_nestloop = off;
set enable_hashjoin = off;
set enable_mergejoin = on;

explain(costs off, verbose on) select * from test_hash_ht where a = 30 order by 1,2,3,4;
explain(costs off, verbose on) select count(a) from test_hash_ht where a = 30;
explain(costs off, verbose on) select count(b) from test_hash_ht where a = 30;
explain(costs off, verbose on) select * from test_range_pt where a = 30 order by 1,2,3,4;
explain(costs off, verbose on) select count(a) from test_range_pt where a = 30;
explain(costs off, verbose on) select count(b) from test_range_pt where a = 30;
explain(costs off, verbose on) select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 order by 1,2,3,4;
explain(costs off, verbose on) select count(t1.a) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5;
explain(costs off, verbose on) select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5 order by 1,2,3,4;
explain(costs off, verbose on) select count(t1.b) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5;
select * from test_hash_ht where a = 30 order by 1,2,3,4;
select count(a) from test_hash_ht where a = 30;
select count(b) from test_hash_ht where a = 30;
select * from test_range_pt where a = 30 order by 1,2,3,4;
select count(a) from test_range_pt where a = 30;
select count(b) from test_range_pt where a = 30;
select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 order by 1,2,3,4;
select count(t1.a) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5;
select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5 order by 1,2,3,4;
select count(t1.b) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5;

--bitmapscan + nestloop
set enable_seqscan = off;
set enable_indexscan = off;
set enable_bitmapscan = on;
set enable_nestloop = on;
set enable_hashjoin = off;
set enable_mergejoin = off;

explain(costs off, verbose on) select * from test_hash_ht where a = 30 order by 1,2,3,4;
explain(costs off, verbose on) select count(a) from test_hash_ht where a = 30;
explain(costs off, verbose on) select count(b) from test_hash_ht where a = 30;
explain(costs off, verbose on) select * from test_range_pt where a = 30 order by 1,2,3,4;
explain(costs off, verbose on) select count(a) from test_range_pt where a = 30;
explain(costs off, verbose on) select count(b) from test_range_pt where a = 30;
explain(costs off, verbose on) select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 order by 1,2,3,4;
explain(costs off, verbose on) select count(t1.a) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5;
explain(costs off, verbose on) select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5 order by 1,2,3,4;
explain(costs off, verbose on) select count(t1.b) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5;
select * from test_hash_ht where a = 30 order by 1,2,3,4;
select count(a) from test_hash_ht where a = 30;
select count(b) from test_hash_ht where a = 30;
select * from test_range_pt where a = 30 order by 1,2,3,4;
select count(a) from test_range_pt where a = 30;
select count(b) from test_range_pt where a = 30;
select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 order by 1,2,3,4;
select count(t1.a) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5;
select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5 order by 1,2,3,4;
select count(t1.b) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5;

--bitmapscan + hashjoin
set enable_seqscan = off;
set enable_indexscan = off;
set enable_bitmapscan = on;
set enable_nestloop = off;
set enable_hashjoin = on;
set enable_mergejoin = off;

explain(costs off, verbose on) select * from test_hash_ht where a = 30 order by 1,2,3,4;
explain(costs off, verbose on) select count(a) from test_hash_ht where a = 30;
explain(costs off, verbose on) select count(b) from test_hash_ht where a = 30;
explain(costs off, verbose on) select * from test_range_pt where a = 30 order by 1,2,3,4;
explain(costs off, verbose on) select count(a) from test_range_pt where a = 30;
explain(costs off, verbose on) select count(b) from test_range_pt where a = 30;
explain(costs off, verbose on) select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 order by 1,2,3,4;
explain(costs off, verbose on) select count(t1.a) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5;
explain(costs off, verbose on) select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5 order by 1,2,3,4;
explain(costs off, verbose on) select count(t1.b) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5;
select * from test_hash_ht where a = 30 order by 1,2,3,4;
select count(a) from test_hash_ht where a = 30;
select count(b) from test_hash_ht where a = 30;
select * from test_range_pt where a = 30 order by 1,2,3,4;
select count(a) from test_range_pt where a = 30;
select count(b) from test_range_pt where a = 30;
select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 order by 1,2,3,4;
select count(t1.a) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5;
select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5 order by 1,2,3,4;
select count(t1.b) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5;

--bitmapscan + mergejoin
set enable_seqscan = off;
set enable_indexscan = off;
set enable_bitmapscan = on;
set enable_nestloop = off;
set enable_hashjoin = off;
set enable_mergejoin = on;

explain(costs off, verbose on) select * from test_hash_ht where a = 30 order by 1,2,3,4;
explain(costs off, verbose on) select count(a) from test_hash_ht where a = 30;
explain(costs off, verbose on) select count(b) from test_hash_ht where a = 30;
explain(costs off, verbose on) select * from test_range_pt where a = 30 order by 1,2,3,4;
explain(costs off, verbose on) select count(a) from test_range_pt where a = 30;
explain(costs off, verbose on) select count(b) from test_range_pt where a = 30;
explain(costs off, verbose on) select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 order by 1,2,3,4;
explain(costs off, verbose on) select count(t1.a) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5;
explain(costs off, verbose on) select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5 order by 1,2,3,4;
explain(costs off, verbose on) select count(t1.b) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5;
select * from test_hash_ht where a = 30 order by 1,2,3,4;
select count(a) from test_hash_ht where a = 30;
select count(b) from test_hash_ht where a = 30;
select * from test_range_pt where a = 30 order by 1,2,3,4;
select count(a) from test_range_pt where a = 30;
select count(b) from test_range_pt where a = 30;
select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 order by 1,2,3,4;
select count(t1.a) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5;
select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5 order by 1,2,3,4;
select count(t1.b) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5;

--seqscan + RowToVec
set enable_seqscan = on;
set enable_indexscan = off;
set enable_bitmapscan = off;
set enable_nestloop = on;
set enable_hashjoin = off;
set enable_mergejoin = off;
set try_vector_engine_strategy = force;

explain(costs off, verbose on) select * from test_hash_ht where a = 30 order by 1,2,3,4;
explain(costs off, verbose on) select count(a) from test_hash_ht where a = 30;
explain(costs off, verbose on) select count(b) from test_hash_ht where a = 30;
explain(costs off, verbose on) select * from test_range_pt where a = 30 order by 1,2,3,4;
explain(costs off, verbose on) select count(a) from test_range_pt where a = 30;
explain(costs off, verbose on) select count(b) from test_range_pt where a = 30;
explain(costs off, verbose on) select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 order by 1,2,3,4;
explain(costs off, verbose on) select count(t1.a) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5;
explain(costs off, verbose on) select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5 order by 1,2,3,4;
explain(costs off, verbose on) select count(t1.b) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5;
select * from test_hash_ht where a = 30 order by 1,2,3,4;
select count(a) from test_hash_ht where a = 30;
select count(b) from test_hash_ht where a = 30;
select * from test_range_pt where a = 30 order by 1,2,3,4;
select count(a) from test_range_pt where a = 30;
select count(b) from test_range_pt where a = 30;
select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 order by 1,2,3,4;
select count(t1.a) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5;
select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5 order by 1,2,3,4;
select count(t1.b) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5;

--indexscan/indexonlyscan + RowToVec
set enable_seqscan = off;
set enable_indexscan = on;
set enable_bitmapscan = off;
set enable_nestloop = on;
set enable_hashjoin = off;
set enable_mergejoin = off;
set try_vector_engine_strategy = force;

explain(costs off, verbose on) select * from test_hash_ht where a = 30 order by 1,2,3,4;
explain(costs off, verbose on) select count(a) from test_hash_ht where a = 30;
explain(costs off, verbose on) select count(b) from test_hash_ht where a = 30;
explain(costs off, verbose on) select * from test_range_pt where a = 30 order by 1,2,3,4;
explain(costs off, verbose on) select count(a) from test_range_pt where a = 30;
explain(costs off, verbose on) select count(b) from test_range_pt where a = 30;
explain(costs off, verbose on) select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 order by 1,2,3,4;
explain(costs off, verbose on) select count(t1.a) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5;
explain(costs off, verbose on) select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5 order by 1,2,3,4;
explain(costs off, verbose on) select count(t1.b) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5;
select * from test_hash_ht where a = 30 order by 1,2,3,4;
select count(a) from test_hash_ht where a = 30;
select count(b) from test_hash_ht where a = 30;
select * from test_range_pt where a = 30 order by 1,2,3,4;
select count(a) from test_range_pt where a = 30;
select count(b) from test_range_pt where a = 30;
select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 order by 1,2,3,4;
select count(t1.a) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5;
select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5 order by 1,2,3,4;
select count(t1.b) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5;

--bitmapscan + RowToVec
set enable_seqscan = off;
set enable_indexscan = off;
set enable_bitmapscan = on;
set enable_nestloop = on;
set enable_hashjoin = off;
set enable_mergejoin = off;
set try_vector_engine_strategy = force;

explain(costs off, verbose on) select * from test_hash_ht where a = 30 order by 1,2,3,4;
explain(costs off, verbose on) select count(a) from test_hash_ht where a = 30;
explain(costs off, verbose on) select count(b) from test_hash_ht where a = 30;
explain(costs off, verbose on) select * from test_range_pt where a = 30 order by 1,2,3,4;
explain(costs off, verbose on) select count(a) from test_range_pt where a = 30;
explain(costs off, verbose on) select count(b) from test_range_pt where a = 30;
explain(costs off, verbose on) select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 order by 1,2,3,4;
explain(costs off, verbose on) select count(t1.a) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5;
explain(costs off, verbose on) select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5 order by 1,2,3,4;
explain(costs off, verbose on) select count(t1.b) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5;
select * from test_hash_ht where a = 30 order by 1,2,3,4;
select count(a) from test_hash_ht where a = 30;
select count(b) from test_hash_ht where a = 30;
select * from test_range_pt where a = 30 order by 1,2,3,4;
select count(a) from test_range_pt where a = 30;
select count(b) from test_range_pt where a = 30;
select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 order by 1,2,3,4;
select count(t1.a) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5;
select * from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5 order by 1,2,3,4;
select count(t1.b) from test_hash_ht t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5;

--tidscan + nestloop
--base
set partition_iterator_elimination = off;
reset try_vector_engine_strategy;
select * from test_hash_ht where a = 30 and ctid = (select ctid from test_hash_ht where a = 30 order by b limit 1);
select * from (select * from test_hash_ht where a = 5 and ctid = (select ctid from test_hash_ht where a = 5 order by b limit 1)) t1 join (select * from test_range_pt where a = 5 and ctid = (select ctid from test_range_pt where a = 5 order by b limit 1)) t2 on t1.a = t2.a where t1.a = 5;
set partition_iterator_elimination = on;

set enable_seqscan = off;
set enable_indexscan = off;
set enable_bitmapscan = off;
set enable_nestloop = on;
set enable_hashjoin = off;
set enable_mergejoin = off;

explain(costs off, verbose on) select * from test_hash_ht where a = 30 and ctid = (select ctid from test_hash_ht where a = 30 limit 1);
explain(costs off, verbose on) select * from (select * from test_hash_ht where a = 5 and ctid = (select ctid from test_hash_ht where a = 5 limit 1)) t1 join (select * from test_range_pt where a = 5 and ctid = (select ctid from test_range_pt where a = 5 limit 1)) t2 on t1.a = t2.a where t1.a = 5;
select * from test_hash_ht where a = 30 and ctid = (select ctid from test_hash_ht where a = 30 order by b limit 1);
select * from (select * from test_hash_ht where a = 5 and ctid = (select ctid from test_hash_ht where a = 5 order by b limit 1)) t1 join (select * from test_range_pt where a = 5 and ctid = (select ctid from test_range_pt where a = 5 order by b limit 1)) t2 on t1.a = t2.a where t1.a = 5;

--tidscan + hashjoin
set enable_seqscan = off;
set enable_indexscan = off;
set enable_bitmapscan = off;
set enable_nestloop = off;
set enable_hashjoin = on;
set enable_mergejoin = off;

explain(costs off, verbose on) select * from test_hash_ht where a = 30 and ctid = (select ctid from test_hash_ht where a = 30 limit 1);
explain(costs off, verbose on) select * from (select * from test_hash_ht where a = 5 and ctid = (select ctid from test_hash_ht where a = 5 limit 1)) t1 join (select * from test_range_pt where a = 5 and ctid = (select ctid from test_range_pt where a = 5 limit 1)) t2 on t1.a = t2.a where t1.a = 5;
select * from test_hash_ht where a = 30 and ctid = (select ctid from test_hash_ht where a = 30 order by b limit 1);
select * from (select * from test_hash_ht where a = 5 and ctid = (select ctid from test_hash_ht where a = 5 order by b limit 1)) t1 join (select * from test_range_pt where a = 5 and ctid = (select ctid from test_range_pt where a = 5 order by b limit 1)) t2 on t1.a = t2.a where t1.a = 5;


--tidscan + mergejoin
set enable_seqscan = off;
set enable_indexscan = off;
set enable_bitmapscan = off;
set enable_nestloop = off;
set enable_hashjoin = off;
set enable_mergejoin = on;

explain(costs off, verbose on) select * from test_hash_ht where a = 30 and ctid = (select ctid from test_hash_ht where a = 30 limit 1);
explain(costs off, verbose on) select * from (select * from test_hash_ht where a = 5 and ctid = (select ctid from test_hash_ht where a = 5 limit 1)) t1 join (select * from test_range_pt where a = 5 and ctid = (select ctid from test_range_pt where a = 5 limit 1)) t2 on t1.a = t2.a where t1.a = 5;
select * from test_hash_ht where a = 30 and ctid = (select ctid from test_hash_ht where a = 30 limit 1);
select * from (select * from test_hash_ht where a = 5 and ctid = (select ctid from test_hash_ht where a = 5 limit 1)) t1 join (select * from test_range_pt where a = 5 and ctid = (select ctid from test_range_pt where a = 5 limit 1)) t2 on t1.a = t2.a where t1.a = 5;

DROP SCHEMA row_partition_iterator_elimination  CASCADE;


