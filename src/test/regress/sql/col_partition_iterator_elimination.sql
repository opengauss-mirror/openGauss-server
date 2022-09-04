DROP SCHEMA col_partition_iterator_elimination CASCADE;
CREATE SCHEMA col_partition_iterator_elimination;
SET CURRENT_SCHEMA TO col_partition_iterator_elimination;

drop table test_range_pt;
create table test_range_pt (a int, b int, c int, d int) WITH (ORIENTATION = COLUMN)
partition by range(a)
(
	partition p1 values less than (20),
	partition p2 values less than (30),
	partition p3 values less than (40),
	partition p4 values less than (50)
)ENABLE ROW MOVEMENT;

create index idx_range_local on test_range_pt(a) local;
insert into test_range_pt values(generate_series(0,49), generate_series(1,100));

drop table test_range_pt1;
create table test_range_pt1 (a int, b int, c int, d int) WITH (ORIENTATION = COLUMN)
partition by range(a)
(
	partition p1 values less than (20),
	partition p2 values less than (30),
	partition p3 values less than (40),
	partition p4 values less than (50)
)ENABLE ROW MOVEMENT;

create index idx_range_local1 on test_range_pt1(a) local;
insert into test_range_pt1 values(generate_series(0,49), generate_series(1,100));

--base 验证数据结果
select * from test_range_pt where a = 30 order by 1,2,3,4;
select count(a) from test_range_pt where a = 30;
select count(b) from test_range_pt where a = 30;
select * from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 order by 1,2,3,4;
select count(t1.a) from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5;
select * from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5 order by 1,2,3,4;
select count(t1.b) from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5;

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

explain(costs off, verbose on) select * from test_range_pt where a = 30 order by 1,2,3,4;
explain(costs off, verbose on) select count(a) from test_range_pt where a = 30;
explain(costs off, verbose on) select count(b) from test_range_pt where a = 30;
explain(costs off, verbose on) select * from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 order by 1,2,3,4;
explain(costs off, verbose on) select count(t1.a) from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5;
explain(costs off, verbose on) select * from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5 order by 1,2,3,4;
explain(costs off, verbose on) select count(t1.b) from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5;
select * from test_range_pt where a = 30 order by 1,2,3,4;
select count(a) from test_range_pt where a = 30;
select count(b) from test_range_pt where a = 30;
select * from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 order by 1,2,3,4;
select count(t1.a) from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5;
select * from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5 order by 1,2,3,4;
select count(t1.b) from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5;

--seqscan + hashjoin
set enable_seqscan = on;
set enable_indexscan = off;
set enable_bitmapscan = off;
set enable_nestloop = off;
set enable_hashjoin = on;
set enable_mergejoin = off;

explain(costs off, verbose on) select * from test_range_pt where a = 30 order by 1,2,3,4;
explain(costs off, verbose on) select count(a) from test_range_pt where a = 30;
explain(costs off, verbose on) select count(b) from test_range_pt where a = 30;
explain(costs off, verbose on) select * from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 order by 1,2,3,4;
explain(costs off, verbose on) select count(t1.a) from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5;
explain(costs off, verbose on) select * from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5 order by 1,2,3,4;
explain(costs off, verbose on) select count(t1.b) from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5;
select * from test_range_pt where a = 30 order by 1,2,3,4;
select count(a) from test_range_pt where a = 30;
select count(b) from test_range_pt where a = 30;
select * from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 order by 1,2,3,4;
select count(t1.a) from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5;
select * from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5 order by 1,2,3,4;
select count(t1.b) from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5;

--seqscan + mergejoin
set enable_seqscan = on;
set enable_indexscan = off;
set enable_bitmapscan = off;
set enable_nestloop = off;
set enable_hashjoin = off;
set enable_mergejoin = on;

explain(costs off, verbose on) select * from test_range_pt where a = 30 order by 1,2,3,4;
explain(costs off, verbose on) select count(a) from test_range_pt where a = 30;
explain(costs off, verbose on) select count(b) from test_range_pt where a = 30;
explain(costs off, verbose on) select * from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 order by 1,2,3,4;
explain(costs off, verbose on) select count(t1.a) from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5;
explain(costs off, verbose on) select * from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5 order by 1,2,3,4;
explain(costs off, verbose on) select count(t1.b) from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5;
select * from test_range_pt where a = 30 order by 1,2,3,4;
select count(a) from test_range_pt where a = 30;
select count(b) from test_range_pt where a = 30;
select * from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 order by 1,2,3,4;
select count(t1.a) from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5;
select * from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5 order by 1,2,3,4;
select count(t1.b) from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5;

--indexscan/indexonlyscan + nestloop
set enable_seqscan = off;
set enable_indexscan = on;
set enable_bitmapscan = off;
set enable_nestloop = on;
set enable_hashjoin = off;
set enable_mergejoin = off;

explain(costs off, verbose on) select * from test_range_pt where a = 30 order by 1,2,3,4;
explain(costs off, verbose on) select count(a) from test_range_pt where a = 30;
explain(costs off, verbose on) select count(b) from test_range_pt where a = 30;
explain(costs off, verbose on) select * from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 order by 1,2,3,4;
explain(costs off, verbose on) select count(t1.a) from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5;
explain(costs off, verbose on) select * from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5 order by 1,2,3,4;
explain(costs off, verbose on) select count(t1.b) from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5;
select * from test_range_pt where a = 30 order by 1,2,3,4;
select count(a) from test_range_pt where a = 30;
select count(b) from test_range_pt where a = 30;
select * from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 order by 1,2,3,4;
select count(t1.a) from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5;
select * from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5 order by 1,2,3,4;
select count(t1.b) from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5;

--indexscan/indexonlyscan + hashjoin
set enable_seqscan = off;
set enable_indexscan = on;
set enable_bitmapscan = off;
set enable_nestloop = off;
set enable_hashjoin = on;
set enable_mergejoin = off;

explain(costs off, verbose on) select * from test_range_pt where a = 30 order by 1,2,3,4;
explain(costs off, verbose on) select count(a) from test_range_pt where a = 30;
explain(costs off, verbose on) select count(b) from test_range_pt where a = 30;
explain(costs off, verbose on) select * from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 order by 1,2,3,4;
explain(costs off, verbose on) select count(t1.a) from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5;
explain(costs off, verbose on) select * from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5 order by 1,2,3,4;
explain(costs off, verbose on) select count(t1.b) from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5;
select * from test_range_pt where a = 30 order by 1,2,3,4;
select count(a) from test_range_pt where a = 30;
select count(b) from test_range_pt where a = 30;
select * from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 order by 1,2,3,4;
select count(t1.a) from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5;
select * from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5 order by 1,2,3,4;
select count(t1.b) from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5;

--indexscan/indexonlyscan + mergejoin
set enable_seqscan = off;
set enable_indexscan = on;
set enable_bitmapscan = off;
set enable_nestloop = off;
set enable_hashjoin = off;
set enable_mergejoin = on;

explain(costs off, verbose on) select * from test_range_pt where a = 30 order by 1,2,3,4;
explain(costs off, verbose on) select count(a) from test_range_pt where a = 30;
explain(costs off, verbose on) select count(b) from test_range_pt where a = 30;
explain(costs off, verbose on) select * from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 order by 1,2,3,4;
explain(costs off, verbose on) select count(t1.a) from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5;
explain(costs off, verbose on) select * from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5 order by 1,2,3,4;
explain(costs off, verbose on) select count(t1.b) from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5;
select * from test_range_pt where a = 30 order by 1,2,3,4;
select count(a) from test_range_pt where a = 30;
select count(b) from test_range_pt where a = 30;
select * from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 order by 1,2,3,4;
select count(t1.a) from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5;
select * from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5 order by 1,2,3,4;
select count(t1.b) from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5;

--bitmapscan + nestloop
set enable_seqscan = off;
set enable_indexscan = off;
set enable_bitmapscan = on;
set enable_nestloop = on;
set enable_hashjoin = off;
set enable_mergejoin = off;

explain(costs off, verbose on) select * from test_range_pt where a = 30 order by 1,2,3,4;
explain(costs off, verbose on) select count(a) from test_range_pt where a = 30;
explain(costs off, verbose on) select count(b) from test_range_pt where a = 30;
explain(costs off, verbose on) select * from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 order by 1,2,3,4;
explain(costs off, verbose on) select count(t1.a) from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5;
explain(costs off, verbose on) select * from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5 order by 1,2,3,4;
explain(costs off, verbose on) select count(t1.b) from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5;
select * from test_range_pt where a = 30 order by 1,2,3,4;
select count(a) from test_range_pt where a = 30;
select count(b) from test_range_pt where a = 30;
select * from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 order by 1,2,3,4;
select count(t1.a) from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5;
select * from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5 order by 1,2,3,4;
select count(t1.b) from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5;

--bitmapscan + hashjoin
set enable_seqscan = off;
set enable_indexscan = off;
set enable_bitmapscan = on;
set enable_nestloop = off;
set enable_hashjoin = on;
set enable_mergejoin = off;

explain(costs off, verbose on) select * from test_range_pt where a = 30 order by 1,2,3,4;
explain(costs off, verbose on) select count(a) from test_range_pt where a = 30;
explain(costs off, verbose on) select count(b) from test_range_pt where a = 30;
explain(costs off, verbose on) select * from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 order by 1,2,3,4;
explain(costs off, verbose on) select count(t1.a) from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5;
explain(costs off, verbose on) select * from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5 order by 1,2,3,4;
explain(costs off, verbose on) select count(t1.b) from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5;
select * from test_range_pt where a = 30 order by 1,2,3,4;
select count(a) from test_range_pt where a = 30;
select count(b) from test_range_pt where a = 30;
select * from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 order by 1,2,3,4;
select count(t1.a) from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5;
select * from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5 order by 1,2,3,4;
select count(t1.b) from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5;

--bitmapscan + mergejoin
set enable_seqscan = off;
set enable_indexscan = off;
set enable_bitmapscan = on;
set enable_nestloop = off;
set enable_hashjoin = off;
set enable_mergejoin = on;

explain(costs off, verbose on) select * from test_range_pt where a = 30 order by 1,2,3,4;
explain(costs off, verbose on) select count(a) from test_range_pt where a = 30;
explain(costs off, verbose on) select count(b) from test_range_pt where a = 30;
explain(costs off, verbose on) select * from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 order by 1,2,3,4;
explain(costs off, verbose on) select count(t1.a) from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5;
explain(costs off, verbose on) select * from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5 order by 1,2,3,4;
explain(costs off, verbose on) select count(t1.b) from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5;
select * from test_range_pt where a = 30 order by 1,2,3,4;
select count(a) from test_range_pt where a = 30;
select count(b) from test_range_pt where a = 30;
select * from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 order by 1,2,3,4;
select count(t1.a) from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5;
select * from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5 order by 1,2,3,4;
select count(t1.b) from test_range_pt1 t1 join test_range_pt t2 on t1.b = t2.b where t1.a = 5 and t2.a = 5;

DROP SCHEMA col_partition_iterator_elimination  CASCADE;



