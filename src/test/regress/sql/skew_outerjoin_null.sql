/*
################################################################################
# TESTCASE NAME : skew_outerjoin_null.py 
# COMPONENT(S)  : 测试由于outer join 补空导致的倾斜场景
# PREREQUISITE  : setup.py
# PLATFORM      : all
# DESCRIPTION   : skew caused by outer join
# TAG           : outer join, skew
# TC LEVEL      : Level 1
################################################################################
*/

--I1.设置guc参数
set current_schema to skew_join;
--S1.skew_option=normal
set skew_option=normal;
--S2.设置计划格式
set explain_perf_mode=normal;
--S3.设置计划生成参数
set enable_broadcast = off;
--S4.设置计划生成参数
set enable_mergejoin = off;
--S5.设置计划生成参数
set enable_nestloop = off;
set query_dop = 1002;

--I1.left outer join产生NULL
--S1.上层join为right join，执行计划，生成倾斜优化计划
explain select x, y, z, a, c from r left join s on r.a = s.b left join t on s.c = t.d order by 1, 2, 3, 4, 5 limit 1;
--S2.上层join为right join，执行查询
select x, y, z, a, c from r left join s on r.a = s.b left join t on s.c = t.d order by 1, 2, 3, 4, 5 limit 1;
--S3.上层join为right join，执行计划，不生成倾斜优化计划
explain select x, y, z, a, c from r left join s on r.a = s.b right join t on s.c = t.d order by 1, 2, 3, 4, 5 limit 1;
--S4.上层join为right join，执行查询
select x, y, z, a, c from r left join s on r.a = s.b right join t on s.c = t.d order by 1, 2, 3, 4, 5 limit 1;
--S5.上层join为full join，执行计划，生成倾斜优化计划
explain select x, y, z, a, c from r left join s on r.a = s.b full join t on s.c = t.d order by 1, 2, 3, 4, 5 limit 1;
--S6.上层join为full join，执行查询
select x, y, z, a, c from r left join s on r.a = s.b full join t on s.c = t.d order by 1, 2, 3, 4, 5 limit 1;

--I2.full outer join
--S1.上层join为right join，执行计划，生成倾斜优化计划
explain select x, y, z, a, c from r full join s on r.a = s.b left join t on s.c = t.d order by 1, 2, 3, 4, 5 limit 1;
--S2.上层join为right join，执行查询
select x, y, z, a, c from r full join s on r.a = s.b left join t on s.c = t.d order by 1, 2, 3, 4, 5 limit 1;
--S3.上层join为right join，执行计划，生成倾斜优化计划
explain select x, y, z, a, c from r full join s on r.a = s.b right join t on s.c = t.d order by 1, 2, 3, 4, 5 limit 1;
--S4.上层join为right join，执行查询
select x, y, z, a, c from r full join s on r.a = s.b right join t on s.c = t.d order by 1, 2, 3, 4, 5 limit 1;
--S5.上层join为full join，执行计划，生成倾斜优化计划
explain select x, y, z, a, c from r full join s on r.a = s.b full join t on s.c = t.d order by 1, 2, 3, 4, 5 limit 1;
--S6.上层join为full join，执行查询
select x, y, z, a, c from r full join s on r.a = s.b full join t on s.c = t.d order by 1, 2, 3, 4, 5 limit 1;

set query_dop = 2002;

