/*
################################################################################
# TCASE NAME : skew_agg_01.py 
# COMPONENT(S)  : hashagg倾斜优化功能测试: 新增的skew hint agg优化与其他影响agg计划因子的交叉测试 + agg下层outer join补空优化
# PREREQUISITE  : skew_setup.py
# PLATFORM      : all
# DESCRIPTION   : hashagg optimize base on skew hint
# TAG           : agg
# TC LEVEL      : Level 1
################################################################################
*/

--I1.设置guc参数
--S1.设置schema
set current_schema = skew_hint;
--S1.关闭sort agg
set enable_sort = off;
--S2.关闭query下推
--S3.设置计划格式
set explain_perf_mode = normal;
--S3.设置query_dop使得explain中倾斜优化生效
set query_dop = 1002;

--I1.only skew_hint
---------------------------------single column--------------------------------------------------------
--S1:single skew column +　distribute_key = skew column + before_plan = DN_REDISTRIBUTE_AGG
explain(verbose on, costs off) select count(c), count(a) from skew_t1 group by a;
explain(verbose on, costs off) select /*+ skew(skew_t1 (a) (1)) */ count(c), count(a) from skew_t1 group by a;

--S2:single skew column +　distribute_key != skew column + before_plan = DN_REDISTRIBUTE_AGG
explain(verbose on, costs off) select /*+ skew(skew_t1 (b) (1)) */ count(c), count(a) from skew_t1 group by a;

--S3:single skew column +　distribute_key = skew column + before_plan != DN_REDISTRIBUTE_AGG
analyze skew_t1;
explain(verbose on, costs off) select count(c), count(a) from skew_t1 group by a;
explain(verbose on, costs off) select /*+ skew(skew_t1 (a) (1)) */ count(c), count(a) from skew_t1 group by a;

--S4:single skew column +　distribute_key != skew column + before_plan != DN_REDISTRIBUTE_AGG
explain(verbose on, costs off) select /*+ skew(skew_t1 (b) (1)) */ count(c), count(a) from skew_t1 group by a;

---------------------------------mutiple columns--------------------------------------------------------
--S5:mutiple skew columns +　distribute_keys = skew columns + before_plan = DN_REDISTRIBUTE_AGG
explain(verbose on, costs off) select count(distinct a) from skew_t2 group by a,b;
explain(verbose on, costs off) select /*+ skew(skew_t2 (a b) (1 1)) */ count(distinct a) from skew_t2 group by a,b;
explain(verbose on, costs off) select /*+ skew(skew_t2 (a b c) (1 1 1)) */ count(distinct a) from skew_t2 group by a,b;

--S6:mutiple skew columns +　distribute_keys != skew columns + before_plan = DN_REDISTRIBUTE_AGG
explain(verbose on, costs off) select /*+ skew(skew_t2 (a c) (1 1)) */ count(distinct a) from skew_t2 group by a,b;
explain(verbose on, costs off) select /*+ skew(skew_t2 (b c) (1 1)) */ count(distinct a) from skew_t2 group by a,b;

--S7:mutiple skew columns +　distribute_keys = skew columns + before_plan != DN_REDISTRIBUTE_AGG
analyze skew_t2;
explain(verbose on, costs off) select count(distinct a) from skew_t2 group by a,b;
explain(verbose on, costs off) select /*+ skew(skew_t2 (a b) (1 1)) */ count(distinct a) from skew_t2 group by a,b;
explain(verbose on, costs off) select /*+ skew(skew_t2 (a b c) (1 1 1)) */ count(distinct a) from skew_t2 group by a,b;

--S8:mutiple skew columns +　distribute_keys != skew columns + before_plan != DN_REDISTRIBUTE_AGG
explain(verbose on, costs off) select /*+ skew(skew_t2 (a c) (1 1)) */ count(distinct a) from skew_t2 group by a,b;
explain(verbose on, costs off) select /*+ skew(skew_t2 (b c) (1 1)) */ count(distinct a) from skew_t2 group by a,b;

--I2:plan_mode_seed + skew_hint
set plan_mode_seed = 1000;

--S1: single skew column + distribute_key = skew column + before_plan = DN_REDISTRIBUTE_AGG
explain(verbose on, costs off) select count(c), count(a) from skew_t1 group by a;
explain(verbose on, costs off) select /*+ skew(skew_t1 (a) (1)) */ count(c), count(a) from skew_t1 group by a;

--S2: mutiple skew columns +　distribute_keys = skew columns + before_plan = DN_REDISTRIBUTE_AGG
explain(verbose on, costs off) select count(distinct a) from skew_t2 group by a,b;
explain(verbose on, costs off) select /*+ skew(skew_t2 (a b) (1 1)) */ count(distinct a) from skew_t2 group by a,b;

reset plan_mode_seed;

-- DN_REDISTRIBUTE_AGG
explain(verbose on, costs off) select count(c), count(a) from skew_t3 group by a;
-- DN_AGG_CN_AGG 
explain(verbose on, costs off) select count(c), count(a) from skew_t3 group by a;
-- DN_AGG_REDISTRIBUTE_AGG: choose the cheapest plan
explain(verbose on, costs off) select /*+ skew(skew_t3 (a) (1)) */ count(c), count(a) from skew_t3 group by a;

--  DN_AGG_REDISTRIBUTE_AGG
explain(verbose on, costs off) select count(c), count(a) from skew_t1 group by a;
-- DN_REDISTRIBUTE_AGG
explain(verbose on, costs off) select count(c), count(a) from skew_t1 group by a;
-- DN_AGG_REDISTRIBUTE_AGG (still choose the cheapest plan)
explain(verbose on, costs off) select /*+ skew(skew_t1 (a) (1)) */ count(c), count(a) from skew_t1 group by a;

-- DN_AGG_REDISTRIBUTE_AGG
explain(verbose on, costs off) select count(c), count(a) from skew_t1 group by a;
-- DN_AGG_REDISTRIBUTE_AGG
explain(verbose on, costs off) select count(c), count(a) from skew_t1 group by a;
-- DN_AGG_REDISTRIBUTE_AGG (still choose the cheapest plan)
explain(verbose on, costs off) select /*+ skew(skew_t1 (a) (1)) */ count(c), count(a) from skew_t1 group by a;
-- DN_AGG_REDISTRIBUTE_AGG, however hint does not work
explain(verbose on, costs off) select /*+ skew(skew_t1 (b) (1)) */ count(c), count(a) from skew_t1 group by a;

--I4: two_level_agg
-- DN_AGG_REDISTRIBUTE_AGG
explain(verbose on, costs off) select count(distinct a) from skew_t1 group by a,b;
-- DN_REDISTRIBUTE_AGG
explain(verbose on, costs off) select count(distinct a) from skew_t1 group by a,b;
-- DN_AGG_REDISTRIBUTE_AGG
explain(verbose on, costs off) select /*+ skew(skew_t1 (a b) (1 1)) */ count(distinct a) from skew_t1 group by a,b; 
-- DN_REDISTRIBUTE_AGG, because hint does not work
explain(verbose on, costs off) select /*+ skew(skew_t1 (a c) (1 1)) */ count(distinct a) from skew_t1 group by a,b;

-- DN_REDISTRIBUTE_AGG
explain(verbose on, costs off) select count(distinct a) from skew_t3 group by a,b;
-- DN_AGG_REDISTRIBUTE_AGG
explain(verbose on, costs off) select /*+ skew(skew_t3 (a b) (1 1)) */ count(distinct a) from skew_t3 group by a,b;
-- DN_REDISTRIBUTE_AGG, because hint does not work
explain(verbose on, costs off) select /*+ skew(skew_t3 (a c) (1 1)) */ count(distinct a) from skew_t3 group by a,b;

--I5: test for distribute key choose optimization
--S1: before optimization: DN_REDISTRIBUTE_AGG + diskey (c1, c2, c3)
explain(verbose on, costs off) select count(*) from skew_t5 group by c1, c2, c3, c4, c5, c5, c7 , c8, c9 limit 10;
--S2: only choose diskey + still DN_REDISTRIBUTE_AGG
explain(verbose on, costs off) select /*+ skew(t (c1 c2 c3 c4)) */count(*) from skew_t5 t group by c1, c2, c3, c4, c5, c5, c7 , c8, c9 limit 10;
--S3: choose diskey + choose plan: DN_AGG_REDISTRIBUTE_AGG
explain(verbose on, costs off) select /*+ skew(t (c1 c2 c3 c4 c5 c7 c8 c9)) */count(*) from skew_t5 t group by c1, c2, c3, c4, c5, c5, c7 , c8, c9 limit 10;
--S4: hint does not work
explain(verbose on, costs off) select /*+ skew(t (c1 c2)) */count(*) from skew_t5 t group by c1, c2, c3, c4, c5, c5, c7 , c8, c9 limit 10;  

--I6: unable to optimization because choosing local_distibute_keys
--S1: before optimization
explain(verbose on, costs off) select a,b,d,e,count(distinct f) from skew_t4 group by a,b,d,e;
--S2: hint does not work : choose the lowest cost diskeys
explain(verbose on, costs off) select /*+ skew(skew_t4 (a b d e f)) */ a,b,d,e,count(distinct f) from skew_t4 group by a,b,d,e;

--I7: test for agg null skew : left join
--S1: turn off skew_option
set skew_option = off;
--DN_REDISTRIBUTE_AGG
explain(verbose on, costs off) select count(agg_null_skew.a) from skew_t3 left outer join agg_null_skew on skew_t3.b  = agg_null_skew.a group by agg_null_skew.a;
explain(verbose on, costs off) select count(agg_null_skew.b) from skew_t3 left outer join agg_null_skew on skew_t3.b  = agg_null_skew.a group by agg_null_skew.a;

--S2: turn on skew_option to normal
--DN_AGG_REDISTRIBUTE_AGG 
set skew_option = normal;
explain(verbose on, costs off) select count(agg_null_skew.a) from skew_t3 left outer join agg_null_skew on skew_t3.b  = agg_null_skew.a group by agg_null_skew.a;
explain(verbose on, costs off) select count(agg_null_skew.b) from skew_t3 left outer join agg_null_skew on skew_t3.b  = agg_null_skew.a group by agg_null_skew.a;

--S3: turn on skew_option to lazy
set skew_option = lazy;
explain(verbose on, costs off) select count(agg_null_skew.a) from skew_t3 left outer join agg_null_skew on skew_t3.b  = agg_null_skew.a group by agg_null_skew.a;

--S4: optimize for agg null skew and join redis skew
explain(verbose on, costs off) select /*+ skew(skew_t3 (b) (12)) */ count(agg_null_skew.b) from skew_t3 left outer join agg_null_skew on skew_t3.b  = agg_null_skew.a group by agg_null_skew.a;

--S5.priority test
--still DN_REDISTRIBUTE_AGG
explain(verbose on, costs off) select count(agg_null_skew.a) from skew_t3 left outer join agg_null_skew on skew_t3.b  = agg_null_skew.a group by agg_null_skew.a;

--I8: right join
--S1: turn off skew_option
set skew_option = off;
--DN_REDISTRIBUTE_AGG
explain(verbose on, costs off) select count(skew_t3.a) from skew_t3 right outer join agg_null_skew on skew_t3.b  = agg_null_skew.a group by skew_t3.a;

--S2: turn on skew_option
set skew_option = normal;
explain(verbose on, costs off) select count(skew_t3.a) from skew_t3 right outer join agg_null_skew on skew_t3.b  = agg_null_skew.a group by skew_t3.a;

--I9: full join
--S1: turn off skew_option
set skew_option = off;
-- right side null
explain(verbose on, costs off) select count(agg_null_skew.a) from skew_t3 full outer join agg_null_skew on skew_t3.b  = agg_null_skew.a group by agg_null_skew.a;
-- left side null
explain(verbose on, costs off) select count(skew_t3.a) from skew_t3 right outer join agg_null_skew on skew_t3.b  = agg_null_skew.a group by skew_t3.a;

--S2: turn on skew_option
--DN_AGG_REDISTRIBUTE_AGG 
set skew_option = normal;
-- right side optimization
explain(verbose on, costs off) select count(agg_null_skew.a) from skew_t3 full outer join agg_null_skew on skew_t3.b  = agg_null_skew.a group by agg_null_skew.a;
-- left side optimization
explain(verbose on, costs off) select count(skew_t3.a) from skew_t3 right outer join agg_null_skew on skew_t3.b  = agg_null_skew.a group by skew_t3.a;

--I10.reset
set query_dop = 2002;
