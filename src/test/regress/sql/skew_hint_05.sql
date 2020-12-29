/*
################################################################################
# TESTCASE NAME : skew_hint_05.py 
# COMPONENT(S)  : skew hint功能测试: 多层hint测试
# PREREQUISITE  : skew_setup.py
# PLATFORM      : all
# DESCRIPTION   : skew hint
# TAG           : hint
# TC LEVEL      : Level 1
################################################################################
*/

--I1.设置guc参数
--S1.设置schema
set current_schema = skew_hint;
--S2.关闭sort agg
set enable_sort = off;
set enable_nestloop = off;
--S3.关闭query下推
--S4.关闭enable_broadcast
set enable_broadcast = off;
--S5.设置计划格式
set explain_perf_mode=normal;
--S6.设置query_dop使得explain中倾斜优化生效
set query_dop = 1002;
--S7.设置倾斜优化策略
set skew_option=normal;

--I2.subquery join base rel
--S1.subquery can not pull up
explain(verbose on, costs off) select count(*) from skew_t1 t1, (select count(s.a) as sa, s.b as sb from skew_t3 s, skew_t2 t2 where s.a = t2.a group by sb)tp(a,b)
where t1.b = tp.b;

explain(verbose on, costs off) select/*+ skew(t1 (b) (10))*/ count(*) from skew_t1 t1, (select /*+ skew((s t2) (sb))*/ count(s.a) as sa, s.b as sb from skew_t3 s, skew_t2 t2 where s.a = t2.a group by sb)tp(a,b) where t1.b = tp.b;

--S2.subquery can not pull up + mutiple hint 
explain(verbose on, costs off) select/*+ skew(t1 (b) (10)) skew((t1 tp) (a))*/ count(*) from skew_t1 t1, (select /*+ skew((s t2) (sb))*/ count(s.a) as sa, s.b as sb
from skew_t3 s, skew_t2 t2 where s.a = t2.a group by sb)tp(aa,b) where t1.b = tp.aa group by t1.a;

--S3.subquery can pull up
explain(verbose on, costs off) select count(*) from skew_t1 t1, (select count(s.a) as sa from skew_t3 s, skew_t2 t2 where s.a = t2.a)tp(aa) where t1.b = tp.aa group
by t1.a;

explain(verbose on, costs off) select/*+ skew((t1 tp) (a))*/ count(*) from skew_t1 t1, (select count(s.a) as sa from skew_t3 s, skew_t2 t2 where s.a = t2.a)tp(aa) where t1.b = tp.aa group by t1.a;

--S4.subquery can pull up + mutiple hint
explain(verbose on, costs off) select count(*) from skew_t1 t1, (select s.a as sa, s.b as sb from skew_t3 s, skew_t2 t2 where s.a = t2.a)tp(a,b) where t1.b = tp.a group by t1.a;

explain(verbose on, costs off) select/*+ skew((t1 tp) (a))*/ count(*) from skew_t1 t1, (select /*+ skew((s t2) (sa))*/ s.a as sa, s.b as sb from skew_t3 s, skew_t2 t2 where s.a = t2.a)tp(aa,b) where t1.b = tp.aa group by t1.a;

--I3.reset
set query_dop = 2002;
