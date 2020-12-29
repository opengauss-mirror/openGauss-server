/*
################################################################################
# TCASE NAME : skew_hint_04.py 
# COMPONENT(S)  : CTE的skew hint功能测试：单CTE、CTE+基本、CTE+subquery
# PREREQUISITE  : skew_setup.py
# PLATFORM      : all
# DESCRIPTION   : multiple rels join optimize base on skew hint
# TAG           : CTE skew hint
# TC LEVEL      : Level 1
################################################################################
*/

--I1.设置guc参数
--S1.设置schema
set current_schema = skew_hint;
--S2.关闭sort agg
set enable_sort = off;
--S3.关闭query下推
--S4.设置计划格式
set explain_perf_mode = normal;
--S5.设置query_dop使得explain中倾斜优化生效
set query_dop = 1002;

--I2.CTE不提升，CTE生成时倾斜
explain(verbose on, costs off) 
with tmp(a,b) as
(
   select count(s.a) as sa, s.b as sb from skew_t3 s, skew_t2 t2 where s.a = t2.a group by sb
)
select count(*) from skew_t1 t1, tmp where t1.b = tmp.b;

--S1.CTE内部
explain(verbose on, costs off) 
with tmp(a,b) as
(
   select /*+ skew((s t2) (sb)) skew(s (a) (12))*/ count(s.a) as sa, s.b as sb from skew_t3 s, skew_t2 t2 where s.a = t2.a group by sb
)
select count(*) from skew_t1 t1, tmp where t1.b = tmp.b;

--S2.基表外部
explain(verbose on, costs off)
with tmp(a,b) as
(
   select count(s.a) as sa, s.b as sb from skew_t3 s, skew_t2 t2 where s.a = t2.a group by sb
)
select /*+ skew(t1 (b) (10))*/ count(*) from skew_t1 t1, tmp where t1.b = tmp.b;

--S3.多层:CTE内部+基表外部
explain(verbose on, costs off)
with tmp(a,b) as
(
   select /*+ skew((s t2) (sb)) skew(s (a) (12))*/ count(s.a) as sa, s.b as sb from skew_t3 s, skew_t2 t2 where s.a = t2.a group by sb
)
select /*+ skew(t1 (b) (10))*/ count(*) from skew_t1 t1, tmp where t1.b = tmp.b;

--I3.CTE不提升，CTE使用时倾斜
--S1.CTE join倾斜
explain(verbose on, costs off)
with tmp(a,b) as
(
  select s.a as sa, s.b as sb from skew_t3 s, skew_t2 t2 where s.a = t2.a group by sa, sb
)
select count(*) from skew_t1 t1, tmp where t1.b = tmp.b group by tmp.b;

explain(verbose on, costs off) 
with tmp(a,b) as
(
  select s.a as sa, s.b as sb from skew_t3 s, skew_t2 t2 where s.a = t2.a group by sa, sb
)
select/*+ skew(tmp (b) (12)) */ count(*) from skew_t1 t1, tmp where t1.b = tmp.b group by tmp.b;

--S2.CTE agg倾斜
explain(verbose on, costs off)
with tmp(a,b) as
(
  select s.a as sa, s.b as sb from skew_t3 s, skew_t2 t2 where s.a = t2.a group by sa, sb
)
select count(distinct a) from tmp group by tmp.b;

explain(verbose on, costs off)
with tmp(a,b) as
(
  select s.a as sa, s.b as sb from skew_t3 s, skew_t2 t2 where s.a = t2.a group by sa, sb
)
select /*+ skew(tmp (b) (12)) */ count(distinct a) from tmp group by tmp.b;

--I4.CTE提升
explain(verbose on, costs off)
with tmp(a,b) as
(
  select s.a as sa, s.b as sb from skew_t3 s, skew_t2 t2 where s.c = t2.a
)
select count(*) from skew_t1 t1, tmp where t1.b = tmp.b;

--S1.CTE生成时倾斜
explain(verbose on, costs off)
with tmp(a,b) as
(
  select/*+ skew(t2 (a) (12))*/ s.a as sa, s.b as sb from skew_t3 s, skew_t2 t2 where s.c = t2.a
)
select count(*) from skew_t1 t1, tmp where t1.b = tmp.b;

--S2.CTE使用时倾斜
explain(verbose on, costs off)
with tmp(a,b) as
(
  select s.a as sa, s.b as sb from skew_t3 s, skew_t2 t2 where s.c = t2.a
)
select /*+ skew(tmp (b) (12))*/ count(*) from skew_t1 t1, tmp where t1.b = tmp.b;

--S3.层:CTE内部+基表外部
explain(verbose on, costs off)
with tmp(a,b) as
(
  select/*+ skew(t2 (a) (12))*/ s.a as sa, s.b as sb from skew_t3 s, skew_t2 t2 where s.c = t2.a
)
select /*+ skew(tmp (b) (12))*/ count(*) from skew_t1 t1, tmp where t1.b = tmp.b;

--I5.CTE + subquery
--S1.CTE提升 +　subquery不提升
explain(verbose on, costs off)
with cte(a,b) as
(
  select s.a as sa, s.b as sb from skew_t3 s, skew_t2 t2 where s.c = t2.a
)
select count(*) from cte, (select sum(t1.a+t2.b) from skew_t1 t1, skew_t2 t2 group by t1.b, t2.a)tmp(b) where cte.b = tmp.b;

explain(verbose on, costs off)
with cte(a,b) as
(
  select /*+skew(t2 (a) (12))*/ s.a as sa, s.b as sb from skew_t3 s, skew_t2 t2 where s.c = t2.a
)
select count(*) from cte, (select /*+ skew((t1 t2) (t1b t2a) (12 12))*/ sum(t1.a+t2.b), t1.b as t1b, t2.a as t2a from skew_t1 t1, skew_t2 t2 group by t1.b, t2.a)tmp(b) where cte.b = tmp.b;

