/*
################################################################################
# TESTCASE NAME : skew_hint_02.py 
# COMPONENT(S)  : skew hint功能测试: 单表（基表）skew测试
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

--I2.基表 单列 列使用原名
--S1.单值
explain(verbose on, costs off) select count(*) from skew_t1 s, skew_t2 t2 where s.b  = t2.c;
explain(verbose on, costs off) select /*+ skew(s (b) (10)) */count(*) from skew_t1 s, skew_t2 t2 where s.b  = t2.c;

--S2.多值
explain(verbose on, costs off) select count(*) from skew_t1 s, skew_t2 t2 where s.b  = t2.c;
explain(verbose on, costs off) select /*+ skew(s (b) (10 20)) */count(*) from skew_t1 s, skew_t2 t2 where s.b  = t2.c;

--S3.列值缺省--agg
explain(verbose on, costs off) select count(c), count(a) from skew_t1 group by a;
explain(verbose on, costs off) select /*+ skew(skew_t1 (a) (1)) */ count(c), count(a) from skew_t1 group by a;

--S4.列值缺省--join：hint被使用但未优化
explain(verbose on, costs off) select count(*) from skew_t1 s, skew_t2 t2 where s.b  = t2.c;  
explain(verbose on, costs off) select /*+ skew(s (b)) */count(*) from skew_t1 s, skew_t2 t2 where s.b  = t2.c;  

--I3.基表 单列 列使用别名
--S1.单值
explain(verbose on, costs off) select s.b as sb from skew_t1 s, skew_t2 t2 where sb  = t2.c;
explain(verbose on, costs off) select /*+ skew(s (sb) (10)) */s.b as sb from skew_t1 s, skew_t2 t2 where sb  = t2.c;

--S2.多值
explain(verbose on, costs off) select s.b as sb from skew_t1 s, skew_t2 t2 where sb  = t2.c;
explain(verbose on, costs off) select /*+ skew(s (sb) (10 20)) */s.b as sb from skew_t1 s, skew_t2 t2 where sb  = t2.c;

--S3.值缺省--agg
explain(verbose on, costs off) select count(c), count(a), skew_t1.a as sa from skew_t1 group by sa;
explain(verbose on, costs off) select /*+ skew(skew_t1 (sa)) */ count(c), count(a), skew_t1.a as sa from skew_t1 group by sa;

--S4.列值缺省--join：hint被使用但提供的信息不足以指导优化
explain(verbose on, costs off) select s.b as sb from skew_t1 s, skew_t2 t2 where sb  = t2.c;
explain(verbose on, costs off) select /*+ skew(s (sb)) */s.b as sb from skew_t1 s, skew_t2 t2 where sb  = t2.c;

--I4.基表 多列 列为原名
--S1.单值
explain(verbose on, costs off) select count(distinct a) from skew_t2 group by a,b;
explain(verbose on, costs off) select /*+ skew(skew_t2 (a b) (1 1)) */ count(distinct a) from skew_t2 group by a,b;
--S2.多值
explain(verbose on, costs off) select /*+ skew(skew_t2 (a b) ((1 1) (2 2))) */ count(distinct a) from skew_t2 group by a,b;
--S3.值缺省
explain(verbose on, costs off) select /*+ skew(skew_t2 (a b)) */ count(distinct a) from skew_t2 group by a,b;

--I5.基表 多列 列为别名
--S1.单值
explain(verbose on, costs off) select count(distinct a), skew_t2.a as sa, skew_t2.b as sb from skew_t2 group by a,b;
explain(verbose on, costs off) select /*+ skew(skew_t2 (sa sb) (1 1)) */ count(distinct a), skew_t2.a as sa, skew_t2.b as sb from skew_t2 group by a,b;
--S2.多值
explain(verbose on, costs off) select /*+ skew(skew_t2 (sa sb) ((1 1) (2 2))) */ count(distinct a), skew_t2.a as sa, skew_t2.b as sb from skew_t2 group by a,b;
--S3.值缺省
explain(verbose on, costs off) select /*+ skew(skew_t2 (sa sb)) */ count(distinct a), skew_t2.a as sa, skew_t2.b as sb from skew_t2 group by a,b;

--I6.单列表达式
--S1.单列表达式的join优化
explain(verbose on, costs off) select sum(s.b) as sb from skew_t3 s, skew_t2 t2 where s.a = t2.a;
explain(verbose on, costs off) select /*+ skew(s (sb) (10))*/ sum(s.b) as sb from skew_t3 s, skew_t2 t2 where s.a = t2.a;

--S2.单列表达式的agg优化
explain(verbose on, costs off) select * from (select sum(s.b) as sb from skew_t3 s, skew_t2 t2 where s.a = t2.a group by s.b)tmp(ssum) group by ssum;
--外层agg优化
explain(verbose on, costs off) select /*+ skew(tmp (ssum))*/ * from (select sum(s.b) as sb from skew_t3 s, skew_t2 t2 where s.a = t2.a group by s.b)tmp(ssum) group by ssum;
--外层+内层agg优化
explain(verbose on, costs off) select /*+ skew(tmp (ssum))*/ * from (select /*+ skew(s (b))*/ sum(s.b) as sb from skew_t3 s, skew_t2 t2 where s.a = t2.a group by s.b)tmp(ssum) group by ssum;
--外层+内层agg+内层join优化
explain(verbose on, costs off) select /*+ skew(tmp (ssum))*/ * from (select /*+ skew(s (b)) skew(s (a) (12))*/ sum(s.b) as sb from skew_t3 s, skew_t2 t2 where s.a = t2.a group by s.b)tmp(ssum) group by ssum;

--S3.表达式列+非表达式列的多表join
explain(verbose on, costs off) select count(*) from skew_t1 t1, (select sum(s.b) as ssb, s.b as sb from skew_t3 s, skew_t2 t2 where s.a = t2.a group by s.b)tp(b) where t1.b = tp.b;
explain(verbose on, costs off) select /*+ skew(tp (b) (10))*/ count(*) from skew_t1 t1, (select /*+ skew((s t2) (sb)) */ sum(s.b) as ssb, s.b as sb from skew_t3 s, skew_t2 t2 where s.a = t2.a group by s.b)tp(b) where t1.b = tp.b;

--S4.表达式列+非表达式列的agg
explain(verbose on, costs off) select * from (select sum(s.b) from skew_t3 s group by s.b)tmp(a), skew_t2 t2 where tmp.a = t2.a;
explain(verbose on, costs off) select/*+ skew(tmp (a) (10))*/ * from (select /*+ skew(s (b) (10))*/sum(s.b) from skew_t3 s group by s.b)tmp(a), skew_t2 t2 where tmp.
a = t2.a;

--I7.多列表达式
--S1.多列表达式的join优化
explain(verbose on, costs off) select s.a + s.b as ssum from skew_t1 s, skew_t2 t2 where ssum  = t2.c;
explain(verbose on, costs off) select /*+ skew(s (ssum) (10)) */ s.a + s.b as ssum from skew_t1 s, skew_t2 t2 where ssum  = t2.c;

--S2.列表达式的agg优化
explain(verbose on, costs off) select s.a + s.b as ssum from skew_t1 s group by ssum;
explain(verbose on, costs off) select/*+ skew(s (ssum))*/ s.a + s.b as ssum from skew_t1 s group by ssum;

--S3.多列表达式内层提升
explain(verbose on, costs off) select * from (select s.a + s.b as ssum from skew_t1 s, skew_t2 t2 where ssum  = t2.c)tmp(a), skew_t3 t3 where tmp.a = t3.a;
--多列表达式在输出列中
explain(verbose on, costs off) select * from (select /*+ skew(s (ssum) (10)) */ s.a + s.b as ssum from skew_t1 s, skew_t2 t2 where ssum  = t2.c)tmp(a), skew_t3 t3 where tmp.a = t3.a;
--多列表达式不在输出列中
explain(verbose on, costs off) select t3.a from (select /*+ skew(s (ssum) (10)) */ s.a + s.b as ssum from skew_t1 s, skew_t2 t2 where ssum  = t2.c)tmp(a), skew_t3 t3 where tmp.a = t3.a;
--非表达式 内层提升
explain(verbose on, costs off) select count(*) from skew_t1 t1, (select /*+ skew(t2 (t2a) (10))*/ s.a as sa, s.b as sb, t2.a as t2a from skew_t3 s, skew_t2 t2 where s.c = t2.a)tp(a,b) where t1.b = tp.b;

--S4.多列表达式内层不提升
explain(verbose on, costs off) select * from (select (s.a + s.b) as ssum, count(s.c) from skew_t1 s, skew_t2 t2 where ssum  = t2.c group by ssum)tmp(a), skew_t3 t3 where tmp.a = t3.a;
explain(verbose on, costs off) select /*+ skew(t3 (a) (12)) */ * from (select /*+ skew(s (ssum) (10)) */ (s.a + s.b) as ssum, count(s.c) from skew_t1 s, skew_t2 t2 where ssum  = t2.c group by ssum)tmp(a), skew_t3 t3 where tmp.a = t3.a;
	
--S5.多列表达式外层
explain(verbose on, costs off) select tmp.a + tmp.b as stmp from (select s.a, s.c from skew_t1 s, skew_t2 t2 where s.b=t2.c)tmp(a,b), skew_t3 t3 where stmp = t3.a;
explain(verbose on, costs off) select /*+ skew(tmp (stmp) (12)) */  tmp.a + tmp.b as stmp from (select s.a, s.c from skew_t1 s, skew_t2 t2 where s.b  = t2.c)tmp(a,b)
, skew_t3 t3 where stmp = t3.a;

--I8.还原设置
--S1.还原query_dop
set query_dop = 2002;
