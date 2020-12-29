/*
################################################################################
# TCASE NAME : skew_agg_02.py 
# COMPONENT(S)  : hashagg倾斜优化功能测试: agg 类型测试
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

--I2.count(distinct)
--S1.单个count(distinct)
explain(verbose on, costs off) select count(distinct a) from skew_t1;
explain(verbose on, costs off) select /*+ skew(skew_t1 (a))*/ count(distinct a) from skew_t1;
--S2.多个count(distinct)
explain(verbose on, costs off) select count(distinct a), count(distinct b) from skew_t1;
explain(verbose on, costs off) select /*+ skew(skew_t1 (a b))*/ count(distinct a), count(distinct b) from skew_t1;
--warning: hint unused.
explain(verbose on, costs off) select /*+ skew(skew_t1 (a)) skew(skew_t1 (b))*/ count(distinct a), count(distinct b) from skew_t1;
--S3.单个count(distinct) + group by 单列
explain(verbose on, costs off) select count(distinct a) from skew_t1 group by b;
explain(verbose on, costs off) select /*+ skew(skew_t1 (b))*/ count(distinct a) from skew_t1 group by b;
--S4.单个count(distinct) + group by 多列
explain(verbose on, costs off) select count(distinct a) from skew_t1 group by a,b;
explain(verbose on, costs off) select /*+ skew(skew_t1 (a b))*/count(distinct a) from skew_t1 group by a,b;
--S5.多个count(distinct) + group by 单列
explain(verbose on, costs off) select count(distinct a), count(distinct b) from skew_t1 group by b;
explain(verbose on, costs off) select /*+ skew(skew_t1 (b))*/ count(distinct a), count(distinct b) from skew_t1  group by b;
--S6.多个count(distinct) + group by 多列
explain(verbose on, costs off) select count(distinct a), count(distinct b) from skew_t1 group by a, b;
explain(verbose on, costs off) select /*+ skew(skew_t1 (a b))*/ count(distinct a), count(distinct b) from skew_t1 group by a, b;

--I3.多个count(distinct) + join
explain(verbose on, costs off) select count(distinct t1.a), count(distinct t1.b) from skew_t1 t1, skew_t2 t2 where t1.a = t2.c;
--S1.Skew Join Optimization 
explain(verbose on, costs off) select /*+ skew(t1 (a) (12)) */ count(distinct t1.a), count(distinct t1.b) from skew_t1 t1, skew_t2 t2 where t1.a = t2.c;
--S2.count(distinct) Optimization
-- a is ambiguous
explain(verbose on, costs off) select /*+ skew((t1 t2) (a) (12)) */ count(distinct t1.a), count(distinct t1.b) from skew_t1 t1, skew_t2 t2 where t1.a = t2.c;
-- using subquery
explain(verbose on, costs off) select t1.a, t1.b from skew_t1 t1, skew_t2 t2 where t1.a = t2.c;
explain(verbose on, costs off) select count(distinct tp.a), count(distinct tp.b) from (select t1.a, t1.b from skew_t1 t1, skew_t2 t2 where t1.a = t2.c)tp(a,b);
-- one of subquery will output warning:unused hint: Skew(tp (b) (12))      
explain(verbose on, costs off) select /*+ skew(tp (b) (12)) */ count(distinct tp.a), count(distinct tp.b) from (select t1.a, t1.b from skew_t1 t1, skew_t2 t2 where t1.a = t2.c)tp(a,b);
--S3.both Optimization
explain(verbose on, costs off) select count(distinct tp.a), count(distinct tp.b) from (select t1.a, t1.b from skew_t1 t1, skew_t2 t2 where t1.a = t2.c)tp(a,b);      
explain(verbose on, costs off) select /*+ skew(tp (b) (12)) skew(t1 (a) (12))*/ count(distinct tp.a), count(distinct tp.b) from (select t1.a, t1.b from skew_t1 t1, skew_t2 t2 where t1.a = t2.c)tp(a,b);
explain(verbose on, costs off) select /*+ skew(tp (a b) (12)) skew(t1 (a) (12))*/ count(distinct tp.a), count(distinct tp.b) from (select t1.a, t1.b from skew_t1 t1, skew_t2 t2 where t1.a = t2.c)tp(a,b);

--I4.多个count(distinct) + join + group by
--S1.优化前
explain(verbose on, costs off) select count(distinct tp.a), count(distinct tp.b) from (select t1.a, t1.b , t1.c from skew_t1 t1, skew_t2 t2 where t1.a = t2.c)tp(a,b,c) group by tp.c;
--S2.优化后
explain(verbose on, costs off) select /*+ skew(tp (c) (12)) skew(t1 (a) (12))*/ count(distinct tp.a), count(distinct tp.b) from (select t1.a, t1.b , t1.c from skew_t1 t1, skew_t2 t2 where t1.a = t2.c)tp(a,b,c) group by tp.c;

--I5.count:对aggregate不进行优化，仅优化hashagg
--S1.多count + group by单列 
explain(verbose on, costs off) select count(a), count(b) from skew_t1 group by a;
explain(verbose on, costs off) select /*+ skew(skew_t1 (a))*/ count(a), count(b) from skew_t1 group by a;
--S2.多count + group by多列
explain(verbose on, costs off) select count(a), count(c) from skew_t1 group by a,b;
explain(verbose on, costs off) select /*+ skew(skew_t1 (a b))*/count(a), count(c) from skew_t1 group by a,b;
--S3.多count + join : join 优化
explain(verbose on, costs off) select count(t1.a), count(t1.b) from skew_t1 t1, skew_t2 t2 where t1.a = t2.c;
explain(verbose on, costs off) select /*+ skew(t1 (a) (12))*/count(t1.a), count(t1.b) from skew_t1 t1, skew_t2 t2 where t1.a = t2.c;
--S4.count + group by + join : join 优化
explain(verbose on, costs off) select count(t1.a), count(t1.b) from skew_t1 t1, skew_t2 t2 where t1.a = t2.c group by t2.a;
explain(verbose on, costs off) select /*+ skew(t1 (a) (12))*/ count(t1.a), count(t1.b) from skew_t1 t1, skew_t2 t2 where t1.a = t2.c group by t2.a;
--S5.count + group by + join : hashagg优化
--直接使用/*+ skew((t1 t2) (a))*/会出现："a" in skew hint is ambiguous.需要改写query
--改写前
select count(t1.a), count(t1.b) from skew_t1 t1, skew_t2 t2 where t1.a = t2.c group by t2.a order by 1;
explain(verbose on, costs off) select count(t1.a), count(t1.b) from skew_t1 t1, skew_t2 t2 where t1.a = t2.c group by t2.a;
--改写后
select count(aa), count(bb) from ( select t1.a, t1.b, t2.a from skew_t1 t1, skew_t2 t2 where t1.a = t2.c)tmp(aa, bb, cc) group by cc order by 1;
explain(verbose on, costs off) select count(aa), count(bb) from ( select t1.a, t1.b, t2.a from skew_t1 t1, skew_t2 t2 where t1.a = t2.c)tmp(aa, bb, cc) group by cc;
--优化
explain(verbose on, costs off) select /*+ skew(tmp (cc)) */ count(aa), count(bb) from ( select t1.a, t1.b, t2.a from skew_t1 t1, skew_t2 t2 where t1.a = t2.c)tmp(aa, bb, cc) group by cc;
--S6.count + group by + join : hashagg和join优化
--结果
select /*+ skew(tmp (cc))  skew(t1 (a) (12))*/ count(aa), count(bb) from ( select t1.a, t1.b, t2.a from skew_t1 t1, skew_t2 t2 where t1.a = t2.c)tmp(aa, bb, cc) group by cc order by 1;
--S2.计划
explain(verbose on, costs off) select /*+ skew(tmp (cc))  skew(t1 (a) (12))*/ count(aa), count(bb) from ( select t1.a, t1.b, t2.a from skew_t1 t1, skew_t2 t2 where t1.a = t2.c)tmp(aa, bb, cc) group by cc;

--I6.sum +　group by多列
--S1.优化前
select sum(a), sum(c) from skew_t1 group by a,b order by 1;
explain(verbose on, costs off) select sum(a), sum(c) from skew_t1 group by a,b;
--S2.优化后
select /*+ skew(skew_t1 (a b))*/ sum(a), sum(c) from skew_t1 group by a,b order by 1;
explain(verbose on, costs off) select /*+ skew(skew_t1 (a b))*/ sum(a), sum(c) from skew_t1 group by a,b;

--I7.max/min + group by多列
--S1.max优化前
select max(a), max(c) from skew_t1 group by a,b order by 1;
explain(verbose on, costs off) select max(a), max(c) from skew_t1 group by a,b;
--S2.max优化后
select /*+ skew(skew_t1 (a b))*/ max(a), max(c) from skew_t1 group by a,b order by 1;
explain(verbose on, costs off) select /*+ skew(skew_t1 (a b))*/ max(a), max(c) from skew_t1 group by a,b;
--S3.min优化前
explain(verbose on, costs off) select min(a), min(c) from skew_t1 group by a,b;
--S4.min优化后
explain(verbose on, costs off) select /*+ skew(skew_t1 (a b))*/ min(a), min(c) from skew_t1 group by a,b;

--I8.不支持的agg场景：force_slvl_agg=true
--S1.rray_agg
--优化前：DN_REDISTRIBUTE_AGG
explain(verbose on, costs off) select array_agg(a) from skew_t1 group by a,b;
--优化后:没有变化,因为hint无用
explain(verbose on, costs off) select /*+ skew(skew_t1 (a b))*/ array_agg(a) from skew_t1 group by a,b;

--S2.string_agg
--优化前：DN_REDISTRIBUTE_AGG
explain(verbose on, costs off) select string_agg(a, ',') from skew_t1 group by a,b; 
--优化后:没有变化，因为hint无用
explain(verbose on, costs off) select /*+ skew(skew_t1 (a b))*/ string_agg(a, ',') from skew_t1 group by a,b; 

--S3.subplan in agg qual
create table agg_qual(a int, b varchar(10));
--分布键为常量无法hint
explain (verbose on, costs off) select 1,b in (select 'g' from agg_qual group by 1) from agg_qual group by 1, 2 having b in (select 'g' from agg_qual group by 1);

--I9.还原设置
--S1.还原query_dop
set query_dop = 2002;
