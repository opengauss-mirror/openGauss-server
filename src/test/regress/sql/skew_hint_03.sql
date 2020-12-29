/*
################################################################################
# TCASE NAME : skew_hint_03.py 
# COMPONENT(S)  : 多表join的中间结果skew功能测试: 子查询与基表join的中间结果倾斜场景下基于skew hint的agg优化
# PREREQUISITE  : skew_setup.py
# PLATFORM      : all
# DESCRIPTION   : multiple rels join optimize base on skew hint
# TAG           : subquery skew hint
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

--I2.skew hint in subquery + subquery can not pull up
--S1.DN_REDISTRIBUTE_AGG
 explain(verbose on, costs off) select count(*) from skew_t1 t1, (select count(s.a) as sa, s.b as sb from skew_t3 s, skew_t2 t2 where s.a = t2.a group by sb)tp(a,b) where t1.b = tp.b;
--S2.DN_AGG_REDISTRIBUTE_AGG
 explain(verbose on, costs off) select count(*) from skew_t1 t1, (select /*+ skew((s t2) (sb))*/ count(s.a) as sa, s.b as sb from skew_t3 s, skew_t2 t2 where s.a = t2.a group by sb)tp(a,b) where t1.b = tp.b;
--S3.two level skew hint
 explain(verbose on, costs off) select/*+ skew(t1 (b) (10))*/ count(*) from skew_t1 t1, (select /*+ skew((s t2) (sb))*/ count(s.a) as sa, s.b as sb from skew_t3 s, skew_t2 t2 where s.a = t2.a group by sb)tp(a,b) where t1.b = tp.b;

--I3.skew hint in subquery + subquery can pull up
--S1.base rel
explain(verbose on, costs off) select s.a as sa, s.b as sb from skew_t3 s, skew_t2 t2 where s.c = t2.a;
explain(verbose on, costs off) select /*+ skew(t2 (t2a) (10))*/  s.a as sa, s.b as sb,  t2.a as t2a from skew_t3 s, skew_t2 t2 where s.c = t2.a;
--S2.base rel and subquery join skew
explain(verbose on, costs off) select count(*) from skew_t1 t1, (select s.a as sa, s.b as sb from skew_t3 s, skew_t2 t2 where s.c = t2.a)tp(a,b) where t1.b = tp.b;
--S3.skew hint in subquery
explain(verbose on, costs off) select count(*) from skew_t1 t1, (select /*+ skew(t2 (t2a) (10))*/ s.a as sa, s.b as sb, t2.a as t2a from skew_t3 s, skew_t2 t2 where
s.c = t2.a)tp(a,b) where t1.b = tp.b;
--S4.two level skew hint
explain(verbose on, costs off) select/*+ skew(t1 (b) (10))*/ count(*) from skew_t1 t1, (select /*+ skew(t2 (t2a) (10))*/ s.a as sa, s.b as sb, t2.a as t2a from skew_t3 s, skew_t2 t2 where s.c = t2.a)tp(a,b) where t1.b = tp.b;

--I4.skew hint out subquery + subquery can not pull up
--S1.column b is ambigous
explain(verbose on, costs off) select/*+ skew((tp t1) (b)) */ count(*) from skew_t1 t1, (select s.a as sa, s.b as sb from skew_t3 s, skew_t2 t2 where s.a = t2.a group by sa, sb)tp(a,b) where t1.b = tp.b group by tp.b;
--S2.base rel column in tgl
--DN_REDISTRIBUTE_AGG
explain(verbose on, costs off) select t1.a as t1a from skew_t1 t1, (select s.a as sa, s.b as sb from skew_t3 s, skew_t2 t2 where s.a = t2.a group by sa, sb)tp(a,b)
where t1.b = tp.a group by t1a;
--DN_AGG_REDISTRIBUTE_AGG
explain(verbose on, costs off) select/*+ skew((t1 tp) (t1a))*/ t1.a as t1a from skew_t1 t1, (select s.a as sa, s.b as sb from skew_t3 s, skew_t2 t2 where s.a = t2.a group by sa, sb)tp(a,b) where t1.b = tp.a group by t1a;
--S3.base rel column not in tgl
-- DN_REDISTRIBUTE_AGG
explain(verbose on, costs off) select count(*) from skew_t1 t1, (select s.a as sa, s.b as sb from skew_t3 s, skew_t2 t2 where s.a = t2.a group by sa, sb)tp(a,b) where t1.b = tp.a group by t1.a;
-- a is ambiguous + DN_AGG_REDISTRIBUTE_AGG
explain(verbose on, costs off) select/*+ skew((t1 tp) (a))*/ count(*) from skew_t1 t1, (select /*+ skew(s (sa))*/ s.a as sa, s.b as sb from skew_t3 s, skew_t2 t2 where s.a = t2.a group by sa, sb)tp(a,b) where t1.b = tp.a group by t1.a;
-- DN_AGG_CN_AGG + DN_AGG_REDISTRIBUTE_AGG
explain(verbose on, costs off) select/*+ skew((t1 tp) (a))*/ count(*) from skew_t1 t1, (select /*+ skew(s (sa))*/ s.a as sa, s.b as sb from skew_t3 s, skew_t2 t2 where s.a = t2.a group by sa, sb)tp(aa,b) where t1.b = tp.aa group by t1.a;
-- mutiple hint
explain(verbose on, costs off) select count(*) from skew_t1 t1, (select count(s.a) as sa, s.b as sb from skew_t3 s, skew_t2 t2 where s.a = t2.a group by sb)tp(aa,b) where t1.b = tp.aa group by t1.a;
explain(verbose on, costs off) select/*+ skew(t1 (b) (10)) skew((t1 tp) (a))*/ count(*) from skew_t1 t1, (select /*+ skew(s (sb))*/ count(s.a) as sa, s.b as sb from skew_t3 s, skew_t2 t2 where s.a = t2.a group by sb)tp(aa,b) where t1.b = tp.aa group by t1.a;

--I4.skew hint out subquery + subquery can pull up
--S1.subquery join when subquery pull up
-- column b is ambigous
explain(verbose on, costs off) select/*+ skew((tp t1) (b)) */ count(*) from skew_t1 t1, (select s.a as sa, s.b as sb from skew_t3 s, skew_t2 t2 where s.a = t2.a)tp(a
,b) where t1.b = tp.b group by tp.b;

--S2.subquery column in tgl
-- DN_REDISTRIBUTE_AGG
explain(verbose on, costs off) select tp.b as tpb from skew_t1 t1, (select s.a as sa, s.b as sb from skew_t3 s, skew_t2 t2 where s.a = t2.a)tp(a,b) where t1.b = tp.a
group by tpb;
set skew_option to off;
explain(verbose on, costs off) select/*+ skew((tp t1) (tpb)) */ tp.b as tpb from skew_t1 t1, (select s.a as sa, s.b as sb from skew_t3 s, skew_t2 t2 where s.a = t2.a
)tp(a,b) where t1.b = tp.a group by tpb;
-- DN_AGG_REDISTRIBUTE_AGG
set skew_option to normal;
explain(verbose on, costs off) select/*+ skew((tp t1) (tpb)) */ tp.b as tpb from skew_t1 t1, (select s.a as sa, s.b as sb from skew_t3 s, skew_t2 t2 where s.a = t2.a
)tp(a,b) where t1.b = tp.a group by tpb;

--S3.subquery column not in tgl
-- DN_REDISTRIBUTE_AGG
explain(verbose on, costs off) select count(*) from skew_t1 t1, (select s.a as sa, s.b as sb from skew_t3 s, skew_t2 t2 where s.a = t2.a)tp(aa,bb) where t1.b = tp.aa group by tp.bb;
-- DN_AGG_REDISTRIBUTE_AGG
explain(verbose on, costs off) select/*+ skew((tp t1) (bb)) */ count(*) from skew_t1 t1, (select s.a as sa, s.b as sb from skew_t3 s, skew_t2 t2 where s.a = t2.a)tp(
aa,bb) where t1.b = tp.aa group by tp.bb;

--S4.base rel column in tgl
-- DN_REDISTRIBUTE_AGG
explain(verbose on, costs off) select t1.b as t1b from skew_t1 t1, (select s.a as sa, s.b as sb from skew_t3 s, skew_t2 t2 where s.a = t2.a)tp(a,b) where t1.a = tp.b group by t1.b;
-- DN_AGG_REDISTRIBUTE_AGG
explain(verbose on, costs off) select/*+ skew((tp t1) (t1b)) */ t1.b as t1b from skew_t1 t1, (select s.a as sa, s.b as sb from skew_t3 s, skew_t2 t2 where s.a = t2.a
)tp(a,b) where t1.a = tp.b group by t1.b;

--S5.base rel column not in tgl
-- DN_REDISTRIBUTE_AGG
explain(verbose on, costs off) select count(*) from skew_t1 t1, (select s.a as sa, s.b as sb from skew_t3 s, skew_t2 t2 where s.a = t2.a)tp(aa,bb) where t1.a = tp.bb group by t1.b;
-- DN_AGG_REDISTRIBUTE_AGG: even thought skew_t3 has same column b, hint still work.
explain(verbose on, costs off) select/*+ skew((tp t1) (b)) */ count(*) from skew_t1 t1, (select s.a as sa, s.b as sb from skew_t3 s, skew_t2 t2 where s.a = t2.a)tp(aa,bb) where t1.a = tp.bb group by t1.b;

--S6.column can not found
explain(verbose on, costs off) select/*+ skew((tp t1) (cc)) */ count(*) from skew_t1 t1, (select s.a as sa, s.b as sb from skew_t3 s, skew_t2 t2 where s.a = t2.a)tp(
aa,bb) where t1.b = tp.bb group by tp.bb;

--I5.还原设置
--S1.还原query_dop
set query_dop = 2002;
                        
