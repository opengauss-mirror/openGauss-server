-- setups
create schema schema_no_expand;
set current_schema = schema_no_expand;
set rewrite_rule = 'magicset, partialpush, uniquecheck, disablerep, intargetlist, predpushforce';
drop table if exists t1;
drop table if exists t2;
drop table if exists t3;
create table t1 (a int, b int, c int);
create table t2 (a int, b int, c int);
create table t3 (a int, b int, c int);
-- insert some data to suppress no statistics warning
insert into t1 values(1,1,1);
insert into t2 values(1,1,1);
insert into t3 values(1,1,1);
analyze t1;
analyze t2;
analyze t3;

\set EXP 'EXPLAIN  (verbose, costs off)'

-- case: duplicate hint does not matter 
:EXP select * from t1 where t1.a in (select /*+ no_expand*/ /*+ no_expand no_expand*/ t2.a from t2);

-- case: non-relavent subquery in from-list
:EXP select * from t1, (select /*+ no_expand */ a from t2 where a < 10) tt2 where t1.a = tt2.a;

-- case: in/not sublink in qual
:EXP select * from t1 where t1.a in (select /*+ no_expand*/ t2.a from t2);
:EXP select * from t1 where t1.a not in (select /*+ no_expand*/ t2.a from t2);

-- case: (not) exists sublink in qual
:EXP select * from t1 where exists (select /*+ no_expand*/ t2.a from t2 where t2.a = t1.a);
:EXP select * from t1 where not exists (select /*+ no_expand*/ t2.a from t2 where t2.a = t1.a);

-- case: any/some sublink in qual
:EXP select * from t1 where t1.a = any (select /*+ no_expand*/ t2.a from t2);
:EXP select * from t1 where t1.a = some (select /*+ no_expand*/ t2.a from t2);

-- case: or/and any sublinks in qual
:EXP select * from t1 where t1.a = any (select /*+ no_expand*/ t2.a from t2) or t1.b = any (select /*+ no_expand*/ t2.b from t2);
:EXP select * from t1 where t1.a = any (select /*+ no_expand*/ t2.a from t2) and t1.b = any (select /*+ no_expand*/ t2.b from t2);

-- case: or/and exists sublinks in qual
:EXP select * from t1 where exists (select /*+ no_expand*/ t2.a from t2 where t2.a = t1.a) or exists(select /*+ no_expand*/ t2.b from t2 where t2.b = t1.b);
:EXP select * from t1 where exists (select /*+ no_expand*/ t2.a from t2 where t2.a = t1.a) and exists(select /*+ no_expand*/ t2.b from t2 where t2.b = t1.b);

-- case: [intargetlist rewrite] limit 1 in qual
:EXP select * from t2 where t2.a = (select /*+ no_expand */ t3.b from t3 where t3.b = t2.c and t3.b < 10 order by 1 limit 1) and t2.c < 50 order by 1,2,3;

-- case: [intargetlist rewrite] limit 1 in targetlist
:EXP select (select /*+ no_expand */ t2.a from t2 where t2.b = t1.b and t2.b<10 order by 1 limit 1), b, c from t1 where t1.b <50 order by 1,2,3;

-- case: [intargetlist rewrite] limit 1 in qual
:EXP select * from t1 where t1.b = (select /*+ no_expand */ t2.b from t2 inner join t3 on t2.c = t3.c where t2.a = t1.a order by 1 limit 1) and t1.c < 10 order by 1,2,3;

-- case: [intargetlist rewrite] limit 1 in sublink targetlist
:EXP select a,(select /*+ no_expand */ t2.b from t2 inner join t3 on t2.c = t3.c where t2.a = t1.a order by 1 limit 1),c from t1 where t1.c < 10 order by 1,2,3;

-- case: [intargetlist rewrite] limit 1 in sublink targetlist + qual
:EXP select (select /*+ no_expand */ t2.a from t2 where t2.b = t1.b and t2.b<10 order by 1 limit 1), b, c from t1  where a = (select t3.a from t3 where t3.c = t1.c and t3.b<10 order by 1 limit 1) and t1.b <50 order by 1,2,3;
:EXP select (select t2.a from t2 where t2.b = t1.b and t2.b<10 order by 1 limit 1), b, c from t1  where a = (select /*+ no_expand */ t3.a from t3 where t3.c = t1.c and t3.b<10 order by 1 limit 1) and t1.b <50 order by 1,2,3;

-- case: [intargetlist rewrite] agg sublink + targetlist sublink
:EXP select (select /*+ no_expand */ count(*) from t1 where t2.b = t1.b), b ,c from t2 where t2.a = (select /*+ no_expand */ t3.b from t3 where t3.b = t2.c and t3.b < 10 order by 1 limit 1) and t2.c < 50 order by 1,2,3;

-- case: [intargetlist rewrite] nvl,concat_ws,listagg+subllink
:EXP select nvl((select /*+ no_expand */ a from t2 where t2.a = t1.b order by 1 limit 1), (select /*+ no_expand */ b from t2 where t2.a = t1.b order by 1 limit 1)) from t1 order by 1; 

-- case: [intargetlist rewrite] count(*)
:EXP select * from t1 where t1.a = (select /*+ no_expand */ count(*) from t2 where t2.b = t1.b) order by t1.b;

-- case: [intargetlist rewrite] coalesce
:EXP select * from t1 where t1.a = (select /*+ no_expand */ coalesce(avg(t2.a),10) from t2 where t2.b = t1.b) order by t1.b;

-- case: [intargetlist rewrite] case-when
:EXP select * from t1 where t1.a = (select /*+ no_expand */ case when avg(t2.a) = 10 then 10 else 0 end from t2 where t2.b = t1.b) order by t1.b;

-- case: [intargetlist rewrite] is null
:EXP select * from t1 where t1.a =(select /*+ no_expand */ avg(t2.a) is null from t2 where t2.b = t1.b) order by t1.a;

-- case: [intargetlist rewrite] opexr targetlist
:EXP select * from t1 where t1.a = (select /*+ no_expand */ count(*) + coalesce(avg(t2.a),10) + 3 from t2 where t2.b = t1.b) order by t1.b;

-- case: [intargetlist rewrite] sublink pullup extend
create table t(a int, b int);
analyze t;
:EXP select * from t where exists (select /*+ no_expand */ * from t t1 where t1.a=t.a and t1.b=(select /*+ no_expand */ max(t2.b) from t t2 where t1.a*2=t2.a));

-- case: [intargetlist rewrite] sublink in targetlist with double agg
set enable_sort = off;
:EXP select count(distinct b), (select /*+ no_expand */ b from t1 t2 where t1.a=t2.a) from t1 group by a;
reset enable_sort;

create table sublink_pullup_t1(a1 int, b1 int, c1 int, d1 int) with (orientation=column);
create table sublink_pullup_t2(a2 int, b2 int, c2 int) with (orientation=column);
analyze sublink_pullup_t1;
analyze sublink_pullup_t2;

-- case: [intargetlist rewrite] test sublink in targetlist
:EXP select a1 from sublink_pullup_t1 where a1=(select /*+ no_expand */ avg(a2) from sublink_pullup_t2 where a2%5=a1) order by 1;

-- case: [intargetlist rewrite] test simple sublink pull up
:EXP select a1, (select /*+ no_expand */ avg(a2) from sublink_pullup_t2 where b1=b2) + 1 from sublink_pullup_t1 order by 1;

-- no_expand hint in view def's subquery can be triggered
create view ne_view1 as select * from t1 where t1.a = any (select /*+ no_expand*/ t2.a from t2);
:EXP select * from ne_view1;

-- no_expand hint in view top level query is not effective
create view ne_view2 as select /*+ no_expand */ * from t1 where t1.a = any (select t2.a from t2);
:EXP select * from ne_view2;

-- cleanup
drop schema schema_no_expand cascade;