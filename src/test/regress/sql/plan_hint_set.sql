-- setups
create schema schema_plan_hint;
set current_schema = schema_plan_hint;
drop table if exists t1;
drop table if exists t2;
drop table if exists t3;
create table t1 (a int primary key, b int, c int);
create table t2 (a int primary key, b int, c int);
create table t3 (a int primary key, b int, c int);
-- insert some data to suppress no statistics warning
insert into t1 values(1,1,1);
insert into t2 values(1,1,1);
insert into t3 values(1,1,1);
analyze t1;
analyze t2;
analyze t3;

set sql_beta_feature = no_unique_index_first;

\set EXP 'EXPLAIN  (verbose off, costs off)'
\set EXPC 'EXPLAIN  (verbose off, costs on)'

-- exception
--- gucs not in whitelist
:EXP select /*+ set(plan_cache_mode force_generic_plan) */ * from t1;
--- invalid value
:EXP select /*+ set(enable_seqscan ok) */ * from t1;
--- false grammar
:EXP select /*+ set(enable_seqscan=on) */ * from t1;
--- set hint in subquery won't work
:EXP select (select /*+ set(enable_seqscan off) */ count(*) from t1 where t2.b = t1.b), b ,c from t2 where t2.a = (select /*+ set(enable_seqscan off) */ t3.b from t3 where t3.b = t2.c and t3.b < 10 order by 1 limit 1) and t2.c < 50 order by 1,2,3;
--- set hint in pull-up subquery won't work
:EXP select * from t1 where t1.a in (select /*+ set(enable_seqscan off) */ t2.a from t2 where t2.a = 1);
--- duplicate hint. only last one is effective
:EXP select /*+ set(enable_seqscan off) set(enable_seqscan on)  */ * from t1 where t1.a = 1;

-- booleans
--- turn off all supported toggles
set enable_bitmapscan           = off;
set enable_hashagg              = off;
set enable_hashjoin             = off;
set enable_index_nestloop       = off;
set enable_indexonlyscan        = off;
set enable_indexscan            = off;
set enable_material             = off;
set enable_mergejoin            = off;
set enable_nestloop             = off;
set enable_seqscan              = off;
set enable_sort                 = off;
set enable_tidscan              = off;

--- scans
:EXP select /*+ set(enable_seqscan on) */ a from t1 where t1.a = 1;
:EXP select /*+ set(enable_indexscan on) */ a from t1 where t1.a = 1;
:EXP select /*+ set(enable_bitmapscan on) */ a from t1 where t1.a = 1;
:EXP select /*+ set(enable_indexscan on) set(enable_indexonlyscan on) */ a from t1 where t1.a = 1;
:EXP select /*+ set(enable_tidscan on) */ a from t1 where t1.ctid = '(0,1)';
set enable_seqscan              = on;
set enable_indexscan            = on;
set enable_bitmapscan           = on;
set enable_indexonlyscan        = on;
set enable_tidscan              = on;
--- joins
:EXP select /*+ set(enable_nestloop on) */ * from t1, t2 where t1.a = t2.a;
:EXP select /*+ set(enable_mergejoin on) */ * from t1, t2 where t1.a = t2.a;
:EXP select /*+ set(enable_hashjoin on) */ * from t1, t2 where t1.a = t2.a;
:EXP select /*+ set(enable_index_nestloop on)  rows(t2 #100000) */ * from t1, t2 where t1.a = t2.a;
set enable_mergejoin            = on;
set enable_nestloop             = on;
set enable_index_nestloop       = on;
--- materials (material is not prefered by cost model when centralized)
:EXP select /*+ set(enable_material on) rows(t1 #10000000) rows(t2 #10000000) */ * from t1, t2 where t1.b = t2.b;
:EXP select /*+ set(enable_sort on) rows(t1 #10000000) rows(t2 #10000000) */ * from t1, t2 where t1.b = t2.b;
set enable_material             = on;
set enable_sort                 = on;
set enable_hashjoin             = on;
--- aggregates
:EXP select /*+ set(enable_hashagg on) */ count(b), a from t1 group by a;
set enable_hashagg              = on;

-- numeric
--- integer
:EXP select /*+ set(query_dop 1064) */ * from t1;

--- float
:EXP select /*+ set(cost_weight_index 100) */ a from t1 where t1.a = 1;
:EXP select /*+ set(cost_weight_index 1e-5) */ a from t1 where t1.a = 1;
:EXP select /*+ set(cost_weight_index 0.0001) */ a from t1 where t1.a = 1;
prepare p1 as select /*+ set(default_limit_rows 100) rows(t1 #1000) */ * from t1 limit $1;
set plan_cache_mode = force_generic_plan;
:EXPC execute p1(100);
prepare p4 as select/*+ set(default_limit_rows 100) rows(t1 #1000000) */ * from t1 limit $1,$2;
:EXPC execute p4(100, 100);

-- cplan/gplan
set plan_cache_mode = force_custom_plan;
prepare p2 as select /*+ use_cplan */ * from t1 where a = $1;
:EXPC execute p2(100);
set plan_cache_mode = force_generic_plan;
prepare p3 as select /*+ use_gplan */ * from t1 where a = $1;
:EXPC execute p3(100);

-- set hint in view is not effective
create view hint_view as select /*+ set(query_dop 1064) */ * from t1;
:EXP select * from hint_view;

-- set hint in create table as
--- no hint
begin;
explain (verbose on, analyze on) create table t4 as select * from t1 where t1.a = 1;
rollback;
--- with hint
begin;
explain (verbose on, analyze on) create table t4 as select /*+ set(enable_seqscan off) */* from t1 where t1.a = 1;
rollback;

-- more on default_limit_rows
create table tt1(c1 int, c2 int);
create table tt2(c1 int, c2 int);
insert into tt1 values(generate_series(1, 10), generate_series(1, 100));
insert into tt2 values(generate_series(1, 10), generate_series(1, 100));
analyze tt1;
analyze tt2;

prepare pd1 as select /*+ set(default_limit_rows -10) */* from tt1, tt2 where tt1.c1 = tt2.c1 limit $1;
:EXPC execute pd1(1); -- hashjoin
prepare pd2 as select /*+ set(default_limit_rows 1) */* from tt1, tt2 where tt1.c1 = tt2.c1 limit $1;
:EXPC execute pd2(1); -- nestloop
prepare pd3 as select /*+ set (default_limit_rows 100) rows(tt1 tt2 #10000000) */* from tt1, tt2 where tt1.c1 = tt2.c1 limit 1 offset $1;
:EXPC execute pd3(1); -- nestloop

-- cleanup
drop schema schema_plan_hint cascade;
