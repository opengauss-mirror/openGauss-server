create schema extendstat;
set search_path=extendstat;

drop table if exists es_test;
CREATE TABLE es_test (
    a   int,
    b   int,
    c   int
)
DISTRIBUTE BY hash(c);
drop table if exists es_test_copy;
CREATE TABLE es_test_copy (
    a   int,
    b   int
)
DISTRIBUTE BY hash(a);
INSERT INTO es_test values(generate_series(1,10000)/100, generate_series(1,10000)/500, 1);
INSERT INTO es_test_copy SELECT a,b FROM es_test;
set default_statistics_target = -2;
set explain_perf_mode = normal;
set query_dop = 1;
ANALYZE es_test;
ANALYZE es_test_copy;
EXPLAIN SELECT * FROM es_test WHERE (a = 1) AND (b = 0);
EXPLAIN SELECT * FROM es_test t1, es_test_copy t2 WHERE (t1.a = t2.a) AND (t1.b = t2.b);
--using extended statistic
delete from pg_statistic where staattnum < 0;
ANALYZE es_test ((a,b));
ANALYZE es_test_copy ((a,b));
select t.relname, starelkind, staattnum, stadistinct, stakind1, stavalues1, stakind2, stakind3, stakind4, stakind5
from pg_namespace n, pg_class t, pg_statistic s
where n.oid = t.relnamespace and t.oid = s.starelid
and n.nspname = 'extendstat'
and staattnum < 0
order by 1,3;
EXPLAIN SELECT * FROM es_test WHERE (a = 1) AND (b = 0);
EXPLAIN SELECT * FROM es_test t1, es_test_copy t2 WHERE (t1.a = t2.a) AND (t1.b = t2.b);
EXPLAIN (ANALYZE OFF, TIMING OFF, COSTS OFF) SELECT * FROM es_test t1 where exists (select * from es_test_copy t2 where (t1.a = t2.a) AND (t1.b = t2.b));
create table es_test_null(c1 int, c2 int, c3 int);

-- create even values
insert into es_test_null select v,v,v from generate_series(1,50) as v;

-- create skew values
insert into es_test_null select 1,1    ,1 from generate_series(1,15) as v;
insert into es_test_null select 1,NULL,1 from generate_series(1,20) as v;
insert into es_test_null select 1,0   ,1 from generate_series(1,25) as v;
insert into es_test_null select 1,2   ,NULL from generate_series(1,30) as v;
insert into es_test_null select 2,NULL,NULL from generate_series(1,35) as v;

analyze es_test_null((c2, c3));
explain select * from es_test_null where c2 is null and c3 = 1;
explain select * from es_test_null where c2 is null and c3 = 2;
explain select * from es_test_null where c2 = 2 and c3 is null;
explain select * from es_test_null where c2 = 5 and c3 is null;
explain select * from es_test_null where c2 is null and c3 is null;
explain select * from es_test_null where c2 is not null and c3 = 1;
explain select * from es_test_null t1, es_test_null t2 where t1.c2=t2.c2 and t1.c3=t2.c3;

reset default_statistics_target;
drop table es_test;
drop table es_test_copy;
drop table es_test_null;

create table item
(
    i_item_sk                 integer               not null,
    i_rec_end_date            date                          ,
    i_brand_id                integer                       ,
    i_class_id                integer                       
)
 with (orientation=column)
 DISTRIBUTE by hash(i_item_sk);

 
set resource_track_cost=10;   

select  1
from item 
where 
i_rec_end_date is null and
substr(i_brand_id,1,1)=i_class_id
;

set query_dop = 2002;
drop schema extendstat cascade;
