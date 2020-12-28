create schema reduce_orderby;
set current_schema = reduce_orderby;
--create table
create table reduce_orderby_t1(a int, b varchar(10));
create table reduce_orderby_t2(a int, b varchar(10));
--insert into hash order by in subquery
explain (costs off) insert into reduce_orderby_t1 select a, b from reduce_orderby_t1 order by b;

explain (costs off) insert into reduce_orderby_t1 select * from (select a, b from reduce_orderby_t1 order by b) union all (select a, b from reduce_orderby_t2 order by a);

explain (costs off) insert into reduce_orderby_t1 select v.b from (select * from (select a, b from reduce_orderby_t1 order by b) union all (select a, b from reduce_orderby_t2 order by a)) v group by 1;

explain (costs off) insert into reduce_orderby_t1 select * from (select * from (select * from reduce_orderby_t1 order by 1));

--table setop and there is order by in subquery
explain (costs off) select * from (select a, b from reduce_orderby_t1 order by b) union all (select a, b from reduce_orderby_t2 order by a);

explain (costs off) select v.b from (select * from (select a, b from reduce_orderby_t1 order by b) union all (select a, b from reduce_orderby_t2 order by a)) v group by 1;

explain (costs off) (select a,b from reduce_orderby_t1 where a<10 group by 1,2 order by 1,2) intersect (select a,b from reduce_orderby_t1 where a>5 group by 1,2 order by 1,2) except all select a,b from reduce_orderby_t2 where b>10 group by 1,2 order by 1,2;

explain (costs off) (select a,b from reduce_orderby_t1 t1 where t1.a in (select b from reduce_orderby_t2 where b<100 order by 1) group by 1,2 order by 1,2) intersect (select tt1.a,tt1.b from reduce_orderby_t1 tt1 left join reduce_orderby_t2 tt2 on tt1.a=tt2.b group by 1,2 order by 1,2) except all select t32.a,t32.b from reduce_orderby_t2 t32 right join reduce_orderby_t1 t31 on t31.b=t32.a group by 1,2 order by 1,2; 

explain (costs off) 
(select a,b from reduce_orderby_t1 t1 where t1.a in (select b from reduce_orderby_t2 union all (select b from reduce_orderby_t1 order by 1) order by 1) group by 1,2 order by 1,2) 
intersect (select tt1.a,tt1.b from reduce_orderby_t1 tt1 left join (select a from reduce_orderby_t2 union (select a from reduce_orderby_t1 order by 1) order by 1) tt2 on tt1.a=tt2.a group by 1,2 order by 1,2) except all select t32.a,t32.b from reduce_orderby_t2 t32 right join reduce_orderby_t1 t31 on t31.b=t32.a group by 1,2 order by 1,2;

--table join and there is order by in subquery
explain (costs off) select a, b from reduce_orderby_t1 order by a;

explain (costs off) select t1.a from reduce_orderby_t1 t1 left join reduce_orderby_t2 t2 on t1.a=t2.b order by t1.a;

explain (costs off) select t1.a from reduce_orderby_t1 t1 where t1.b in (select t3.b from reduce_orderby_t1 t3 left join reduce_orderby_t2 t2 on t3.a=t2.b order by 1) order by t1.a;

--drop table
drop table reduce_orderby_t1;
drop table reduce_orderby_t2;
--drop schema
reset current_schema;
drop schema reduce_orderby cascade;
