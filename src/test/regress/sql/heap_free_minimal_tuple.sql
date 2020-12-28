create table heap_free_minimal_tuple_1(a int, b int);
insert into heap_free_minimal_tuple_1 select generate_series(1,5), generate_series(1,5);
analyze heap_free_minimal_tuple_1;
with x as
(select a, b from heap_free_minimal_tuple_1)
select 1 from heap_free_minimal_tuple_1 s 
where s.a not in 
(select a from x)
or exists
(select 1 from x where x.a=s.a)
or s.a >     
(select avg(a) from x where x.b=s.b);
drop table heap_free_minimal_tuple_1 cascade;





create function foo1(n integer, out a text, out b text)
  returns setof record
  language sql
  as $$ select 'foo ' || i, 'bar ' || i from generate_series(1,$1) i $$;

set work_mem='64kB';
select t.a, t, t.a from foo1(10000) t limit 1;
reset work_mem;
select t.a, t, t.a from foo1(10000) t limit 1;

drop function foo1(n integer);




create table heap_free_minimal_tuple_2(c1 int);
insert into heap_free_minimal_tuple_2 values (1);
-- Create Table and Insert Data
create table t_agg1(a int, b int, c int, d int, e int, f int, g regproc);
create table t_agg2(a int, b int, c int);
insert into t_agg1 select generate_series(1, 10000), generate_series(1, 10000)%5000, generate_series(1, 10000)%500, generate_series(1, 10000)%5, 500, 3, 'sin' from heap_free_minimal_tuple_2;
insert into t_agg2 select generate_series(1, 10), generate_series(11, 2, -1), generate_series(3, 12);
/*select * from table_skewness('t_agg1', 'b,c') order by 1, 2, 3;*/
analyze t_agg1;
analyze t_agg2;
explain (costs off) select a, b, count(c) from (select t_agg1.c a, t_agg1.b b, t_agg2.a-t_agg2.b c from t_agg1 join t_agg2 on t_agg1.b=t_agg2.c) group by a, b; 
select a, b, count(c) from (select t_agg1.c a, t_agg1.b b, t_agg2.a-t_agg2.b c from t_agg1 join t_agg2 on t_agg1.b=t_agg2.c) group by a, b order by a, b limit 10;
drop table heap_free_minimal_tuple_2 cascade;
drop table t_agg1 cascade;
drop table t_agg2 cascade;