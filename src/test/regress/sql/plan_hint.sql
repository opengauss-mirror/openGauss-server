create schema plan_hint;
set current_schema = plan_hint;
create table src(a int);
insert into src values(1);

create table hint_t1(a int, b int, c int);
insert into hint_t1 select generate_series(1, 2000), generate_series(1, 1000), generate_series(1, 500) from src;

create table hint_t2(a int, b int, c int);
insert into hint_t2 select generate_series(1, 1000), generate_series(1, 500), generate_series(1, 100) from src;

create table hint_t3(a int, b int, c int);
insert into hint_t3 select generate_series(1, 100), generate_series(1, 50), generate_series(1, 25) from src;

create table hint_t4(a int, b int, c int);
insert into hint_t4 select generate_series(1, 10), generate_series(1, 5), generate_series(1, 2) from src;

create table hint_t5(a int, b int, c int);
insert into hint_t5 select generate_series(1, 5), generate_series(1, 5), generate_series(1, 2) from src;

analyze hint_t1;
analyze hint_t2;
analyze hint_t3;
analyze hint_t4;
analyze hint_t5;

--leading hint error
--table num < 2
explain(costs off) select /*+ leading() */  * from hint_t1 join hint_t2 on (1 = 1);
explain(costs off) select /*+ leading(hint_t1) */  * from hint_t1 join hint_t2 on (1 = 1);
--more than one leading, they will all discard
explain(costs off) select /*+ leading(hint_t1 hint_t2) leading(hint_t2 hint_t3) */  * from hint_t1 join hint_t2 on (1 = 1) join hint_t3 on(1 = 1);
--inner outer, syntax error
explain (costs off)select /*+ leading((hint_t1 hint_t2) hint_t3)*/  * from hint_t1 join hint_t2 on (1 = 1) join hint_t3 on(1 = 1);
--outer inner couple
explain(costs off) select /*+ leading((hint_t1 hint_t2 hint_t3))*/  * from hint_t1 join hint_t2 on (1 = 1) join hint_t3 on(1 = 1);
explain(costs off) select /*+ leading((hint_t1))*/  * from hint_t1 join hint_t2 on (1 = 1) join hint_t3 on(1 = 1);
explain(costs off) select /*+ leading(((hint_t1) (hint_t2)))*/  * from hint_t1 join hint_t2 on (1 = 1) join hint_t3 on(1 = 1);

--join hint error
--table num < 2
explain(costs off) select /*+ nestloop()*/  * from hint_t1 join hint_t2 on (1 = 1) join hint_t3 on(1 = 1);
explain(costs off) select /*+ nestloop(hint_t1)*/  * from hint_t1 join hint_t2 on (1 = 1) join hint_t3 on(1 = 1);

--rows hint error
--table num < 2
explain(costs off) select /*+ rows()*/  * from hint_t1 join hint_t2 on (1 = 1) join hint_t3 on(1 = 1);
explain(costs off) select /*+ rows(hint_t1)*/  * from hint_t1 join hint_t2 on (1 = 1) join hint_t3 on(1 = 1);
--support type '#'  '+'  '-'  '*'
explain(costs off) select /*+ rows(hint_t1 hint_t2 10)*/  * from hint_t1 join hint_t2 on (1 = 1) join hint_t3 on(1 = 1);
explain(costs off) select /*+ rows(hint_t1 hint_t2 @10)*/  * from hint_t1 join hint_t2 on (1 = 1) join hint_t3 on(1 = 1);
--last one need num
explain(costs off) select /*+ rows(hint_t1 hint_t2 #1jk)*/  * from hint_t1 join hint_t2 on (1 = 1) join hint_t3 on (1 = 1);
--duplicate rows hint
explain (costs off)select /*+ rows(hint_t1 hint_t2 +100) rows(hint_t1 hint_t2 +100)*/ * from hint_t1 join hint_t2 on (1 = 1) join hint_t3 on(1 = 1);
explain (costs off)select /*+ rows(hint_t1 hint_t2 +100) rows(hint_t1 hint_t2 #100)*/ * from hint_t1 join hint_t2 on (1 = 1) join hint_t3 on(1 = 1);
explain (costs off)select /*+ rows(hint_t1 hint_t2 +100) rows(hint_t1 hint_t2 +100) rows(hint_t1 hint_t2 #100)*/
* from hint_t1 join hint_t2
on (1 = 1) join hint_t3
on(1 = 1);

explain (costs off)select /*+ rows(hint_t1 hint_t2 +100) rows(hint_t1 +10) rows(hint_t1 hint_t2 +100) rows(hint_t1 *10)
rows(hint_t1 hint_t2 #100) rows(hint_t2 #100) rows(hint_t1 #10) rows(hint_t2 #100)*/
* from hint_t1 join hint_t2
on (1 = 1) join hint_t3
on(1 = 1);
--stream hint error
--table num >= 1
explain(costs off) select * from hint_t1 join hint_t2 on (1 = 1) join hint_t3 on(1 = 1);
explain(costs off) select * from hint_t1 join hint_t2 on (1 = 1) join hint_t3 on(1 = 1);

--table name not exists
explain (costs off) select /*+ nestloop(t10 t20) leading(t100 t200) rows(t1000 t2000 #100) */
* from hint_t1 as t1 join hint_t2 as t2 on (1 = 1) join hint_t3 as t3 on(1 = 1);
--hint can not include duplicate rel name
explain (costs off) select /*+ nestloop(hint_t1 hint_t1) leading(hint_t2 hint_t2) rows(hint_t2 hint_t2 #10) */
* from hint_t1 join hint_t2 on (1 = 1) join hint_t3 on(1 = 1);
--drop duplicate hint
explain (costs off)select /*+ nestloop(hint_t1 hint_t2) nestloop(hint_t1 hint_t2)
    rows(hint_t1 hint_t2 #100) rows(hint_t1 hint_t2 +100) */
  * from hint_t1 join hint_t2 on (1 = 1) join hint_t3 on(1 = 1);

--join hint
explain (costs off)
select
/*+ nestloop(t1 t2)*/
 *
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b);

--can not generate t1 join t3 path, because t1, t3 havr not join qual.
--(join_search_one_level)
explain (costs off)
select /*+ nestloop(t1 t3)*/
  *
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b);

explain (costs off)
select /*+ nestloop(t2 t3)*/
  *
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b);

explain (costs off)
select
/*+ mergejoin(t1 t2)*/
 *
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b);

explain (costs off)
select /*+ mergejoin(t2 t3)*/
  *
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b);

explain (costs off)
select
/*+ hashjoin(t1 t2)*/
 *
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b);

explain (costs off)
select /*+ hashjoin(t2 t3)*/
  *
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b);

explain (costs off)
select /*+ no hashjoin(t2 t3)*/
  *
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b);

explain (costs off)
select /*+ no hashjoin(t2 t3) no mergejoin(t2 t3)*/
  *
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b);

explain (costs off)
select /*+ nestloop(t1 t2 t3)*/
  *
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b);

explain (costs off)
select /*+ mergejoin(t1 t2 t3)*/
  *
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b);

explain (costs off)
select /*+ hashjoin(t1 t2 t3)*/
  *
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b);

explain (costs off)
select /*+ hashjoin(T1 "T2" t3)*/
  *
from hint_t1 as T1
join hint_t2 as "T2"
on(t1.a = "T2".b)
join hint_t3 as t3
on ("T2".a = t3.b);

explain (costs off)
select /*+ hashjoin(t1 t2) mergejoin (t1 t2 t3) nestloop(t1 t2 t3 t4)*/
  *
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b)
join hint_t4 as t4
on (t4.c = t3.c);

--leading hint
explain (costs off)
select /*+ leading(t1 t2)*/
  *
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b);

--can not generate t1 join t3 path, because t1, t3 have not join qual
--(join_search_one_level)
explain (costs off) select /*+ leading(t1 t3)*/
  *
from hint_t1 t1
join hint_t2 t2
on(t1.a = t2.b)
join hint_t3 t3
on (t2.a = t3.b);

explain (costs off)
select /*+ leading(t2 t3)*/
  *
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b);

explain (costs off) select
/*+ leading(t2 t3 t4)*/
  *
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b)
join hint_t4 as t4
on (t4.c = t3.c);

explain (costs off)
select /*+ leading((((t1 t2) t3) t4)) */
  *
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b)
join hint_t4 as t4
on (t4.c = t3.c);


explain (costs off)
select /*+ leading(((t1 t2) (t3 t4))) */
  *
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b)
join hint_t4 as t4
on (t4.c = t3.c);

--row hint
set explain_perf_mode = pretty;
explain select
  *
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b);

explain
select  /*+ rows(t2 t3 #230)*/
  *
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b);

explain select  /*+ rows(t2 t3 +100)*/
  *
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b);

explain select  /*+ rows(t2 t3 +10.5)*/
  *
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b);

explain select  /*+ rows(t2 t3 -10)*/
  *
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b);

explain select  /*+ rows(t1 t2 *10) rows(t2 t3 *10)*/
  *
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b);

explain select  /*+ rows(t1 t2 t3 *10)*/
  *
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b);

explain select  /*+ rows(t1 t2 t3 *0.5)*/
  *
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b);

reset explain_perf_mode;

--stream hint
explain (costs off)
select
  *
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b);

explain (costs off)
select
  *
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b);

explain (costs off)
select
  *
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b);

explain (costs off)
select
 * from hint_t1 as t1
join (select sum(a) as a, b from hint_t2 as t2 group by b) as AA
on(t1.b = AA.a);

explain (costs off)
select /*+ hashjoin(t1 t2) mergejoin (t1 t2 t3) nestloop(t1 t2 t3 t4) */
  *
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b)
join hint_t4 as t4
on (t4.c = t3.c);

--leading join hint
explain (costs off)
select /*+ leading(t1 t2) no hashjoin(t1 t2)*/
  *
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b);

explain (costs off)
select /*+ leading(t1 t2) mergejoin(t1 t2)*/
  *
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b);

explain (costs off)
select /*+ leading((t2 t1)) mergejoin(t1 t2)*/
  *
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b);

explain (costs off)
select /*+ leading((t2 t1)) nestloop(t1 t2)*/
  *
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b);

explain (costs off)
select /*+ leading((t2 t1)) hashjoin(t1 t2)*/
  *
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b);

explain (costs off)
select /*+ leading(t2 t3) no hashjoin(t2 t3)*/
  *
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b);

explain (costs off)
select /*+ leading(t2 t3) mergejoin(t2 t3)*/
  *
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b);

explain(costs off) select /*+ nestloop(t1 t2) mergejoin(t1 t2) leading((t2 t1)) leading((t1 t2))*/ *
from hint_t1 as t1 join hint_t2 as t2
on (t1.a = t2.a)
join hint_t3 as t3
on (t2.b = t3.b);

--subquery include hint
--subquery pull up
explain (costs off)
select 1 from hint_t1 as t1 join (select /*+ mergejoin(t2 t3) */ t2.a from hint_t2 as t2 join hint_t3 as t3 on (t2.a = t3.a)) as AA on(t1.a = AA.a);

explain (costs off)
select /*+ leading(t1 aa) hashjoin(t1 aa)*/  1 from hint_t1 as t1 join (select /*+ leading((t3 t2)) mergejoin(t2 t3) */
t2.a from hint_t2 as t2 join hint_t3 as t3
on (t2.a = t3.a)) as AA on(t1.a = AA.a);

--subquey can not pull up
explain (costs off)
select /*+ nestloop(t1 aa)*/  1 from hint_t1 as t1 join
(select /*+ leading((t3 t2)) mergejoin(t2 t3) */  sum(t2.a) as a from hint_t2 as t2 join hint_t3 as t3
on (t2.a = t3.a)) as AA on(t1.a = AA.a);

explain (costs off)
select /*+ mergejoin(t1 aa)*/ 1 from hint_t1 as t1 join
(select  sum(t2.a) as a from hint_t2 as t2 join hint_t3 as t3
on (t2.a = t3.a)) as AA on(t1.a = AA.a);

explain (costs off)
select /*+ mergejoin(t1 aa)*/ 1 from hint_t1 as t1 join
(select /*+ mergejoin(t2 t3)*/ sum(t2.a) as a from hint_t2 as t2 join hint_t3 as t3
on (t2.a = t3.a)) as AA on(t1.a = AA.a);

--sublink
explain (costs off)
select 1 from hint_t1 as t1 where t1.a =
(select /*+ mergejoin(t2 t3)*/ sum(t2.a) as a from hint_t2 as t2 join hint_t3 as t3
on (t2.a = t3.a));

explain (costs off)
select 1 from hint_t1 as t1 where t1.a =
(select /*+ leading((t3 t2)) mergejoin(t2 t3)*/ sum(t2.a) as a from hint_t2 as t2 join hint_t3 as t3
on (t2.a = t3.a) where t1.b = t2.b);

explain (costs off)
select 1 from hint_t1 as t1 where t1.a =
(select /*+ nestloop(t2 t3)*/ sum(t2.a) as a from hint_t2 as t2 join hint_t3 as t3
on (t2.a = t3.a) where t1.b = t2.b);

explain (costs off)
select 1 from hint_t1 as t1 where t1.a in
(select /*+ nestloop(t2 t3)*/ t2.a as a from hint_t2 as t2 join hint_t3 as t3
on (t2.a = t3.a));

explain (costs off)
select 1 from hint_t1 as t1 where t1.a in
(select /*+ hahsjoin(t2 t3)*/ t2.a as a from hint_t2 as t2 join hint_t3 as t3
on (t2.a = t3.a));

explain (costs off)
select 1 from hint_t1 as t1 where t1.a not in
(select /*+ nestloop(t2 t3)*/ t2.a as a from hint_t2 as t2 join hint_t3 as t3
on (t2.a = t3.a));



--view keep hint
create view hint_view_1 as
select /*+ leading(t2 t3) mergejoin(t2 t3)*/
  t1.a
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b);

explain (costs off)select * from hint_view_1;

create view hint_view_2 as
select /*+ leading((t3 t2)) mergejoin(t2 t3)*/
  t1.a
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b);

explain (costs off)select * from hint_view_2;

set explain_perf_mode = pretty;

create view hint_view_3 as
select /*+ nestloop(t2 t3) rows(t2 t3 #200)*/
  t1.a
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b);

explain  select * from hint_view_3;


create view hint_view_4 as
select /*+ nestloop(t2 t3) rows(t2 t3 #200)*/
  t1.a
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b);

explain select * from hint_view_4;

create view hint_view_5 as
select /*+ nestloop(t2 t3) rows(t2 t3 #200)*/
  t1.a
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b);

explain select * from hint_view_5;

create view hint_view_6 as
select /*+ leading(t2 t3) nestloop(t2 t3) rows(t2 t3 #200)*/
  t1.a
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b);

explain select * from hint_view_6;

create view hint_view_7 as
select /*+ leading(t2 t3) nestloop(t2 t3) rows(t2 t3 #200)*/
  t1.a
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b);

explain select * from hint_view_7;

explain
select 1 from hint_t1 as t1 where t1.a in
(select /*+ nestloop(t2 t3) rows(t2 t3 #500)*/ sum(t2.a) as a from hint_t2 as t2 join hint_t3 as t3
on (t2.a = t3.a));

reset explain_perf_mode;

create view hint_view_8 as
select /*+ nestloop(t2 t3)*/
  t1.a
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b);

explain (costs off)select * from hint_view_8;

create view hint_view_9 as
select /*+ nestloop(t2 t3) */
  t1.a
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b);

explain (costs off)select * from hint_view_9;

create view hint_view_10 as
select /*+ leading(((t1 t2) t3)) nestloop(t2 t3) rows(t2 t3 #200)*/
  t1.a
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b);

explain (costst off)select * from hint_view_10;

\d+ hint_view_10

create index hintt1_index on hint_t1(a);
create index hintt2_index on hint_t1(a);
explain (costs off) select * from  hint_t1 where a = 10 and b = 10;
explain (costs off) select /*+ tablescan(hint_t1)*/ * from  hint_t1 where a = 10 and b = 10;
explain (costs off) select /*+ indexscan(hint_t1)*/ * from  hint_t1 where a = 10 and b = 10;
explain (costs off) select /*+ indexscan(hint_t1 hintt1_index)*/ * from  hint_t1 where a = 10 and b = 10;
explain (costs off) select /*+ indexscan(hint_t1 hintt2_index)*/ * from  hint_t1 where a = 10 and b = 10;
explain (costs off) select /*+ indexonlyscan(hint_t1 hintt1_index)*/ * from  hint_t1 where a = 10 and b = 10;
explain (costs off) select a from  hint_t1 where a = 10;
explain (costs off) select /*+ indexonlyscan(hint_t1)*/ a from  hint_t1 where a = 10;
explain (costs off) select /*+ indexonlyscan(hint_t1 hintt1_index)*/ a from  hint_t1 where a = 10;
explain (costs off) select /*+ indexonlyscan(hint_t1 hintt2_index)*/ a from  hint_t1 where a = 10;

create view hint_view1 as select /*+ tablescan(hint_t1)*/ * from  hint_t1 where a = 10 and b = 10;
create view hint_view2 as select /*+ indexscan(hint_t1)*/ * from  hint_t1 where a = 10 and b = 10;
create view hint_view3 as select /*+ indexscan(hint_t1 hintt1_index)*/ * from  hint_t1 where a = 10 and b = 10;
create view hint_view4 as select /*+ indexonlyscan(hint_t1 hintt1_index)*/ a from  hint_t1 where a = 10;

select /*+ tablescan(hint_t1 hint_t2 hint_t3)*/ * from  hint_t1 where a = 10 and b = 10;

explain (costs off) select * from hint_view1;
explain (costs off) select * from hint_view2;
explain (costs off) select * from hint_view3;
explain (costs off) select * from hint_view4;

explain (costs off)select /*+ tablescan(t1) tablescan(t1) indexscan(t1) indexscan(t1) indexscan(t1 hintt1_index) indexscan(t1 hintt1_index)*/
* from hint_t1 as t1;

create table hint_vec(a int, b int, c int) with(orientation = column) ;
create index hint_vec_index on hint_vec(a);
create index hint_vec_index2 on hint_vec(b);

explain (costs off) select a from hint_vec where a = 10;
explain (costs off) select /*+ tablescan(hint_vec)*/ a from hint_vec where a = 10;
explain (costs off) select /*+ indexscan(hint_vec hint_vec_index)*/ a from hint_vec where a = 10;
explain (costs off) select /*+ indexonlyscan(hint_vec hint_vec_index)*/ a from hint_vec where a = 10;

-- test conflict
explain (costs off) select /*+ tablescan(hint_vec) indexonlyscan(hint_vec hint_vec_index)*/ a from hint_vec where a = 10;
explain (costs off) select /*+ tablescan(hint_vec) no indexonlyscan(hint_vec hint_vec_index2)*/ a from hint_vec where a = 10;
explain (costs off) select /*+ indexscan(hint_vec) no indexscan(hint_vec hint_vec_index2)*/ a from hint_vec where a = 10;
explain (costs off) select /*+ no indexscan(hint_vec hint_vec_index2) indexscan(hint_vec)*/ a from hint_vec where a = 10;
explain (costs off) select /*+ no indexscan(hint_vec hint_vec_index) indexscan(hint_vec)*/ a from hint_vec where a = 10;
explain (costs off) select /*+ no tablescan no indexonlyscan(hint_vec hint_vec_index2)*/ a from hint_vec where a = 10;
explain (costs off) select /*+ no indexonlyscan(hint_vec hint_vec_index) no indexonlyscan(hint_vec hint_vec_index2)*/ a from hint_vec where a = 10;

-- same keyword
-- yes yes
explain (costs off) select /*+ indexonlyscan(hint_vec) indexonlyscan(hint_vec hint_vec_index)*/ a from hint_vec where a = 10;
explain (costs off) select /*+ indexonlyscan(hint_vec) indexonlyscan(hint_vec)*/ a from hint_vec where a = 10;
explain (costs off) select /*+ indexonlyscan(hint_vec hint_vec_index) indexonlyscan(hint_vec hint_vec_index2)*/ a from hint_vec where a = 10;

-- yes no
explain (costs off) select /*+ indexonlyscan(hint_vec) no indexonlyscan(hint_vec hint_vec_index)*/ a from hint_vec where a = 10;
explain (costs off) select /*+ indexonlyscan(hint_vec hint_vec_index) no indexonlyscan(hint_vec)*/ a from hint_vec where a = 10;
explain (costs off) select /*+ indexonlyscan(hint_vec) no indexonlyscan(hint_vec)*/ a from hint_vec where a = 10;
explain (costs off) select /*+ indexonlyscan(hint_vec hint_vec_index) no indexonlyscan(hint_vec hint_vec_index2)*/ a from hint_vec where a = 10;

-- no yes
explain (costs off) select /*+ no indexonlyscan(hint_vec) indexonlyscan(hint_vec hint_vec_index)*/ a from hint_vec where a = 10;
explain (costs off) select /*+ no indexonlyscan(hint_vec hint_vec_index) indexonlyscan(hint_vec)*/ a from hint_vec where a = 10;
explain (costs off) select /*+ no indexonlyscan(hint_vec) indexonlyscan(hint_vec)*/ a from hint_vec where a = 10;
explain (costs off) select /*+ no indexonlyscan(hint_vec hint_vec_index) indexonlyscan(hint_vec hint_vec_index2)*/ a from hint_vec where a = 10;

-- no no
explain (costs off) select /*+ no indexonlyscan(hint_vec) no indexonlyscan(hint_vec hint_vec_index)*/ a from hint_vec where a = 10;
explain (costs off) select /*+ no indexonlyscan(hint_vec hint_vec_index) no indexonlyscan(hint_vec)*/ a from hint_vec where a = 10;
explain (costs off) select /*+ no indexonlyscan(hint_vec) no indexonlyscan(hint_vec)*/ a from hint_vec where a = 10;
explain (costs off) select /*+ no indexonlyscan(hint_vec hint_vec_index) no indexonlyscan(hint_vec hint_vec_index2)*/ a from hint_vec where a = 10;

-- different keyword
-- yes yes
explain (costs off) select /*+ indexonlyscan(hint_vec) indexscan(hint_vec hint_vec_index)*/ a from hint_vec where a = 10;
explain (costs off) select /*+ indexonlyscan(hint_vec) indexscan(hint_vec)*/ a from hint_vec where a = 10;
explain (costs off) select /*+ indexonlyscan(hint_vec hint_vec_index) indexscan(hint_vec hint_vec_index2)*/ a from hint_vec where a = 10;

-- yes no
explain (costs off) select /*+ indexonlyscan(hint_vec) no indexscan(hint_vec hint_vec_index)*/ a from hint_vec where a = 10;
explain (costs off) select /*+ indexonlyscan(hint_vec hint_vec_index) no indexscan(hint_vec)*/ a from hint_vec where a = 10;
explain (costs off) select /*+ indexonlyscan(hint_vec) no indexscan(hint_vec)*/ a from hint_vec where a = 10;
explain (costs off) select /*+ indexonlyscan(hint_vec hint_vec_index) no indexscan(hint_vec hint_vec_index2)*/ a from hint_vec where a = 10;

-- no yes
explain (costs off) select /*+ no indexonlyscan(hint_vec) indexscan(hint_vec hint_vec_index)*/ a from hint_vec where a = 10;
explain (costs off) select /*+ no indexonlyscan(hint_vec hint_vec_index) indexscan(hint_vec)*/ a from hint_vec where a = 10;
explain (costs off) select /*+ no indexonlyscan(hint_vec) indexscan(hint_vec)*/ a from hint_vec where a = 10;
explain (costs off) select /*+ no indexonlyscan(hint_vec hint_vec_index) indexscan(hint_vec hint_vec_index2)*/ a from hint_vec where a = 10;

-- no no
explain (costs off) select /*+ no indexonlyscan(hint_vec) no indexscan(hint_vec hint_vec_index)*/ a from hint_vec where a = 10;
explain (costs off) select /*+ no indexonlyscan(hint_vec hint_vec_index) no indexscan(hint_vec)*/ a from hint_vec where a = 10;
explain (costs off) select /*+ no indexonlyscan(hint_vec) no indexscan(hint_vec)*/ a from hint_vec where a = 10;
explain (costs off) select /*+ no indexonlyscan(hint_vec hint_vec_index) no indexscan(hint_vec hint_vec_index2)*/ a from hint_vec where a = 10;

explain (costs off) select hint_t1.a from hint_t1
join  (select /*+ hashjoin(t2 aa)*/ a from hint_t2 as t2
      where t2.a in (select /*+ blockname(aa)*/ a from hint_t3 group by a))as bb
  on (hint_t1.a = bb.a);

explain (costs off) select /*+ nestloop(t1 aa)*/ * from hint_t1 as t1 where t1.a in (select /*+ blockname(aa)*/ a from hint_t2 as t2 group by a);

explain (costs off) select a from hint_t1 where a in (select /*+ blockname(t1) blockname(t2)*/ a from hint_t2);
explain (costs off) select a from hint_t1 where a in (select /*+ blockname(tt) blockname(tt) blockname(t1)*/ a from hint_t2);

\d+ hint_view1
\d+ hint_view3

explain (costs off, verbose on)
select /*+ leading(("T1" t2)) nestloop("T1" t2) mergejoin("T1" T2)*/ t2.a from hint_t1 as "T1" join hint_t2 as t2 on ("T1".a = t2.a);

-- duplicate name of blockname and table name
explain (costs off, verbose on)
select /*+ mergejoin(t1 t2)*/ t1.a from hint_t1 as t1 where t1.a in (select /*+blockname(t2)*/ b from hint_t2 as t2);

-- bms double free issue
explain (costs off) select /*+ leading((t1 (t2 t4) t3)) */
  t1.a
from hint_t1 as t1
join hint_t2 as t2
on(t1.a = t2.b)
join hint_t3 as t3
on (t2.a = t3.b);

-- test distribute hint
explain (costs off) select /*+ multinode */ a from hint_vec where a = 10;
explain (costs off) select /*+ broadcast(hint_vec) */ a from hint_vec where a = 10;
explain (costs off) select /*+ redistribute(hint_vec) */ a from hint_vec where a = 10;
explain (costs off) select /*+ no broadcast(hint_vec) */ a from hint_vec where a = 10;
explain (costs off) select /*+ no redistribute(hint_vec) */ a from hint_vec where a = 10;
explain (costs off) select /*+ skew(hint_vec(a)) */ a from hint_vec where a = 10;

explain (costs off) select /*+indexscan(''')*/ 1;
explain (costs off) select /*+indexscan(""")*/ 1;
explain (costs off) select /*+indexscan($$$)*/ 1;
create table subpartition_hash_hash (
    c1 int,
    c2 int,
    c3 text,
    c4 varchar(20),
    c5 int generated always as(2 * c1) stored
) partition by hash(c1) subpartition by hash(c2) (
    partition p1 (
        subpartition p1_1,
        subpartition p1_2,
        subpartition p1_3,
        subpartition p1_4,
        subpartition p1_5
    ),
    partition p2 (
        subpartition p2_1,
        subpartition p2_2,
        subpartition p2_3,
        subpartition p2_4,
        subpartition p2_5
    ),
    partition p3 (
        subpartition p3_1,
        subpartition p3_2,
        subpartition p3_3,
        subpartition p3_4,
        subpartition p3_5
    ),
    partition p4 (
        subpartition p4_1,
        subpartition p4_2,
        subpartition p4_3,
        subpartition p4_4,
        subpartition p4_5
    ),
    partition p5 (
        subpartition p5_1,
        subpartition p5_2,
        subpartition p5_3,
        subpartition p5_4,
        subpartition p5_5
    )
);
create index subpartition_hash_hash_i1 on subpartition_hash_hash(c1) local;
create index subpartition_hash_hash_i2 on subpartition_hash_hash(c2) local;
create index subpartition_hash_hash_i3 on subpartition_hash_hash(c3) local;
create index subpartition_hash_hash_i4 on subpartition_hash_hash(c4) local;
create index subpartition_hash_hash_i5 on subpartition_hash_hash(c5) local;
create table partition_range (c1 int, c2 int, c3 text, c4 varchar(20)) with(orientation = column) partition by range(c1, c2) (
    partition p1
    values less than(10000, 10000),
        partition p2
    values less than(20000, 20000),
        partition p3
    values less than(30000, 30000),
        partition p4
    values less than(40000, 40000),
        partition p5
    values less than(50000, 50000),
        partition p6
    values less than(60000, 60000),
        partition p7
    values less than(70000, 70000),
        partition p8
    values less than(80000, 80000),
        partition p9
    values less than(90000, 90000),
        partition p10
    values less than(MAXVALUE, MAXVALUE)
);
create index partition_range_i1 on partition_range using btree(c1) local;
create index partition_range_i2 on partition_range using psort(c2) local;
create index partition_range_i3 on partition_range using btree(c3) local;
create index partition_range_i4 on partition_range using btree(c4) local;

explain (analyse,timing off,costs off) create table tb_create_merge_append6 as (
    select
        /*+ indexscan(subpartition_hash_hash subpartition_hash_hash_i1)*/
        subpartition_hash_hash.c1 c1,
        subpartition_hash_hash.c3 c2,
        partition_range.c1 c3
    from subpartition_hash_hash
        join partition_range on subpartition_hash_hash.c2 = partition_range.c2
        and subpartition_hash_hash.c1 > 8888
        and subpartition_hash_hash.c1 < 88888
    order by subpartition_hash_hash.c1
    limit 100 offset 10
);

drop view hint_view_1;
drop view hint_view_2;
drop view hint_view_3;
drop view hint_view_4;
drop view hint_view_5;
drop view hint_view_6;
drop view hint_view_7;
drop view hint_view_8;
drop view hint_view_9;
drop view hint_view_10;
drop view hint_view1;
drop view hint_view2;
drop view hint_view3;
drop view hint_view4;
drop table hint_t1;
drop table hint_t2;
drop table hint_t3;
drop table hint_t4;
drop table hint_t5;
drop table hint_vec;

drop schema plan_hint cascade;
