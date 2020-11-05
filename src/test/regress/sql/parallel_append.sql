CREATE SCHEMA parallel_append_schema;
SET CURRENT_SCHEMA TO parallel_append_schema;

create table a (a1 int, a2 int, a3 int);
create table b (b1 int, b2 int, b3 int);
create table c (c1 int, c2 int, c3 int);
create table d (d1 int, d2 int, d3 int);
create table e (e1 int, e2 int, e3 int);
insert into a values(1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5);
insert into b values(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8);
insert into c values(7,7,7),(8,8,8),(9,9,9),(0,0,0),(0,0,0);
insert into d select a1, a2 + 1, a3 + 2 from a;
insert into d select a1, a2 + 2, a3 * 2 from a;

insert into e select * from b where b1 < 7;
insert into e select * from c where c1 = 0 or c1 > 7;
analyze a;
analyze b;
analyze c;
analyze d;
analyze e;

set max_parallel_workers_per_gather to 3;
set force_parallel_mode to on;
set min_parallel_table_scan_size to 0;
set parallel_tuple_cost to 0.00000005;
set parallel_setup_cost to 0;
set enable_parallel_append to on;


-------------------------------------------
-- 1. union && union all
-------------------------------------------
explain select * from a union select * from b;
explain select * from a union all select * from b;
explain select * from a where a1 > 4 union select * from b where b1 < 6;
explain select * from a where a1 > 4 union all select * from b where b1 < 6;
explain select * from c where c1 in (select a1 from a union select b1 from b);
explain select * from (select * from a union all select * from b) as ta, c where ta.a1 = c.c1;
explain select * from d left outer join (select * from a union all select * from b) as t on d.d1=t.a1;
explain select d.d1, sum(d.d2), sum(t.a2) from (select * from a union all select * from b) t, d where t.a1=d1 group by d.d1 order by 1,2;

select * from a union select * from b;
select * from a union all select * from b;
select * from a where a1 > 4 union select * from b where b1 < 6;
select * from a where a1 > 4 union all select * from b where b1 < 6;
select * from c where c1 in (select a1 from a union select b1 from b);
select * from (select * from a union all select * from b) as ta, c where ta.a1 = c.c1;
select * from d left outer join (select * from a union all select * from b) as t on d.d1=t.a1;
select d.d1, sum(d.d2), sum(t.a2) from (select * from a union all select * from b) t, d where t.a1=d1 group by d.d1 order by 1,2;


---------------------------------------
-- 2. except && except all
---------------------------------------
select * from c except select * from b where b1 >4;
select * from c except all select * from b where b1 >4;

explain select * from c except select * from b where b1 >4;
explain select * from c except all select * from b where b1 >4;


---------------------------------------
-- 3. intersect && intersect all
---------------------------------------
select * from e intersect select * from c;
select * from e intersect all select * from c where c1 != 8;

explain select * from e intersect select * from c;
explain select * from e intersect all select * from c where c1 != 8;


--------------------------------------
-- 4. case: 3+ tables, union + except + intersect
--------------------------------------
select * from e intersect (select * from a except select * from b union select * from c);
select d2 from d except all (select d2 from d except select c1 from c) union all select e1 from e;
select * from a union all (select * from b union select * from c where c1 < 5);
select * from a except select * from b union select * from c;
select * from b union all (select * from (select * from a union all select * from b));
select * from (select * from a union all select * from b)as x, (select * from d union all select* from e)as y
    where x.a1 = y.d1 order by 1, 2, 3, 4, 5, 6;

explain select * from e intersect (select * from a except select * from b union select * from c);
explain select d2 from d except all (select d2 from d except select c1 from c) union all select e1 from e;
explain select * from a union all (select * from b union select * from c where c1 < 5);
explain select * from a except select * from b union select * from c;
explain select * from b union all (select * from (select * from a union all select * from b));
explain select * from (select * from a union all select * from b)as x, (select * from d union all select* from e)as y
    where x.a1 = y.d1 order by 1, 2, 3, 4, 5, 6;
----------------------------------------
-- clean up
----------------------------------------
reset max_parallel_workers_per_gather;
reset force_parallel_mode;
reset min_parallel_table_scan_size;
reset parallel_tuple_cost;
reset parallel_setup_cost;
reset enable_parallel_append;
drop schema parallel_append_schema cascade;
