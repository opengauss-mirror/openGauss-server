create table heap_tbl_a(c1 int, c2 int, c3 int);
create table heap_tbl_b(c1 int, c2 int, c3 int);

create table heap_tbl_c(c1 int, c2 int, c3 int)
partition by range (c2) (
  partition p1 values less than(5), 
  partition p2 values less than(10), 
  partition p3 values less than(15), 
  partition p8 values less than(maxvalue)
);

insert into heap_tbl_a select v%20,v,v from generate_series(1, 100) as v;
insert into heap_tbl_b select * from heap_tbl_a;
insert into heap_tbl_c select * from heap_tbl_a;

analyze heap_tbl_a;
analyze heap_tbl_b;
analyze heap_tbl_c;
update pg_class set reltuples = 1200000 
where relname in ('heap_tbl_a', 'heap_tbl_b', 'heap_tbl_c');

-- test nestloop seqscan
set enable_indexscan=off;
set enable_indexonlyscan=off;
set enable_bitmapscan=off;
set enable_material=off;
set enable_seqscan=on;

-- test nestloop
explain (costs off)
select /*+ nestloop(heap_tbl_a heap_tbl_b) */ heap_tbl_a.c1 ac1, heap_tbl_b.c2 bc2 
from heap_tbl_a, heap_tbl_b
where heap_tbl_a.c2 = heap_tbl_b.c2;

select ac1, sum(bc2) from (
	select /*+ nestloop(heap_tbl_a heap_tbl_b) */ heap_tbl_a.c1 ac1, heap_tbl_b.c2 bc2
	from heap_tbl_a, heap_tbl_b
	where heap_tbl_a.c2 = heap_tbl_b.c2
) group by 1 order by 1;

explain (costs off)
select /*+ nestloop(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2
from heap_tbl_a, heap_tbl_c
where heap_tbl_a.c2 = heap_tbl_c.c2;

select ac1, sum(bc2) from (
	select /*+ nestloop(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2
	from heap_tbl_a, heap_tbl_c
	where heap_tbl_a.c2 = heap_tbl_c.c2
) group by 1 order by 1;

--------------------------------------
-- test hashjoin
explain (costs off)
select /*+ hashjoin(heap_tbl_a heap_tbl_b) */ heap_tbl_a.c1 ac1, heap_tbl_b.c2 bc2 
from heap_tbl_a, heap_tbl_b
where heap_tbl_a.c2 = heap_tbl_b.c2;

select ac1, sum(bc2) from (
	select /*+ hashjoin(heap_tbl_a heap_tbl_b) */ heap_tbl_a.c1 ac1, heap_tbl_b.c2 bc2 
	from heap_tbl_a, heap_tbl_b
	where heap_tbl_a.c2 = heap_tbl_b.c2
) group by 1 order by 1;

explain (costs off)
select /*+ hashjoin(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2 
from heap_tbl_a, heap_tbl_c
where heap_tbl_a.c2 = heap_tbl_c.c2;

select ac1, sum(bc2) from (
	select /*+ hashjoin(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2 
	from heap_tbl_a, heap_tbl_c
	where heap_tbl_a.c2 = heap_tbl_c.c2
) group by 1 order by 1;

-- test mergejoin
explain (costs off)
select /*+ mergejoin(heap_tbl_a heap_tbl_b) */ heap_tbl_a.c1 ac1, heap_tbl_b.c2 bc2 
from heap_tbl_a, heap_tbl_b
where heap_tbl_a.c2 = heap_tbl_b.c2;

select ac1, sum(bc2) from (
	select /*+ mergejoin(heap_tbl_a heap_tbl_b) */ heap_tbl_a.c1 ac1, heap_tbl_b.c2 bc2 
	from heap_tbl_a, heap_tbl_b
	where heap_tbl_a.c2 = heap_tbl_b.c2
) group by 1 order by 1;

explain (costs off)
select /*+ mergejoin(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2
from heap_tbl_a, heap_tbl_c
where heap_tbl_a.c2 = heap_tbl_c.c2;

select ac1, sum(bc2) from (
	select /*+ mergejoin(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2
	from heap_tbl_a, heap_tbl_c
	where heap_tbl_a.c2 = heap_tbl_c.c2
) group by 1 order by 1;

set enable_material=on;
explain (costs off)
select /*+ nestloop(heap_tbl_a heap_tbl_b) */ heap_tbl_a.c1 ac1, heap_tbl_b.c2 bc2 
from heap_tbl_a, heap_tbl_b
where heap_tbl_a.c2 = heap_tbl_b.c2;

select ac1, sum(bc2) from (
	select /*+ nestloop(heap_tbl_a heap_tbl_b) */ heap_tbl_a.c1 ac1, heap_tbl_b.c2 bc2 
	from heap_tbl_a, heap_tbl_b
	where heap_tbl_a.c2 = heap_tbl_b.c2
) group by 1 order by 1;

explain (costs off)
select /*+ nestloop(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2 
from heap_tbl_a, heap_tbl_c
where heap_tbl_a.c2 = heap_tbl_c.c2;

select ac1, sum(bc2) from (
	select /*+ nestloop(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2
	from heap_tbl_a, heap_tbl_c
	where heap_tbl_a.c2 = heap_tbl_c.c2
) group by 1 order by 1;

-- test three table join
explain (costs off)
select /*+ nestloop(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2 
from heap_tbl_a, heap_tbl_b, heap_tbl_c
where heap_tbl_a.c2 = heap_tbl_c.c2 and heap_tbl_c.c2 = heap_tbl_b.c2;

select ac1, sum(bc2) from (
	select /*+ nestloop(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2 
	from heap_tbl_a, heap_tbl_b, heap_tbl_c
	where heap_tbl_a.c2 = heap_tbl_c.c2 and heap_tbl_c.c2 = heap_tbl_b.c2
) group by 1 order by 1;

explain (costs off)
select /*+ nestloop(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2 
from heap_tbl_a, heap_tbl_b, heap_tbl_c
where heap_tbl_a.c2 = heap_tbl_c.c2 and heap_tbl_c.c2 = heap_tbl_b.c2 and heap_tbl_a.c1 = 1
order by 1, 2;

select /*+ nestloop(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2 
from heap_tbl_a, heap_tbl_b, heap_tbl_c
where heap_tbl_a.c2 = heap_tbl_c.c2 and heap_tbl_c.c2 = heap_tbl_b.c2 and heap_tbl_a.c1 = 1
order by 1, 2;

explain (costs off)
select /*+ hashjoin(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2 
from heap_tbl_a, heap_tbl_b, heap_tbl_c
where heap_tbl_a.c2 = heap_tbl_c.c2 and heap_tbl_c.c2 = heap_tbl_b.c2;

select ac1, sum(bc2) from (
	select /*+ hashjoin(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2 
	from heap_tbl_a, heap_tbl_b, heap_tbl_c
	where heap_tbl_a.c2 = heap_tbl_c.c2 and heap_tbl_c.c2 = heap_tbl_b.c2
) group by 1 order by 1;

explain (costs off)
select /*+ hashjoin(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2 
from heap_tbl_a, heap_tbl_b, heap_tbl_c
where heap_tbl_a.c2 = heap_tbl_c.c2 and heap_tbl_c.c2 = heap_tbl_b.c2 and heap_tbl_a.c1 = 1
order by 1, 2;

select /*+ hashjoin(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2 
from heap_tbl_a, heap_tbl_b, heap_tbl_c
where heap_tbl_a.c2 = heap_tbl_c.c2 and heap_tbl_c.c2 = heap_tbl_b.c2 and heap_tbl_a.c1 = 1
order by 1, 2;

explain (costs off)
select /*+ mergejoin(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2 
from heap_tbl_a, heap_tbl_b, heap_tbl_c
where heap_tbl_a.c2 = heap_tbl_c.c2 and heap_tbl_c.c2 = heap_tbl_b.c2;

select ac1, sum(bc2) from (
	select /*+ mergejoin(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2 
	from heap_tbl_a, heap_tbl_b, heap_tbl_c
	where heap_tbl_a.c2 = heap_tbl_c.c2 and heap_tbl_c.c2 = heap_tbl_b.c2
) group by 1 order by 1;

explain (costs off)
select /*+ mergejoin(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2 
from heap_tbl_a, heap_tbl_b, heap_tbl_c
where heap_tbl_a.c2 = heap_tbl_c.c2 and heap_tbl_c.c2 = heap_tbl_b.c2 and heap_tbl_a.c1 = 1
order by 1, 2;

select /*+ mergejoin(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2 
from heap_tbl_a, heap_tbl_b, heap_tbl_c
where heap_tbl_a.c2 = heap_tbl_c.c2 and heap_tbl_c.c2 = heap_tbl_b.c2 and heap_tbl_a.c1 = 1
order by 1, 2;

---------------------------------------------------------------------------------
-- index test 
create index heap_tbl_b_index_0b on heap_tbl_b(c2);
create index heap_tbl_c_index_0c on heap_tbl_c(c2) local;

set enable_indexscan=on;
set enable_indexonlyscan=off;
set enable_bitmapscan=off;
set enable_material=off;
set enable_seqscan=off;

-- test nestloop
explain (costs off)
select /*+ nestloop(heap_tbl_a heap_tbl_b) */ heap_tbl_a.c1 ac1, heap_tbl_b.c2 bc2 
from heap_tbl_a, heap_tbl_b
where heap_tbl_a.c2 = heap_tbl_b.c2;

select ac1, sum(bc2) from (
	select /*+ nestloop(heap_tbl_a heap_tbl_b) */ heap_tbl_a.c1 ac1, heap_tbl_b.c2 bc2 
	from heap_tbl_a, heap_tbl_b
	where heap_tbl_a.c2 = heap_tbl_b.c2
) group by 1 order by 1;

explain (costs off)
select /*+ nestloop(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2
from heap_tbl_a, heap_tbl_c
where heap_tbl_a.c2 = heap_tbl_c.c2;

select ac1, sum(bc2) from (
	select /*+ nestloop(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2
	from heap_tbl_a, heap_tbl_c
	where heap_tbl_a.c2 = heap_tbl_c.c2
) group by 1 order by 1;


set enable_indexscan=on;
set enable_indexonlyscan=on;
set enable_bitmapscan=off;
set enable_material=off;
set enable_seqscan=off;

explain (costs off)
select /*+ nestloop(heap_tbl_a heap_tbl_b) */ heap_tbl_a.c1 ac1, heap_tbl_b.c2 bc2
from heap_tbl_a, heap_tbl_b
where heap_tbl_a.c2 = heap_tbl_b.c2;

select ac1, sum(bc2) from (
	select /*+ nestloop(heap_tbl_a heap_tbl_b) */ heap_tbl_a.c1 ac1, heap_tbl_b.c2 bc2
	from heap_tbl_a, heap_tbl_b
	where heap_tbl_a.c2 = heap_tbl_b.c2
) group by 1 order by 1;

explain (costs off)
select /*+ mergejoin(heap_tbl_a heap_tbl_b) */ heap_tbl_a.c1 ac1, heap_tbl_b.c2 bc2
from heap_tbl_a, heap_tbl_b
where heap_tbl_a.c2 = heap_tbl_b.c2;

select ac1, sum(bc2) from (
	select /*+ mergejoin(heap_tbl_a heap_tbl_b) */ heap_tbl_a.c1 ac1, heap_tbl_b.c2 bc2
	from heap_tbl_a, heap_tbl_b
	where heap_tbl_a.c2 = heap_tbl_b.c2
) group by 1 order by 1;

explain (costs off)
select /*+ mergejoin(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2
from heap_tbl_a, heap_tbl_c
where heap_tbl_a.c2 = heap_tbl_c.c2;

select ac1, sum(bc2) from (
	select /*+ mergejoin(heap_tbl_a heap_tbl_b) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2
	from heap_tbl_a, heap_tbl_c
	where heap_tbl_a.c2 = heap_tbl_c.c2
) group by 1 order by 1;

explain (costs off)
select /*+ hashjoin(heap_tbl_a heap_tbl_b) */ heap_tbl_a.c1 ac1, heap_tbl_b.c2 bc2
from heap_tbl_a, heap_tbl_b
where heap_tbl_a.c2 = heap_tbl_b.c2;

select ac1, sum(bc2) from (
	select /*+ hashjoin(heap_tbl_a heap_tbl_b) */ heap_tbl_a.c1 ac1, heap_tbl_b.c2 bc2
	from heap_tbl_a, heap_tbl_b
	where heap_tbl_a.c2 = heap_tbl_b.c2
) group by 1 order by 1;

explain (costs off)
select /*+ hashjoin(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2
from heap_tbl_a, heap_tbl_c
where heap_tbl_a.c2 = heap_tbl_c.c2;

select ac1, sum(bc2) from (
	select /*+ hashjoin(heap_tbl_a heap_tbl_b) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2
	from heap_tbl_a, heap_tbl_c
	where heap_tbl_a.c2 = heap_tbl_c.c2
) group by 1 order by 1;

-- test three table join
explain (costs off)
select /*+ nestloop(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2 
from heap_tbl_a, heap_tbl_b, heap_tbl_c
where heap_tbl_a.c2 = heap_tbl_c.c2 and heap_tbl_c.c2 = heap_tbl_b.c2;

select ac1, sum(bc2) from (
	select /*+ nestloop(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2 
	from heap_tbl_a, heap_tbl_b, heap_tbl_c
	where heap_tbl_a.c2 = heap_tbl_c.c2 and heap_tbl_c.c2 = heap_tbl_b.c2
) group by 1 order by 1;

explain (costs off)
select /*+ nestloop(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2 
from heap_tbl_a, heap_tbl_b, heap_tbl_c
where heap_tbl_a.c2 = heap_tbl_c.c2 and heap_tbl_c.c2 = heap_tbl_b.c2 and heap_tbl_a.c1 = 1
order by 1, 2;

select /*+ nestloop(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2 
from heap_tbl_a, heap_tbl_b, heap_tbl_c
where heap_tbl_a.c2 = heap_tbl_c.c2 and heap_tbl_c.c2 = heap_tbl_b.c2 and heap_tbl_a.c1 = 1
order by 1, 2;

explain (costs off)
select /*+ hashjoin(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2 
from heap_tbl_a, heap_tbl_b, heap_tbl_c
where heap_tbl_a.c2 = heap_tbl_c.c2 and heap_tbl_c.c2 = heap_tbl_b.c2;

select ac1, sum(bc2) from (
	select /*+ hashjoin(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2 
	from heap_tbl_a, heap_tbl_b, heap_tbl_c
	where heap_tbl_a.c2 = heap_tbl_c.c2 and heap_tbl_c.c2 = heap_tbl_b.c2
) group by 1 order by 1;

explain (costs off)
select /*+ hashjoin(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2 
from heap_tbl_a, heap_tbl_b, heap_tbl_c
where heap_tbl_a.c2 = heap_tbl_c.c2 and heap_tbl_c.c2 = heap_tbl_b.c2 and heap_tbl_a.c1 = 1
order by 1, 2;

select /*+ hashjoin(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2 
from heap_tbl_a, heap_tbl_b, heap_tbl_c
where heap_tbl_a.c2 = heap_tbl_c.c2 and heap_tbl_c.c2 = heap_tbl_b.c2 and heap_tbl_a.c1 = 1
order by 1, 2;

explain (costs off)
select /*+ mergejoin(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2 
from heap_tbl_a, heap_tbl_b, heap_tbl_c
where heap_tbl_a.c2 = heap_tbl_c.c2 and heap_tbl_c.c2 = heap_tbl_b.c2;

select ac1, sum(bc2) from (
	select /*+ mergejoin(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2 
	from heap_tbl_a, heap_tbl_b, heap_tbl_c
	where heap_tbl_a.c2 = heap_tbl_c.c2 and heap_tbl_c.c2 = heap_tbl_b.c2
) group by 1 order by 1;

explain (costs off)
select /*+ mergejoin(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2 
from heap_tbl_a, heap_tbl_b, heap_tbl_c
where heap_tbl_a.c2 = heap_tbl_c.c2 and heap_tbl_c.c2 = heap_tbl_b.c2 and heap_tbl_a.c1 = 1
order by 1, 2;

select /*+ mergejoin(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2 
from heap_tbl_a, heap_tbl_b, heap_tbl_c
where heap_tbl_a.c2 = heap_tbl_c.c2 and heap_tbl_c.c2 = heap_tbl_b.c2 and heap_tbl_a.c1 = 1
order by 1, 2;


set enable_indexscan=off;
set enable_indexonlyscan=off;
set enable_bitmapscan=on;
set enable_material=off;
set enable_seqscan=off;

explain (costs off)
select /*+ nestloop(heap_tbl_a heap_tbl_b) */ heap_tbl_a.c1 ac1, heap_tbl_b.c2 bc2 
from heap_tbl_a, heap_tbl_b
where heap_tbl_a.c2 = heap_tbl_b.c2;

select ac1, sum(bc2) from (
	select /*+ nestloop(heap_tbl_a heap_tbl_b) */ heap_tbl_a.c1 ac1, heap_tbl_b.c2 bc2 
	from heap_tbl_a, heap_tbl_b
	where heap_tbl_a.c2 = heap_tbl_b.c2
) group by 1 order by 1;

explain (costs off)
select /*+ nestloop(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2 
from heap_tbl_a, heap_tbl_c
where heap_tbl_a.c2 = heap_tbl_c.c2;

select ac1, sum(bc2) from (
	select /*+ nestloop(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2 
	from heap_tbl_a, heap_tbl_c
	where heap_tbl_a.c2 = heap_tbl_c.c2
) group by 1 order by 1;

explain (costs off)
select /*+ mergejoin(heap_tbl_a heap_tbl_b) */ heap_tbl_a.c1 ac1, heap_tbl_b.c2 bc2 
from heap_tbl_a, heap_tbl_b
where heap_tbl_a.c2 = heap_tbl_b.c2;

select ac1, sum(bc2) from (
	select /*+ mergejoin(heap_tbl_a heap_tbl_b) */ heap_tbl_a.c1 ac1, heap_tbl_b.c2 bc2 
	from heap_tbl_a, heap_tbl_b
	where heap_tbl_a.c2 = heap_tbl_b.c2
) group by 1 order by 1;

explain (costs off)
select /*+ mergejoin(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2 
from heap_tbl_a, heap_tbl_c
where heap_tbl_a.c2 = heap_tbl_c.c2;

select ac1, sum(bc2) from (
	select /*+ mergejoin(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2 
	from heap_tbl_a, heap_tbl_c
	where heap_tbl_a.c2 = heap_tbl_c.c2
) group by 1 order by 1;

explain (costs off)
select /*+ hashjoin(heap_tbl_a heap_tbl_b) */ heap_tbl_a.c1 ac1, heap_tbl_b.c2 bc2 
from heap_tbl_a, heap_tbl_b
where heap_tbl_a.c2 = heap_tbl_b.c2;

select ac1, sum(bc2) from (
	select /*+ hashjoin(heap_tbl_a heap_tbl_b) */ heap_tbl_a.c1 ac1, heap_tbl_b.c2 bc2 
	from heap_tbl_a, heap_tbl_b
	where heap_tbl_a.c2 = heap_tbl_b.c2
) group by 1 order by 1;

explain (costs off)
select /*+ hashjoin(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2 
from heap_tbl_a, heap_tbl_c
where heap_tbl_a.c2 = heap_tbl_c.c2;

select ac1, sum(bc2) from (
	select /*+ hashjoin(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2 
	from heap_tbl_a, heap_tbl_c
	where heap_tbl_a.c2 = heap_tbl_c.c2
) group by 1 order by 1;

-- test three table join
explain (costs off)
select /*+ nestloop(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2 
from heap_tbl_a, heap_tbl_b, heap_tbl_c
where heap_tbl_a.c2 = heap_tbl_c.c2 and heap_tbl_c.c2 = heap_tbl_b.c2;

select ac1, sum(bc2) from (
	select /*+ nestloop(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2 
	from heap_tbl_a, heap_tbl_b, heap_tbl_c
	where heap_tbl_a.c2 = heap_tbl_c.c2 and heap_tbl_c.c2 = heap_tbl_b.c2
) group by 1 order by 1;

explain (costs off)
select /*+ nestloop(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2 
from heap_tbl_a, heap_tbl_b, heap_tbl_c
where heap_tbl_a.c2 = heap_tbl_c.c2 and heap_tbl_c.c2 = heap_tbl_b.c2 and heap_tbl_a.c1 = 1
order by 1, 2;

select /*+ nestloop(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2 
from heap_tbl_a, heap_tbl_b, heap_tbl_c
where heap_tbl_a.c2 = heap_tbl_c.c2 and heap_tbl_c.c2 = heap_tbl_b.c2 and heap_tbl_a.c1 = 1
order by 1, 2;

explain (costs off)
select /*+ hashjoin(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2 
from heap_tbl_a, heap_tbl_b, heap_tbl_c
where heap_tbl_a.c2 = heap_tbl_c.c2 and heap_tbl_c.c2 = heap_tbl_b.c2;

select ac1, sum(bc2) from (
	select /*+ hashjoin(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2 
	from heap_tbl_a, heap_tbl_b, heap_tbl_c
	where heap_tbl_a.c2 = heap_tbl_c.c2 and heap_tbl_c.c2 = heap_tbl_b.c2
) group by 1 order by 1;

explain (costs off)
select /*+ hashjoin(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2 
from heap_tbl_a, heap_tbl_b, heap_tbl_c
where heap_tbl_a.c2 = heap_tbl_c.c2 and heap_tbl_c.c2 = heap_tbl_b.c2 and heap_tbl_a.c1 = 1
order by 1, 2;

select /*+ hashjoin(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2 
from heap_tbl_a, heap_tbl_b, heap_tbl_c
where heap_tbl_a.c2 = heap_tbl_c.c2 and heap_tbl_c.c2 = heap_tbl_b.c2 and heap_tbl_a.c1 = 1
order by 1, 2;

explain (costs off)
select /*+ mergejoin(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2 
from heap_tbl_a, heap_tbl_b, heap_tbl_c
where heap_tbl_a.c2 = heap_tbl_c.c2 and heap_tbl_c.c2 = heap_tbl_b.c2;

select ac1, sum(bc2) from (
	select /*+ mergejoin(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2 
	from heap_tbl_a, heap_tbl_b, heap_tbl_c
	where heap_tbl_a.c2 = heap_tbl_c.c2 and heap_tbl_c.c2 = heap_tbl_b.c2
) group by 1 order by 1;

explain (costs off)
select /*+ mergejoin(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2 
from heap_tbl_a, heap_tbl_b, heap_tbl_c
where heap_tbl_a.c2 = heap_tbl_c.c2 and heap_tbl_c.c2 = heap_tbl_b.c2 and heap_tbl_a.c1 = 1
order by 1, 2;

select /*+ mergejoin(heap_tbl_a heap_tbl_c) */ heap_tbl_a.c1 ac1, heap_tbl_c.c2 bc2 
from heap_tbl_a, heap_tbl_b, heap_tbl_c
where heap_tbl_a.c2 = heap_tbl_c.c2 and heap_tbl_c.c2 = heap_tbl_b.c2 and heap_tbl_a.c1 = 1
order by 1, 2;

drop table heap_tbl_a;
drop table heap_tbl_b;
drop table heap_tbl_c;
