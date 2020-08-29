--clean and create data
drop table if exists gpi_index_test;
create table gpi_index_test(a int, b int, c text) partition by range(a) (partition p1 values less than (100), partition p2 values  less than (200), partition p3 values less than (500), partition p4 values less than (maxvalue));
insert into gpi_index_test select r,r,'gpi_index_test' from generate_series(0,1000) as r;
create index gpi_index_test_global_a on gpi_index_test(a) global;
vacuum analyze gpi_index_test;

-- select in parition key and with global index
-- use global index
set enable_bitmapscan = off;
explain (costs off) select * from gpi_index_test where a < 200;
explain (costs off) select * from gpi_index_test where a < 200 order by a;
select count(*) from gpi_index_test where a < 200;

explain (costs off) select avg(b), a from gpi_index_test where a < 10 group by (a) having avg(b) < 100 order by a;
select avg(b), a from gpi_index_test where a < 10 group by (a) having avg(b) < 100 order by a;

-- multi index with global index
create index gpi_index_test_global_b on gpi_index_test(b) global;
explain (costs off) select * from gpi_index_test where a < 200 and b > 100;
explain (costs off) select * from gpi_index_test where a < 200 and b > 100 order by a;
select * from gpi_index_test where a < 200 and b > 190 order by a;

explain (costs off) select * from gpi_index_test where a < 200 or b > 100;
explain (costs off) select * from gpi_index_test where a < 200 or b > 100 order by a;
select * from gpi_index_test where a < 200 and b > 190 order by a;

-- multi column index with global index
create index gpi_index_test_global_a_b on gpi_index_test(a,b) global;
explain (costs off) select * from gpi_index_test where a = 200 and b = 100;
select * from gpi_index_test where a = 100 and b = 100 order by a;

-- select with local index and global index
drop index gpi_index_test_global_a_b;
drop index gpi_index_test_global_a;
drop index gpi_index_test_global_b;

create index gpi_index_test_local_a on gpi_index_test(a) local;
create index gpi_index_test_global_b on gpi_index_test(b) global;
vacuum analyze gpi_index_test;

explain (costs off) select * from gpi_index_test where (a < 200 or a < 300) and b > 100;

select * from gpi_index_test where a < 200 and b > 190 order by b;

-- use bitmapscan
reset enable_bitmapscan;
set force_bitmapand = on;
set enable_indexscan = off;
-- use bitmapand local index and global index and use Partition Iterator
explain (costs off) select * from gpi_index_test where a = 800 and b%4 <> 0 and b = 50;
select * from gpi_index_test where a < 800 and b%4 <> 0 and b <= 50;

-- must use seqscan
explain (costs off) select * from gpi_index_test where a = 200 or (b%4 = 0 and b <= 50);
select * from gpi_index_test where a = 200 or (b%4 = 0 and b <= 50);

drop index gpi_index_test_global_b;
create index gpi_index_test_local_b on gpi_index_test(b) local;
-- use bitmapor
explain (costs off) select * from gpi_index_test where a = 200 or (b%4 = 0 and b <= 50);
select * from gpi_index_test where a = 200 or (b%4 = 0 and b <= 50);
drop index gpi_index_test_local_b;
reset enable_indexscan;
reset force_bitmapand;

--two table join use global index scan
drop table if exists gpi_index_test1;
create table gpi_index_test1(a int, b int, c text) partition by range(a) (partition p1 values less than (100), partition p2 values  less than (200), partition p3 values less than (500), partition p4 values less than (maxvalue));
insert into gpi_index_test1 select r,r,'gpi_index_test' from generate_series(0,1000) as r;
create index gpi_index_test1_global_a on gpi_index_test1(a) global;
vacuum analyze gpi_index_test1;


-- single global index
--hash join
explain (costs off) select * from gpi_index_test1 a, gpi_index_test b where a.a = b.a;
select count(*) from gpi_index_test1 a, gpi_index_test b where a.a = b.a;

-- merge join
explain (costs off) select * from gpi_index_test1 a, gpi_index_test b where a.a = b.a and (a.b < 10 or b.b > 9990) order by a.a;
select * from gpi_index_test1 a, gpi_index_test b where a.a = b.a and (a.b < 10 or b.b > 9990) order by a.a;
select count(*) from gpi_index_test1 a, gpi_index_test b where a.a = b.a and (a.b < 10 or b.b > 10);

-- nestloop 
explain (costs off) select * from gpi_index_test1 a, gpi_index_test b where a.a < b.a and (a.b < 10 or b.b > 9990) order by a.a;
select count(*) from gpi_index_test1 a, gpi_index_test b where a.a < b.a and (a.b < 10 or b.b > 9990);

create index gpi_index_test_global_b on gpi_index_test(b) global;
explain (costs off) select count(*) from (select a from gpi_index_test where gpi_index_test.b < 100) as t1 inner join gpi_index_test1 t2 on t1.a = t2.a;
select count(*) from (select a from gpi_index_test where gpi_index_test.b < 100) as t1 inner join gpi_index_test1 t2 on t1.a = t2.a;

explain (costs off) select count(*) from gpi_index_test as t1 NATURAL inner join gpi_index_test1 t2 where t1.b < 200 and t2.a = 50;
select count(*) from gpi_index_test as t1 NATURAL inner join gpi_index_test1 t2 where t1.b < 200 and t2.a = 50;

explain (costs off) select * from gpi_index_test t1 full join gpi_index_test1 t2 on t1.b >= t2.a where t1.b < 200 and t2.a = 50 and t1.a = 100;
select * from gpi_index_test t1 full join gpi_index_test1 t2 on t1.b >= t2.a where t1.b < 200 and t2.a = 50 and t1.a = 100;

explain (costs off) select * from gpi_index_test t1 left join gpi_index_test1 t2 on (t1.b >= t2.a and (t1.a != 60 or t1.b != 30)) where t1.b < 200 and t2.a < 1000;
select count(*) from gpi_index_test t1 left join gpi_index_test1 t2 on (t1.b >= t2.a and (t1.a != 60 or t1.b != 30)) where t1.b < 200 and t2.a < 1000;

explain (costs off) select * from gpi_index_test t1 right join gpi_index_test1 t2 on (t1.b >= t2.a and (t1.a < 60 or t2.b != 30)) where t1.b < 200 and t2.a < 100;
select count(*) from gpi_index_test t1 left join gpi_index_test1 t2 on (t1.b >= t2.a and (t1.a != 60 or t1.b != 30)) where t1.b < 200 and t2.a < 100;


--three table join use global index scan
drop table if exists gpi_index_test2;
create table gpi_index_test2(a int, b int, c text) partition by range(a) (partition p1 values less than (100), partition p2 values  less than (200), partition p3 values less than (500), partition p4 values less than (maxvalue));
insert into gpi_index_test2 select r,r,'gpi_index_test' from generate_series(0,1000) as r;
create index gpi_index_test2_global_b on gpi_index_test2(b) global;
create index gpi_index_test2_global_a on gpi_index_test2(a) global;
vacuum analyze gpi_index_test2;

explain (costs off) select count(*) from (select a from gpi_index_test where gpi_index_test.b < 100) as t1 inner join gpi_index_test1 t2 on t1.a = t2.a left join gpi_index_test2 t3 on (t1.a = t3.a) where t3.b <= 10;
select count(*) from (select a from gpi_index_test where gpi_index_test.b < 100) as t1 inner join gpi_index_test1 t2 on t1.a = t2.a left join gpi_index_test2 t3 on (t1.a = t3.a) where t3.b <= 10;

create index gpi_index_test1_global_b on gpi_index_test1(b) global;
explain (costs off) select count(*) from (select a from gpi_index_test where gpi_index_test.b < 100) as t1 inner join gpi_index_test1 t2 on t1.a = t2.a left join gpi_index_test2 t3 on (t1.a = t3.a) where t3.b in (select b from gpi_index_test1 where gpi_index_test1.a <= 100);
select count(*) from (select a from gpi_index_test where gpi_index_test.b < 100) as t1 inner join gpi_index_test1 t2 on t1.a = t2.a left join gpi_index_test2 t3 on (t1.a = t3.a) where t3.b in (select b from gpi_index_test1 where gpi_index_test1.a <= 100);

insert into gpi_index_test1 values(100, 100, 'test_my');
insert into gpi_index_test2 values(100, 100, 'test_my');
insert into gpi_index_test values(100, 100, 'test_my');
explain (costs off) select * from (select a from gpi_index_test where gpi_index_test.b <= 100) as t1 inner join gpi_index_test1 t2 on t1.a = t2.a left join gpi_index_test2 t3 on (t1.a = t3.a) where t3.b in (select b from gpi_index_test1 where gpi_index_test1.b <= 100 and t3.c = 'test_my');
select * from (select a from gpi_index_test where gpi_index_test.b <= 100) as t1 inner join gpi_index_test1 t2 on t1.a = t2.a left join gpi_index_test2 t3 on (t1.a = t3.a) where t3.b in (select b from gpi_index_test1 where gpi_index_test1.b <= 100 and t3.c = 'test_my');

insert into gpi_index_test1 values(50, 40, 'test_my');
insert into gpi_index_test values(60, 60, 'test_my');
explain (costs off) select * from gpi_index_test t1 cross join gpi_index_test1 t2 where t1.a > t2.b and (t1.a + t2.b = 100) and t1.c = 'test_my'  order by t1.b;
select * from gpi_index_test t1 cross join gpi_index_test1 t2 where t1.a > t2.b and (t1.a + t2.b = 100) and t1.c = 'test_my' order by t1.b;

-- clean data
drop table gpi_index_test;
drop table gpi_index_tes1;
drop table gpi_index_test2;
