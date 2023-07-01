create schema sortgroupagg;
set search_path=sortgroupagg;

create table tbl_10k(id bigint, v1 numeric, v2 char(150));
insert into tbl_10k select generate_series(1, 10 * 1000), (RANDOM() * 67)::int::numeric + 10e-100, (RANDOM() * 77)::int::numeric+10e-100;
analyze tbl_10k;

set enable_sortgroup_agg=on;

explain (costs off) select sum(id), v1,v2 from tbl_10k group by v1,v2 order by v1,v2 limit 1;

-- order keys are not contained in group keys, needs sorts after aggregation
explain (costs off) select sum(id), v1,v2 from tbl_10k group by v1,v2 order by v1,sum(id) limit 10;
create table agg_1 as
select sum(id), v1,v2 from tbl_10k group by v1,v2 order by v1,sum(id) limit 10 offset 11;

set enable_sortgroup_agg=off;
create table agg_2 as
select sum(id), v1,v2 from tbl_10k group by v1,v2 order by v1,sum(id) limit 10 offset 11;

-- Compare results to hash aggregation results
(select * from agg_1 except select * from agg_2)
  union all
(select * from agg_2 except select * from agg_1);

drop table agg_1, agg_2;


set enable_sortgroup_agg=on;
-- In the following cases, we cannot perform sortgroup

-- 1. plain agg
explain (costs off) select  count(*) from tbl_10k limit 1;

-- 2. HAVING clauses
explain (costs off) select sum(id), v1,v2 from tbl_10k group by v1,v2 having v1+v2>0 order by v1,v2  limit 1;

--3. distinct
explain (costs off) select distinct(v1,v2) from tbl_10k group by v1,v2 limit 1;

--4. grouping sets
explain (costs off) select sum(v1),v1 from tbl_10k group by grouping sets((v1),(v2)) order by v1 desc limit 1;

-- 5. winows
explain (costs off) SELECT v1, avg(v1) OVER (PARTITION BY v2) FROM tbl_10k group by v1, v2 order by v1,v2 limit 1;

--6. no LIMIT cluases
explain (costs off) select sum(id), v1,v2 from tbl_10k group by v1,v2 order by v1,v2;

set enable_hashagg =off;
set enable_sort=off;

-- GROUP BY single key
explain (costs off) select avg(v2), v2 from tbl_10k group by v2 order by v2 limit 1000 offset 10;

create table agg_sortgroup_1 as
select avg(v2), v2 from tbl_10k group by v2 order by v2 limit 1000 offset 10;
create table agg_sortgroup_2 as
select avg(v1), v1 from tbl_10k group by v1 order by v1 desc limit 1000 offset 10;

set work_mem =64;
create table agg_sortgroup_disk_1 as
select avg(v2), v2 from tbl_10k group by v2 order by v2 limit 1000 offset 10;
create table agg_sortgroup_disk_2 as
select avg(v1), v1 from tbl_10k group by v1 order by v1 desc limit 1000 offset 10;
set work_mem =default;

set enable_hashagg =on;
set enable_sortgroup_agg=off;
set enable_sort=on;

create table agg_hashagg_1 as
select avg(v2), v2 from tbl_10k group by v2 order by v2 limit 1000 offset 10;
create table agg_hashagg_2 as
select avg(v1), v1 from tbl_10k group by v1 order by v1 desc limit 1000 offset 10;

-- Compare results to hash aggregation results
(select * from agg_sortgroup_1 except select * from agg_hashagg_1)
  union all
(select * from agg_hashagg_1 except select * from agg_sortgroup_1);

(select * from agg_sortgroup_2 except select * from agg_hashagg_2)
  union all
(select * from agg_hashagg_2 except select * from agg_sortgroup_2);

(select * from agg_sortgroup_disk_1 except select * from agg_hashagg_1)
  union all
(select * from agg_hashagg_1 except select * from agg_sortgroup_disk_1);

(select * from agg_sortgroup_disk_2 except select * from agg_hashagg_2)
  union all
(select * from agg_hashagg_2 except select * from agg_sortgroup_disk_2);

drop table agg_sortgroup_1,agg_sortgroup_2,agg_hashagg_1,agg_hashagg_2, agg_sortgroup_disk_1, agg_sortgroup_disk_2;

-- GROUP BY multiple keys
set enable_sortgroup_agg=on;
set enable_hashagg =off;
set enable_sort=off;

explain (costs off) select sum(v2+v1), v2,v1 from tbl_10k group by v2,v1 order by v2 desc ,v1 asc limit 1000 offset 10;

create table agg_sortgroup_1 as
select sum(v2+v1), v2,v1 from tbl_10k group by v2,v1 order by v2 desc ,v1 asc limit 1000 offset 10;
create table agg_sortgroup_2 as
select sum(v2+v1), v2,v1 from tbl_10k group by v1,v2 order by v1 asc ,v2 desc limit 1000 offset 10;

set work_mem =64;
create table agg_sortgroup_disk_1 as
select sum(v2+v1), v2,v1 from tbl_10k group by v2,v1 order by v2 desc ,v1 asc limit 1000 offset 10;
create table agg_sortgroup_disk_2 as
select sum(v2+v1), v2,v1 from tbl_10k group by v1,v2 order by v1 asc ,v2 desc limit 1000 offset 10;
set work_mem =default;

set enable_sortgroup_agg=off;
set enable_hashagg =on;
set enable_sort=on;

create table agg_hashagg_1 as
select sum(v2+v1), v2,v1 from tbl_10k group by v2,v1 order by v2 desc ,v1 asc limit 1000 offset 10;
create table agg_hashagg_2 as
select sum(v2+v1), v2,v1 from tbl_10k group by v1,v2 order by v1 asc ,v2 desc limit 1000 offset 10;

-- Compare results to hash aggregation results
(select * from agg_sortgroup_1 except select * from agg_hashagg_1)
  union all
(select * from agg_hashagg_1 except select * from agg_sortgroup_1);

(select * from agg_sortgroup_2 except select * from agg_hashagg_2)
  union all
(select * from agg_hashagg_2 except select * from agg_sortgroup_2);


(select * from agg_sortgroup_disk_1 except select * from agg_hashagg_1)
  union all
(select * from agg_hashagg_1 except select * from agg_sortgroup_disk_1);

(select * from agg_sortgroup_disk_2 except select * from agg_hashagg_2)
  union all
(select * from agg_hashagg_2 except select * from agg_sortgroup_disk_2);

drop table agg_sortgroup_1,agg_sortgroup_2,agg_hashagg_1,agg_hashagg_2,agg_sortgroup_disk_1, agg_sortgroup_disk_2;

-- already sorted, we don't consider sortgroup aggregation
set enable_sortgroup_agg=on;
set enable_hashagg =off;
set enable_sort=off;

explain (costs off) select avg(v1), v1 from tbl_10k group by v1 order by v1 limit 1;

set enable_seqscan=off;
create index v1_index on tbl_10k (v1);
analyze tbl_10k;

explain (costs off) select avg(v1), v1 from tbl_10k group by v1 order by v1 limit 1;
drop index v1_index;
set enable_seqscan=on;


-- test ExecReScanSortGroup

set enable_sortgroup_agg=on;
set enable_hashagg =off;
set enable_sort=off;
set enable_material =off;

explain (costs off)
WITH t1 AS (
    SELECT v1::int % 10 as a1, SUM(id) as b1
    FROM tbl_10k
    GROUP BY v1::int % 10 order by v1::int % 10 limit 100
), t2 AS (
    SELECT v2::char(5)::numeric::int % 10 as a2, SUM(id) as b2
    FROM tbl_10k
    GROUP BY v2::char(5)::numeric::int % 10 order by v2::char(5)::numeric::int % 10 limit 101
)
select a1, a2, b1+b2 from t1 inner join t2 on (b1 + b2 > 10);


create table mem_rescan_1 as
WITH t1 AS (
    SELECT v1::int % 10 as a1, SUM(id) as b1
    FROM tbl_10k
    GROUP BY v1::int % 10 order by v1::int % 10 limit 100
), t2 AS (
    SELECT v2::char(5)::numeric::int % 10 as a2, SUM(id) as b2
    FROM tbl_10k
    GROUP BY v2::char(5)::numeric::int % 10 order by v2::char(5)::numeric::int % 10 limit 101
)
select a1, a2, b1+b2 from t1 inner join t2 on (b1 + b2 > 10);

set work_mem =64;

create table disk_rescan_1 as
WITH t1 AS (
    SELECT v1::int % 10 as a1, SUM(id) as b1
    FROM tbl_10k
    GROUP BY v1::int % 10 order by v1::int % 10 limit 100
), t2 AS (
    SELECT v2::char(5)::numeric::int % 10 as a2, SUM(id) as b2
    FROM tbl_10k
    GROUP BY v2::char(5)::numeric::int % 10 order by v2::char(5)::numeric::int % 10 limit 101
)
select a1, a2, b1+b2 from t1 inner join t2 on (b1 + b2 > 10);

-- Compare results between MEMORY SORT and DISK SORT
(select * from mem_rescan_1 except select * from disk_rescan_1)
  union all
(select * from disk_rescan_1 except select * from mem_rescan_1);

set work_mem =default;
set enable_sortgroup_agg=off;
set enable_hashagg =on;
set enable_sort=on;

create table hashagg_rescan_1 as
WITH t1 AS (
    SELECT v1::int % 10 as a1, SUM(id) as b1
    FROM tbl_10k
    GROUP BY v1::int % 10 order by v1::int % 10 limit 100
), t2 AS (
    SELECT v2::char(5)::numeric::int % 10 as a2, SUM(id) as b2
    FROM tbl_10k
    GROUP BY v2::char(5)::numeric::int % 10 order by v2::char(5)::numeric::int % 10 limit 101
)
select a1, a2, b1+b2 from t1 inner join t2 on (b1 + b2 > 10);

-- Compare results to hash aggregation results

-- hashagg_rescan_1 = mem_rescan_1 = disk_rescan_1

(select * from mem_rescan_1 except select * from hashagg_rescan_1)
  union all
(select * from hashagg_rescan_1 except select * from mem_rescan_1);

drop table mem_rescan_1,hashagg_rescan_1,disk_rescan_1;

drop table tbl_10k;

create table tbl_cstore_10k(id bigint, v1 numeric, v2 numeric) with (orientation = column);
insert into tbl_cstore_10k select generate_series(1, 10 * 1000), (RANDOM() * 67)::int::numeric, (RANDOM() * 77)::int::numeric;
analyze tbl_cstore_10k;

set enable_sortgroup_agg=on;
set enable_hashagg =off;
set enable_sort=off;

explain (costs off) select sum(id), v1,v2 from tbl_cstore_10k group by v1,v2 order by v1,v2 limit 11 offset 10;
create table agg_sortgroup_1 as
select sum(id), v1,v2 from tbl_cstore_10k group by v1,v2 order by v1,v2 limit 11 offset 10;

set enable_sortgroup_agg=off;
set enable_hashagg =on;
set enable_sort=on;

create table agg_vecagg_1 as
select sum(id), v1,v2 from tbl_cstore_10k group by v1,v2 order by v1,v2 limit 11 offset 10;


(select * from agg_sortgroup_1 except select * from agg_vecagg_1)
  union all
(select * from agg_vecagg_1 except select * from agg_sortgroup_1);

drop table tbl_cstore_10k, agg_sortgroup_1,agg_vecagg_1;


create table tbl_ustore_10k(id bigint, v1 numeric, v2 numeric) with (storage_type=ustore);
insert into tbl_ustore_10k select generate_series(1, 10 * 1000), (RANDOM() * 67)::int::numeric, (RANDOM() * 77)::int::numeric;
analyze tbl_ustore_10k;

set enable_sortgroup_agg=on;
set enable_hashagg =off;
set enable_sort=off;

explain (costs off) select sum(id), v1,v2 from tbl_ustore_10k group by v1,v2 order by v1,v2 limit 11 offset 10;
create table agg_sortgroup_1 as
select sum(id), v1,v2 from tbl_ustore_10k group by v1,v2 order by v1,v2 limit 11 offset 10;

set enable_sortgroup_agg=off;
set enable_hashagg =on;
set enable_sort=on;

create table agg_hashagg_1 as
select sum(id), v1,v2 from tbl_ustore_10k group by v1,v2 order by v1,v2 limit 11 offset 10;


(select * from agg_sortgroup_1 except select * from agg_hashagg_1)
  union all
(select * from agg_hashagg_1 except select * from agg_sortgroup_1);

drop table tbl_ustore_10k, agg_sortgroup_1,agg_hashagg_1;

drop schema sortgroupagg cascade;