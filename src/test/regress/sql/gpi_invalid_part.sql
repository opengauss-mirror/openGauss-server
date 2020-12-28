drop table if exists gpi_test_invalid_part;
create table gpi_test_invalid_part(a int, b int,c int) partition by range(a) (partition p1 values less than (100), partition p2 values  less than (200), partition p3 values less than (500));
select a.relname,a.parttype,a.reloptions from pg_partition a, pg_class b where a.parentid = b.oid and b.relname = 'gpi_test_invalid_part' order by 1,2,3;
insert into gpi_test_invalid_part select r,r,100 from generate_series(0,400) as r;
create index global_index_gpi_test_invalid_part_b on gpi_test_invalid_part (b) global;
create index local_index_gpi_test_invalid_part_c on gpi_test_invalid_part (c) local;
vacuum analyze gpi_test_invalid_part;
--Scenario 1: abort one create partition
start transaction;
alter table gpi_test_invalid_part add partition p6 values less than (900);
select a.relname,a.parttype,a.reloptions from pg_partition a, pg_class b where a.parentid = b.oid and b.relname = 'gpi_test_invalid_part' and a.reloptions[3] like '%wait_clean_gpi=y%' order by 1,2,3;
insert into gpi_test_invalid_part select r,r,100 from generate_series(500,800) as r;
--p6/gpi_test_invalid_part reloptions have "wait_clean_gpi=y"
select a.relname,a.parttype,a.reloptions from pg_partition a, pg_class b where a.parentid = b.oid and b.relname = 'gpi_test_invalid_part' and a.reloptions[3] like '%wait_clean_gpi=y%' order by 1,2,3;
abort;

--gpi_test_invalid_part reloptions have "wait_clean_gpi=y"
select a.relname,a.parttype,a.reloptions from pg_partition a, pg_class b where a.parentid = b.oid and b.relname = 'gpi_test_invalid_part' and a.reloptions[3] like '%wait_clean_gpi=y%' order by 1,2,3;

start transaction read only;
set enable_show_any_tuples = on;
set enable_indexscan = off;
set enable_bitmapscan = off;
--p6 reloptions have "wait_clean_gpi=y"
select a.relname,a.parttype,a.reloptions from pg_partition a, pg_class b where a.parttype = 'p' and a.relname = 'p6' and b.relname = 'gpi_test_invalid_part' and a.reloptions[3] like '%wait_clean_gpi=y%' order by 1,2,3;
commit;

reset enable_indexscan;
reset enable_bitmapscan;
reset enable_show_any_tuples;
explain (costs off) select count(*) from gpi_test_invalid_part where b > 700;
--select nothing
select count(*) from gpi_test_invalid_part where b > 700;
--skip vacuum full
vacuum full pg_partition;
explain (costs off) select count(*) from gpi_test_invalid_part where b > 700;
--select nothing
select count(*) from gpi_test_invalid_part where b > 700;

--Scenario 2: abort one create partition in sub transaction
start transaction;
alter table gpi_test_invalid_part add partition p7 values less than (900);
savepoint s1;
alter table gpi_test_invalid_part add partition p8 values less than (1000);
insert into gpi_test_invalid_part select r,r,100 from generate_series(500,950) as r;
-- gpi_test_invalid_part and p7/p8 reloptions have "wait_clean_gpi=y"
select a.relname,a.parttype,a.reloptions from pg_partition a, pg_class b where a.parentid = b.oid and b.relname = 'gpi_test_invalid_part' and a.reloptions[3] like '%wait_clean_gpi=y%' order by 1,2,3;
rollback to s1;
commit;

--gpi_test_invalid_part reloptions have "wait_clean_gpi=y"
select a.relname,a.parttype,a.reloptions from pg_partition a, pg_class b where a.parentid = b.oid and b.relname = 'gpi_test_invalid_part' order by 1,2,3;

start transaction read only;
set enable_show_any_tuples = on;
set enable_indexscan = off;
set enable_bitmapscan = off;
--p7/p8 reloptions have "wait_clean_gpi=y"
select a.relname,a.parttype,a.reloptions from pg_partition a, pg_class b where a.parttype = 'p' and (a.relname = 'p7' or a.relname = 'p8') and b.relname = 'gpi_test_invalid_part' and a.reloptions[3] like '%wait_clean_gpi=y%' order by 1,2,3;
commit;

reset enable_indexscan;
reset enable_bitmapscan;
reset enable_show_any_tuples;

explain (costs off) select count(*) from gpi_test_invalid_part where b > 700;
--select nothing
select count(*) from gpi_test_invalid_part where b > 700;
--skip vacuum full
vacuum full pg_partition;
explain (costs off) select count(*) from gpi_test_invalid_part where b > 700;
--select nothing
select count(*) from gpi_test_invalid_part where b > 700;


--Scenario 3: drop one create partition
start transaction;
alter table gpi_test_invalid_part add partition p9 values less than (1000);
insert into gpi_test_invalid_part select r,r,100 from generate_series(950,990) as r;
--p9 reloptions have "wait_clean_gpi=y"
select a.relname,a.parttype,a.reloptions from pg_partition a, pg_class b  where a.parttype = 'p' and a.relname = 'p9' and a.reloptions[3] like '%wait_clean_gpi=y%' and b.relname = 'gpi_test_invalid_part' order by 1,2,3;
alter table gpi_test_invalid_part drop partition p9;
--p9 not exists
select a.relname,a.parttype,a.reloptions from pg_partition a, pg_class b  where a.parttype = 'p' and a.relname = 'p9' and a.reloptions[3] like '%wait_clean_gpi=y%' and b.relname = 'gpi_test_invalid_part' order by 1,2,3;
commit;

-- gpi_test_invalid_part reloptions have "wait_clean_gpi=y"
select a.relname,a.parttype,a.reloptions from pg_partition a, pg_class b where a.reloptions[3] like '%wait_clean_gpi=y%' and b.relname = 'gpi_test_invalid_part' order by 1,2,3;

start transaction read only;
set enable_show_any_tuples = on;
set enable_indexscan = off;
set enable_bitmapscan = off;
--p9 reloptions have "wait_clean_gpi=y"
select a.relname,a.parttype,a.reloptions from pg_partition a, pg_class b where a.parttype = 'p' and a.relname = 'p9' and a.reloptions[3] like '%wait_clean_gpi=y%' and b.relname = 'gpi_test_invalid_part' order by 1,2,3;
commit;

reset enable_indexscan;
reset enable_bitmapscan;
reset enable_show_any_tuples;
--use seqscan
explain (costs off) select count(*) from gpi_test_invalid_part where b > 700;
--drop partition set global index unusable
reindex index global_index_gpi_test_invalid_part_b;
explain (costs off) select count(*) from gpi_test_invalid_part where b > 700;
--select nothing
select count(*) from gpi_test_invalid_part where b > 700;
--skip vacuum full
vacuum full pg_partition;
explain (costs off) select count(*) from gpi_test_invalid_part where b > 700;
--select nothing
select count(*) from gpi_test_invalid_part where b > 700;

--Scenario 4: create one partition, sub transaction drop and rollback
start transaction;
alter table gpi_test_invalid_part add partition p9 values less than (1000);
insert into gpi_test_invalid_part select r,r,100 from generate_series(950,990) as r;
savepoint s1;
--p9 reloptions have "wait_clean_gpi=y"
select a.relname,a.parttype,a.reloptions from pg_partition a, pg_class b where a.parttype = 'p' and a.relname = 'p9' and a.reloptions[3] like '%wait_clean_gpi=y%' and b.relname = 'gpi_test_invalid_part' order by 1,2,3;
alter table gpi_test_invalid_part drop partition p9;
--p9 not exists
select a.relname,a.parttype,a.reloptions from pg_partition a, pg_class b where a.parttype = 'p' and a.relname = 'p9' and a.reloptions[3] like '%wait_clean_gpi=y%' and b.relname = 'gpi_test_invalid_part' order by 1,2,3;
rollback to s1;
--p9 reloptions have "wait_clean_gpi=y"
select a.relname,a.parttype,a.reloptions from pg_partition a, pg_class b where a.parttype = 'p' and a.relname = 'p9' and a.reloptions[3] like '%wait_clean_gpi=y%' and b.relname = 'gpi_test_invalid_part' order by 1,2,3;
commit;

-- gpi_test_invalid_part and p9 reloptions have "wait_clean_gpi=y"
select a.relname,a.parttype,a.reloptions from pg_partition a, pg_class b where a.reloptions[3] like '%wait_clean_gpi=y%' and b.relname = 'gpi_test_invalid_part' order by 1,2,3;

explain (costs off) select count(*) from gpi_test_invalid_part where b > 700;
--select insert 41 rows
select count(*) from gpi_test_invalid_part where b > 700;
--skip vacuum full
vacuum full pg_partition;
explain (costs off) select count(*) from gpi_test_invalid_part where b > 700;
--select insert 41 rows
select count(*) from gpi_test_invalid_part where b > 700;


--Scenario 5: create partition, sub transaction create one partition, rollback;
drop table if exists gpi_test_create_invalid;
start transaction;
create table gpi_test_create_invalid(a int, b int,c int) partition by range(a) (partition p1 values less than (100), partition p2 values  less than (200), partition p3 values less than (500));
insert into  gpi_test_create_invalid select r,r,100 from generate_series(0,400) as r;
--gpi_test_create_invalid have wait_clean_gpi=n
select a.relname,a.parttype,a.reloptions from pg_partition a, pg_class b where a.parentid = b.oid and b.relname = 'gpi_test_create_invalid' and a.reloptions[3] like '%wait_clean_gpi=y%' order by 1,2,3;
create index global_gpi_test_create_invalid_b on gpi_test_create_invalid (b) global;
create index local_gpi_test_create_invalid_c on gpi_test_create_invalid (c) local;
savepoint s1;
alter table gpi_test_create_invalid add partition p6 values less than (800);
insert into gpi_test_create_invalid select r,r,100 from generate_series(500,790) as r;
--p9 reloptions have "wait_clean_gpi=y"
select a.relname,a.parttype,a.reloptions from pg_partition a, pg_class b where a.parttype = 'p' and a.relname = 'p6' and a.reloptions[3] like '%wait_clean_gpi=y%' and b.relname = 'gpi_test_create_invalid' order by 1,2,3;
rollback to s1;
commit;

--gpi_test_create_invalid have wait_clean_gpi=y
select a.relname,a.parttype,a.reloptions from pg_partition a, pg_class b where a.parentid = b.oid and b.relname = 'gpi_test_create_invalid' and a.reloptions[3] like '%wait_clean_gpi=y%' order by 1,2,3;

vacuum analyze gpi_test_create_invalid;
explain (costs off) select count(*) from gpi_test_create_invalid where b > 700;
--select nothing
select count(*) from gpi_test_create_invalid where b > 700;
----skip vacuum full
vacuum full pg_partition;
explain (costs off) select count(*) from gpi_test_create_invalid where b > 700;
--select nothing
select count(*) from gpi_test_create_invalid where b > 700;

--clean data
drop table if exists gpi_test_invalid_part;
drop table if exists gpi_test_create_invalid;
