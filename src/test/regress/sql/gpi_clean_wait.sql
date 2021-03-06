drop table if exists test_gpi_more_invalid;
create table test_gpi_more_invalid(a int, b int, c int) partition by range(a) (partition p1 values less than (1001), partition p2 values  less than (2001), partition p3 values less than (3001));
insert into test_gpi_more_invalid select r,r,100 from generate_series(1,1000) as r;
insert into test_gpi_more_invalid select r,r,200 from generate_series(1001,2000) as r;
insert into test_gpi_more_invalid select r,r,300 from generate_series(2001,3000) as r;

create index global_index_gpi_more_invalid_b on test_gpi_more_invalid (a) global;
create index global_index_gpi_more_invalid_c on test_gpi_more_invalid (c) global;
create unique index global_index_gpi_more_invalid_a on test_gpi_more_invalid (b) global;
create index local_index_gpi_more_invalid_a_b on test_gpi_more_invalid (a,b) local;
create index global_index_gpi_more_invalid_a_b_c on test_gpi_more_invalid(a,b,c) global;
vacuum analyze test_gpi_more_invalid;

start transaction;
alter table test_gpi_more_invalid add partition p6 values less than (4001);
update test_gpi_more_invalid set a = 3000 + a where a <= 100;
select a.relname,a.parttype,a.reloptions from pg_partition a, pg_class b where a.parentid = b.oid and b.relname = 'test_gpi_more_invalid' and a.reloptions[3] like '%wait_clean_gpi=y%' order by 1,2,3;
explain (costs off) select * from test_gpi_more_invalid where a >= 3000;
--100 rows
select count(*) from test_gpi_more_invalid where a > 3000;
abort;

-- delete 100
delete test_gpi_more_invalid where b % 10 = 0 and c = 100;
-- delete 100
delete test_gpi_more_invalid where b % 10 = 0 and c = 200;
-- delete 100
delete test_gpi_more_invalid where b % 10 = 0 and c = 300;

-- after select's where condition
insert into test_gpi_more_invalid values(10, 100000, 100);
insert into test_gpi_more_invalid values(1020, 200000, 200);
insert into test_gpi_more_invalid values(2030, 300000, 300);

explain (costs off) select count(*) from test_gpi_more_invalid where a <= 100 and b = 100000;
explain (costs off) select count(*) from test_gpi_more_invalid where a > 3000;
-- 1 rows
select count(*) from test_gpi_more_invalid where a <= 100 and b = 100000;
-- 0 rows
select count(*) from test_gpi_more_invalid where a >= 3000;
-- test_gpi_more_invalid have wait_clean_gpi=y
select a.relname,a.parttype,a.reloptions from pg_partition a, pg_class b where a.parentid = b.oid and b.relname = 'test_gpi_more_invalid' and a.reloptions[3] like '%wait_clean_gpi=y%' order by 1,2,3;
vacuum analyze test_gpi_more_invalid;
-- test_gpi_more_invalid have wait_clean_gpi=n
select a.relname,a.parttype,a.reloptions from pg_partition a, pg_class b where a.parentid = b.oid and b.relname = 'test_gpi_more_invalid' and a.reloptions[3] like '%wait_clean_gpi=y%' order by 1,2,3;

explain (costs off) select count(*) from test_gpi_more_invalid where a <= 100;
explain (costs off) select count(*) from test_gpi_more_invalid where a >= 4000;
-- 91 rows
select count(*) from test_gpi_more_invalid where a <= 100;
-- 0 rows
select count(*) from test_gpi_more_invalid where a >= 4000;

explain (costs off) select count(*) from test_gpi_more_invalid where c = 100;
-- 900 rows
select count(*) from test_gpi_more_invalid where c = 100;
explain (costs off) select count(*) from test_gpi_more_invalid where c = 200;
-- 900 rows
select count(*) from test_gpi_more_invalid where c = 200;
explain (costs off) select count(*) from test_gpi_more_invalid where c = 300;
-- 900 rows
select count(*) from test_gpi_more_invalid where c = 300;

explain (costs off) delete test_gpi_more_invalid where b%5 != 0 and (c = 100 or c = 200 or c = 300);
delete test_gpi_more_invalid where b%5 != 0 and (c = 100 or c = 200 or c = 300);

start transaction;
alter table test_gpi_more_invalid add partition p6 values less than (4001);
insert into test_gpi_more_invalid select r,r,300 from generate_series(3001,4000) as r;
--failed
update test_gpi_more_invalid set b = 1001 where c = 200;
commit;

-- test_gpi_more_invalid have wait_clean_gpi=y
select a.relname,a.parttype,a.reloptions from pg_partition a, pg_class b where a.parentid = b.oid and b.relname = 'test_gpi_more_invalid' and a.reloptions[3] like '%wait_clean_gpi=y%' order by 1,2,3;

set enable_seqscan = off;
-- rows 100
explain (costs off) select count(*) from test_gpi_more_invalid where c = 200;
select count(*) from test_gpi_more_invalid where c = 200;

explain (costs off) select count(*) from test_gpi_more_invalid where c = 300;
select count(*) from test_gpi_more_invalid where c = 300;

explain (costs off) update test_gpi_more_invalid set a = a - 500 where a < 1500 and a >= 1000;
update test_gpi_more_invalid set a = a - 500 where a < 1500 and a >= 1000;
explain (costs off) update test_gpi_more_invalid set a = a + 500 where a < 1000 and a >= 500;
update test_gpi_more_invalid set a = a + 500 where a < 1000 and a >= 500;

explain (costs off) select count(*) from test_gpi_more_invalid where a < 1500 and a >= 1000;
select count(*) from test_gpi_more_invalid where a < 1500 and a >= 1000;
explain (costs off) select count(*) from test_gpi_more_invalid where a <= 1000;
select count(*) from test_gpi_more_invalid where a <= 1000;

set force_bitmapand = on;
-- partition 1
explain (costs off) select * from test_gpi_more_invalid where b = 100000 and c = 100;
select * from test_gpi_more_invalid where b = 100000 and c = 100;

--partition 2
explain (costs off) select * from test_gpi_more_invalid where b = 200000 and c = 200;
select * from test_gpi_more_invalid where b = 200000 and c = 200;
reset force_bitmapand;

alter table test_gpi_more_invalid DISABLE ROW MOVEMENT;
update test_gpi_more_invalid set a = a - 500 where a < 2000 and a > 1500;
-- failed
update test_gpi_more_invalid set a = a - 500 where a <= 1500;

alter table test_gpi_more_invalid ENABLE ROW MOVEMENT;

set force_bitmapand = on;
--partition 2
explain (costs off) select * from test_gpi_more_invalid where b = 200000 and c = 200;
select * from test_gpi_more_invalid where b = 200000 and c = 200;
reset force_bitmapand;

start transaction;
alter table test_gpi_more_invalid add partition p5 values less than (4001);
select * from test_gpi_more_invalid where b = 300000 and c = 700 for update;
update test_gpi_more_invalid set a = a + 1000 where a > 1000 or a < 500;
select count(*) from test_gpi_more_invalid where c = 100 and b = 2000;
-- test_gpi_more_invalid/p5 have wait_clean_gpi=y
select a.relname,a.parttype,a.reloptions from pg_partition a, pg_class b where a.parentid = b.oid and b.relname = 'test_gpi_more_invalid' and a.reloptions[3] like '%wait_clean_gpi=y%' order by 1,2,3;
abort;

--failed
set xc_maintenance_mode = on;
vacuum full pg_partition;
set xc_maintenance_mode = off;

-- test_gpi_more_invalid have wait_clean_gpi=y
select a.relname,a.parttype,a.reloptions from pg_partition a, pg_class b where a.parentid = b.oid and b.relname = 'test_gpi_more_invalid' and a.reloptions[3] like '%wait_clean_gpi=y%' order by 1,2,3;
vacuum analyze test_gpi_more_invalid;
-- test_gpi_more_invalid have wait_clean_gpi=n
select a.relname,a.parttype,a.reloptions from pg_partition a, pg_class b where a.parentid = b.oid and b.relname = 'test_gpi_more_invalid' and a.reloptions[3] like '%wait_clean_gpi=y%' order by 1,2,3;

-- success
set xc_maintenance_mode = on;
vacuum full pg_partition;
set xc_maintenance_mode = off;

set force_bitmapand = on;
-- partition 1
explain (costs off) select * from test_gpi_more_invalid where b = 100000 and c = 100;
select * from test_gpi_more_invalid where b = 100000 and c = 100;

--partition 2
explain (costs off) select * from test_gpi_more_invalid where b = 200000 and c = 200;
select * from test_gpi_more_invalid where b = 200000 and c = 200;

--partition 3
explain (costs off) select * from test_gpi_more_invalid where b = 300000 and c = 300;
select * from test_gpi_more_invalid where b = 300000 and c = 300;
reset force_bitmapand;

start transaction;
alter table test_gpi_more_invalid add partition p5 values less than (4001);
update test_gpi_more_invalid set a = a + 1000 where a > 1000 or a < 500;
alter table test_gpi_more_invalid drop partition p5;
commit;

-- all global index unusuable 
select c.relname, i.indisusable from pg_index i join pg_class c on i.indexrelid = c.oid where i.indrelid = 'test_gpi_more_invalid'::regclass ORDER BY c.relname;
-- test_gpi_more_invalid have wait_clean_gpi=y
select a.relname,a.parttype,a.reloptions from pg_partition a, pg_class b where a.parentid = b.oid and b.relname = 'test_gpi_more_invalid' and a.reloptions[3] like '%wait_clean_gpi=y%' order by 1,2,3;

vacuum full test_gpi_more_invalid;

-- all global index unusuable
select c.relname, i.indisusable from pg_index i join pg_class c on i.indexrelid = c.oid where i.indrelid = 'test_gpi_more_invalid'::regclass ORDER BY c.relname;
-- test_gpi_more_invalid have wait_clean_gpi=y
select a.relname,a.parttype,a.reloptions from pg_partition a, pg_class b where a.parentid = b.oid and b.relname = 'test_gpi_more_invalid' and a.reloptions[3] like '%wait_clean_gpi=y%' order by 1,2,3;

alter index local_index_gpi_more_invalid_a_b unusable;

reindex table test_gpi_more_invalid;
-- all global index unusuable
select c.relname, i.indisusable from pg_index i join pg_class c on i.indexrelid = c.oid where i.indrelid = 'test_gpi_more_invalid'::regclass ORDER BY c.relname;
alter index global_index_gpi_more_invalid_a rebuild;
alter index global_index_gpi_more_invalid_a_b_c rebuild;
alter index global_index_gpi_more_invalid_b rebuild;
alter index global_index_gpi_more_invalid_c rebuild;
-- all global index usuable
select c.relname, i.indisusable from pg_index i join pg_class c on i.indexrelid = c.oid where i.indrelid = 'test_gpi_more_invalid'::regclass ORDER BY c.relname;

reset enable_seqscan；
drop table test_gpi_more_invalid;

drop table if exists interval_normal_date;
CREATE TABLE interval_normal_date (logdate date not null, b int, c int)
PARTITION BY RANGE (logdate)
INTERVAL ('1 day')
(
        PARTITION interval_normal_date_p1 VALUES LESS THAN ('2020-03-01'),
        PARTITION interval_normal_date_p2 VALUES LESS THAN ('2020-05-01'),
        PARTITION interval_normal_date_p3 VALUES LESS THAN ('2020-06-01')
);

create index global_interval_index on interval_normal_date(b) global;
insert into interval_normal_date select '2020-6-01', 1000,r  from generate_series(0, 10000) as r;
vacuum interval_normal_date;

start transaction;
insert into interval_normal_date select '2020-6-02', r,r  from generate_series(0,500) as r;
explain (costs off) select count(*) from interval_normal_date where b <= 500;
select a.relname,a.parttype,a.reloptions from pg_partition a, pg_class b where a.parentid = b.oid and b.relname = 'interval_normal_date' and a.reloptions[3] like '%wait_clean_gpi=y%' order by 1,2,3;
alter table interval_normal_date drop partition sys_p2;
commit;
select a.relname,a.parttype,a.reloptions from pg_partition a, pg_class b where a.parentid = b.oid and b.relname = 'interval_normal_date' and a.reloptions[3] like '%wait_clean_gpi=y%' order by 1,2,3;
vacuum  analyze interval_normal_date;
select a.relname,a.parttype,a.reloptions from pg_partition a, pg_class b where a.parentid = b.oid and b.relname = 'interval_normal_date' and a.reloptions[3] like '%wait_clean_gpi=y%' order by 1,2,3;

drop table if exists interval_partition_table_vacuum;
create table interval_partition_table_vacuum
(
	c1 int,
	c2 int,
	logdate date not null,
	PRIMARY KEY(c2,logdate)
)
partition by range (logdate)
INTERVAL ('1 day')
(
	PARTITION interval_partition_table_004_p0 VALUES LESS THAN ('2020-03-01'),
	PARTITION interval_partition_table_004_p1 VALUES LESS THAN ('2020-04-01'),
	PARTITION interval_partition_table_004_p2 VALUES LESS THAN ('2020-05-01')
);

create index global_index_interval_partition_table_vacuum_a on interval_partition_table_vacuum(c1) global;

\parallel on
insert into interval_partition_table_vacuum values (generate_series(1,10), generate_series(1,10), generate_series(TO_DATE('1990-01-01', 'YYYY-MM-DD'),TO_DATE('2020-12-01', 'YYYY-MM-DD'),'1 day'));
vacuum 	interval_partition_table_vacuum;
vacuum interval_partition_table_vacuum;
vacuum analyze interval_partition_table_vacuum;
\parallel off

set enable_bitmapscan = off;
explain (costs off) select count(*) from interval_partition_table_vacuum where c1 = 1;
-- 11293 rows
select count(*) from interval_partition_table_vacuum where c1 = 1;

-- all new add partition have wait_clean_gpi=y return true
select true from (select count(*) as count from pg_partition a, pg_class b where a.parentid = b.oid and b.relname = 'interval_partition_table_vacuum' and a.reloptions[3] like '%wait_clean_gpi=y%') wait_clean_gpi where wait_clean_gpi.count = 0 or wait_clean_gpi.count = 216;
vacuum analyze interval_partition_table_vacuum;
-- 0 rows
select count(*) from pg_partition a, pg_class b where a.parentid = b.oid and b.relname = 'interval_partition_table_vacuum' and a.reloptions[3] like '%wait_clean_gpi=y%';
reset enable_bitmapscan;

-- clean table
drop table interval_partition_table_vacuum;
drop table interval_normal_date;
drop table test_gpi_more_invalid;
