-- MERGE PARTITIONS
set client_min_messages=error;
drop table if exists test_merge_llt;
create table test_merge_llt (a int, b int)
partition by range (a)
(
partition test_merge_llt_p1 values less than (10),
partition test_merge_llt_p2 values less than (20),
partition test_merge_llt_p3 values less than (30),
partition test_merge_llt_p4 values less than (maxvalue)
);
create index test_merge_llt_idx on test_merge_llt(a) global;
create index test_merge_llt_idx_local on test_merge_llt(b) local;
insert into test_merge_llt select generate_series(0,1000), generate_series(0,1000);
select relname, boundaries from pg_partition where parentid in (select oid from pg_class where relname = 'test_merge_llt') order by 2;
vacuum analyze test_merge_llt;
-- indexscan
explain (costs false) select * from test_merge_llt where a=40;
-- 1 rows
select * from test_merge_llt where a=40;

alter table test_merge_llt merge partitions test_merge_llt_p1, test_merge_llt_p2 into partition test_merge_llt_px;
select relname, boundaries from pg_partition where parentid in (select oid from pg_class where relname = 'test_merge_llt') order by 2;
select c.relname, i.indisusable from pg_index i join pg_class c on i.indexrelid = c.oid where i.indrelid = 'test_merge_llt'::regclass ORDER BY c.relname;
set enable_bitmapscan=off;
set enable_seqscan=off;
-- seqscan
explain (costs false) select * from test_merge_llt where a=40;
select * from test_merge_llt where a=40;

reindex index test_merge_llt_idx;
-- indexscan
explain (costs false) select * from test_merge_llt where a=40;
select * from test_merge_llt where a=40;
set enable_bitmapscan=on;
set enable_seqscan=on;
drop table if exists test_merge_llt;

-- End. Clean up

---truncate partition table with index
set client_min_messages=error;
drop table if exists test_truncate_llt;
create table test_truncate_llt (a int, b int)
partition by range (a)
(
partition test_truncate_llt_p1 values less than (10),
partition test_truncate_llt_p2 values less than (20),
partition test_truncate_llt_p3 values less than (30),
partition test_truncate_llt_p4 values less than (maxvalue)
);

create index test_truncate_llt_idx on test_truncate_llt(a) global;
insert into test_truncate_llt select generate_series(0,1000), generate_series(0,1000);
select relname, boundaries from pg_partition where parentid in (select oid from pg_class where relname = 'test_truncate_llt') order by 2;
vacuum analyze test_truncate_llt;
-- indexscan
explain (costs false) select * from test_truncate_llt where a=40;
select * from test_truncate_llt where a=40;

alter table test_truncate_llt truncate partition test_truncate_llt_p3;
select relname, boundaries from pg_partition where parentid in (select oid from pg_class where relname = 'test_truncate_llt') order by 2;
-- test_truncate_llt_idx unusable
select c.relname, i.indisusable from pg_index i join pg_class c on i.indexrelid = c.oid where i.indrelid = 'test_truncate_llt'::regclass ORDER BY c.relname;
set enable_bitmapscan=off;
set enable_seqscan=off;
-- seqscan
explain (costs false) select * from test_truncate_llt where a = 40;
select * from test_truncate_llt where a = 40;

alter index test_truncate_llt_idx rebuild;
-- test_truncate_llt_idx usable
select c.relname, i.indisusable from pg_index i join pg_class c on i.indexrelid = c.oid where i.indrelid = 'test_truncate_llt'::regclass ORDER BY c.relname;

set enable_bitmapscan=on;
set enable_seqscan=on;
-- indexscan
explain (costs false) select * from test_truncate_llt where a=40;
select * from test_truncate_llt where a=40;
drop table if exists test_truncate_llt;

-- End. Clean up

--exchange partition
set client_min_messages=error;
drop table if exists test_exchange_llt;
drop table if exists test_ord;
create table test_exchange_llt (a int, b int)
partition by range (a)
(
partition test_exchange_llt_p1 values less than (10),
partition test_exchange_llt_p2 values less than (20),
partition test_exchange_llt_p3 values less than (30),
partition test_exchange_llt_p4 values less than (maxvalue)
);
create index test_exchange_llt_idx on test_exchange_llt(a) global;
insert into test_exchange_llt select generate_series(0,1000), 100;
select relname, boundaries from pg_partition where parentid in (select oid from pg_class where relname = 'test_exchange_llt') order by 2;

create table test_ord (a int, b int);
insert into test_ord select 13, generate_series(0,1000);
vacuum analyze test_exchange_llt;
-- indexscan
explain (costs false) select * from test_exchange_llt where a=40;
select * from test_exchange_llt where a=40;

-- exchange
alter table test_exchange_llt exchange partition (test_exchange_llt_p2) with table test_ord with validation;
select relname, boundaries from pg_partition where parentid in (select oid from pg_class where relname = 'test_exchange_llt') order by 2;
-- test_exchange_llt_idx unusable
select c.relname, i.indisusable from pg_index i join pg_class c on i.indexrelid = c.oid where i.indrelid = 'test_exchange_llt'::regclass ORDER BY c.relname;

set enable_bitmapscan=off;
set enable_seqscan=off;
--seqcan
explain (costs false) select * from test_exchange_llt where a=40;
select * from test_exchange_llt where a=40;

-- rebuild
reindex table test_exchange_llt;
-- test_exchange_llt_idx usable
select c.relname, i.indisusable from pg_index i join pg_class c on i.indexrelid = c.oid where i.indrelid = 'test_exchange_llt'::regclass ORDER BY c.relname;

set enable_bitmapscan=on;
set enable_seqscan=on;
-- indexscan
explain (costs false) select * from test_exchange_llt where a=40;
select * from test_exchange_llt where a=40;

drop table if exists test_exchange_llt;
drop table if exists test_ord;

-- End. Clean up

--split partition
set client_min_messages=error;
drop table if exists test_split_llt;
create table if not exists test_split_llt (a int, b int)
partition by range(a)
(
partition test_split_llt_p1 values less than(10),
partition test_split_llt_p2 values less than(20),
partition test_split_llt_p3 values less than(30),
partition test_split_llt_p4 values less than(maxvalue)
);
create index test_split_llt_idx1 on  test_split_llt(a) global;
insert into test_split_llt select generate_series(0,1000), generate_series(0,1000);
select relname, boundaries from pg_partition where parentid in (select oid from pg_class where relname = 'test_split_llt') order by 2;
vacuum analyze test_split_llt;
-- indexscan
explain (costs false) select * from test_split_llt where a=40;
select * from test_split_llt where a=40;

alter table test_split_llt
        merge partitions test_split_llt_p1, test_split_llt_p2
        into partition test_split_llt_p1_2;
select relname, boundaries from pg_partition where parentid in (select oid from pg_class where relname = 'test_split_llt') order by 2;

alter table test_split_llt
        split partition test_split_llt_p1_2
        into (partition test_split_llt_p1 values less than (10), partition test_split_llt_p2 values less than (20));
select relname, boundaries from pg_partition where parentid in (select oid from pg_class where relname = 'test_split_llt') order by 2;
-- test_split_llt_idx1 unusable
select c.relname, i.indisusable from pg_index i join pg_class c on i.indexrelid = c.oid where i.indrelid = 'test_split_llt'::regclass ORDER BY c.relname;

set enable_bitmapscan=off;
set enable_seqscan=off;
--seqscan
explain (costs false) select * from test_split_llt where a=40;
select * from test_split_llt where a=40;
set enable_bitmapscan=on;
set enable_seqscan=on;

reindex database postgres;
-- test_split_llt_idx1 unusable
select c.relname, i.indisusable from pg_index i join pg_class c on i.indexrelid = c.oid where i.indrelid = 'test_split_llt'::regclass ORDER BY c.relname;
--seqscan
explain (costs false) select * from test_split_llt where a=40;
select * from test_split_llt where a=40;
drop table if exists test_split_llt;

-- End. Clean up

--drop partition
set client_min_messages=error;
drop table if exists test_drop_llt;
create table test_drop_llt (a int, b int)
partition by range (a)
(
partition test_drop_llt_p1 values less than (10),
partition test_drop_llt_p2 values less than (20),
partition test_drop_llt_p3 values less than (30),
partition test_drop_llt_p4 values less than (maxvalue)
);
create index test_drop_llt_idx on test_drop_llt(a) global;
insert into test_drop_llt select generate_series(0,1000), generate_series(0,1000);
select relname, boundaries from pg_partition where parentid in (select oid from pg_class where relname = 'test_drop_llt') order by 2;
vacuum analyze test_drop_llt;
--indexscan
explain (costs false) select * from test_drop_llt where a=40;
select * from test_drop_llt where a=40;

alter table test_drop_llt drop partition test_drop_llt_p1;
select relname, boundaries from pg_partition where parentid in (select oid from pg_class where relname = 'test_drop_llt') order by 2;
-- test_drop_llt_idx unusable
select c.relname, i.indisusable from pg_index i join pg_class c on i.indexrelid = c.oid where i.indrelid = 'test_drop_llt'::regclass ORDER BY c.relname;

set enable_bitmapscan=off;
set enable_seqscan=off;
--seqscan
explain (costs false) select * from test_drop_llt where a=40;
select * from test_drop_llt where a=40;

vacuum full test_drop_llt;
-- test_drop_llt_idx unusable
select c.relname, i.indisusable from pg_index i join pg_class c on i.indexrelid = c.oid where i.indrelid = 'test_drop_llt'::regclass ORDER BY c.relname;

--seqscan
explain (costs false) select * from test_drop_llt where a=40;
select * from test_drop_llt where a=40;

set enable_bitmapscan=on;
set enable_seqscan=on;
drop table if exists test_drop_llt;

-- End. Clean up
