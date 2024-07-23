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

--astore
CREATE TABLE web_returns_p_a
(
    sk_date       INTEGER,
    cm_num        INTEGER,
    nv_num        INTEGER,
    cn_name       INTEGER
)
with (STORAGE_TYPE=ASTORE)
PARTITION BY RANGE(sk_date)
(
    PARTITION P1 VALUES LESS THAN(1),
    PARTITION P2 VALUES LESS THAN(2),
    PARTITION P3 VALUES LESS THAN(3),
    PARTITION P4 VALUES LESS THAN(4),
    PARTITION P5 VALUES LESS THAN(5),
    PARTITION P6 VALUES LESS THAN(6),
    PARTITION P7 VALUES LESS THAN(7),
    PARTITION P8 VALUES LESS THAN(8),
    PARTITION P9 VALUES LESS THAN(9),
    PARTITION Pmax VALUES LESS THAN(MAXVALUE)
);

insert into web_returns_p_a values (1,1,1,1);
insert into web_returns_p_a values (2,2,2,2);
insert into web_returns_p_a values (3,3,3,3);
insert into web_returns_p_a values (4,4,4,4);
insert into web_returns_p_a values (5,5,5,5);
insert into web_returns_p_a values (6,6,6,6);
insert into web_returns_p_a values (7,7,7,7);
insert into web_returns_p_a values (8,8,8,8);
insert into web_returns_p_a values (9,9,9,9);

create index idx_cm_num_a on web_returns_p_a(cm_num) global;
create index idx_nv_num_a on web_returns_p_a(nv_num) local;
create unique index idx_uq_a on web_returns_p_a(sk_date) global;

set behavior_compat_options = 'update_unusable_unique_index_on_iud';
alter table web_returns_p_a drop partition p2;
insert into web_returns_p_a values (1,1,1,1);
insert into web_returns_p_a values (1,1,1,1);
update web_returns_p_a set sk_date = 1 where true;
set enable_opfusion = off;
insert into web_returns_p_a values (1,1,1,1);
insert into web_returns_p_a values (1,1,1,1);
update web_returns_p_a set sk_date = 1 where true;
set enable_opfusion = on;
alter index idx_uq_a rebuild;
alter table web_returns_p_a drop partition p3;
reset behavior_compat_options;
insert into web_returns_p_a values (2,2,2,2);
insert into web_returns_p_a values (2,2,2,2);
reset behavior_compat_options;

-- ustore
CREATE TABLE web_returns_p_u
(
    sk_date       INTEGER,
    cm_num        INTEGER,
    nv_num        INTEGER,
    cn_name       INTEGER
)
with (STORAGE_TYPE=ASTORE)
PARTITION BY RANGE(sk_date)
(
    PARTITION P1 VALUES LESS THAN(1),
    PARTITION P2 VALUES LESS THAN(2),
    PARTITION P3 VALUES LESS THAN(3),
    PARTITION P4 VALUES LESS THAN(4),
    PARTITION P5 VALUES LESS THAN(5),
    PARTITION P6 VALUES LESS THAN(6),
    PARTITION P7 VALUES LESS THAN(7),
    PARTITION P8 VALUES LESS THAN(8),
    PARTITION P9 VALUES LESS THAN(9),
    PARTITION Pmax VALUES LESS THAN(MAXVALUE)
);

insert into web_returns_p_u values (1,1,1,1);
insert into web_returns_p_u values (2,2,2,2);
insert into web_returns_p_u values (3,3,3,3);
insert into web_returns_p_u values (4,4,4,4);
insert into web_returns_p_u values (5,5,5,5);
insert into web_returns_p_u values (6,6,6,6);
insert into web_returns_p_u values (7,7,7,7);
insert into web_returns_p_u values (8,8,8,8);
insert into web_returns_p_u values (9,9,9,9);

create index idx_cm_num_u on web_returns_p_u(cm_num) global;
create index idx_nv_num_u on web_returns_p_u(nv_num) local;
create unique index idx_uq_u on web_returns_p_u(sk_date) global;

set behavior_compat_options = 'update_unusable_unique_index_on_iud';
alter table web_returns_p_u drop partition p2;
insert into web_returns_p_u values (1,1,1,1);
insert into web_returns_p_u values (1,1,1,1);
update web_returns_p_u set sk_date = 1 where true;
set enable_opfusion = off;
insert into web_returns_p_u values (1,1,1,1);
insert into web_returns_p_u values (1,1,1,1);
update web_returns_p_u set sk_date = 1 where true;
set enable_opfusion = on;
alter index idx_uq_u rebuild;

alter table web_returns_p_u drop partition p3;
reset behavior_compat_options;
insert into web_returns_p_u values (2,2,2,2);
insert into web_returns_p_u values (2,2,2,2);

drop index idx_cm_num_a;
drop index idx_nv_num_a;
drop index idx_uq_a;
drop table web_returns_p_a;
drop index idx_cm_num_u;
drop index idx_nv_num_u;
drop index idx_uq_u;
drop table web_returns_p_u;


CREATE TABLE web_returns_p_a
(
    sk_date       INTEGER,
    cm_num        INTEGER,
    nv_num        INTEGER,
    cn_name       INTEGER
)
with (STORAGE_TYPE=ASTORE)
PARTITION BY RANGE(sk_date)
(
    PARTITION P1 VALUES LESS THAN(1),
    PARTITION P2 VALUES LESS THAN(2),
    PARTITION P3 VALUES LESS THAN(3),
    PARTITION P4 VALUES LESS THAN(4),
    PARTITION P5 VALUES LESS THAN(5),
    PARTITION P6 VALUES LESS THAN(6),
    PARTITION P7 VALUES LESS THAN(7),
    PARTITION P8 VALUES LESS THAN(8),
    PARTITION P9 VALUES LESS THAN(9),
    PARTITION Pmax VALUES LESS THAN(MAXVALUE)
);

insert into web_returns_p_a values (1,1,1,1);
insert into web_returns_p_a values (2,2,2,2);
insert into web_returns_p_a values (3,3,3,3);
insert into web_returns_p_a values (4,4,4,4);
insert into web_returns_p_a values (5,5,5,5);
insert into web_returns_p_a values (6,6,6,6);
insert into web_returns_p_a values (7,7,7,7);
insert into web_returns_p_a values (8,8,8,8);
insert into web_returns_p_a values (9,9,9,9);

create index idx_cm_num_a on web_returns_p_a(cm_num) global;
create index idx_nv_num_a on web_returns_p_a(nv_num) local;
create unique index idx_uq_a on web_returns_p_a(sk_date) global;


\d+ web_returns_p_a;
set behavior_compat_options = 'update_global_index_on_partition_change';
alter table web_returns_p_a drop partition p2;
\d+ web_returns_p_a;

reset behavior_compat_options;
alter table web_returns_p_a drop partition p3;
\d+ web_returns_p_a;

drop index idx_cm_num_a;
drop index idx_nv_num_a;
drop index idx_uq_a;
drop table web_returns_p_a;

-- test unusable local index
create table ALTER_TABLE_MERGE_TABLE_057
(
c_smallint smallint,
c_varchar varchar(100)
) partition by range (c_varchar)
(
partition ALTER_TABLE_MERGE_TABLE_057_1 values less than ('a222') ,
partition ALTER_TABLE_MERGE_TABLE_057_2 values less than ('a444') ,
partition ALTER_TABLE_MERGE_TABLE_057_3 values less than ('a777') ,
partition ALTER_TABLE_MERGE_TABLE_057_4 values less than ('a999')
);

create unique index INDEX_ALTER_TABLE_MERGE_TABLE_057_1 ON ALTER_TABLE_MERGE_TABLE_057 USING btree (c_smallint, c_varchar) local;
alter table ALTER_TABLE_MERGE_TABLE_057 modify partition ALTER_TABLE_MERGE_TABLE_057_3 unusable local indexes;

set behavior_compat_options = 'update_unusable_unique_index_on_iud';
insert into ALTER_TABLE_MERGE_TABLE_057 values (1, 'a555');
insert into ALTER_TABLE_MERGE_TABLE_057 values (1, 'a555'); --error

insert into ALTER_TABLE_MERGE_TABLE_057 values (2, 'a111');
insert into ALTER_TABLE_MERGE_TABLE_057 values (2, 'a111'); --error

reset behavior_compat_options;
insert into ALTER_TABLE_MERGE_TABLE_057 values (1, 'a555');

insert into ALTER_TABLE_MERGE_TABLE_057 values (3, 'a333');
insert into ALTER_TABLE_MERGE_TABLE_057 values (3, 'a333'); --error

drop table ALTER_TABLE_MERGE_TABLE_057;
-- End. Clean u
