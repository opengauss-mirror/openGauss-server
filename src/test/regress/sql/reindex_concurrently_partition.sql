--
-- REINDEX CONCURRENTLY PARTITION
--

drop table if exists t1;
create table t1(
 c_id  varchar,
 c_w_id  integer,
 c_date     date
 )
partition by range (c_date,c_w_id)
(
  PARTITION t1_1 values less than ('20170331',5),
  PARTITION t1_2 values less than ('20170731',450),
  PARTITION t1_3 values less than ('20170930',1062),
  PARTITION t1_4 values less than ('20171231',1765),
  PARTITION t1_5 values less than ('20180331',2024),
  PARTITION t1_6 values less than ('20180731',2384),
  PARTITION t1_7 values less than ('20180930',2786),
  PARTITION t1_8 values less than (maxvalue,maxvalue)
);

insert into t1 values('gauss1',4,'20170301');
insert into t1 values('gauss2',400,'20170625');
insert into t1 values('gauss3',480,'20170920');
insert into t1 values('gauss4',1065,'20170920');
insert into t1 values('gauss5',1800,'20170920');
insert into t1 values('gauss6',2030,'20170920');
insert into t1 values('gauss7',2385,'20170920');
insert into t1 values('gauss8',2789,'20191020');
insert into t1 values('gauss9',2789,'20171020');

create index idx_t1 on t1 using btree(c_id) LOCAL;
create index idx2_t1 on t1 using btree(c_id) LOCAL (
                PARTITION t1_1_index,
                PARTITION t1_2_index,
                PARTITION t1_3_index,
                PARTITION t1_4_index,
                PARTITION t1_5_index,
                PARTITION t1_6_index,
                PARTITION t1_7_index,
                PARTITION t1_8_index
);
reindex index CONCURRENTLY idx_t1;
reindex index CONCURRENTLY idx2_t1 partition t1_1_index;
reindex table CONCURRENTLY t1;
reindex table CONCURRENTLY t1 partition t1_1;

-- Check handling of unusable partitioned index
alter index idx2_t1 UNUSABLE;
\d t1
reindex table concurrently t1;
\d t1
reindex table concurrently t1 partition t1_1;
\d t1
reindex index concurrently idx2_t1 partition t1_1_index;
\d t1
reindex index concurrently idx2_t1;
\d t1

-- Check handling of unusable index partition
alter index idx2_t1 modify partition t1_2_index UNUSABLE;
select indisusable from pg_partition where relname = 't1_2_index';
reindex table CONCURRENTLY t1;
reindex table CONCURRENTLY t1 partition t1_2;
reindex index CONCURRENTLY idx2_t1;
select indisusable from pg_partition where relname = 't1_2_index';
alter index idx2_t1 modify partition t1_2_index UNUSABLE;
select indisusable from pg_partition where relname = 't1_2_index';
reindex index CONCURRENTLY idx2_t1 partition t1_2_index;
select indisusable from pg_partition where relname = 't1_2_index';

drop index idx_t1;
drop index idx2_t1;

-- reindex concurrently global index partition
create index idx_t1 on t1 using btree(c_id);
reindex index idx_t1;
reindex index CONCURRENTLY idx_t1; --ERROR, can't reindex concurrently global index partition
reindex table t1;
reindex table CONCURRENTLY t1; --WARNING, can't reindex concurrently global index partition

drop table t1;