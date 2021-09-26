------ The file is for llt

-- indexGetPartitionOidList
create table test_reindex_llt (a int)
partition by range (a)
(
	partition test_reindex_llt_p1 values less than (10),
	partition test_reindex_llt_p2 values less than (20)
);

create index test_reindex_llt_index on test_reindex_llt (a) local;
reindex index test_reindex_llt_index;
drop table test_reindex_llt;


-- isTupleLocatePartition
create table test_exchange_llt (a int)
partition by range (a)
(
	partition test_exchange_llt_p1 values less than (10),
	partition test_exchange_llt_p2 values less than (20)
);

create table test_ord (a int);
insert into test_ord values (11);
alter table test_exchange_llt exchange partition (test_exchange_llt_p2) with table test_ord with validation;
drop table test_exchange_llt;
drop table test_ord;


-- get_rel_oids
create table test_get_rel_oids_llt (a int)
partition by range (a)
(
	partition test_get_rel_oids_llt_p1 values less than (10),
	partition test_get_rel_oids_llt_p2 values less than (20)
);

vacuum test_get_rel_oids_llt partition (test_get_rel_oids_llt_p1);
drop table test_get_rel_oids_llt;


-- pg_stat_... funcs
create table test_pg_stat_funcs (a int)
partition by range (a)
(
	partition test_pg_stat_funcs_p1 values less than (10),
	partition test_pg_stat_funcs_p2 values less than (20)
);

create index test_pg_stat_funcs_index on test_pg_stat_funcs (a) local;

select pg_stat_get_numscans(oid)>-1 from pg_class where relname='test_pg_stat_funcs';
select pg_stat_get_numscans(oid)>-1 from pg_class where relname='test_pg_stat_funcs_index';
select pg_stat_get_tuples_returned(oid) from pg_class where relname='test_pg_stat_funcs';
select pg_stat_get_tuples_returned(oid) from pg_class where relname='test_pg_stat_funcs_index';
select pg_stat_get_tuples_fetched(oid) from pg_class where relname='test_pg_stat_funcs';
select pg_stat_get_tuples_fetched(oid) from pg_class where relname='test_pg_stat_funcs_index';
select pg_stat_get_tuples_inserted(oid) from pg_class where relname='test_pg_stat_funcs';
select pg_stat_get_tuples_inserted(oid) from pg_class where relname='test_pg_stat_funcs_index';
select pg_stat_get_tuples_updated(oid) from pg_class where relname='test_pg_stat_funcs';
select pg_stat_get_tuples_updated(oid) from pg_class where relname='test_pg_stat_funcs_index';
select pg_stat_get_tuples_deleted(oid) from pg_class where relname='test_pg_stat_funcs';
select pg_stat_get_tuples_deleted(oid) from pg_class where relname='test_pg_stat_funcs_index';
select pg_stat_get_tuples_hot_updated(oid) from pg_class where relname='test_pg_stat_funcs';
select pg_stat_get_tuples_hot_updated(oid) from pg_class where relname='test_pg_stat_funcs_index';
select pg_stat_get_live_tuples(oid) from pg_class where relname='test_pg_stat_funcs';
select pg_stat_get_live_tuples(oid) from pg_class where relname='test_pg_stat_funcs_index';
select pg_stat_get_dead_tuples(oid) from pg_class where relname='test_pg_stat_funcs';
select pg_stat_get_dead_tuples(oid) from pg_class where relname='test_pg_stat_funcs_index';
select pg_stat_get_blocks_fetched(oid) from pg_class where relname='test_pg_stat_funcs';
select pg_stat_get_blocks_fetched(oid) from pg_class where relname='test_pg_stat_funcs_index';
select pg_stat_get_blocks_hit(oid) from pg_class where relname='test_pg_stat_funcs';
select pg_stat_get_blocks_hit(oid) from pg_class where relname='test_pg_stat_funcs_index';
select pg_stat_get_last_vacuum_time(oid) from pg_class where relname='test_pg_stat_funcs';
select pg_stat_get_last_vacuum_time(oid) from pg_class where relname='test_pg_stat_funcs_index';
select pg_stat_get_last_autovacuum_time(oid) from pg_class where relname='test_pg_stat_funcs';
select pg_stat_get_last_autovacuum_time(oid) from pg_class where relname='test_pg_stat_funcs_index';
select pg_stat_get_last_analyze_time(oid) from pg_class where relname='test_pg_stat_funcs';
select pg_stat_get_last_analyze_time(oid) from pg_class where relname='test_pg_stat_funcs_index';
select pg_stat_get_last_autoanalyze_time(oid) from pg_class where relname='test_pg_stat_funcs';
select pg_stat_get_last_autoanalyze_time(oid) from pg_class where relname='test_pg_stat_funcs_index';
select pg_stat_get_vacuum_count(oid) from pg_class where relname='test_pg_stat_funcs';
select pg_stat_get_vacuum_count(oid) from pg_class where relname='test_pg_stat_funcs_index';
select pg_stat_get_autovacuum_count(oid) from pg_class where relname='test_pg_stat_funcs';
select pg_stat_get_autovacuum_count(oid) from pg_class where relname='test_pg_stat_funcs_index';
select pg_stat_get_analyze_count(oid) from pg_class where relname='test_pg_stat_funcs';
select pg_stat_get_analyze_count(oid) from pg_class where relname='test_pg_stat_funcs_index';
select pg_stat_get_autoanalyze_count(oid) from pg_class where relname='test_pg_stat_funcs';
select pg_stat_get_autoanalyze_count(oid) from pg_class where relname='test_pg_stat_funcs_index';
analyze test_pg_stat_funcs;
select pg_sleep(1);
select pg_stat_get_xact_tuples_updated(oid) from pg_class where relname='test_pg_stat_funcs';
select pg_stat_get_xact_tuples_updated(oid) from pg_class where relname='test_pg_stat_funcs_index';
select pg_stat_get_xact_tuples_deleted(oid) from pg_class where relname='test_pg_stat_funcs';
select pg_stat_get_xact_tuples_deleted(oid) from pg_class where relname='test_pg_stat_funcs_index';
select pg_stat_get_xact_tuples_inserted(oid) from pg_class where relname='test_pg_stat_funcs';
select pg_stat_get_xact_tuples_inserted(oid) from pg_class where relname='test_pg_stat_funcs_index';
select pg_stat_get_xact_tuples_hot_updated(oid) from pg_class where relname='test_pg_stat_funcs';
select pg_stat_get_xact_tuples_hot_updated(oid) from pg_class where relname='test_pg_stat_funcs_index';
select pg_stat_get_xact_blocks_fetched(oid) from pg_class where relname='test_pg_stat_funcs';
select pg_stat_get_xact_blocks_fetched(oid) from pg_class where relname='test_pg_stat_funcs_index';
select pg_stat_get_xact_blocks_hit(oid) from pg_class where relname='test_pg_stat_funcs';
select pg_stat_get_xact_blocks_hit(oid) from pg_class where relname='test_pg_stat_funcs_index';
select pg_stat_get_xact_numscans(oid)>-1 from pg_class where relname='test_pg_stat_funcs';
select pg_stat_get_xact_numscans(oid)>-1 from pg_class where relname='test_pg_stat_funcs_index';
select pg_stat_get_xact_tuples_returned(oid) from pg_class where relname='test_pg_stat_funcs';
select pg_stat_get_xact_tuples_returned(oid) from pg_class where relname='test_pg_stat_funcs_index';
select pg_stat_get_xact_tuples_fetched(oid) from pg_class where relname='test_pg_stat_funcs';
select pg_stat_get_xact_tuples_fetched(oid) from pg_class where relname='test_pg_stat_funcs_index';
select pg_stat_get_last_analyze_time(oid) is not NULL as A from pg_class where relname='test_pg_stat_funcs';
drop table test_pg_stat_funcs;


-- PWJ
create table test_pwj_1 (a int)
distribute by replication
partition by range (a)
(
	partition test_pwj_1_p1 values less than (10),
	partition test_pwj_1_p2 values less than (20)
);

create table test_pwj_2 (b int)
distribute by replication
partition by range (b)
(
	partition test_pwj_2_p1 values less than (10),
	partition test_pwj_2_p2 values less than (20)
);

insert into test_pwj_1 values (generate_series(0, 19));
insert into test_pwj_2 values (generate_series(0, 19));

set enable_partitionwise = on;

-- try_nestloop_path
set enable_nestloop = on;
set enable_mergejoin = off;
set enable_hashjoin = off;
select * from test_pwj_1 inner join test_pwj_2 on a=b;

-- try_mergejoin_path
set enable_nestloop = off;
set enable_mergejoin = on;
set enable_hashjoin = off;
select * from test_pwj_1 inner join test_pwj_2 on a=b;

-- try_hasjoin_path
set enable_nestloop = off;
set enable_mergejoin = on;
set enable_hashjoin = off;
select * from test_pwj_1 inner join test_pwj_2 on a=b;

drop table test_pwj_1;
drop table test_pwj_2;

-- pwj with index
create table test_pwj_idx_1 (c1 int, c2 int)
distribute by replication
partition by range (c1)
(
	partition test_pwj_idx_1_p1 values less than (10),
	partition test_pwj_idx_1_p2 values less than (20)
);
create index idx_test_pwj_idx_1 on test_pwj_idx_1(c1) local;

create table test_pwj_idx_2 (c3 int, c4 int)
distribute by replication
partition by range (c3)
(
	partition test_pwj_idx_2_p1 values less than (10),
	partition test_pwj_idx_2_p2 values less than (20)
);
create index idx_test_pwj_idx_2 on test_pwj_idx_2(c3) local;

insert into test_pwj_idx_1 values (generate_series(0, 19), generate_series(0, 19));
insert into test_pwj_idx_2 values (generate_series(0, 19), generate_series(0, 19));

set enable_partitionwise = on;
set enable_seqscan = off;

-- try_nestloop_path
alter index idx_test_pwj_idx_1 modify partition test_pwj_idx_1_p1_c1_idx unusable;
set enable_nestloop = on;
set enable_mergejoin = off;
set enable_hashjoin = off;
select c1, c3 from test_pwj_idx_1 inner join test_pwj_idx_2 on c1=c3;

-- try_mergejoin_path
alter index idx_test_pwj_idx_2 modify partition test_pwj_idx_2_p1_c3_idx unusable;
set enable_nestloop = off;
set enable_mergejoin = on;
set enable_hashjoin = off;
select c1, c3 from test_pwj_idx_1 inner join test_pwj_idx_2 on c1=c3;

-- try_hasjoin_path
alter index idx_test_pwj_idx_1 unusable;
alter index idx_test_pwj_idx_2 unusable;
set enable_nestloop = off;
set enable_mergejoin = on;
set enable_hashjoin = off;
select c1, c3 from test_pwj_idx_1 inner join test_pwj_idx_2 on c1=c3;

drop table test_pwj_idx_1;
drop table test_pwj_idx_2;

-- reindexPartIndex
create table test_reindexPartIndex (a int)
partition by range (a)
(
	partition test_reindexPartIndex_p1 values less than (10),
	partition test_reindexPartIndex_p2 values less than (20)
);

create index test_reindexPartIndex_index on test_reindexPartIndex (a) local
(
	partition test_reindexPartIndex_p1_index,
	partition test_reindexPartIndex_p2_index
);

reindex index test_reindexPartIndex_index;

drop table test_reindexPartIndex;


-- estimatePartitionSize
create table test_estimatePartitionSize (a int)
partition by range (a)
(
	partition test_reindexPartIndex_p1 values less than (10),
	partition test_reindexPartIndex_p2 values less than (20)
);

create index test_estimatePartitionSize_index on test_estimatePartitionSize (a) local;

insert into test_estimatePartitionSize values (1);
analyze test_estimatePartitionSize;
cluster test_estimatePartitionSize using test_estimatePartitionSize_index;

drop table test_estimatePartitionSize;


-- get_rel_oids
create table test_get_rel_oids(a int)
partition by range (a)
(
	partition test_get_rel_oids_p1 values less than (10),
	partition test_get_rel_oids_p2 values less than (20)
);

vacuum;

drop table test_get_rel_oids;


create table PARTITION_EXCHANGE_TABLE_028_1(c_varchar varchar(100)) partition by range (c_varchar) (
partition PARTITION_EXCHANGE_TABLE_028_1_1  values less than ('AAAA') ,
partition PARTITION_EXCHANGE_TABLE_028_1_2  values less than ('ZZZZ') ,
partition PARTITION_EXCHANGE_TABLE_028_1_3  values less than ('ZZZZa'));

create table PARTITION_EXCHANGE_TABLE_028_2(a varchar);

alter table PARTITION_EXCHANGE_TABLE_028_1 exchange partition for('ZZZZ','AAAA') with table PARTITION_EXCHANGE_TABLE_028_2 with validation;

alter table PARTITION_EXCHANGE_TABLE_028_1 merge partitions PARTITION_EXCHANGE_TABLE_028_1_1,PARTITION_EXCHANGE_TABLE_028_1_3 into partition PARTITION_EXCHANGE_TABLE_028_1_4;

drop table PARTITION_EXCHANGE_TABLE_028_1;
drop table PARTITION_EXCHANGE_TABLE_028_2;


create table setop_002 (
    w_warehouse_sk            integer               not null,
    w_warehouse_id            char(16)              not null,
    w_warehouse_date          varchar(50)                   ,
    w_warehouse_sq_ft         integer                       ,
    w_street_number           smallint                      ,
    w_street_name             varchar(60)                   ,
    w_street_type             char(15)                      ,
    w_suite_number            char(10)                      ,
    w_city                    varchar(60)                   ,
    w_county                  varchar(30)                   ,
    w_state                   char(2)                       ,
    w_country                 Boolean                   ,
    w_gmt_offset              decimal(5,2)  
)distribute by hash(w_gmt_offset,w_state,w_county,w_street_number,w_warehouse_sq_ft); 
create table call_center
(
    cc_call_center_sk         integer               not null,
    cc_call_center_id         char(16)              not null,
    cc_rec_start_date         date                          ,
    cc_rec_end_date           date                          ,
    cc_closed_date_sk         integer                       ,
    cc_open_date_sk           integer                       ,
    cc_name                   varchar(50)                   ,
    cc_class                  varchar(50)                   ,
    cc_employees              integer                       ,
    cc_sq_ft                  integer                       ,
    cc_hours                  char(20)                      ,
    cc_manager                varchar(40)                   ,
    cc_mkt_id                 integer                       ,
    cc_mkt_class              char(50)                      ,
    cc_mkt_desc               varchar(100)                  ,
    cc_market_manager         varchar(40)                   ,
    cc_division               integer                       ,
    cc_division_name          varchar(50)                   ,
    cc_company                integer                       ,
    cc_company_name           char(50)                      ,
    cc_street_number          char(10)                      ,
    cc_street_name            varchar(60)                   ,
    cc_street_type            char(15)                      ,
    cc_suite_number           char(10)                      ,
    cc_city                   varchar(60)                   ,
    cc_county                 varchar(30)                   ,
    cc_state                  char(2)                       ,
    cc_zip                    char(10)                      ,
    cc_country                varchar(20)                   ,
    cc_gmt_offset             decimal(5,2)                  ,
    cc_tax_percentage         decimal(5,2)                  
 ) distribute by hash (cc_call_center_id);
create table web_page
(
    wp_web_page_sk            integer               not null,
    wp_web_page_id            char(16)              not null,
    wp_rec_start_date         date                          ,
    wp_rec_end_date           date                          ,
    wp_creation_date_sk       integer                       ,
    wp_access_date_sk         integer                       ,
    wp_autogen_flag           char(1)                       ,
    wp_customer_sk            integer                       ,
    wp_url                    varchar(100)                  ,
    wp_type                   char(50)                      ,
    wp_char_count             integer                       ,
    wp_link_count             integer                       ,
    wp_image_count            integer                       ,
    wp_max_ad_count           integer                       
 ) distribute by hash (wp_rec_start_date,wp_autogen_flag)
partition by range(wp_rec_start_date)
(
        partition p1 values less than('1990-01-01'),
        partition p2 values less than('1995-01-01'),
        partition p3 values less than('2000-01-01'),
        partition p4 values less than('2005-01-01'),
        partition p5 values less than('2010-01-01'),
        partition p6 values less than('2015-01-01'),
        partition p7 values less than(maxvalue)
);
select w_warehouse_date from setop_002
where w_warehouse_date in (select cc_rec_start_date from call_center union all select wp_rec_start_date from web_page);
drop table setop_002;
drop table call_center;
drop table web_page;

create table t10 (c1 int, c2 date)
partition by range(c2)
(
	partition "01" values less than ('2015-01-01'),
	partition "02" values less than ('2018-09-06'),
	partition p01 values less than ('2018-09-07'),
	partition p02 values less than ('2018-09-08')
) ENABLE ROW MOVEMENT;

create index t10_idx on t10(c2) local(
	partition "01",
	partition "02",
	partition p01,
	partition p02
);
select pg_get_indexdef('t10_idx'::regclass::oid);
\d+ t10

drop table t10;
