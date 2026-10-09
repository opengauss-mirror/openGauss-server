--
-- WDR enhance fastcheck (stable): shared memory keys, protocol n_calls,
-- per-database SQL stat, unused indexes detection source.
-- Covers commits for Shared Memory / Protocol+DB SQL / Unused Indexes.
-- Avoids generate_wdr_report HTML and long WDR snapshot sleeps.
--

-- ============================================================
-- 1) Shared Memory Statistics source (ShmemIndex key components)
-- ============================================================
select count(*) >= 5 as shmem_key_components_ok
from gs_shared_memory_detail
where contextname in (
    'Buffer Blocks',
    'Buffer Descriptors',
    'Buffer Descriptors Extra',
    'Checkpoint BufferIds',
    'XLOG Ctl',
    'clog',
    'CSNLOG Ctl')
  and totalsize > 0;

select count(*) >= 1 as shmem_pgstat_ok
from gs_shared_memory_detail
where contextname = 'PgStat' and totalsize > 0;

select count(*) >= 1 as dbe_perf_shared_memory_ok
from dbe_perf.shared_memory_detail
where contextname in ('Buffer Blocks', 'PgStat') and totalsize > 0;

-- ============================================================
-- 2) Protocol Message Statistics source (instance_time.n_calls)
-- ============================================================
select count(*) = 8 as protocol_stat_names_ok
from dbe_perf.instance_time
where stat_name in (
    'SRT2_SIMPLE_QUERY',
    'SRT6_P',
    'SRT7_B',
    'SRT8_E',
    'SRT9_D',
    'SRT10_S',
    'SRT11_C',
    'SRT12_U');

select count(*) = 1 as instance_time_has_n_calls
from information_schema.columns
where table_schema = 'dbe_perf'
  and table_name = 'instance_time'
  and column_name = 'n_calls';

-- Delta on Simple Query path must be stable (>= workload size).
create temporary table wdr_proto_before as
select n_calls from dbe_perf.instance_time where stat_name = 'SRT2_SIMPLE_QUERY';
select 1 as wdr_proto_q1;
select 2 as wdr_proto_q2;
select 3 as wdr_proto_q3;
select (
    (select n_calls from dbe_perf.instance_time where stat_name = 'SRT2_SIMPLE_QUERY')
    - (select n_calls from wdr_proto_before)
) >= 3 as srt2_n_calls_increased;

-- ============================================================
-- 3) SQL Stats by Database source (database_sql_stat)
-- ============================================================
select count(*) = 1 as database_sql_stat_has_n_calls
from information_schema.columns
where table_schema = 'dbe_perf'
  and table_name = 'database_sql_stat'
  and column_name = 'n_calls';

select count(*) = 1 as database_sql_stat_has_elapse
from information_schema.columns
where table_schema = 'dbe_perf'
  and table_name = 'database_sql_stat'
  and column_name = 'total_elapse_time';

select reset_unique_sql('global', 'ALL', 0);
create temporary table wdr_db_sql_before as
select coalesce(
    (select n_calls from dbe_perf.database_sql_stat where datname = current_database()),
    0) as n;
-- Unique-SQL completion path updates per-database counters.
select 1 as wdr_db_sql_q1;
select 1 as wdr_db_sql_q2;
select (
    (select coalesce(max(n_calls), 0) from dbe_perf.database_sql_stat
     where datname = current_database())
    - (select n from wdr_db_sql_before)
) >= 1 as database_sql_n_calls_increased;

select count(*) >= 1 as summary_database_sql_stat_ok
from dbe_perf.summary_database_sql_stat
where datname = current_database() and n_calls >= 0;

-- ============================================================
-- 4) Unused Indexes detection source (pg_stat_user_indexes)
-- ============================================================
drop table if exists wdr_enhance_unused_t cascade;
create table wdr_enhance_unused_t(id int, name text);
create index wdr_enhance_unused_i_unused on wdr_enhance_unused_t(name);
create index wdr_enhance_unused_i_used on wdr_enhance_unused_t(id);
insert into wdr_enhance_unused_t select generate_series(1, 50), 'x' || generate_series(1, 50);

-- Never touch i_unused: idx_scan delta == 0 is the WDR Unused Indexes rule.
select count(*) = 1 as unused_index_scan_zero
from pg_stat_user_indexes
where schemaname = 'public'
  and indexrelname = 'wdr_enhance_unused_i_unused'
  and idx_scan = 0;

drop table wdr_enhance_unused_t cascade;
