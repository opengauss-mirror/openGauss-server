
CREATE SCHEMA gtt_stats;

set search_path=gtt_stats,sys;

-- expect 0
select count(*) from pg_gtt_attached_pids;

-- expect 0
select count(*) from pg_list_gtt_relfrozenxids();

create global temp table gtt_stats.gtt(a int primary key, b text) on commit PRESERVE rows;
-- expect 0
select count(*) from pg_gtt_attached_pids;

-- expect 0
select count(*) from pg_list_gtt_relfrozenxids();

insert into gtt values(generate_series(1,10000),'test');

-- expect 1
select count(*) from pg_gtt_attached_pids;

-- expect 2
select count(*) from pg_list_gtt_relfrozenxids();

-- expect 2
select schemaname, tablename, relpages, reltuples, relallvisible from pg_gtt_relstats where schemaname = 'gtt_stats' order by tablename;

-- expect 0
select * from pg_gtt_stats order by tablename, attname;

reindex table gtt;

reindex index gtt_pkey;

analyze gtt;

select schemaname, tablename, relpages, reltuples, relallvisible from pg_gtt_relstats where schemaname = 'gtt_stats' order by tablename;

select * from pg_gtt_stats order by tablename, attname;

reset search_path;

drop schema gtt_stats cascade;

