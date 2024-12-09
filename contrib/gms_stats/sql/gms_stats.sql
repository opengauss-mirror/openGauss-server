create extension gms_stats;
create schema gms_stats_test;
set search_path=gms_stats_test;
create table normal_table(a int, b char(10));
insert into normal_table select generate_series(1,500), 'abc';

create table partition_table(a int) partition by range(a) (partition p1 values less than(100),partition p2 values less than(maxvalue));
insert into partition_table select generate_series(1,600);

create materialized view mv_tb as select * from normal_table;

select schemaname, tablename, attname, avg_width, most_common_vals, most_common_freqs from pg_stats where schemaname='gms_stats_test' order by tablename, attname;

begin
gms_stats.gather_schema_stats('gms_stats_test');
end;
/
select schemaname, tablename, attname, avg_width, most_common_vals, most_common_freqs from pg_stats where schemaname='gms_stats_test' order by tablename, attname;

create table normal_table2(a int, b char(10));
insert into normal_table2 select generate_series(1,700), 'abc';
call gms_stats.gather_schema_stats('gms_stats_test');

select schemaname, tablename, attname, avg_width, most_common_vals, most_common_freqs from pg_stats where schemaname='gms_stats_test' order by tablename, attname;

drop schema gms_stats_test cascade;

create schema sc_stats;
set current_schema = sc_stats;

create or replace procedure prepare_data() as
begin
    drop table if exists t_stats;
    create table t_stats (id int, c2 text, c3 char(1), constraint t_stats_pk primary key (id));

    insert into t_stats values (generate_series(1, 100), 'aabbcc', 'Y');
    insert into t_stats values (generate_series(101, 200), '123dfg', 'N');
    insert into t_stats values (generate_series(201, 300), '人面桃花相映红', 'N');
    insert into t_stats values (generate_series(301, 400), 'fortunate', 'Y');
    insert into t_stats values (generate_series(401, 500), 'open@gauss', 'Y');
    insert into t_stats values (generate_series(501, 600), '127.0.0.1', 'N');
    insert into t_stats values (generate_series(601, 700), '!@#$!%#!', 'N');
    insert into t_stats values (generate_series(701, 800), '[1,2,3,4]', 'Y');
    insert into t_stats values (generate_series(801, 900), '{"name":"张三","age":18}', 'Y');
    insert into t_stats values (generate_series(901, 1000), '', 'N');
end;
/
create or replace procedure prepare_part() as
begin
    drop table if exists t_part;
    create table t_part(c1 int, c2 char(1), c3 text)
    partition by list(c2) (
        partition t_part_list_r values ('r'),
        partition t_part_list_v values ('v'),
        partition t_part_list_i values ('i')
    );

    insert into t_part values (generate_series(1, 100), 'r', 'aabbcc');
    insert into t_part values (generate_series(101, 200), 'v', '123dfg');
    insert into t_part values (generate_series(201, 300), 'i', '人面桃花相映红');
    insert into t_part values (generate_series(301, 400), 'r', 'fortunate');
    insert into t_part values (generate_series(401, 500), 'v', 'open@gauss');
    insert into t_part values (generate_series(501, 600), 'i', '127.0.0.1');
    insert into t_part values (generate_series(601, 700), 'r', '!@#$!%#!');
    insert into t_part values (generate_series(701, 800), 'v', '{"name":"张三","age":18}');
    insert into t_part values (generate_series(801, 900), 'i', '');
    insert into t_part values (generate_series(901, 920), 'r', 'Hello');
    insert into t_part values (generate_series(921, 960), 'v', 'Kitty');
    insert into t_part values (generate_series(961, 1000), 'v', 'Cats');
    insert into t_part values (1001, 'i', 'Dog');
end;
/
create or replace procedure prepare_subpart() as
begin
    drop table if exists t_sub_part;
    create table t_sub_part(c1 int, c2 char(1), c3 varchar2(100))
    partition by range(c1) subpartition by list(c2) (
        partition p_less_300 values less than(300) (
            subpartition subp_less_300_r values ('r'),
            subpartition subp_less_300_v values ('v'),
            subpartition subp_less_300_i values ('i')
        ),
        partition p_less_600 values less than(600) (
            subpartition subp_less_600_r values ('r'),
            subpartition subp_less_600_v values ('v'),
            subpartition subp_less_600_i values ('i')
        ),
        partition p_max values less than(maxvalue) (
            subpartition subp_max_r values ('r'),
            subpartition subp_max_v values ('v'),
            subpartition subp_max_i values ('i')
        )
    );

    insert into t_sub_part values (generate_series(1, 100), 'r', 'aabbcc');
    insert into t_sub_part values (generate_series(101, 200), 'v', '123dfg');
    insert into t_sub_part values (generate_series(201, 300), 'i', '人面桃花相映红');
    insert into t_sub_part values (generate_series(301, 400), 'r', 'fortunate');
    insert into t_sub_part values (generate_series(401, 500), 'v', 'open@gauss');
    insert into t_sub_part values (generate_series(501, 600), 'i', '127.0.0.1');
    insert into t_sub_part values (generate_series(601, 700), 'r', '!@#$!%#!');
    insert into t_sub_part values (generate_series(701, 800), 'v', '{"name":"张三","age":18}');
    insert into t_sub_part values (generate_series(801, 900), 'i', '');
    insert into t_sub_part values (generate_series(901, 920), 'r', 'Hello');
    insert into t_sub_part values (generate_series(921, 960), 'v', 'Kitty');
    insert into t_sub_part values (generate_series(961, 1000), 'v', 'Cats');
    insert into t_sub_part values (1001, 'i', 'Dog');
end;
/
create or replace procedure prepare_ustore() as
begin
    drop table if exists t_stats_us;
    create table t_stats_us (c1 int, c2 text, c3 char(1), constraint t_stats_us_pk primary key (c1))
    with (storage_type=ustore);

    insert into t_stats_us values (generate_series(1, 100), 'aabbcc', 'Y');
    insert into t_stats_us values (generate_series(101, 200), '123dfg', 'N');
    insert into t_stats_us values (generate_series(201, 300), '人面桃花相映红', 'N');
    insert into t_stats_us values (generate_series(301, 400), 'fortunate', 'Y');
    insert into t_stats_us values (generate_series(401, 500), 'open@gauss', 'Y');
    insert into t_stats_us values (generate_series(501, 600), '127.0.0.1', 'N');
    insert into t_stats_us values (generate_series(601, 700), '!@#$!%#!', 'N');
    insert into t_stats_us values (generate_series(701, 800), '[1,2,3,4]', 'Y');
    insert into t_stats_us values (generate_series(801, 900), '{"name":"张三","age":18}', 'Y');
    insert into t_stats_us values (generate_series(901, 1000), '', 'N');
end;
/
create or replace procedure prepare_column() as
begin
    drop table if exists t_stats_col;
    create table t_stats_col (c1 int, c2 text, c3 char(1), constraint t_stats_col_pk primary key (c1))
    with (orientation = column);

    insert into t_stats_col values (generate_series(1, 100), 'aabbcc', 'Y');
    insert into t_stats_col values (generate_series(101, 200), '123dfg', 'N');
    insert into t_stats_col values (generate_series(201, 300), '人面桃花相映红', 'N');
    insert into t_stats_col values (generate_series(301, 400), 'fortunate', 'Y');
    insert into t_stats_col values (generate_series(401, 500), 'open@gauss', 'Y');
    insert into t_stats_col values (generate_series(501, 600), '127.0.0.1', 'N');
    insert into t_stats_col values (generate_series(601, 700), '!@#$!%#!', 'N');
    insert into t_stats_col values (generate_series(701, 800), '[1,2,3,4]', 'Y');
    insert into t_stats_col values (generate_series(801, 900), '{"name":"张三","age":18}', 'Y');
    insert into t_stats_col values (generate_series(901, 1000), '', 'N');
end;
/

call prepare_data();
call prepare_ustore();

call gms_stats.create_stat_table('sc_stats', 't_tmp_stats');
call gms_stats.create_stat_table('sc_stats', 't_tmp_stats2');

select gms_stats.get_stats_history_retention;
select gms_stats.get_stats_history_retention();
select gms_stats.get_stats_history_retention(1234); -- error

declare
    exists_flag boolean;
begin
    select exists (select gms_stats.get_stats_history_availability()) into exists_flag;
    if exists_flag then
        raise info 'execute succeed, select gms_stats.get_stats_history_availability()';
    else
        raise exception 'execute failed, select gms_stats.get_stats_history_availability()';
    end if;
end;
/
select gms_stats.get_stats_history_availability(1234); -- error

-- TEST LOCK
call gms_stats.lock_table_stats('sc_stats', 't_stats');
select c.nspname, a.stalocktype, b.relname, a.lock  from pg_statistic_lock a, pg_class b, pg_namespace c where a.namespaceid = c.Oid and a.relid = b.Oid and b.relname = 't_stats' and a.stalocktype = 't';

call gms_stats.gather_database_stats(); -- t_stats should skip
select b.relname, a.starelkind, a.staattnum, a.stanullfrac, a.stadistinct from pg_statistic a left join pg_class b on a.starelid = b.Oid where b.relname in ('t_stats', 't_stats_us') order by b.relname, a.starelkind, a.staattnum;
call gms_stats.gather_schema_stats('sc_stats'); -- t_stats should skip
select b.relname, a.starelkind, a.staattnum, a.stanullfrac, a.stadistinct from pg_statistic a left join pg_class b on a.starelid = b.Oid where b.relname in ('t_stats', 't_stats_us') order by b.relname, a.starelkind, a.staattnum;
call gms_stats.gather_schema_stats('sc_stats', force=>true);
call gms_stats.gather_table_stats('sc_stats', 't_stats'); -- error, locked
call gms_stats.gather_table_stats('sc_stats', 't_stats', force=>true);
call gms_stats.gather_index_stats('sc_stats', 't_stats_pk'); -- error, locked
call gms_stats.gather_index_stats('sc_stats', 't_stats_pk', force=>true);

call gms_stats.delete_schema_stats('sc_stats'); -- skip t_stats
select b.relname, a.starelkind, a.staattnum from pg_statistic a left join pg_class b on a.starelid = b.Oid where b.relname in ('t_stats', 't_stats_us') order by b.relname, a.starelkind, a.staattnum;
call gms_stats.delete_schema_stats('sc_stats', force=>true);
select b.relname, a.starelkind, a.staattnum from pg_statistic a left join pg_class b on a.starelid = b.Oid where b.relname in ('t_stats', 't_stats_us') order by b.relname, a.starelkind, a.staattnum;
call gms_stats.delete_index_stats('sc_stats', 't_stats_pk'); -- error, locked
call gms_stats.delete_index_stats('sc_stats', 't_stats_pk', force=>true);
call gms_stats.delete_column_stats('sc_stats', 't_stats', 'c3'); -- error, locked
call gms_stats.delete_column_stats('sc_stats', 't_stats', 'c3', force=>true);
call gms_stats.delete_table_stats('sc_stats', 't_stats'); -- error, locked
call gms_stats.delete_table_stats('sc_stats', 't_stats', force=>true);

call gms_stats.set_table_stats('sc_stats', 't_stats', numrows:='1100'); -- error, locked
call gms_stats.set_table_stats('sc_stats', 't_stats', numrows:='1100', force=>true);
call gms_stats.set_index_stats('sc_stats', 't_stats_pk', numdist:=1000); -- error, locked
call gms_stats.set_index_stats('sc_stats', 't_stats_pk', numdist:=1000, force=>true);
call gms_stats.set_column_stats('sc_stats', 't_stats', 'c2', nullcnt=>0.2); -- error, locked
call gms_stats.set_column_stats('sc_stats', 't_stats', 'c2', nullcnt=>0.2, force=>true);

call gms_stats.delete_schema_stats('sc_stats', force=>true);
call gms_stats.create_stat_table('sc_stats', 't_tmp_stats');
call gms_stats.gather_schema_stats('sc_stats', stattab=>'t_tmp_stats', force=>true);
select b.relname, a.statype, a.staattnum from t_tmp_stats a left join pg_class b on a.starelid = b.Oid where b.relname in ('t_stats', 't_stats_us');

call gms_stats.import_schema_stats('sc_stats', stattab=>'t_tmp_stats'); -- skip t_stats
select b.relname, a.starelkind, a.staattnum from pg_statistic a left join pg_class b on a.starelid = b.Oid where b.relname in ('t_stats', 't_stats_us') order by b.relname, a.starelkind, a.staattnum;
call gms_stats.import_schema_stats('sc_stats', stattab=>'t_tmp_stats', force=>true);
select b.relname, a.starelkind, a.staattnum from pg_statistic a left join pg_class b on a.starelid = b.Oid where b.relname in ('t_stats', 't_stats_us') order by b.relname, a.starelkind, a.staattnum;
call gms_stats.import_index_stats('sc_stats', 't_stats_pk', stattab=>'t_tmp_stats'); -- error, locked
call gms_stats.import_index_stats('sc_stats', 't_stats_pk', stattab=>'t_tmp_stats', force=>true);
call gms_stats.import_column_stats('sc_stats', 't_stats', 'c3', stattab=>'t_tmp_stats'); -- error, locked
call gms_stats.import_column_stats('sc_stats', 't_stats', 'c3', stattab=>'t_tmp_stats', force=>true);
call gms_stats.import_table_stats('sc_stats', 't_stats', stattab=>'t_tmp_stats'); -- error, locked
call gms_stats.import_table_stats('sc_stats', 't_stats', stattab=>'t_tmp_stats', force=>true);

call gms_stats.restore_schema_stats('sc_stats', now()); -- skip t_stats
call gms_stats.restore_schema_stats('sc_stats', now(), force=>true);
call gms_stats.restore_table_stats('sc_stats', 't_stats', now()); -- error, locked
call gms_stats.restore_table_stats('sc_stats', 't_stats', now(), force=>true);

select relname, reltuples, relpages from pg_class where relname in ('t_stats', 't_stats_us');
select b.relname, a.starelkind, a.staattnum, a.stanullfrac, a.stadistinct from pg_statistic a left join pg_class b on a.starelid = b.Oid where b.relname in ('t_stats', 't_stats_us') order by b.relname, a.starelkind, a.staattnum;
select b.relname, a.statype, a.staattnum from pg_statistic_history a left join pg_class b on a.starelid = b.Oid where b.relname in ('t_stats', 't_stats_us') order by b.relname, a.statype, a.staattnum;

call gms_stats.unlock_table_stats('sc_stats', 't_stats');
select c.nspname, a.stalocktype, b.relname, a.lock  from pg_statistic_lock a, pg_class b, pg_namespace c where a.namespaceid = c.Oid and a.relid = b.Oid and b.relname = 't_stats' and a.stalocktype = 't';

-- lock for partition
call prepare_part();
call gms_stats.lock_partition_stats('sc_stats', 't_part', 't_part_list_r');
select c.nspname, a.stalocktype, b.relname, d.relname partname, a.lock from pg_statistic_lock a, pg_class b, pg_namespace c, pg_partition d 
    where a.namespaceid = c.Oid and a.relid = b.Oid and a.partid = d.Oid and b.relname = 't_part' and d.relname = 't_part_list_r' and a.stalocktype = 'p';

call gms_stats.gather_table_stats('sc_stats', 't_part', 't_part_list_r'); -- error
call gms_stats.delete_table_stats('sc_stats', 't_part', 't_part_list_r'); -- error
call gms_stats.set_table_stats('sc_stats', 't_part', 't_part_list_r', numrows=>1100); -- error
call gms_stats.import_table_stats('sc_stats', 't_part', 't_part_list_r', stattab=>'t_tmp_stats'); -- error

call gms_stats.unlock_partition_stats('sc_stats', 't_part', 't_part_list_r');
select c.nspname, a.stalocktype, b.relname, d.relname partname, a.lock from pg_statistic_lock a, pg_class b, pg_namespace c, pg_partition d 
    where a.namespaceid = c.Oid and a.relid = b.Oid and a.partid = d.Oid and b.relname = 't_part' and d.relname = 't_part_list_r' and a.stalocktype = 'p';

-- TEST GATHER_* AND DELETE_*
call prepare_data();
call prepare_part();
call prepare_subpart();
call prepare_ustore();
call prepare_column();

call gms_stats.gather_database_stats();
select relname, reltuples, relpages from pg_class where relname in ('t_stats', 't_stats_us', 't_stats_col', 't_part', 't_subpart') order by relname;
select b.relname, a.starelkind, a.staattnum, a.stanullfrac, a.stadistinct from pg_statistic a left join pg_class b on a.starelid = b.Oid
    where b.relname in ('t_stats', 't_stats_us', 't_stats_col', 't_part', 't_subpart') order by b.relname, a.starelkind, a.staattnum;

call gms_stats.delete_schema_stats('sc_stats');
select relname, reltuples, relpages from pg_class where relname in ('t_stats', 't_stats_us', 't_stats_col', 't_part', 't_subpart') order by relname;
select b.relname, a.starelkind, a.staattnum, a.stanullfrac, a.stadistinct from pg_statistic a left join pg_class b on a.starelid = b.Oid
    where b.relname in ('t_stats', 't_stats_us', 't_stats_col', 't_part', 't_subpart') order by b.relname, a.starelkind, a.staattnum;

call gms_stats.gather_schema_stats('sc_stats');
select relname, reltuples, relpages from pg_class where relname in ('t_stats', 't_stats_us', 't_stats_col', 't_part', 't_subpart') order by relname;
select b.relname, a.starelkind, a.staattnum, a.stanullfrac, a.stadistinct from pg_statistic a left join pg_class b on a.starelid = b.Oid
    where b.relname in ('t_stats', 't_stats_us', 't_stats_col', 't_part', 't_subpart') order by b.relname, a.starelkind, a.staattnum;

call gms_stats.delete_schema_stats('sc_stats');
select relname, reltuples, relpages from pg_class where relname in ('t_stats', 't_stats_us', 't_stats_col', 't_part', 't_subpart') order by relname;
select b.relname, a.starelkind, a.staattnum, a.stanullfrac, a.stadistinct from pg_statistic a left join pg_class b on a.starelid = b.Oid
    where b.relname in ('t_stats', 't_stats_us', 't_stats_col', 't_part', 't_subpart') order by b.relname, a.starelkind, a.staattnum;

select b.relname, count(*) from pg_statistic_history a left join pg_class b on a.starelid = b.Oid where b.relname in ('t_stats', 't_stats_us', 't_stats_col', 't_part', 't_subpart') group by b.relname order by b.relname;

call gms_stats.gather_table_stats('sc_stats', 't_stats');
select relname, reltuples, relpages from pg_class where relname = 't_stats';
select b.relname, a.starelkind, a.staattnum, a.stanullfrac, a.stadistinct from pg_statistic a left join pg_class b on a.starelid = b.Oid where b.relname = 't_stats' order by a.starelkind, a.staattnum;

call gms_stats.delete_table_stats('sc_stats', 't_stats');
select relname, reltuples, relpages from pg_class where relname = 't_stats';
select b.relname, a.starelkind, a.staattnum, a.stanullfrac, a.stadistinct from pg_statistic a left join pg_class b on a.starelid = b.Oid where b.relname = 't_stats' order by a.starelkind, a.staattnum;

call gms_stats.gather_table_stats('sc_stats', 't_stats_us');
select relname, reltuples, relpages from pg_class where relname = 't_stats_us';
select b.relname, a.starelkind, a.staattnum, a.stanullfrac, a.stadistinct from pg_statistic a left join pg_class b on a.starelid = b.Oid where b.relname = 't_stats_us' order by a.starelkind, a.staattnum;

call gms_stats.delete_table_stats('sc_stats', 't_stats_us');
select relname, reltuples, relpages from pg_class where relname = 't_stats_us';
select b.relname, a.starelkind, a.staattnum, a.stanullfrac, a.stadistinct from pg_statistic a left join pg_class b on a.starelid = b.Oid where b.relname = 't_stats_us' order by a.starelkind, a.staattnum;

call gms_stats.gather_table_stats('sc_stats', 't_stats_col');
select relname, reltuples, relpages from pg_class where relname = 't_stats_col';
select b.relname, a.starelkind, a.staattnum, a.stanullfrac, a.stadistinct from pg_statistic a left join pg_class b on a.starelid = b.Oid where b.relname = 't_stats_col' order by a.starelkind, a.staattnum;

call gms_stats.delete_table_stats('sc_stats', 't_stats_col');
select relname, reltuples, relpages from pg_class where relname = 't_stats_col';
select b.relname, a.starelkind, a.staattnum, a.stanullfrac, a.stadistinct from pg_statistic a left join pg_class b on a.starelid = b.Oid where b.relname = 't_stats_col' order by a.starelkind, a.staattnum;

call gms_stats.gather_table_stats('sc_stats', 't_part');
select relname, reltuples, relpages from pg_class where relname = 't_part';
select b.relname, a.starelkind, a.staattnum, a.stanullfrac, a.stadistinct from pg_statistic a left join pg_class b on a.starelid = b.Oid where b.relname = 't_part' order by a.starelkind, a.staattnum;

call gms_stats.delete_table_stats('sc_stats', 't_part');
select relname, reltuples, relpages from pg_class where relname = 't_part';
select b.relname, a.starelkind, a.staattnum, a.stanullfrac, a.stadistinct from pg_statistic a left join pg_class b on a.starelid = b.Oid where b.relname = 't_part' order by a.starelkind, a.staattnum;

select b.relname, count(*) from pg_statistic_history a left join pg_class b on a.starelid = b.Oid where b.relname in ('t_stats', 't_stats_us', 't_stats_col', 't_part', 't_subpart') group by b.relname order by b.relname;

call gms_stats.gather_index_stats('sc_stats', 't_stats_pk'); -- gather index performs same like gather table
select relname, reltuples, relpages from pg_class where relname = 't_stats';
select b.relname, a.starelkind, a.staattnum, a.stanullfrac, a.stadistinct from pg_statistic a left join pg_class b on a.starelid = b.Oid where b.relname = 't_stats' order by a.starelkind, a.staattnum;

call gms_stats.delete_index_stats('sc_stats', 't_stats_pk');
select relname, reltuples, relpages from pg_class where relname = 't_stats';
select b.relname, a.starelkind, a.staattnum, a.stanullfrac, a.stadistinct from pg_statistic a left join pg_class b on a.starelid = b.Oid where b.relname = 't_stats' order by a.starelkind, a.staattnum;

call gms_stats.delete_column_stats('sc_stats', 't_stats', 'c3');
select relname, reltuples, relpages from pg_class where relname = 't_stats' order by relname;
select b.relname, a.starelkind, a.staattnum, a.stanullfrac, a.stadistinct from pg_statistic a left join pg_class b on a.starelid = b.Oid where b.relname = 't_stats' order by b.relname, a.starelkind, a.staattnum;

-- gather to user-table
call gms_stats.delete_schema_stats('sc_stats');
call gms_stats.delete_schema_stats('sc_stats', stattab=>'t_tmp_stats');
call gms_stats.delete_schema_stats('sc_stats', stattab=>'t_tmp_stats2');

select relname, reltuples, relpages from pg_class where relname in ('t_stats', 't_stats_us', 't_stats_col', 't_part', 't_subpart')  order by relname;
select b.relname, a.starelkind, a.staattnum, a.stanullfrac, a.stadistinct from pg_statistic a left join pg_class b on a.starelid = b.Oid
    where b.relname in ('t_stats', 't_stats_us', 't_stats_col', 't_part', 't_subpart')  order by b.relname, a.starelkind, a.staattnum;
select b.relname, a.statype, a.staattnum, a.relpages, a.reltuples, a.stanullfrac, a.stadistinct from t_tmp_stats a left join pg_class b on a.starelid = b.Oid
    where b.relname in ('t_stats', 't_stats_us', 't_stats_col', 't_part', 't_subpart')  order by b.relname, a.statype, a.staattnum;
select b.relname, count(*) from pg_statistic_history a left join pg_class b on a.starelid = b.Oid where b.relname in ('t_stats', 't_stats_us', 't_stats_col', 't_part', 't_subpart') group by b.relname order by b.relname;

call gms_stats.gather_database_stats(stattab=>'t_tmp_stats', statown=>'sc_stats');
select relname, reltuples, relpages from pg_class where relname in ('t_stats', 't_stats_us', 't_stats_col', 't_part', 't_subpart')  order by relname;
select b.relname, a.starelkind, a.staattnum, a.stanullfrac, a.stadistinct from pg_statistic a left join pg_class b on a.starelid = b.Oid
    where b.relname in ('t_stats', 't_stats_us', 't_stats_col', 't_part', 't_subpart') order by b.relname, a.starelkind, a.staattnum;
select b.relname, a.statype, a.staattnum, a.relpages, a.reltuples, a.stanullfrac, a.stadistinct from t_tmp_stats a left join pg_class b on a.starelid = b.Oid 
    where b.relname in ('t_stats', 't_stats_us', 't_stats_col', 't_part', 't_subpart') order by b.relname, a.statype, a.staattnum;

call gms_stats.delete_schema_stats('sc_stats', stattab=>'t_tmp_stats');
select b.relname, a.statype, a.staattnum, a.relpages, a.reltuples, a.stanullfrac, a.stadistinct from t_tmp_stats a left join pg_class b on a.starelid = b.Oid 
    where b.relname in ('t_stats', 't_stats_us', 't_stats_col', 't_part', 't_subpart') order by b.relname, a.statype, a.staattnum;

call gms_stats.gather_schema_stats('sc_stats', stattab=>'t_tmp_stats');
select relname, reltuples, relpages from pg_class where relname in ('t_stats', 't_stats_us', 't_stats_col', 't_part', 't_subpart')  order by relname;
select b.relname, a.starelkind, a.staattnum, a.stanullfrac, a.stadistinct from pg_statistic a left join pg_class b on a.starelid = b.Oid
    where b.relname in ('t_stats', 't_stats_us', 't_stats_col', 't_part', 't_subpart') order by b.relname, a.starelkind, a.staattnum;
select b.relname, a.statype, a.staattnum, a.relpages, a.reltuples, a.stanullfrac, a.stadistinct from t_tmp_stats a left join pg_class b on a.starelid = b.Oid 
    where b.relname in ('t_stats', 't_stats_us', 't_stats_col', 't_part', 't_subpart') order by b.relname, a.statype, a.staattnum;

call gms_stats.delete_schema_stats('sc_stats', stattab=>'t_tmp_stats');
select b.relname, a.statype, a.staattnum, a.relpages, a.reltuples, a.stanullfrac, a.stadistinct from t_tmp_stats a left join pg_class b on a.starelid = b.Oid 
    where b.relname in ('t_stats', 't_stats_us', 't_stats_col', 't_part', 't_subpart') order by b.relname, a.statype, a.staattnum;

call gms_stats.gather_table_stats('sc_stats', 't_stats', stattab=>'t_tmp_stats');
select relname, reltuples, relpages from pg_class where relname in ('t_stats') order by relname;
select b.relname, a.starelkind, a.staattnum, a.stanullfrac, a.stadistinct from pg_statistic a left join pg_class b on a.starelid = b.Oid where b.relname = 't_stats' order by b.relname, a.starelkind, a.staattnum;
select b.relname, a.statype, a.staattnum, a.relpages, a.reltuples, a.stanullfrac, a.stadistinct from t_tmp_stats a left join pg_class b on a.starelid = b.Oid where b.relname = 't_stats' order by a.statype, a.staattnum;

call gms_stats.delete_table_stats('sc_stats', 't_stats', stattab=>'t_tmp_stats');
select b.relname, a.statype, a.staattnum, a.relpages, a.reltuples, a.stanullfrac, a.stadistinct from t_tmp_stats a left join pg_class b on a.starelid = b.Oid where b.relname = 't_stats'order by a.statype, a.staattnum;

insert into t_stats values (generate_series(1001, 1500), 'Noooo!!!', NULL);
call gms_stats.gather_table_stats('sc_stats', 't_stats', stattab=>'t_tmp_stats');
select relname, reltuples, relpages from pg_class where relname in ('t_stats') order by relname;
select b.relname, a.starelkind, a.staattnum, a.stanullfrac, a.stadistinct from pg_statistic a left join pg_class b on a.starelid = b.Oid where b.relname = 't_stats' order by b.relname, a.starelkind, a.staattnum;
select b.relname, a.statype, a.staattnum, a.relpages, a.reltuples, a.stanullfrac, a.stadistinct from t_tmp_stats a left join pg_class b on a.starelid = b.Oid where b.relname = 't_stats' order by a.statype, a.staattnum;

call gms_stats.delete_table_stats('sc_stats', 't_stats', stattab=>'t_tmp_stats');
select b.relname, a.statype, a.staattnum, a.relpages, a.reltuples, a.stanullfrac, a.stadistinct from t_tmp_stats a left join pg_class b on a.starelid = b.Oid where b.relname = 't_stats' order by a.statype, a.staattnum;

call gms_stats.gather_table_stats('sc_stats', 't_stats_us', stattab=>'t_tmp_stats');
select relname, reltuples, relpages from pg_class where relname in ('t_stats_us') order by relname;
select b.relname, a.starelkind, a.staattnum, a.stanullfrac, a.stadistinct from pg_statistic a left join pg_class b on a.starelid = b.Oid where b.relname = 't_stats_us' order by b.relname, a.starelkind, a.staattnum;
select b.relname, a.statype, a.staattnum, a.relpages, a.reltuples, a.stanullfrac, a.stadistinct from t_tmp_stats a left join pg_class b on a.starelid = b.Oid where b.relname = 't_stats_us' order by a.statype, a.staattnum;

call gms_stats.delete_table_stats('sc_stats', 't_stats_us', stattab=>'t_tmp_stats');
select b.relname, a.statype, a.staattnum, a.relpages, a.reltuples, a.stanullfrac, a.stadistinct from t_tmp_stats a left join pg_class b on a.starelid = b.Oid where b.relname = 't_stats_us'order by a.statype, a.staattnum;

call gms_stats.gather_table_stats('sc_stats', 't_stats_col', stattab=>'t_tmp_stats');
select relname, reltuples, relpages from pg_class where relname in ('t_stats_col') order by relname;
select b.relname, a.starelkind, a.staattnum, a.stanullfrac, a.stadistinct from pg_statistic a left join pg_class b on a.starelid = b.Oid where b.relname = 't_stats_col' order by b.relname, a.starelkind, a.staattnum;
select b.relname, a.statype, a.staattnum, a.relpages, a.reltuples, a.stanullfrac, a.stadistinct from t_tmp_stats a left join pg_class b on a.starelid = b.Oid where b.relname = 't_stats_col' order by a.statype, a.staattnum;

call gms_stats.delete_table_stats('sc_stats', 't_stats_col', stattab=>'t_tmp_stats');
select b.relname, a.statype, a.staattnum, a.relpages, a.reltuples, a.stanullfrac, a.stadistinct from t_tmp_stats a left join pg_class b on a.starelid = b.Oid where b.relname = 't_stats_col'order by a.statype, a.staattnum;

call gms_stats.gather_table_stats('sc_stats', 't_part', stattab=>'t_tmp_stats');
select relname, reltuples, relpages from pg_class where relname in ('t_part') order by relname;
select b.relname, a.starelkind, a.staattnum, a.stanullfrac, a.stadistinct from pg_statistic a left join pg_class b on a.starelid = b.Oid where b.relname = 't_part' order by b.relname, a.starelkind, a.staattnum;
select b.relname, a.statype, a.staattnum, a.relpages, a.reltuples, a.stanullfrac, a.stadistinct from t_tmp_stats a left join pg_class b on a.starelid = b.Oid where b.relname = 't_part' order by a.statype, a.staattnum;

call gms_stats.delete_table_stats('sc_stats', 't_part', stattab=>'t_tmp_stats');
select b.relname, a.statype, a.staattnum, a.relpages, a.reltuples, a.stanullfrac, a.stadistinct from t_tmp_stats a left join pg_class b on a.starelid = b.Oid where b.relname = 't_part'order by a.statype, a.staattnum;

call gms_stats.gather_index_stats('sc_stats', 't_stats_pk', stattab=>'t_tmp_stats');
select relname, reltuples, relpages from pg_class where relname in ('t_stats') order by relname;
select b.relname, a.starelkind, a.staattnum, a.stanullfrac, a.stadistinct from pg_statistic a left join pg_class b on a.starelid = b.Oid where b.relname = 't_stats' order by b.relname, a.starelkind, a.staattnum;
select b.relname, a.statype, a.staattnum, a.relpages, a.reltuples, a.stanullfrac, a.stadistinct from t_tmp_stats a left join pg_class b on a.starelid = b.Oid where b.relname = 't_stats' order by a.statype, a.staattnum;

call gms_stats.delete_index_stats('sc_stats', 't_stats_pk', stattab=>'t_tmp_stats');
select b.relname, a.statype, a.staattnum, a.relpages, a.reltuples, a.stanullfrac, a.stadistinct from t_tmp_stats a left join pg_class b on a.starelid = b.Oid where b.relname = 't_stats' order by a.statype, a.staattnum;

call gms_stats.delete_column_stats('sc_stats', 't_stats', 'c3', stattab=>'t_tmp_stats');
select b.relname, a.statype, a.staattnum, a.relpages, a.reltuples, a.stanullfrac, a.stadistinct from t_tmp_stats a left join pg_class b on a.starelid = b.Oid where b.relname = 't_stats' order by a.statype, a.staattnum;

select b.relname, count(*) from pg_statistic_history a left join pg_class b on a.starelid = b.Oid where b.relname in ('t_stats', 't_stats_us', 't_stats_col', 't_part', 't_subpart') group by b.relname order by b.relname;

-- TEST HISTORY, drop table will drop history info
call prepare_data();
select b.relname, count(*) from pg_statistic_history a left join pg_class b on a.starelid = b.Oid where b.relname = 't_stats' group by b.relname order by b.relname;

call prepare_part();
select b.relname, count(*) from pg_statistic_history a left join pg_class b on a.starelid = b.Oid where b.relname = 't_part' group by b.relname order by b.relname;

call prepare_subpart();
select b.relname, count(*) from pg_statistic_history a left join pg_class b on a.starelid = b.Oid where b.relname = 't_subpart' group by b.relname order by b.relname;

call prepare_ustore();
select b.relname, count(*) from pg_statistic_history a left join pg_class b on a.starelid = b.Oid where b.relname = 't_stats_us' group by b.relname order by b.relname;

call prepare_column();
select b.relname, count(*) from pg_statistic_history a left join pg_class b on a.starelid = b.Oid where b.relname = 't_stats_col' group by b.relname order by b.relname;

-- TEST SET
call gms_stats.delete_schema_stats('sc_stats', stattab=>'t_tmp_stats');

call gms_stats.gather_table_stats('sc_stats', 't_stats');
call gms_stats.gather_table_stats('sc_stats', 't_stats_us');
call gms_stats.gather_table_stats('sc_stats', 't_stats_col');
call gms_stats.export_table_stats('sc_stats', 't_stats', stattab=>'t_tmp_stats');
call gms_stats.export_table_stats('sc_stats', 't_stats_us', stattab=>'t_tmp_stats');
call gms_stats.export_table_stats('sc_stats', 't_stats_col', stattab=>'t_tmp_stats');
select relname, relkind, relpages, reltuples from pg_class where relname in ('t_stats', 't_stats_us', 't_stats_col') order by relname;
select b.relname, a.starelkind, a.staattnum, a.stanullfrac, a.stadistinct from pg_statistic a left join pg_class b on a.starelid = b.Oid
    where b.relname in ('t_stats', 't_stats_us', 't_stats_col') order by b.relname, a.starelkind, a.staattnum;

call gms_stats.set_table_stats('sc_stats', 't_stats', numrows=>2345);
select relname, relkind, relpages, reltuples from pg_class where relname = 't_stats';
call gms_stats.set_table_stats('sc_stats', 't_stats', numblks=>16);
select relname, relkind, relpages, reltuples from pg_class where relname = 't_stats';

call gms_stats.set_table_stats('sc_stats', 't_stats_us', numrows=>1234);
select relname, relkind, relpages, reltuples from pg_class where relname = 't_stats_us';
call gms_stats.set_table_stats('sc_stats', 't_stats_us', numblks=>8);
select relname, relkind, relpages, reltuples from pg_class where relname = 't_stats_us';

call gms_stats.set_table_stats('sc_stats', 't_stats_col', numrows=>789);
select relname, relkind, relpages, reltuples from pg_class where relname = 't_stats_col';
call gms_stats.set_table_stats('sc_stats', 't_stats_col', numblks=>15);
select relname, relkind, relpages, reltuples from pg_class where relname = 't_stats_col';

call gms_stats.set_index_stats('sc_stats', 't_stats_pk', numdist=>100);
select staattnum, stanullfrac, stadistinct from pg_statistic where starelid = 't_stats'::regclass and staattnum = 1;

call gms_stats.set_index_stats('sc_stats', 't_stats_us_pk', numdist=>200);
select staattnum, stanullfrac, stadistinct from pg_statistic where starelid = 't_stats_us'::regclass and staattnum = 1;

call gms_stats.set_index_stats('sc_stats', 't_stats_col_pk', numdist=>300);
select staattnum, stanullfrac, stadistinct from pg_statistic where starelid = 't_stats_col'::regclass and staattnum = 1;

call gms_stats.set_column_stats('sc_stats', 't_stats', 'c2', nullcnt=>0.2);
select staattnum, stanullfrac, stadistinct from pg_statistic where starelid = 't_stats'::regclass and staattnum = 2;
call gms_stats.set_column_stats('sc_stats', 't_stats', 'c2', distcnt=>1000);
select staattnum, stanullfrac, stadistinct from pg_statistic where starelid = 't_stats'::regclass and staattnum = 2;

call gms_stats.set_column_stats('sc_stats', 't_stats_us', 'c2', nullcnt=>0.2);
select staattnum, stanullfrac, stadistinct from pg_statistic where starelid = 't_stats_us'::regclass and staattnum = 2;
call gms_stats.set_column_stats('sc_stats', 't_stats_us', 'c2', distcnt=>1000);
select staattnum, stanullfrac, stadistinct from pg_statistic where starelid = 't_stats_us'::regclass and staattnum = 2;

call gms_stats.set_column_stats('sc_stats', 't_stats_col', 'c2', nullcnt=>0.2);
select staattnum, stanullfrac, stadistinct from pg_statistic where starelid = 't_stats_col'::regclass and staattnum = 2;
call gms_stats.set_column_stats('sc_stats', 't_stats_col', 'c2', distcnt=>1000);
select staattnum, stanullfrac, stadistinct from pg_statistic where starelid = 't_stats_col'::regclass and staattnum = 2;

select b.relname, a.statype, a.staattnum, a.relpages, a.reltuples, a.stanullfrac, a.stadistinct from t_tmp_stats a left join pg_class b on a.starelid = b.Oid 
    where b.relname in ('t_stats', 't_stats_us', 't_stats_col') order by b.relname, a.statype, a.staattnum;

call gms_stats.set_table_stats('sc_stats', 't_stats', stattab=>'t_tmp_stats', numrows=>1100);
select staattnum, relpages, reltuples, stanullfrac, stadistinct from t_tmp_stats where starelid = 't_stats'::regclass order by staattnum;
call gms_stats.set_table_stats('sc_stats', 't_stats', stattab=>'t_tmp_stats', numblks=>10);
select staattnum, relpages, reltuples, stanullfrac, stadistinct from t_tmp_stats where starelid = 't_stats'::regclass order by staattnum;

call gms_stats.set_table_stats('sc_stats', 't_stats_us', stattab=>'t_tmp_stats', numrows=>1100);
select staattnum, relpages, reltuples, stanullfrac, stadistinct from t_tmp_stats where starelid = 't_stats_us'::regclass order by staattnum;
call gms_stats.set_table_stats('sc_stats', 't_stats_us', stattab=>'t_tmp_stats', numblks=>10);
select staattnum, relpages, reltuples, stanullfrac, stadistinct from t_tmp_stats where starelid = 't_stats_us'::regclass order by staattnum;

call gms_stats.set_table_stats('sc_stats', 't_stats_col', stattab=>'t_tmp_stats', numrows=>1100);
select staattnum, relpages, reltuples, stanullfrac, stadistinct from t_tmp_stats where starelid = 't_stats_col'::regclass order by staattnum;
call gms_stats.set_table_stats('sc_stats', 't_stats_col', stattab=>'t_tmp_stats', numblks=>10);
select staattnum, relpages, reltuples, stanullfrac, stadistinct from t_tmp_stats where starelid = 't_stats_col'::regclass order by staattnum;

call gms_stats.set_index_stats('sc_stats', 't_stats_pk', stattab=>'t_tmp_stats', numdist=>100);
select staattnum, relpages, reltuples, stanullfrac, stadistinct from t_tmp_stats where starelid = 't_stats'::regclass order by staattnum;

call gms_stats.set_index_stats('sc_stats', 't_stats_us_pk', stattab=>'t_tmp_stats', numdist=>200);
select staattnum, relpages, reltuples, stanullfrac, stadistinct from t_tmp_stats where starelid = 't_stats_us'::regclass order by staattnum;

call gms_stats.set_index_stats('sc_stats', 't_stats_col_pk', stattab=>'t_tmp_stats', numdist=>300);
select staattnum, relpages, reltuples, stanullfrac, stadistinct from t_tmp_stats where starelid = 't_stats_col'::regclass order by staattnum;

call gms_stats.set_column_stats('sc_stats', 't_stats', 'c2', stattab=>'t_tmp_stats', nullcnt=>0.2);
select staattnum, relpages, reltuples, stanullfrac, stadistinct from t_tmp_stats where starelid = 't_stats'::regclass order by staattnum;
call gms_stats.set_column_stats('sc_stats', 't_stats', 'c2', stattab=>'t_tmp_stats', distcnt=>1000);
select staattnum, relpages, reltuples, stanullfrac, stadistinct from t_tmp_stats where starelid = 't_stats'::regclass order by staattnum;

call gms_stats.set_column_stats('sc_stats', 't_stats_us', 'c2', stattab=>'t_tmp_stats', nullcnt=>0.2);
select staattnum, relpages, reltuples, stanullfrac, stadistinct from t_tmp_stats where starelid = 't_stats_us'::regclass order by staattnum;
call gms_stats.set_column_stats('sc_stats', 't_stats_us', 'c2', stattab=>'t_tmp_stats', distcnt=>1000);
select staattnum, relpages, reltuples, stanullfrac, stadistinct from t_tmp_stats where starelid = 't_stats_us'::regclass order by staattnum;

call gms_stats.set_column_stats('sc_stats', 't_stats_col', 'c2', stattab=>'t_tmp_stats', nullcnt=>0.2);
select staattnum, relpages, reltuples, stanullfrac, stadistinct from t_tmp_stats where starelid = 't_stats_col'::regclass order by staattnum;
call gms_stats.set_column_stats('sc_stats', 't_stats_col', 'c2', stattab=>'t_tmp_stats', distcnt=>1000);
select staattnum, relpages, reltuples, stanullfrac, stadistinct from t_tmp_stats where starelid = 't_stats_col'::regclass order by staattnum;

-- TEST IMPORT AND EXPORT
call prepare_data();
call prepare_part();
call prepare_subpart();
call prepare_ustore();
call prepare_column();
call gms_stats.delete_schema_stats('sc_stats', stattab=>'t_tmp_stats');
call gms_stats.gather_table_stats('sc_stats', 't_stats');
call gms_stats.gather_table_stats('sc_stats', 't_stats_us');
call gms_stats.gather_table_stats('sc_stats', 't_stats_col');
call gms_stats.export_schema_stats('sc_stats', stattab=>'t_tmp_stats');

select b.relname, a.statype, a.staattnum, a.relpages, a.reltuples, a.stanullfrac, a.stadistinct from t_tmp_stats a left join pg_class b on a.starelid = b.Oid 
    where b.relname in ('t_stats', 't_stats_us', 't_stats_col') order by b.relname, a.statype, a.staattnum;

insert into t_stats values (generate_series(1001, 1500), 'Noooo!!!', NULL);
insert into t_stats_us values (generate_series(1001, 1800), 'YES', NULL);
insert into t_stats_col values (generate_series(1001, 1300), 'BBBBBBYY', NULL);

call gms_stats.gather_table_stats('sc_stats', 't_stats');
call gms_stats.gather_table_stats('sc_stats', 't_stats_us');
call gms_stats.gather_table_stats('sc_stats', 't_stats_col');
call gms_stats.export_schema_stats('sc_stats', stattab=>'t_tmp_stats2');

select b.relname, a.statype, a.staattnum, a.relpages, a.reltuples, a.stanullfrac, a.stadistinct from t_tmp_stats2 a left join pg_class b on a.starelid = b.Oid 
    where b.relname in ('t_stats', 't_stats_us', 't_stats_col') order by b.relname, a.statype, a.staattnum;

select relname, relkind, relpages, reltuples from pg_class where relname in ('t_stats', 't_stats_us', 't_stats_col') order by relname;
select b.relname, a.starelkind, a.staattnum, a.stanullfrac, a.stadistinct from pg_statistic a left join pg_class b on a.starelid = b.Oid
    where b.relname in ('t_stats', 't_stats_us', 't_stats_col') order by b.relname, a.starelkind, a.staattnum;

call gms_stats.import_schema_stats('sc_stats', stattab=>'t_tmp_stats');
select relname, relkind, relpages, reltuples from pg_class where relname in ('t_stats', 't_stats_us', 't_stats_col') order by relname;
select b.relname, a.starelkind, a.staattnum, a.stanullfrac, a.stadistinct from pg_statistic a left join pg_class b on a.starelid = b.Oid
    where b.relname in ('t_stats', 't_stats_us', 't_stats_col') order by b.relname, a.starelkind, a.staattnum;

call gms_stats.import_table_stats('sc_stats', 't_stats', stattab=>'t_tmp_stats2');
select relname, relkind, relpages, reltuples from pg_class where relname in ('t_stats', 't_stats_us', 't_stats_col') order by relname;
select b.relname, a.starelkind, a.staattnum, a.stanullfrac, a.stadistinct from pg_statistic a left join pg_class b on a.starelid = b.Oid
    where b.relname in ('t_stats', 't_stats_us', 't_stats_col') order by b.relname, a.starelkind, a.staattnum;

call gms_stats.delete_table_stats('sc_stats', 't_stats', stattab=>'t_tmp_stats2');
select b.relname, a.statype, a.staattnum, a.relpages, a.reltuples, a.stanullfrac, a.stadistinct from t_tmp_stats2 a left join pg_class b on a.starelid = b.Oid 
    where b.relname in ('t_stats', 't_stats_us', 't_stats_col') order by b.relname, a.statype, a.staattnum;

call gms_stats.export_index_stats('sc_stats', 't_stats_pk', stattab=>'t_tmp_stats2');
select b.relname, a.statype, a.staattnum, a.relpages, a.reltuples, a.stanullfrac, a.stadistinct from t_tmp_stats2 a left join pg_class b on a.starelid = b.Oid 
    where b.relname in ('t_stats', 't_stats_us', 't_stats_col') order by b.relname, a.statype, a.staattnum;

call gms_stats.export_column_stats('sc_stats', 't_stats', 'c2', stattab=>'t_tmp_stats2');
select b.relname, a.statype, a.staattnum, a.relpages, a.reltuples, a.stanullfrac, a.stadistinct from t_tmp_stats2 a left join pg_class b on a.starelid = b.Oid 
    where b.relname in ('t_stats', 't_stats_us', 't_stats_col') order by b.relname, a.statype, a.staattnum;

call gms_stats.delete_table_stats('sc_stats', 't_stats');
select staattnum, stanullfrac, stadistinct from pg_statistic where starelid = 't_stats'::regclass order by staattnum;

call gms_stats.import_index_stats('sc_stats', 't_stats_pk', stattab=>'t_tmp_stats');
select staattnum, stanullfrac, stadistinct from pg_statistic where starelid = 't_stats'::regclass order by staattnum;

call gms_stats.import_column_stats('sc_stats', 't_stats', 'c2', stattab=>'t_tmp_stats');
select staattnum, stanullfrac, stadistinct from pg_statistic where starelid = 't_stats'::regclass order by staattnum;

-- drop column then export or import
call gms_stats.delete_schema_stats('sc_stats', stattab=>'t_tmp_stats');
call gms_stats.delete_schema_stats('sc_stats', stattab=>'t_tmp_stats2');

call gms_stats.gather_table_stats('sc_stats', 't_stats', stattab=>'t_tmp_stats');
select relname, relkind, relpages, reltuples from pg_class where relname = 't_stats';
select staattnum, stanullfrac, stadistinct from pg_statistic where starelid = 't_stats'::regclass order by staattnum;
select b.relname, a.statype, a.staattnum, a.relpages, a.reltuples, a.stanullfrac, a.stadistinct from t_tmp_stats a left join pg_class b on a.starelid = b.Oid where b.relname = 't_stats' order by a.staattnum;

alter table t_stats drop c3; -- do drop column

call gms_stats.import_table_stats('sc_stats', 't_stats', stattab:='t_tmp_stats'); -- skip c3
call gms_stats.import_column_stats('sc_stats', 't_stats', 'c3', stattab:='t_tmp_stats'); -- error

select staattnum, stanullfrac, stadistinct from pg_statistic where starelid = 't_stats'::regclass order by staattnum;
select staattnum, relpages, reltuples, stanullfrac, stadistinct from t_tmp_stats where starelid = 't_stats'::regclass order by staattnum;

call gms_stats.export_table_stats('sc_stats', 't_stats', stattab:='t_tmp_stats'); -- delete c3 in t_tmp_stats
call gms_stats.export_column_stats('sc_stats', 't_stats', 'c3', stattab:='t_tmp_stats'); -- error

select staattnum, stanullfrac, stadistinct from pg_statistic where starelid = 't_stats'::regclass order by staattnum;
select staattnum, relpages, reltuples, stanullfrac, stadistinct from t_tmp_stats where starelid = 't_stats'::regclass order by staattnum;

-- PURGE AND RESTORE
call prepare_data();
call prepare_ustore();
call prepare_column();

call gms_stats.gather_schema_stats('sc_stats');
select pg_sleep(2);
insert into t_stats values (generate_series(1001, 1500), 'Noooo!!!', NULL);
insert into t_stats_us values (generate_series(1001, 1800), 'YES', NULL);
insert into t_stats_col values (generate_series(1001, 1300), 'BBBBBBYY', NULL);

select relname, relkind, relpages, reltuples from pg_class where relname in ('t_stats', 't_stats_us', 't_stats_col') order by relname;
select b.relname, staattnum, stanullfrac, stadistinct from pg_statistic a left join pg_class b on a.starelid = b.Oid where b.relname in ('t_stats', 't_stats_us', 't_stats_col') order by b.relname, staattnum;

declare
    asOfTime timestamp with time zone;
begin
    -- get first analyze time
    select current_analyzetime into asOfTime from pg_statistic_history where starelid = 't_stats'::regclass order by current_analyzetime limit 1;
    -- restore
    gms_stats.restore_table_stats('sc_stats', 't_stats', asOfTime);
end;
/
select relname, relkind, relpages, reltuples from pg_class where relname = 't_stats' order by relname;
select b.relname, staattnum, stanullfrac, stadistinct from pg_statistic a left join pg_class b on a.starelid = b.Oid where b.relname = 't_stats' order by staattnum;

declare
    asOfTime timestamp with time zone;
begin
    -- get first analyze time
    select current_analyzetime into asOfTime from pg_statistic_history where starelid = 't_stats_us'::regclass order by current_analyzetime limit 1;
    -- restore
    gms_stats.restore_table_stats('sc_stats', 't_stats_us', asOfTime);
end;
/
select relname, relkind, relpages, reltuples from pg_class where relname = 't_stats_us' order by relname;
select b.relname, staattnum, stanullfrac, stadistinct from pg_statistic a left join pg_class b on a.starelid = b.Oid where b.relname = 't_stats_us' order by staattnum;

declare
    asOfTime timestamp with time zone;
begin
    -- get first analyze time
    select current_analyzetime into asOfTime from pg_statistic_history where starelid = 't_stats_col'::regclass order by current_analyzetime limit 1;
    -- restore
    gms_stats.restore_table_stats('sc_stats', 't_stats_col', asOfTime);
end;
/
select relname, relkind, relpages, reltuples from pg_class where relname = 't_stats_col' order by relname;
select b.relname, staattnum, stanullfrac, stadistinct from pg_statistic a left join pg_class b on a.starelid = b.Oid where b.relname = 't_stats_col' order by staattnum;

select relname, relkind, relpages, reltuples from pg_class where relname in ('t_stats', 't_stats_us', 't_stats_col') order by relname;
select b.relname, staattnum, stanullfrac, stadistinct from pg_statistic a left join pg_class b on a.starelid = b.Oid where b.relname in ('t_stats', 't_stats_us', 't_stats_col') order by b.relname, staattnum;
select b.relname, a.statype, a.staattnum, a.stanullfrac, a.stadistinct, a.reltuples, a.relpages, stalocktype 
    from pg_statistic_history a left join pg_class b on a.starelid = b.Oid where b.relname in ('t_stats', 't_stats_us', 't_stats_col') order by b.relname, current_analyzetime, a.statype, a.staattnum;

declare
    asOfTime timestamp with time zone;
begin
    -- get first analyze time
    select current_analyzetime into asOfTime from pg_statistic_history where starelid in ('t_stats'::regclass, 't_stats_us'::regclass, 't_stats_col'::regclass) order by current_analyzetime limit 1;
    asOfTime := asOfTime - INTERVAL '1 hour';
    -- restore
    gms_stats.restore_schema_stats('sc_stats', asOfTime);
end;
/
select relname, relkind, relpages, reltuples from pg_class where relname in ('t_stats', 't_stats_us', 't_stats_col') order by relname;
select b.relname, staattnum, stanullfrac, stadistinct from pg_statistic a left join pg_class b on a.starelid = b.Oid where b.relname in ('t_stats', 't_stats_us', 't_stats_col') order by b.relname, staattnum;

-- TEST PRIVILEGES
create user user01 password 'stats@1234';
grant all privileges on schema sc_stats to user01;
grant select on table sc_stats.t_stats to user01;
grant vacuum on table sc_stats.t_stats_us to user01;
set session authorization user01 password 'stats@1234';

create table t_stats_pri (c1 int, c2 varchar2(100), constraint t_stats_pri_pk primary key (c1));
insert into t_stats_pri values (generate_series(1, 2000), 'OK');

-- gather table need vacuum or dba
call gms_stats.gather_table_stats('sc_stats', 't_stats'); -- error
call gms_stats.gather_table_stats('sc_stats', 't_stats_us');
call gms_stats.gather_table_stats('sc_stats', 't_stats_pri');

-- gather_database
call gms_stats.gather_database_stats(); -- error

-- set_* need class owner or dba privileges
call gms_stats.set_table_stats('sc_stats', 't_stats', numblks=>10); -- error
call gms_stats.set_table_stats('sc_stats', 't_stats_us', numblks=>10); -- error
call gms_stats.set_table_stats('sc_stats', 't_stats_pri', numblks=>10);

call gms_stats.set_index_stats('sc_stats', 't_stats_pk', numdist=>10); -- error
call gms_stats.set_index_stats('sc_stats', 't_stats_us_pk', numdist=>10); -- error
call gms_stats.set_index_stats('sc_stats', 't_stats_pri_pk', numdist=>10);

call gms_stats.set_column_stats('sc_stats', 't_stats', 'c2', distcnt=>10); -- error
call gms_stats.set_column_stats('sc_stats', 't_stats_us', 'c2', distcnt=>10); -- error
call gms_stats.set_column_stats('sc_stats', 't_stats_pri', 'c2', distcnt=>10);

-- delete_* need classowner or dba
call gms_stats.delete_table_stats('sc_stats', 't_stats'); -- error
call gms_stats.delete_table_stats('sc_stats', 't_stats_us'); -- error
call gms_stats.delete_table_stats('sc_stats', 't_stats_pri');

call gms_stats.delete_index_stats('sc_stats', 't_stats_pk'); -- error
call gms_stats.delete_index_stats('sc_stats', 't_stats_us_pk'); -- error
call gms_stats.delete_index_stats('sc_stats', 't_stats_pri_pk');

call gms_stats.delete_column_stats('sc_stats', 't_stats', 'c2'); -- error
call gms_stats.delete_column_stats('sc_stats', 't_stats_us', 'c2'); -- error
call gms_stats.delete_column_stats('sc_stats', 't_stats_pri', 'c2');

call gms_stats.delete_schema_stats('sc_stats'); -- error

-- export_* need classowner or dba
call gms_stats.export_schema_stats('sc_stats', stattab=>'t_tmp_stats'); -- error

call gms_stats.export_table_stats('sc_stats', 't_stats', stattab=>'t_tmp_stats'); -- error
call gms_stats.export_table_stats('sc_stats', 't_stats_us', stattab=>'t_tmp_stats'); -- error
call gms_stats.export_table_stats('sc_stats', 't_stats_pri', stattab=>'t_tmp_stats');

call gms_stats.export_index_stats('sc_stats', 't_stats_pk', stattab=>'t_tmp_stats'); -- error
call gms_stats.export_index_stats('sc_stats', 't_stats_us_pk', stattab=>'t_tmp_stats'); -- error
call gms_stats.export_index_stats('sc_stats', 't_stats_pri_pk', stattab=>'t_tmp_stats');

call gms_stats.export_column_stats('sc_stats', 't_stats', 'c2', stattab=>'t_tmp_stats'); -- error
call gms_stats.export_column_stats('sc_stats', 't_stats_us', 'c2', stattab=>'t_tmp_stats'); -- error
call gms_stats.export_column_stats('sc_stats', 't_stats_pri', 'c2', stattab=>'t_tmp_stats');

-- import_* need classowner or dba
call gms_stats.import_schema_stats('sc_stats', stattab=>'t_tmp_stats');

call gms_stats.import_table_stats('sc_stats', 't_stats', stattab=>'t_tmp_stats'); -- error
call gms_stats.import_table_stats('sc_stats', 't_stats_us', stattab=>'t_tmp_stats'); -- error
call gms_stats.import_table_stats('sc_stats', 't_stats_pri', stattab=>'t_tmp_stats');

call gms_stats.import_index_stats('sc_stats', 't_stats_pk', stattab=>'t_tmp_stats'); -- error
call gms_stats.import_index_stats('sc_stats', 't_stats_us_pk', stattab=>'t_tmp_stats'); -- error
call gms_stats.import_index_stats('sc_stats', 't_stats_pri_pk', stattab=>'t_tmp_stats');

call gms_stats.import_column_stats('sc_stats', 't_stats', 'c2', stattab=>'t_tmp_stats'); -- error
call gms_stats.import_column_stats('sc_stats', 't_stats_us', 'c2', stattab=>'t_tmp_stats'); -- error
call gms_stats.import_column_stats('sc_stats', 't_stats_pri', 'c2', stattab=>'t_tmp_stats');

drop table t_stats_pri;
reset session authorization;

revoke create, drop on schema sc_stats from user01;
set session authorization user01 password 'stats@1234';
call gms_stats.drop_stat_table('sc_stats', 't_tmp_stats'); -- error

reset session authorization;
drop user user01 cascade;

call gms_stats.drop_stat_table('sc_stats', 't_tmp_stats');
call gms_stats.drop_stat_table('sc_stats', 't_tmp_stats2');

drop table if exists t_stats;
drop table if exists t_part;
drop table if exists t_sub_part;
drop table if exists t_stats_us;
drop table if exists t_stats_col;

drop procedure prepare_data();
drop procedure prepare_part();
drop procedure prepare_subpart();
drop procedure prepare_ustore();
drop procedure prepare_column();

reset current_schema;
drop schema sc_stats cascade;
