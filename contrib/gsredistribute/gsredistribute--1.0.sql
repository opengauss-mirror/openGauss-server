/* contrib/gsredistribute/gsredistribute--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION gsredistribute" to load this file. \quit

CREATE OR REPLACE FUNCTION pg_catalog.pg_get_redis_rel_end_ctid(text, name, int, int)
RETURNS tid
AS 'MODULE_PATHNAME','pg_get_redis_rel_end_ctid'
LANGUAGE C STABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pg_get_redis_rel_start_ctid(text, name, int, int)
RETURNS tid
AS 'MODULE_PATHNAME','pg_get_redis_rel_start_ctid'
LANGUAGE C STABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pg_enable_redis_proc_cancelable()
RETURNS bool
AS 'MODULE_PATHNAME','pg_enable_redis_proc_cancelable'
LANGUAGE C IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pg_disable_redis_proc_cancelable()
RETURNS bool
AS 'MODULE_PATHNAME','pg_disable_redis_proc_cancelable'
LANGUAGE C IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pg_tupleid_get_ctid_to_bigint(tid)
RETURNS bigint
AS 'MODULE_PATHNAME','pg_tupleid_get_ctid_to_bigint'
LANGUAGE C STABLE SHIPPABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pg_tupleid_get_blocknum(tid)
RETURNS bigint
AS 'MODULE_PATHNAME','pg_tupleid_get_blocknum'
LANGUAGE C STABLE SHIPPABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pg_tupleid_get_offset(tid)
RETURNS int
AS 'MODULE_PATHNAME','pg_tupleid_get_offset'
LANGUAGE C STABLE SHIPPABLE NOT FENCED;

-- cancel the task of one table, redis_oid means the oid of redis_old_ table oid
CREATE OR REPLACE FUNCTION pg_catalog.cancel_redis_task(
                  IN        redis_oid           oid
)
RETURNS void  
AS
$$
DECLARE
    database_name   name;
    var_r           record;
    sql             text;
BEGIN
    select current_database() into database_name;
    if exists (select * from pg_job j, pg_job_proc p 
            where j.dbname = database_name and j.job_id = p.job_id and 
            p.what like 'call redis_ts_table('||redis_oid||'%') then
        for var_r in (select j.job_id 
                        from pg_job j, pg_job_proc p 
                        where j.dbname = database_name and j.job_id = p.job_id and 
                        p.what like 'call redis_ts_table('||redis_oid||'%') loop
            sql := 'select DBE_TASK.cancel('||var_r.job_id||');';
            EXECUTE sql;
        end loop;
    end if;
END;
$$
LANGUAGE plpgsql;

-- cancel all the task in the database, task function is redis_ts_table
CREATE OR REPLACE FUNCTION pg_catalog.cancel_all_redis_task()
RETURNS void  
AS
$$
DECLARE
    v_count int;
    database_name name;
    sql text;
BEGIN
    select current_database() into database_name;
    if exists (select * from pg_job j, pg_job_proc p where j.dbname = database_name and j.job_id = p.job_id 
                    and p.what like 'call redis_ts_table(%') then
        for var_r in (select j.job_id 
                        from pg_job j, pg_job_proc p 
                        where j.dbname = database_name and j.job_id = p.job_id 
                        and p.what like 'call redis_ts_table(%') loop
            sql := 'select DBE_TASK.cancel('||var_r.job_id||');';
            EXECUTE sql;
        end loop;
    end if;  
END;
$$
LANGUAGE plpgsql;

-- submit the task using function cancel_unuse_redis
CREATE OR REPLACE FUNCTION pg_catalog.submit_cancel_redis_task(
                    IN        schedule_interval    interval default '1 hour')
RETURNS void  
AS
$$
DECLARE
    v_count int;
    database_name name;
    sql text;
    job_id integer;
    time_interval numeric;
BEGIN
    select current_database() into database_name;
    if schedule_interval < interval '30 minutes' then
        raise exception 'This task interval cannot be less than 30 minutes';
    end if;
    if not exists (select j.job_id, p.what 
                        from pg_job j, pg_job_proc p 
                        where j.dbname = database_name and j.job_id = p.job_id and p.what like 'call redis_ts_table(%') then
        return;
    end if;
    -- the job can only submit once
    if exists (select * from pg_job j, pg_job_proc p 
                where j.dbname = database_name and j.job_id = p.job_id and p.what like 'call cancel_unuse_redis()') then
        raise notice 'The task for cancel_unuse_redis exists, cannot sumbit again.';
        return;
    end if;
    sql := ' SELECT EXTRACT(epoch FROM interval '''||schedule_interval||''')/3600';
    EXECUTE sql INTO time_interval;
    sql := 'SELECT DBE_TASK.submit(''call cancel_unuse_redis()'', sysdate, ''sysdate + '||time_interval||' / 24'');';
    EXECUTE sql into job_id;
END;
$$
LANGUAGE plpgsql;

-- check the job using redis_ts_table, if satisfy some conditins, will cancal the task redis_ts_table
-- and delete from table redis_timeseries_detail, table redis_timeseries_detail records the table that does not 
-- finish the redistribiute process
CREATE OR REPLACE FUNCTION pg_catalog.cancel_unuse_redis()
RETURNS void  
AS
$$
DECLARE
    database_name name;
    sql text;
    var_r record;
    var_jobid record;
BEGIN
    select current_database() into database_name;
    if not exists (select * from public.redis_timeseries_detail) then
        raise exception 'no ts_table is in redistribution';
        return;
    end if;
    for var_r in (select reloid from public.redis_timeseries_detail) loop
        if exists(select * from pg_class where oid = var_r.reloid) then
            continue;
        end if;
        for var_jobid in (select j.job_id, p.what
                        from pg_job j, pg_job_proc p
                        where j.dbname = database_name and j.job_id = p.job_id and
                        p.what like 'call redis_ts_table('||var_r.reloid||'%') loop
            sql := 'select DBE_TASK.cancel('||var_jobid.job_id||');';
            EXECUTE sql;
            sql := 'delete from redis_timeseries_detail where reloid = '||var_r.reloid||';';
            execute sql;
        end loop;
    end loop;   
END;
$$
LANGUAGE plpgsql;

-- the job to transfer the data from redis_old to redis_new, the transfer is the time delta, 
-- for timeseries table, have one tstime, using the condition by transfer_interval on tstime
-- drop the partition if it is empty, if it is last partition, just drop the table
CREATE OR REPLACE FUNCTION pg_catalog.redis_ts_table(
    --the old table in old nodes, relname is redis_old_
            IN        redis_oid           oid)
RETURNS void  
AS
$$
DECLARE
    v_count int;
    old_rel_name name;
    new_rel_name name;
    origin_relname name;
    schemaname name;
    schemaoid oid;
    part_name name;
    start_time timestamptz;
    end_time timestamptz;
    i_count bigint;
    sql text;
    rulename name;
    max_try_transfer int;
BEGIN
    if not exists(select * from pg_class where oid = redis_oid) then
        raise notice 'oid % does not exists.', redis_oid;
        return;
    end if;
    -- revoke this function must guarantee the redis_old and redis_new exists, and it is timeseries table
    select c.relname, n.nspname, c.relnamespace from pg_class c, pg_namespace n 
                                                where c.oid = redis_oid and n.oid = c.relnamespace
                                                into old_rel_name, schemaname, schemaoid;
    origin_relname = right(old_rel_name, length(old_rel_name) - length('redis_old_'));
    new_rel_name = 'redis_new_' || origin_relname;
    --check if table exists, because last job may drop the table, but next job still execute
    if not exists(select * from pg_class where relname = old_rel_name and relnamespace = schemaoid) then
        raise notice 'Table %.% does not exists.', schemaname, old_rel_name;
        return;
    end if;
    if not exists(select * from pg_class where relname = new_rel_name and relnamespace = schemaoid) then
        raise notice 'Table %.% does not exists.', schemaname, new_rel_name;
        return;
    end if;

    -- cannot just set in current transaction, the kernel check the usess, input parameter must be false
    sql = 'select set_config(''enable_cluster_resize'', ''on'', false)';
    execute sql;
    -- table is empty drop it
    sql = 'select count(*) from "'||schemaname||'"."'||old_rel_name||'";';
    execute sql into v_count;
    -- no data in old_rel_name drop the table
    if v_count = 0 then
        -- drop the redis_view and rule cascade
        sql = 'drop table if exists "'||schemaname||'"."'||old_rel_name||'" cascade;';
        execute sql;
        sql = 'alter table "'||schemaname||'"."'||new_rel_name||'" rename to "'||origin_relname||'";';
        execute sql;
        return;
    end if;
    -- we have already handle condition with empty table, at least one partition should have data
    while 1 loop
        select relname from pg_partition where parttype = 'p' and parentid = redis_oid 
            order by boundaries[1] desc limit 1 into part_name;

        sql := 'select count(*) from "'||schemaname||'"."'||old_rel_name||'" partition('||part_name||');';
        execute sql into v_count;
        -- if this partition has no value, drop this partition
        if v_count = 0 then
            sql = 'alter table "'||schemaname||'"."'||old_rel_name||'" drop partition "'||part_name||'";';
            execute sql;
            CONTINUE;
        else
            EXIT;
        end if;
    end loop;
    -- the partition has value, so execute delete this time
    sql = 'set session_timeout = 0;';
    execute sql;
    sql = 'set enable_analyze_check = off;';
    execute sql;
    sql = 'insert into "'||schemaname||'"."'||new_rel_name||
        '" select * from "'||schemaname||'"."'||old_rel_name||'" partition("'||part_name||'") ';
    execute sql;
    sql = 'select count(*) from pg_partition where parentid = '||redis_oid||' and parttype = ''p'';';
    execute sql into v_count;
    -- if only only one partition left drop table cascade
    if v_count = 1 then
        sql = 'drop table if exists "'||schemaname||'"."'||old_rel_name||'" cascade;';
        execute sql;
        sql = 'alter table "'||schemaname||'"."'||new_rel_name||'" rename to "'||origin_relname||'";';
        execute sql;
    else
        sql = 'alter table "'||schemaname||'"."'||old_rel_name||'" drop partition "'||part_name||'";';
        execute sql;
    end if;
END;
$$
LANGUAGE plpgsql;

-- submit the redis_ts_table task for one table, old_oid means redis_old_ which exists in the old group
CREATE OR REPLACE FUNCTION pg_catalog.submit_redis_task(
                  IN        old_oid         oid,
                  IN        schedule_interval    interval default '1 minute'
)
RETURNS void  
AS
$$
DECLARE
    sql                    text;
    time_interval          numeric;
    database_name          name;
    job_id                 int;
BEGIN
    select current_database() into database_name;
    if exists (select * from pg_job j, pg_job_proc p 
                where j.dbname = database_name and j.job_id = p.job_id and 
                p.what like 'call redis_ts_table('||old_oid||'%' limit 1) then
        raise notice 'The task for table oid % exists, cannot sumbit again.', old_oid;
        return;
    end if;
    sql := ' SELECT EXTRACT(epoch FROM interval '''||schedule_interval||''')/3600';
    EXECUTE sql INTO time_interval;
    sql := 'SELECT DBE_TASK.submit(''call redis_ts_table('||old_oid||');'',
                                 sysdate,  ''sysdate + '||time_interval||' / 24'');';
    EXECUTE sql into job_id;
    -- if drop the table redis_new_ or redis_old_, must cancel the task mannual
END;
$$
LANGUAGE plpgsql;

-- the first is the time column that transfer to other table, the second is the intervel that call the job
CREATE OR REPLACE FUNCTION pg_catalog.submit_all_redis_task(
                    IN        schedule_interval    interval default '1 minute')
RETURNS void  
AS
$$
DECLARE
    old_group_name name;
    var_r record;
    v_count int;
    sql text;
BEGIN
    select count(*) from pgxc_group where in_redistribution = 'y' into v_count;
    if v_count = 0 then
        raise exception 'No old group in pgxc_group, no need to redistribute';
    end if;
    select group_name from pgxc_group where in_redistribution = 'y' into old_group_name;
    sql := 'select count(*) from pg_class c, pgxc_class xc where array[''orientation=timeseries''] && c.reloptions and 
            c.relkind = ''r'' and c.parttype=''p'' and xc.pgroup='''||old_group_name||''' and xc.pcrelid = c.oid';
    execute sql into v_count;
    if v_count = 0 then
        raise notice 'No ts table to redis.';
        return;
    end if;
    sql := 'select c.oid from pg_class c, pgxc_class xc where array[''orientation=timeseries''] && c.reloptions and
            c.relkind = ''r'' and c.parttype=''p'' and xc.pgroup='''||old_group_name||'''
            and xc.pcrelid = c.oid and c.relname like ''redis_old_%''';
    for var_r in execute sql loop
        sql := 'select pg_catalog.submit_redis_task('||var_r.oid||',
                                                    '||quote_literal(schedule_interval)||');';
        execute sql;
    end loop;
END;
$$
LANGUAGE plpgsql;

-- just flush redistribute instead rule, if do something, user may need to flush the rule
CREATE OR REPLACE FUNCTION pg_catalog.flush_depend_rule(relid Oid)
RETURNS void  
AS
$$
DECLARE
    relname name;
    schemaname name;
    schemaoid oid;
    newname name;
    oldname name;
    def text;
    leftpos int;
    rightpos int;
    var_r record;
    sql text;
BEGIN
    if not exists(select oid from pg_class where oid = relid) then
        raise exception 'Table with oid % does not exists', relid;
    end if;
    select c.relname, c.relnamespace, n.nspname from pg_class c, pg_namespace n 
                                                where c.oid = relid and n.oid = c.relnamespace 
                                                into relname, schemaoid, schemaname;
    if not exists(select rulename from pg_rewrite where ev_class = relid and is_instead = 't') then
        raise notice 'Have no rule to flush, nothing need to do';
        return;
    end if;
    sql := 'select set_config(''enable_cluster_resize'', ''on'', false)';
    execute sql;
    newname = 'redis_new_' ||relname||;
    oldname = 'redis_old_' ||relname||;
    if not exists(select oid from pg_class where relnamespace = schemaoid and relname = newname) then
        raise notice '% does not exists, nothing need to do', newname;
        return;
    end if;
    if not exists(select oid from pg_class where relnamespace = schemaoid and relname = oldname) then
        raise notice '% does not exists, nothing need to do', oldname;
        return;
    end if;
    for var_r in (select oid, rulename, ev_type from pg_catalog.pg_rewrite) loop
        sql := 'select definition from pg_rules where rulename = '||quote_ident(var_r.rulename)||
                ' and schemaname = '||quote_ident(schemaname)||' and tablename = '||quote_ident(relname);
        execute sql into def;
        def = "CREATE OR REPLACE" || substring(def, 7, length(def));
        if var_r.ev_type = 6 then
        --copy rule and alter table rule
        --do not need to rewrite when only alter table add column, the query tree in pg_rewrite does not have the column indo
            execute def;
        elsif var_r.ev_type = 3 then
            sql = split_part(def, ') VALUES (', 1) || ') VALUES (new.*);';
            execute sql;
        elsif var_r.ev_type = 1 then
            sql = split_part(def, 'DO INSTEAD SELECT', 1) || 'DO INSTEAD SELECT * FROM '||quote_ident(schemaname)||'.'||quote_ident(oldname)
                    ||' UNION ALL SELECT * FROM '||quote_ident(schemaname)||'.'||quote_ident(newname)||';';
            execute sql;
        end if;
    end loop;
END;
$$
LANGUAGE plpgsql;
