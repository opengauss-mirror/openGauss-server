
do $$DECLARE ans boolean;
BEGIN
    for ans in select case when count(*)=1 then true else false end as ans  from (select nspname from pg_namespace where nspname='sqladvisor' limit 1)
    LOOP
        if ans = true then
            DROP FUNCTION IF EXISTS sqladvisor.init("char", boolean, boolean, boolean, integer, integer ) cascade;
            DROP FUNCTION IF EXISTS sqladvisor.analyze_query(text, integer) cascade;
            DROP FUNCTION IF EXISTS sqladvisor.get_analyzed_result(OUT schema_name text, OUT table_name text, OUT col_name text, OUT operator text, OUT count integer) cascade;
            DROP FUNCTION IF EXISTS sqladvisor.run() cascade;
            DROP FUNCTION IF EXISTS sqladvisor.clean() cascade;
            DROP FUNCTION IF EXISTS sqladvisor.assign_table_type(text) cascade;
            DROP FUNCTION IF EXISTS sqladvisor.clean_workload() cascade;
            DROP FUNCTION IF EXISTS sqladvisor.get_distribution_key(OUT db_name text, OUT schema_name text, OUT table_name text, OUT distribution_type text, OUT distribution_key text, OUT start_time timestamp with time zone, OUT end_time timestamp with time zone, OUT cost_improve text, OUT comment text) cascade;
            DROP FUNCTION IF EXISTS sqladvisor.set_weight_params(real, real, real) cascade;
            DROP FUNCTION IF EXISTS sqladvisor.set_cost_params(bigint, boolean, text) cascade;
            DROP FUNCTION IF EXISTS sqladvisor.start_collect_workload(integer, integer) cascade;
            DROP FUNCTION IF EXISTS sqladvisor.end_collect_workload() cascade;
            DROP FUNCTION IF EXISTS sqladvisor.analyze_workload() cascade;
        end if;
        exit;
    END LOOP;
END$$;

DROP SCHEMA IF EXISTS sqladvisor cascade;
