SET LOCAL d_format_behavior_compat_options = '';

-- rebuild some views in verion before shark 3.0
drop view if exists sys.sysobjects;
drop view if exists sys.objects;
drop view if exists sys.views;
drop view if exists sys.procedures;
drop view if exists sys.all_objects;
drop view if exists sys.tables;
drop view if exists sys.all_columns;
drop function if exists sys.ts_procedure_object_internal;
drop function if exists sys.ts_tables_obj_internal;


create or replace view sys.all_columns as
select
  a.attrelid as object_id,
  a.attname as name,
  cast(a.attnum as int) as column_id,
  a.atttypid as system_type_id,
  a.atttypid as user_type_id,
  sys.tsql_type_max_length_helper(t.typname, a.attlen, a.atttypmod) as max_length,
  sys.ts_numeric_precision_helper(t.typname, a.atttypmod) as precision,
  sys.ts_numeric_scale_helper(t.typname, a.atttypmod) as scale,
  coll.collname as collation_name,
  cast(case a.attnotnull when 't' then 0 else 1 end as bit) as is_nullable,
  cast(0 as bit) as is_ansi_padded,
  cast(0 as bit) as is_rowguidcol,
  cast(0 as bit) as is_identity,
  cast(case when d.adgencol = 'p' then 1 else 0 end as bit) as is_computed,
  cast(0 as bit) as is_filestream,
  sys.ts_is_publication_helper(a.attrelid) as is_replicated,
  cast(0 as bit) as is_non_sql_subscribed,
  cast(0 as bit) as is_merge_published,
  cast(0 as bit) as is_dts_replicated,
  cast(0 as bit) as is_xml_document,
  cast(0 as oid) as xml_collection_id,
  d.oid as default_object_id,
  cast(0 as int) as rule_object_id,
  cast(0 as bit) as is_sparse,
  cast(0 as bit) as is_column_set,
  cast(0 as tinyint) as generated_always_type,
  cast('NOT_APPLICABLE' as nvarchar(60)) as generated_always_type_desc
from pg_attribute a
inner join pg_class c on c.oid = attrelid
inner join pg_namespace s on s.oid = c.relnamespace
inner join pg_type t on t.oid = a.atttypid
left join pg_attrdef d on a.attrelid = d.adrelid and a.attnum = d.adnum
left join pg_collation coll on coll.oid = a.attcollation
where not a.attisdropped and a.attnum > 0
and c.relkind in ('r', 'v', 'm', 'f')
and has_column_privilege(c.oid, a.attname, 'SELECT');

create or replace function sys.ts_tables_obj_internal()
returns table (
	out_name name,
    out_object_id oid,
    out_principal_id oid,
    out_schema_id oid,
    out_schema_name name,
    out_parent_object_id oid,
    out_type char(2),
    out_type_desc nvarchar(60),
    out_create_date timestamp,
    out_modify_date timestamp,
    out_ms_shipped bit,
    out_published bit,
    out_schema_published bit
)
as $$
begin
return query
select
  t.relname,
  t.oid,
  cast(case s.nspowner when t.relowner then null else t.relowner end as oid),
  s.oid,
  s.nspname, 
  cast(0 as oid),
  cast(case s.nspname
        when 'information_schema' then 'S'
        when 'pg_catalog' then 'S'
        when 'sys' then 'S'
        when 'information_schema_tsql' then 'S'
        else 'U' end as char(2)),
  cast(case s.nspname
        when 'information_schema' then 'SYSTEM_TABLE'
        when 'pg_catalog' then 'SYSTEM_TABLE'
        when 'sys' then 'SYSTEM_TABLE'
        when 'information_schema_tsql' then 'SYSTEM_TABLE'
        else 'USER_TABLE' end as nvarchar(60)),
  cast(o.ctime as timestamp), 
  cast(o.mtime as timestamp),
  cast(case s.nspname
        when 'information_schema' then 1
        when 'pg_catalog' then 1
        when 'sys' then 1
        when 'information_schema_tsql' then 1
        else 0 end as bit),
  ts_is_publication_helper(t.oid),
  cast(0 as bit)
from pg_class t
inner join pg_namespace s on s.oid = t.relnamespace
left join pg_object o on o.object_oid = t.oid
where t.relpersistence in ('p', 'u', 't')
and (t.relkind = 'r' or t.relkind = 'f')
and has_table_privilege(t.oid, 'SELECT');
end $$
language plpgsql;

create or replace function sys.ts_procedure_object_internal()
returns table (
  out_name name,
  out_object_id oid,
  out_principal_id oid,
  out_schema_id oid,
  out_scheam name,
  out_parent_object_id oid,
  out_type char(2),
  out_type_desc nvarchar(60),
  out_create_date timestamp,
  out_modify_date timestamp,
  out_ms_shipped bit,
  out_published bit,
  out_schema_published bit
) as $$
begin
return query
select
  p.proname,
  p.oid,
  cast(case s.nspowner when p.proowner then null else p.proowner end as oid),
  s.oid,
  s.nspname,
  cast(0 as oid),
  cast(case p.prokind
       	when 'f' then
       		case p.proisagg when 't' then 'AF' else 'FN' end
       	else 'P' end
       as char(2)) as type,
  cast(case p.prokind
       	when 'f' then
       		case p.proisagg when 't' then 'AGGREGATE_FUNCTION' else 'SQL_SCALAR_FUNCTION' end
       	else 'SQL_STORED_PROCEDURE' end
       as nvarchar(60)) as type,
  cast(o.ctime as timestamp), 
  cast(o.mtime as timestamp),
  cast(0 as bit),
  cast(0 as bit),
  cast(0 as bit)
from pg_proc p
inner join pg_namespace s on s.oid = p.pronamespace
left join pg_object o on o.object_oid = p.oid
where has_function_privilege(p.oid, 'EXECUTE');
end $$
language plpgsql;

-- sys.tables
create or replace view sys.tables as
select
  ti.out_name as name,
  ti.out_object_id as object_id,
  ti.out_principal_id as principal_id,
  ti.out_schema_id as schema_id,
  ti.out_parent_object_id as parent_object_id,
  ti.out_type as type,
  ti.out_type_desc as type_desc,
  ti.out_create_date as create_date,
  ti.out_modify_date as modify_date,
  ti.out_ms_shipped as is_ms_shipped,
  ti.out_published as is_published,
  ti.out_schema_published as is_schema_published,
  t.reltoastrelid as lob_data_space_id,
  cast(null as int) as filestream_data_space_id,
  cast(t.relnatts as int) as max_column_id_used,
  cast(0 as bit) as lock_on_bulk_load,
  cast(1 as bit) as uses_ansi_nulls,
  cast(1 as bit) as is_replicated,
  cast(0 as bit) as has_replication_filter,
  cast(0 as bit) as is_merge_published,
  cast(0 as bit) as is_sync_tran_subscribed,
  cast(0 as bit) as has_unchecked_assembly_data,
  cast(0 as int) as text_in_row_limit,
  cast(0 as bit) as large_value_types_out_of_row,
  cast(0 as tinyint) as is_tracked_by_cdc,
  cast(1 as tinyint) as lock_escalation,
  cast('DISABLE' as nvarchar(60)) as lock_escalation_desc,
  cast(0 as bit) as is_filetable,
  sys.ts_is_mot_table_helper(t.oid) as is_memory_optimized,
  cast(0 as tinyint) as durability,
  cast('SCHEMA_AND_DATA' as nvarchar(60)) as durability_desc,
  cast(case t.relpersistence when 't' then 2 else 0 end as tinyint) as temporal_type,
  cast(case t.relpersistence when 't' then 'SYSTEM_VERSIONED_TEMPORAL_TABLE' else 'NON_TEMPORAL_TABLE' end as nvarchar(60)) as temporal_type_desc,
  cast(null as int) as history_table_id,
  cast(0 as bit) as is_remote_data_archive_enabled,
  cast(case t.relkind when 'f' then 1 else 0 end as bit) as is_external,
  cast(0 as int) as history_retention_period,
  cast(-1 as int) as history_retention_period_unit,
  cast('INFINITE' as nvarchar(10)) as history_retention_period_unit_desc,
  cast(case when sys.ts_graph_type_helper(t.oid, 'v') then 1 else 0 end as bit) as is_node,
  cast(case when sys.ts_graph_type_helper(t.oid, 'e') then 1 else 0 end as bit) as is_edge
from sys.ts_tables_obj_internal() ti
inner join pg_class t on ti.out_object_id = t.oid
where ti.out_type = 'U';

-- sys.all_objects
create or replace view sys.all_objects as
select
  ti.out_name as name,
  ti.out_object_id as object_id,
  ti.out_principal_id as principal_id,
  ti.out_schema_id as schema_id,
  ti.out_parent_object_id as parent_object_id,
  ti.out_type as type,
  ti.out_type_desc as type_desc,
  ti.out_create_date as create_date,
  ti.out_modify_date as modify_date,
  ti.out_ms_shipped as is_ms_shipped,
  ti.out_published as is_published,
  ti.out_schema_published as is_schema_published
from sys.ts_tables_obj_internal() ti
union all
select
  c.relname as name,
  c.oid as object_id,
  cast(case s.nspowner when c.relowner then null else c.relowner end as oid) as principal_id,
  s.oid as schema_id,
  cast(0 as oid) as parent_object_id,
  cast('SO' as char(2)) as type,
  cast('SEQUENCE_OBJECT' as nvarchar(60)) as type_desc,
  cast(o.ctime as timestamp) as create_date, 
  cast(o.mtime as timestamp) as modify_date,
  cast(0 as bit) as is_ms_shipped,
  cast(0 as bit) as is_published,
  cast(0 as bit) as is_schema_published
from pg_class c
inner join pg_namespace s on s.oid = c.relnamespace
left join pg_object o on o.object_oid = c.oid
where relkind in ('S', 'L', 'z', 'Z')
and has_table_privilege(c.oid, 'SELECT')
union all
select
  c.relname as name,
  c.oid as object_id,
  cast(case s.nspowner when c.relowner then null else c.relowner end as oid) as principal_id,
  s.oid as schema_id,
  cast(0 as oid) as parent_object_id,
  cast('V' as char(2)) as type,
  cast('VIEW' as nvarchar(60)) as type_desc,
  cast(o.ctime as timestamp) as create_date, 
  cast(o.mtime as timestamp) as modify_date,
  cast(0 as bit) as is_ms_shipped,
  cast(0 as bit) as is_published,
  cast(0 as bit) as is_schema_published
from pg_class c
inner join pg_namespace s on c.relnamespace = s.oid
left join pg_object o on o.object_oid = c.oid 
where c.relkind in ('v', 'm')
and has_table_privilege(c.oid, 'SELECT')
union all
select
  pi.out_name as name,
  pi.out_object_id as object_id,
  pi.out_principal_id as principal_id,
  pi.out_schema_id as schema_id,
  pi.out_parent_object_id as parent_object_id,
  pi.out_type as type,
  pi.out_type_desc as type_desc,
  pi.out_create_date as create_date,
  pi.out_modify_date as modify_date,
  pi.out_ms_shipped as is_ms_shipped,
  pi.out_published as is_published,
  pi.out_schema_published as is_schema_published
from sys.ts_procedure_object_internal() pi
union all
select
  con.conname as name,
  con.oid as object_id,
  cast(null as oid) as principal_id,
  con.connamespace as schema_id,
  con.conrelid as parent_object_id,
  cast(case con.contype 
       	when 'c' then 'C'
       	when 'p' then 'PK'
       	when 'u' then 'UQ'
       	when 'f' then 'F'
       end as char(2)) as type,
  cast(case con.contype 
       	when 'c' then 'CHECK_CONSTRAINT'
       	when 'p' then 'PRIMARY_KEY_CONSTRAINT'
       	when 'u' then 'UNIQUE_CONSTRAINT'
       	when 'f' then 'FOREIGN_KEY_CONSTRAINT'
       end as nvarchar(60)) as type_desc,
  cast(null as timestamp) as create_date,
  cast(null as timestamp) as modify_date,
  cast(0 as bit) as is_ms_shipped,
  cast(0 as bit) as is_published,
  cast(0 as bit) as is_schema_published
from pg_constraint con
inner join pg_class c on c.oid = con.conrelid
inner join pg_namespace s on s.oid = con.connamespace
where con.contype in ('c', 'p', 'u', 'f')
and has_table_privilege(c.oid, 'SELECT')
union all
select
  tg.tgname as name,
  tg.oid as object_id,
  cast(null as oid) as principal_id,
  c.relnamespace as schema_id,
  tg.tgrelid as parent_object_id,
  cast('TR' as char(2)) as type,
  cast('SQL DML trigger' as nvarchar(60)) as type_desc,
  cast(o.ctime as timestamp) as create_date,
  cast(o.mtime as timestamp) as modify_date,
  cast(0 as bit) as is_ms_shipped,
  cast(0 as bit) as is_published,
  cast(0 as bit) as is_schema_published
from pg_trigger tg
inner join pg_class c on c.oid = tg.tgrelid
inner join pg_namespace s on s.oid = c.relnamespace
left join pg_object o on o.object_oid = tg.oid
where has_table_privilege(c.oid, 'SELECT')
union all
select
  cast(null as name) as name,
  ad.oid as object_id,
  cast(null as oid) as principal_id,
  c.relnamespace as schema_id,
  ad.adrelid as parent_object_id,
  cast('D' as char(2)) as type,
  cast('DEFAULT' as nvarchar(60)) as type_desc,
  cast(o.ctime as timestamp) as create_date,
  cast(o.mtime as timestamp) as modify_date,
  cast(0 as bit) as is_ms_shipped,
  cast(0 as bit) as is_published,
  cast(0 as bit) as is_schema_published
from pg_attrdef ad
inner join pg_class c on c.oid = ad.adrelid
inner join pg_namespace s on s.oid = c.relnamespace
left join pg_object o on o.object_oid = ad.adrelid
where has_table_privilege(c.oid, 'SELECT')
union all
select
  syn.synname as name,
  syn.oid as object_id,
  cast(case s.nspowner when syn.synowner then null else syn.synowner end as oid) as principal_id,
  syn.synnamespace as schema_id,
  cast(0 as oid) as parent_object_id,
  cast('SN' as char(2)) as type,
  cast('SYNONYM' as nvarchar(60)) as type_desc,
  cast(null as timestamp) as create_date,
  cast(null as timestamp) as modify_date,
  cast(0 as bit) as is_ms_shipped,
  cast(0 as bit) as is_published,
  cast(0 as bit) as is_schema_published
from pg_synonym syn
inner join pg_namespace s on s.oid = syn.synnamespace;

-- sys.procedures
create or replace view sys.procedures as
select
  pi.out_name as name,
  pi.out_object_id as object_id,
  pi.out_principal_id as principal_id,
  pi.out_schema_id as schema_id,
  pi.out_parent_object_id as parent_object_id,
  pi.out_type as type,
  pi.out_type_desc as type_desc,
  pi.out_create_date as create_date,
  pi.out_modify_date as modify_date,
  pi.out_ms_shipped as is_ms_shipped,
  pi.out_published as is_published,
  pi.out_schema_published as is_schema_published,
  cast(0 as bit) as is_auto_executed,
  cast(0 as bit) as is_execution_replicated,
  cast(0 as bit) as is_repl_serializable_only,
  cast(0 as bit) as skips_repl_constraints
from sys.ts_procedure_object_internal() pi
where pi.out_type = 'P'
and pi.out_scheam not in ('pg_catalog', 'information_schema', 'sys', 'information_schema_tsql');

-- sys.views
create or replace view sys.views as
select
  t.relname as name,
  t.oid as object_id,
  cast(case s.nspowner when t.relowner then null else t.relowner end as oid) as principal_id,
  s.oid as schema_id,
  cast(0 as oid) as parent_object_id,
  cast('V' as char(2)) as type,
  cast('VIEW' as nvarchar(60)) as type_desc,
  cast(o.ctime as timestamp) as create_date, 
  cast(o.mtime as timestamp) as modify_date,
  cast(0 as bit) as is_ms_shipped,
  cast(0 as bit) as is_published,
  cast(0 as bit) as is_schema_published,
  cast(0 as bit) as is_replicated,
  cast(0 as bit) as has_replication_filter,
  cast(0 as bit) as has_opaque_metadata,
  cast(0 as bit) as has_unchecked_assembly_data,
  cast(case when sys.tsql_relation_reloptions_helper(t.reloptions, 'check_option') is null then 0 else 1 end as bit) as with_check_option,
  cast(0 as bit) as is_date_correlation_view,
  cast(0 AS bit) AS is_tracked_by_cdc
from pg_class t
inner join pg_namespace s on t.relnamespace = s.oid
inner join pg_object o on o.object_oid = t.oid 
where t.relkind in ('v', 'm')
and has_table_privilege(t.oid, 'SELECT')
and s.nspname not in ('information_schema', 'pg_catalog', 'dbe_perf', 'sys', 'information_schema_tsql');

-- sys.objects
create or replace view sys.objects as
select
  t.name as name,
  t.object_id as object_id,
  t.principal_id as principal_id,
  t.schema_id as schema_id,
  t.parent_object_id as parent_object_id,
  t.type as type,
  t.type_desc as type_desc,
  t.create_date as create_date,
  t.modify_date as modify_date,
  t.is_ms_shipped as is_ms_shipped,
  t.is_published as is_published,
  t.is_schema_published as is_schema_published
from sys.tables t
union all
select
  c.relname as name,
  c.oid as object_id,
  cast(case s.nspowner when c.relowner then null else c.relowner end as oid) as principal_id,
  s.oid as schema_id,
  cast(0 as oid) as parent_object_id,
  cast('SO' as char(2)) as type,
  cast('SEQUENCE_OBJECT' as nvarchar(60)) as type_desc,
  cast(o.ctime as timestamp) as create_date, 
  cast(o.mtime as timestamp) as modify_date,
  cast(0 as bit) as is_ms_shipped,
  cast(0 as bit) as is_published,
  cast(0 as bit) as is_schema_published
from pg_class c
inner join pg_namespace s on s.oid = c.relnamespace
inner join pg_object o on o.object_oid = c.oid
where relkind in ('S', 'L', 'z', 'Z')
and s.nspname not in ('information_schema', 'pg_catalog', 'sys', 'information_schema_tsql')
and has_table_privilege(c.oid, 'SELECT')
union all
select
  v.name as name,
  v.object_id as object_id,
  v.principal_id as principal_id,
  v.schema_id as schema_id,
  v.parent_object_id as parent_object_id,
  v.type as type,
  v.type_desc as type_desc,
  v.create_date as create_date,
  v.modify_date as modify_date,
  v.is_ms_shipped as is_ms_shipped,
  v.is_published as is_published,
  v.is_schema_published as is_schema_published
from sys.views v
union all
select
  p.name,
  p.object_id,
  p.principal_id,
  p.schema_id,
  p.parent_object_id,
  p.type,
  p.type_desc,
  p.create_date,
  p.modify_date,
  p.is_ms_shipped,
  p.is_published,
  p.is_schema_published
from sys.procedures p
union all
select
  con.conname as name,
  con.oid as object_id,
  cast(null as oid) as principal_id,
  con.connamespace as schema_id,
  con.conrelid as parent_object_id,
  cast(case con.contype 
       	when 'c' then 'C'
       	when 'p' then 'PK'
       	when 'u' then 'UQ'
       	when 'f' then 'F'
       end as char(2)) as type,
  cast(case con.contype 
       	when 'c' then 'CHECK_CONSTRAINT'
       	when 'p' then 'PRIMARY_KEY_CONSTRAINT'
       	when 'u' then 'UNIQUE_CONSTRAINT'
       	when 'f' then 'FOREIGN_KEY_CONSTRAINT'
       end as nvarchar(60)) as type_desc,
  cast(null as timestamp) as create_date,
  cast(null as timestamp) as modify_date,
  cast(0 as bit) as is_ms_shipped,
  cast(0 as bit) as is_published,
  cast(0 as bit) as is_schema_published
from pg_constraint con
inner join pg_class c on c.oid = con.conrelid
inner join pg_namespace s on s.oid = con.connamespace
where con.contype in ('c', 'p', 'u', 'f')
and has_table_privilege(c.oid, 'SELECT')
and s.nspname not in ('information_schema', 'pg_catalog', 'sys', 'information_schema_tsql')
union all
select
  tg.tgname as name,
  tg.oid as object_id,
  cast(null as oid) as principal_id,
  c.relnamespace as schema_id,
  tg.tgrelid as parent_object_id,
  cast('TR' as char(2)) as type,
  cast('SQL DML trigger' as nvarchar(60)) as type_desc,
  cast(o.ctime as timestamp) as create_date,
  cast(o.mtime as timestamp) as modify_date,
  cast(0 as bit) as is_ms_shipped,
  cast(0 as bit) as is_published,
  cast(0 as bit) as is_schema_published
from pg_trigger tg
inner join pg_class c on c.oid = tg.tgrelid
inner join pg_namespace s on s.oid = c.relnamespace
inner join pg_object o on o.object_oid = tg.oid
where has_table_privilege(c.oid, 'SELECT')
and s.nspname not in ('information_schema', 'pg_catalog', 'sys', 'information_schema_tsql')
union all
select
  cast(null as name) as name,
  ad.oid as object_id,
  cast(null as oid) as principal_id,
  c.relnamespace as schema_id,
  ad.adrelid as parent_object_id,
  cast('D' as char(2)) as type,
  cast('DEFAULT' as nvarchar(60)) as type_desc,
  cast(o.ctime as timestamp) as create_date,
  cast(o.mtime as timestamp) as modify_date,
  cast(0 as bit) as is_ms_shipped,
  cast(0 as bit) as is_published,
  cast(0 as bit) as is_schema_published
from pg_attrdef ad
inner join pg_class c on c.oid = ad.adrelid
inner join pg_namespace s on s.oid = c.relnamespace
inner join pg_object o on o.object_oid = ad.adrelid
where has_table_privilege(c.oid, 'SELECT')
and s.nspname not in ('information_schema', 'pg_catalog', 'sys', 'information_schema_tsql')
union all
select
  syn.synname as name,
  syn.oid as object_id,
  cast(case s.nspowner when syn.synowner then null else syn.synowner end as oid) as principal_id,
  syn.synnamespace as schema_id,
  cast(0 as oid) as parent_object_id,
  cast('SN' as char(2)) as type,
  cast('Synonym' as nvarchar(60)) as type_desc,
  cast(null as timestamp) as create_date,
  cast(null as timestamp) as modify_date,
  cast(0 as bit) as is_ms_shipped,
  cast(0 as bit) as is_published,
  cast(0 as bit) as is_schema_published
from pg_synonym syn
inner join pg_namespace s on s.oid = syn.synnamespace
where s.nspname not in ('information_schema', 'pg_catalog', 'sys', 'information_schema_tsql');

-- sys.sysobjects
create or replace view sys.sysobjects as
select
  cast(t.relname as name) as name,
  cast(t.oid as oid) as id,
  cast(case t.relkind 
  	when 'r' then
      case s.nspname 
        when 'information_schema' then 'S'
        when 'pg_catalog' then 'S'
        when 'sys' then 'S'
        when 'information_schema_tsql' then 'S'
        else 'U'
      end
  	when 'v'  then 'V'
  	when 'm' then 'V'
  	else 'SO'
  end as char(2)) as xtype,
  cast(t.relnamespace as oid) as uid,
  cast(0 as smallint) as info,
  cast(0 as int) as status,
  cast(0 as int) as base_schema_ver,
  cast(0 as int) as replinfo,
  cast(0 as oid) as parent_obj,
  cast(null as timestamp(3)) as crdate,
  cast(0 as smallint) as ftcatid,
  cast(0 as int) as schema_ver,
  cast(0 as int) as stats_schema_ver,
  cast(case t.relkind 
    when 'r' then
      case s.nspname 
        when 'information_schema' then 'S'
        when 'pg_catalog' then 'S'
        when 'sys' then 'S'
        when 'information_schema_tsql' then 'S'
        else 'U'
      end
  	when 'r' then 'U'
  	when 'v'  then 'V'
  	when 'm' then 'V'
  	else 'SO'
  end as char(2)) as type,
  cast(0 as smallint) as userstat,
  cast(0 as smallint) as sysstat,
  cast(0 as smallint) as indexdel,
  cast(null as timestamp(3)) as refdate,
  cast(0 as int) as version,
  cast(0 as int) as deltrig,
  cast(0 as int) as instrig,
  cast(0 as int) as updtrig,
  cast(0 as int) as seltrig,
  cast(0 as int) as category,
  cast(0 as smallint) as cache
from pg_class t
inner join pg_namespace s on s.oid = t.relnamespace
where t.relpersistence in ('p', 'u', 't')
and t.relkind in ('r', 'v', 'm', 'S', 'L', 'z', 'Z')
and has_table_privilege(t.oid, 'SELECT')
union all
select 
  cast(c.conname as name) as name,
  cast(c.oid as oid) as id,
  cast(case c.contype
  	when 'f' then 'F'
  	when 'c' then 'C'
  	when 'p' then 'PK'
  	when 'u' then 'UQ'
  end as char(2) )as xtype,
  cast(c.connamespace as oid) as uid,
  cast(0 as smallint) as info,
  cast(0 as int) as status,
  cast(0 as int) as base_schema_ver,
  cast(0 as int) as replinfo,
  cast(c.conrelid as int) as parent_obj,
  cast(null as timestamp(3)) as crdate,
  cast(0 as smallint) as ftcatid,
  cast(0 as int) as schema_ver,
  cast(0 as int) as stats_schema_ver,
  cast(case c.contype
  	when 'f' then 'F'
  	when 'c' then 'C'
  	when 'p' then 'K'
  	when 'u' then 'K'
  end as char(2)) as type,
  cast(0 as smallint) as userstat,
  cast(0 as smallint) as sysstat,
  cast(0 as smallint) as indexdel,
  cast(null as timestamp(3)) as refdate,
  cast(0 as int) as version,
  cast(0 as int) as deltrig,
  cast(0 as int) as instrig,
  cast(0 as int) as updtrig,
  cast(0 as int) as seltrig,
  cast(0 as int) as category,
  cast(0 as smallint) as cache
from pg_constraint c
inner join pg_class t on c.conrelid = t.oid
inner join pg_namespace s on s.oid = c.connamespace
where c.contype in ('f', 'c', 'p', 'u')
and has_table_privilege(t.oid, 'SELECT')
union all
select 
  cast(null as name) as name,
  cast(ad.oid as oid) as id,
  cast('D' as char(2)) as xtype,
  cast(c.relnamespace as oid) as uid,
  cast(0 as smallint) as info,
  cast(0 as int) as status,
  cast(0 as int) as base_schema_ver,
  cast(0 as int) as replinfo,
  cast(ad.adrelid as oid) as object_id,
  cast(null as timestamp(3)) as crdate,
  cast(0 as smallint) as ftcatid,
  cast(0 as int) as schema_ver,
  cast(0 as int) as stats_schema_ver,
  cast('D' as char(2)) as type,
  cast(0 as smallint) as userstat,
  cast(0 as smallint) as sysstat,
  cast(0 as smallint) as indexdel,
  cast(null as timestamp(3)) as refdate,
  cast(0 as int) as version,
  cast(0 as int) as deltrig,
  cast(0 as int) as instrig,
  cast(0 as int) as updtrig,
  cast(0 as int) as seltrig,
  cast(0 as int) as category,
  cast(0 as smallint) as cache
from pg_attrdef ad
inner join pg_class c on ad.adrelid = c.oid
inner join pg_namespace s on c.relnamespace = s.oid
and has_table_privilege(c.oid, 'SELECT')
union all
select
  cast(p.proname as name) as name,
  cast(p.oid as oid) as id,
  cast(case p.prokind
  	when 'f' then 
  		case p.proisagg when true then 'AF' else 'FN' end
  	else 'P'
  end as char(2)) as xtype,
  cast(p.pronamespace as oid) as uid,
  cast(0 as smallint) as info,
  cast(0 as int) as status,
  cast(0 as int) as base_schema_ver,
  cast(0 as int) as replinfo,
  cast(0 as int) as parent_obj,
  cast(null as timestamp(3)) as crdate,
  cast(0 as smallint) as ftcatid,
  cast(0 as int) as schema_ver,
  cast(0 as int) as stats_schema_ver,
  cast(case p.prokind
  	when 'f' then
  		case p.proisagg when true then 'AF' else 'FN' end
  	else 'P'
  end as char(2)) as type,
  cast(0 as smallint) as userstat,
  cast(0 as smallint) as sysstat,
  cast(0 as smallint) as indexdel,
  cast(null as timestamp(3)) as refdate,
  cast(0 as int) as version,
  cast(0 as int) as deltrig,
  cast(0 as int) as instrig,
  cast(0 as int) as updtrig,
  cast(0 as int) as seltrig,
  cast(0 as int) as category,
  cast(0 as smallint) as cache
from pg_proc p
inner join pg_namespace s on s.oid = p.pronamespace
and has_function_privilege(p.oid, 'EXECUTE')
union all
select 
  cast(t.tgname as name) as name,
  cast(t.oid as oid) as id,
  cast('TR' as char(2)) as xtype,
  cast(c.relnamespace as oid) as uid,
  cast(0 as smallint) as info,
  cast(0 as int) as status,
  cast(0 as int) as base_schema_ver,
  cast(0 as int) as replinfo,
  cast(0 as int) as parent_obj,
  cast(null as timestamp(3)) as crdate,
  cast(0 as smallint) as ftcatid,
  cast(0 as int) as schema_ver,
  cast(0 as int) as stats_schema_ver,
  cast('TR' as char(2)) as type,
  cast(0 as smallint) as userstat,
  cast(0 as smallint) as sysstat,
  cast(0 as smallint) as indexdel,
  cast(null as timestamp(3)) as refdate,
  cast(0 as int) as version,
  cast(0 as int) as deltrig,
  cast(0 as int) as instrig,
  cast(0 as int) as updtrig,
  cast(0 as int) as seltrig,
  cast(0 as int) as category,
  cast(0 as smallint) as cache
from pg_trigger t
inner join pg_class c on t.tgrelid = t.oid
inner join pg_namespace s on c.relnamespace = s.oid
where has_table_privilege(c.oid, 'SELECT,TRIGGER')
union all
select
  cast(y.synname as name) as name,
  cast(y.oid as oid) as id,
  cast('SN' as char(2)) as xtype,
  cast(y.synnamespace as oid) as uid,
  cast(0 as smallint) as info,
  cast(0 as int) as status,
  cast(0 as int) as base_schema_ver,
  cast(0 as int) as replinfo,
  cast(0 as int) as parent_obj,
  cast(null as timestamp(3)) as crdate,
  cast(0 as smallint) as ftcatid,
  cast(0 as int) as schema_ver,
  cast(0 as int) as stats_schema_ver,
  cast('SN' as char(2)) as type,
  cast(0 as smallint) as userstat,
  cast(0 as smallint) as sysstat,
  cast(0 as smallint) as indexdel,
  cast(null as timestamp(3)) as refdate,
  cast(0 as int) as version,
  cast(0 as int) as deltrig,
  cast(0 as int) as instrig,
  cast(0 as int) as updtrig,
  cast(0 as int) as seltrig,
  cast(0 as int) as category,
  cast(0 as smallint) as cache
from pg_synonym y;
grant select on sys.sysobjects to public;

CREATE OR REPLACE FUNCTION sys.newid() RETURNS uuid LANGUAGE C VOLATILE as '$libdir/shark', 'uuid_generate';

drop view if exists sys.columns;
create or replace view sys.columns as
select
  a.attrelid as object_id,
  a.attname as name,
  cast(a.attnum as int) as column_id,
  a.atttypid as system_type_id,
  a.atttypid as user_type_id,
  sys.tsql_type_max_length_helper(t.typname, a.attlen, a.atttypmod) as max_length,
  sys.ts_numeric_precision_helper(t.typname, a.atttypmod) as precision,
  sys.ts_numeric_scale_helper(t.typname, a.atttypmod) as scale,
  coll.collname as collation_name,
  cast(case a.attnotnull when 't' then 0 else 1 end as bit) as is_nullable,
  cast(0 as bit) as is_ansi_padded,
  cast(0 as bit) as is_rowguidcol,
  cast(case when right(pg_get_serial_sequence(quote_ident(s.nspname)||'.'||quote_ident(c.relname), a.attname),
                        13) = '_seq_identity' then 1 else 0 end as bit) as is_identity,
  cast(case when (d.adgencol = 's' or d.adgencol = 'p') then 1 else 0 end as bit) as is_computed,
  cast(0 as bit) as is_filestream,
  sys.ts_is_publication_helper(a.attrelid) as is_replicated,
  cast(0 as bit) as is_non_sql_subscribed,
  cast(0 as bit) as is_merge_published,
  cast(0 as bit) as is_dts_replicated,
  cast(0 as bit) as is_xml_document,
  cast(0 as oid) as xml_collection_id,
  d.oid as default_object_id,
  cast(0 as int) as rule_object_id,
  cast(0 as bit) as is_sparse,
  cast(0 as bit) as is_column_set,
  cast(0 as tinyint) as generated_always_type,
  cast('NOT_APPLICABLE' as nvarchar(60)) as generated_always_type_desc,
  cast(case e.encryption_type when 2 then 1 else 2 end as int) as encryption_type,
  cast(case e.encryption_type when 2 then 'RANDOMIZED' else 'DETERMINISTIC' end as nvarchar(64)) as encryption_type_desc,
  cast((select value from gs_column_keys_args where column_key_id = e.column_key_id and key = 'ALGORITHM') as name) as encryption_algorithm_name,
  e.column_key_id as column_encryption_key_id,
  cast(null as name) as column_encryption_key_database_name,
  cast(0 as bit) as is_hidden,
  cast(0 as bit) as is_masked,
  cast(null as int) as graph_type,
  cast(null as nvarchar(60)) as graph_type_desc
from pg_attribute a
inner join pg_class c on c.oid = attrelid
inner join pg_namespace s on s.oid = c.relnamespace
inner join pg_type t on t.oid = a.atttypid
left join pg_attrdef d on a.attrelid = d.adrelid and a.attnum = d.adnum
left join pg_collation coll on coll.oid = a.attcollation
left join gs_encrypted_columns e on e.rel_id = a.attrelid and e.column_name = a.attname
where not a.attisdropped and a.attnum > 0
and c.relkind in ('r', 'v', 'm', 'f')
and has_column_privilege(c.oid, a.attname, 'SELECT')
and s.nspname not in ('information_schema', 'pg_catalog', 'dbe_perf', 'sys', 'information_schema_tsql');


CREATE OR REPLACE FUNCTION sys.int16_sqlvariant(int16, int)
RETURNS sys.SQL_VARIANT
AS '$libdir/shark', 'int16_sqlvariant'
LANGUAGE C IMMUTABLE STRICT ;

CREATE CAST (int16 AS sys.SQL_VARIANT)
WITH FUNCTION sys.int16_sqlvariant (int16, int) AS IMPLICIT;

create or replace view sys.identity_columns as
select
  a.attrelid as object_id,
  a.attname as name,
  cast(a.attnum as int) as column_id,
  a.atttypid as system_type_id,
  a.atttypid as user_type_id,
  sys.tsql_type_max_length_helper(t.typname, a.attlen, a.atttypmod) as max_length,
  sys.ts_numeric_precision_helper(t.typname, a.atttypmod) as precision,
  sys.ts_numeric_scale_helper(t.typname, a.atttypmod) as scale,
  coll.collname as collation_name,
  cast(case a.attnotnull when 't' then 0 else 1 end as bit) as is_nullable,
  cast(0 as bit) as is_ansi_padded,
  cast(0 as bit) as is_rowguidcol,
  cast(case when right(pg_get_serial_sequence(quote_ident(s.nspname)||'.'||quote_ident(c.relname), a.attname),
                        13) = '_seq_identity' then 1 else 0 end as bit) as is_identity,
  cast(case when (d.adgencol = 's' or d.adgencol = 'p') then 1 else 0 end as bit) as is_computed,
  cast(0 as bit) as is_filestream,
  sys.ts_is_publication_helper(a.attrelid) as is_replicated,
  cast(0 as bit) as is_non_sql_subscribed,
  cast(0 as bit) as is_merge_published,
  cast(0 as bit) as is_dts_replicated,
  cast(0 as bit) as is_xml_document,
  cast(0 as oid) as xml_collection_id,
  d.oid as default_object_id,
  cast(0 as int) as rule_object_id,
  cast(0 as bit) as is_sparse,
  cast(0 as bit) as is_column_set,
  cast(0 as tinyint) as generated_always_type,
  cast('NOT_APPLICABLE' as nvarchar(60)) as generated_always_type_desc,
  cast(case e.encryption_type when 2 then 1 else 2 end as int) as encryption_type,
  cast(case e.encryption_type when 2 then 'RANDOMIZED' else 'DETERMINISTIC' end as nvarchar(64)) as encryption_type_desc,
  cast((select value from gs_column_keys_args where column_key_id = e.column_key_id and key = 'ALGORITHM') as name) as encryption_algorithm_name,
  e.column_key_id as column_encryption_key_id,
  cast(null as name) as column_encryption_key_database_name,
  cast(0 as bit) as is_hidden,
  cast(0 as bit) as is_masked,
  cast(null as int) as graph_type,
  cast(null as nvarchar(60)) as graph_type_desc,
  cast(((pg_catalog.pg_sequence_all_parameters(
          pg_get_serial_sequence(quote_ident(s.nspname)||'.'||quote_ident(c.relname), a.attname)
        )).start_value)
      as sql_variant) as seed_value,
  cast(((pg_catalog.pg_sequence_all_parameters(
          pg_get_serial_sequence(quote_ident(s.nspname)||'.'||quote_ident(c.relname), a.attname)
        )).increment)
      as sql_variant) as increment_value,
  cast(((pg_catalog.pg_sequence_all_parameters(
          pg_get_serial_sequence(quote_ident(s.nspname)||'.'||quote_ident(c.relname), a.attname)
        )).last_used_value)
      as sql_variant) as last_value,
  cast(0 as bit) as is_not_for_replication
from pg_attribute a
inner join pg_class c on c.oid = attrelid
inner join pg_namespace s on s.oid = c.relnamespace
inner join pg_type t on t.oid = a.atttypid
left join pg_attrdef d on a.attrelid = d.adrelid and a.attnum = d.adnum
left join pg_collation coll on coll.oid = a.attcollation
left join gs_encrypted_columns e on e.rel_id = a.attrelid and e.column_name = a.attname
where not a.attisdropped and a.attnum > 0
and c.relkind in ('r', 'v', 'm', 'f')
and has_column_privilege(c.oid, a.attname, 'SELECT')
and s.nspname not in ('information_schema', 'pg_catalog', 'dbe_perf', 'sys', 'information_schema_tsql')
and is_identity = 1::bit;

CREATE OR REPLACE VIEW sys.server_principals
AS SELECT
CAST(Role.rolname AS NAME) AS name,
CAST(Role.oid AS INT) AS principal_id,
CAST(CAST(Role.oid as INT) AS sys.varbinary(85)) AS sid,
CAST(
   CASE
    WHEN 
      Role.rolauditadmin = true OR 
      Role.rolsystemadmin = true OR 
      Role.rolmonitoradmin = true OR 
      Role.roloperatoradmin = true OR 
      Role.rolpolicyadmin = true THEN 'R'
    WHEN
      Role.rolcanlogin = true THEN 'S'
    ELSE
      NULL
   END
   AS CHAR(1)) AS type,
CAST(
    CASE
      WHEN 
        Role.rolauditadmin = true OR 
        Role.rolsystemadmin = true OR 
        Role.rolmonitoradmin = true OR 
        Role.roloperatoradmin = true OR 
        Role.rolpolicyadmin = true THEN 'SERVER_ROLE'
      WHEN
        Role.rolcanlogin = true THEN 'SQL_LOGIN'
      ELSE
        NULL
   END
    AS NVARCHAR2(60)) AS type_desc,
CAST(
    CASE
      WHEN Role.rolcanlogin = true THEN 0
      ELSE 1
    END
    AS INT) AS is_disbaled,
CAST(NULL AS TIMESTAMP) AS create_date,
CAST(NULL AS TIMESTAMP) AS modify_date,
CAST(NULL AS NAME) AS default_database_name,
CAST('english' AS NAME) AS default_language_name,
CAST(-1 AS INT) AS creadential_id,
CAST(-1 AS INT) AS owning_principal_id,
CAST(-1 AS INT) AS is_fixed_role
FROM pg_catalog.pg_roles AS Role;

-- datepart
CREATE OR REPLACE FUNCTION sys.datepart(cstring, int)
RETURNS integer
language c
immutable strict NOT FENCED NOT SHIPPABLE
AS '$libdir/shark', $function$datepartint$function$;

CREATE OR REPLACE FUNCTION sys.datepart(text, text) RETURNS integer LANGUAGE SQL STABLE as 'select sys.datepart($1::cstring, $2::timestamp without time zone)';

-- sys.foreign_key_columns
CREATE OR REPLACE VIEW sys.foreign_key_columns AS
SELECT
  con.oid AS constraint_object_id,
  CAST((generate_series(1, ARRAY_LENGTH(con.conkey, 1))) AS INT) AS constraint_column_id,
  con.conrelid AS parent_object_id,
  CAST((UNNEST(con.conkey)) AS INT) AS parent_column_id,
  con.confrelid AS referenced_object_id,
  CAST((UNNEST(con.confkey)) AS INT) AS referenced_column_id
FROM pg_constraint con
INNER JOIN pg_class c ON c.oid = con.conrelid
INNER JOIN pg_namespace s ON s.oid = con.connamespace
WHERE con.contype = 'f'
AND s.nspname NOT IN ('information_schema', 'pg_catalog', 'sys', 'information_schema_tsql')
AND (pg_catalog.pg_has_role(c.relowner, 'USAGE')
  OR pg_catalog.has_table_privilege(c.oid, 'INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER')
  OR pg_catalog.has_any_column_privilege(c.oid, 'INSERT, UPDATE, REFERENCES'));

-- sys.foreign_keys
CREATE OR REPLACE VIEW sys.foreign_keys AS
SELECT
  con.conname AS name,
  con.oid AS object_id,
  CAST(NULL AS INT) AS principal_id,
  con.connamespace AS schema_id,
  con.conrelid AS parent_object_id,
  CAST('F' AS CHAR(2)) AS type,
  CAST('FOREIGN_KEY_CONSTRAINT' AS NVARCHAR(60)) AS type_desc,
  CAST(NULL AS TIMESTAMP) AS create_date,
  CAST(NULL AS TIMESTAMP) AS modify_date,
  CAST(0 AS BIT) AS is_ms_shipped,
  CAST(0 AS BIT) AS is_published,
  CAST(0 AS BIT) AS is_schema_published,
  con.confrelid AS referenced_object_id,
  con.conindid AS key_index_id,
  CAST(CAST(con.condisable AS INT) AS BIT) AS is_disabled,
  CAST(0 AS BIT) AS is_not_for_replication,
  CAST(CAST(NOT con.convalidated AS INT) AS BIT) AS is_not_trusted,
  CAST(
    (CASE con.confdeltype
      WHEN 'a' THEN 0
      WHEN 'r' THEN 0
      WHEN 'c' THEN 1
      WHEN 'n' THEN 2
      WHEN 'd' THEN 3
      ELSE -1
    END)
    AS TINYINT) AS delete_referential_action,
  CAST(
    (CASE con.confdeltype
      WHEN 'a' THEN 'NO_ACTION'
      WHEN 'r' THEN 'NO_ACTION'
      WHEN 'c' THEN 'CASCADE'
      WHEN 'n' THEN 'SET_NULL'
      WHEN 'd' THEN 'SET_DEFAULT'
      ELSE ''
    END)
    AS NVARCHAR(60)) AS delete_referential_action_desc,
  CAST(
    (CASE con.confupdtype
      WHEN 'a' THEN 0
      WHEN 'r' THEN 0
      WHEN 'c' THEN 1
      WHEN 'n' THEN 2
      WHEN 'd' THEN 3
      ELSE -1
    END)
    AS TINYINT) AS update_referential_action,
  CAST(
    (CASE con.confupdtype
      WHEN 'a' THEN 'NO_ACTION'
      WHEN 'r' THEN 'NO_ACTION'
      WHEN 'c' THEN 'CASCADE'
      WHEN 'n' THEN 'SET_NULL'
      WHEN 'd' THEN 'SET_DEFAULT'
      ELSE ''
    END)
    AS NVARCHAR(60)) update_referential_action_desc,
  CAST(1 AS BIT) AS is_system_named
FROM pg_constraint con
INNER JOIN pg_class c ON c.oid = con.conrelid
INNER JOIN pg_namespace s ON s.oid = con.connamespace
WHERE con.contype = 'f'
AND s.nspname NOT IN ('information_schema', 'pg_catalog', 'sys', 'information_schema_tsql')
AND (pg_catalog.pg_has_role(c.relowner, 'USAGE')
  OR pg_catalog.has_table_privilege(c.oid, 'INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER')
  OR pg_catalog.has_any_column_privilege(c.oid, 'INSERT, UPDATE, REFERENCES'));

-- sys.key_constraints
CREATE OR REPLACE VIEW sys.key_constraints AS
SELECT
  con.conname AS name,
  con.oid AS object_id,
  CAST(null AS INT) AS principal_id,
  con.connamespace AS schema_id,
  con.conrelid AS parent_object_id,
  CAST(CASE con.contype
        WHEN 'p' THEN 'PK'
        WHEN 'u' THEN 'UQ'
        ELSE ''
      END AS CHAR(2)) AS type,
  CAST(CASE con.contype
        WHEN 'p' THEN 'PRIMARY_KEY_CONSTRAINT'
        WHEN 'u' THEN 'UNIQUE_CONSTRAINT'
        ELSE ''
      END AS NVARCHAR(60)) AS type_desc,
  CAST(NULL AS TIMESTAMP) AS create_date,
  CAST(NULL AS TIMESTAMP) AS modify_date,
  CAST(0 AS BIT) AS is_ms_shipped,
  CAST(0 AS BIT) AS is_published,
  CAST(0 AS BIT) AS is_schema_published,
  con.conindid AS unique_index_id,
  CAST(
    (CASE WHEN con.contype = 'p' AND con.conname = format('%s_pkey', c.relname) THEN 1
      WHEN con.contype = 'u' AND con.conname = format('%s_%s_key', c.relname,
        array_to_string(ARRAY(
           SELECT attname
           FROM pg_attribute att
           WHERE att.attrelid = con.conrelid
             AND att.attnum = ANY(con.conkey)
           ORDER BY array_position(con.conkey, att.attnum)
       ), '_')) THEN 1
      ELSE 0
    END)
    AS BIT) AS is_system_named,
  CAST(CAST(NOT con.condisable AS INT) AS BIT) AS is_enforced
FROM pg_constraint con
INNER JOIN pg_class c ON c.oid = con.conrelid
INNER JOIN pg_namespace s ON s.oid = con.connamespace
WHERE con.contype IN ('p', 'u')
AND s.nspname NOT IN ('information_schema', 'pg_catalog', 'sys', 'information_schema_tsql')
AND (pg_catalog.pg_has_role(c.relowner, 'USAGE')
  OR pg_catalog.has_table_privilege(c.oid, 'INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER')
  OR pg_catalog.has_any_column_privilege(c.oid, 'INSERT, UPDATE, REFERENCES'));

-- sys.sysforeignkeys
CREATE OR REPLACE VIEW sys.sysforeignkeys AS
SELECT
  con.oid AS constid,
  con.conrelid AS fkeyid,
  con.confrelid AS rkeyid,
  CAST((UNNEST(con.conkey)) AS SMALLINT) AS fkey,
  CAST((UNNEST(con.confkey)) AS SMALLINT) AS rkey,
  CAST((generate_series(1, ARRAY_LENGTH(con.conkey, 1))) AS SMALLINT) AS keyno
FROM pg_constraint con
INNER JOIN pg_class c ON c.oid = con.conrelid
INNER JOIN pg_namespace s ON s.oid = con.connamespace
WHERE con.contype = 'f'
AND s.nspname NOT IN ('information_schema', 'pg_catalog', 'sys', 'information_schema_tsql')
AND (pg_catalog.pg_has_role(c.relowner, 'USAGE')
  OR pg_catalog.has_table_privilege(c.oid, 'INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER')
  OR pg_catalog.has_any_column_privilege(c.oid, 'INSERT, UPDATE, REFERENCES'));

-- sys.synonyms
CREATE OR REPLACE VIEW sys.synonyms AS
SELECT
  syn.synname AS name,
  syn.oid AS object_id,
  CAST(CASE s.nspowner WHEN syn.synowner THEN NULL ELSE syn.synowner END AS OID) AS principal_id,
  syn.synnamespace AS schema_id,
  CAST(0 AS OID) AS parent_object_id,
  CAST('SN' AS char(2)) AS type,
  CAST('Synonym' AS nvarchar(60)) AS type_desc,
  CAST(NULL AS TIMESTAMP) AS create_date,
  CAST(NULL AS TIMESTAMP) AS modify_date,
  CAST(0 AS bit) AS is_ms_shipped,
  CAST(0 AS bit) AS is_published,
  CAST(0 AS bit) AS is_schema_published,
  CAST(quote_ident(syn.synobjschema)||'.'||quote_ident(syn.synobjname) AS NVARCHAR(1035)) AS base_object_name
FROM pg_synonym syn
INNER JOIN pg_namespace s ON s.oid = syn.synnamespace
WHERE s.nspname NOT IN ('information_schema', 'pg_catalog', 'sys', 'information_schema_tsql');

-- sys.all_views
CREATE OR REPLACE VIEW sys.all_views AS
SELECT
  t.relname AS name,
  t.oid AS object_id,
  CAST(CASE s.nspowner WHEN t.relowner THEN NULL ELSE t.relowner END AS OID) AS principal_id,
  s.oid AS schema_id,
  CAST(0 AS OID) AS parent_object_id,
  CAST('V' AS CHAR(2)) AS type,
  CAST('VIEW' AS NVARCHAR(60)) AS type_desc,
  CAST(o.ctime AS TIMESTAMP) AS create_date,
  CAST(o.mtime AS TIMESTAMP) AS modify_date,
  CAST(0 AS BIT) AS is_ms_shipped,
  CAST(0 AS BIT) AS is_published,
  CAST(0 AS BIT) AS is_schema_published,
  CAST(0 AS BIT) AS is_replicated,
  CAST(0 AS BIT) AS has_replication_filter,
  CAST(0 AS BIT) AS has_opaque_metadata,
  CAST(0 AS BIT) AS has_unchecked_ASsembly_data,
  CAST(CASE WHEN sys.tsql_relation_reloptions_helper(t.reloptions, 'check_option') is NULL THEN 0 ELSE 1 END AS BIT) AS with_check_option,
  CAST(0 AS BIT) AS is_date_correlation_view,
  CAST(0 AS BIT) AS is_tracked_by_cdc
FROM pg_class t
INNER JOIN pg_namespace s ON t.relnamespace = s.oid
LEFT JOIN pg_object o ON o.object_oid = t.oid
WHERE t.relkind IN ('v', 'm')
AND (pg_catalog.pg_has_role(t.relowner, 'USAGE')
  OR pg_catalog.has_table_privilege(t.oid, 'SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER')
  OR pg_catalog.has_any_column_privilege(t.oid, 'SELECT, INSERT, UPDATE, REFERENCES'));

-- sys.trigger_events
CREATE OR REPLACE VIEW sys.trigger_events AS
SELECT
  pt.oid AS object_id,
  CAST(t_events.type AS INT) AS type,
  CAST(t_events.type_desc AS NVARCHAR(60)) AS type_desc,
  CAST(0 AS BIT) AS is_first,
  CAST(0 AS BIT) AS is_last,
  CAST(NULL AS INT) AS event_group_type,
  CAST(NULL AS NVARCHAR(60)) AS event_group_type_desc,
  CAST(1 AS BIT) AS is_trigger_event
FROM pg_trigger pt
    CROSS JOIN LATERAL (
        SELECT * FROM (
            VALUES
                (1, 'INSERT', 4),
                (2, 'UPDATE', 16),
                (3, 'DELETE', 8),
                (4, 'TRUNCATE', 32)
        ) AS v(type, type_desc, bitmask)
        WHERE (pt.tgtype & bitmask) != 0
    ) AS t_events
INNER JOIN pg_class c ON pt.tgrelid = c.oid
WHERE pg_catalog.pg_has_role(c.relowner, 'USAGE')
OR pg_catalog.has_table_privilege(c.oid, 'INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER')
OR pg_catalog.has_any_column_privilege(c.oid, 'INSERT, UPDATE, REFERENCES')
UNION ALL
SELECT
  pet.oid AS object_id,
  CAST(et_events.type AS INT) AS type,
  CAST(et_events.type_desc AS NVARCHAR(60)) AS type_desc,
  CAST(0 AS BIT) AS is_first,
  CAST(0 AS BIT) AS is_last,
  CAST(NULL AS INT) AS event_group_type,
  CAST(NULL AS NVARCHAR(60)) AS event_group_type_desc,
  CAST(1 AS BIT) AS is_trigger_event
FROM pg_event_trigger pet
    CROSS JOIN LATERAL (
        SELECT * FROM (
            VALUES
                (5, 'DDL_COMMAND_START'),
                (6, 'DDL_COMMAND_END'),
                (7, 'TABLE_REWRITE'),
                (8, 'SQL_DROP')
        ) AS v(type, type_desc)
        WHERE pet.evtevent = lower(type_desc)
    ) AS et_events
ORDER BY object_id, type;

-- sys.triggers
CREATE OR REPLACE VIEW sys.triggers AS
SELECT
  pt.tgname AS name,
  pt.oid AS object_id,
  CAST(1 AS TINYINT) AS parent_class,
  CAST('OBJECT_OR_COLUMN' AS NVARCHAR(60)) AS parent_class_desc,
  pt.tgrelid AS parent_id,
  CAST('TR' AS CHAR(2)) AS type,
  CAST('SQL_TRIGGER' AS NVARCHAR(60)) AS type_desc,
  CAST(pt.tgtime AS TIMESTAMP) AS create_date,
  CAST(NULL AS TIMESTAMP) AS modify_date,
  CAST(0 AS BIT) AS is_ms_shipped,
  CAST(
    CASE
      WHEN pt.tgenabled = 'D' THEN 1
      ELSE 0
    END AS BIT
  )	AS is_disabled,
  CAST(0 AS BIT) AS is_not_for_replication,
  CAST(
    CASE
        WHEN (pt.tgtype >> 6 & 1) = 1 THEN 1
        WHEN (pt.tgtype >> 1 & 1) = 1 THEN 2
        ELSE 0
    END AS TINYINT
  ) AS is_instead_of_trigger
FROM pg_trigger pt
INNER JOIN pg_class c ON pt.tgrelid = c.oid
WHERE pg_catalog.pg_has_role(c.relowner, 'USAGE')
OR pg_catalog.has_table_privilege(c.oid, 'INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER')
OR pg_catalog.has_any_column_privilege(c.oid, 'INSERT, UPDATE, REFERENCES')
UNION ALL
SELECT
  pet.evtname AS name,
  pet.oid AS object_id,
  CAST(0 AS TINYINT) AS parent_class,
  CAST('DDL' AS NVARCHAR(60)) AS parent_class_desc,
  CAST(0 AS OID) AS parent_id,
  CAST('TR' AS CHAR(2)) AS type,
  CAST('SQL_TRIGGER' AS NVARCHAR(60)) AS type_desc,
  CAST(NULL AS TIMESTAMP) AS create_date,
  CAST(NULL AS TIMESTAMP) AS modify_date,
  CAST(0 AS BIT) AS is_ms_shipped,
  CAST(
    CASE
      WHEN pet.evtenabled = 'D' THEN 1
      ELSE 0
    END AS BIT
  )	AS is_disabled,
  CAST(0 AS BIT) AS is_not_for_replication,
  CAST(0 AS TINYINT) AS is_instead_of_trigger
FROM pg_event_trigger pet;

-- sys.configurations
CREATE OR REPLACE VIEW sys.configurations AS
SELECT
  CAST(NULL AS INT) AS configuration_id,
  CAST(s.name AS NVARCHAR(35)) AS name,
  CAST(s.setting AS SQL_VARIANT) AS value,
  CAST(s.min_val AS SQL_VARIANT) AS minimum,
  CAST(s.max_val AS SQL_VARIANT) AS maximum,
  CAST(s.setting AS SQL_VARIANT) AS value_in_use,
  CAST(s.short_desc AS NVARCHAR(255)) AS description,
  CAST(
    CASE
      WHEN s.context = 'postmaster' or s.context = 'internal' THEN 0
      ELSE 1
    END AS BIT
  ) AS is_dynamic,
  CAST(0 AS BIT) AS is_advanced
FROM pg_settings s;

-- sys.syscurconfigs
CREATE OR REPLACE VIEW sys.syscurconfigs AS
SELECT
  CAST(s.setting AS SQL_VARIANT) AS value,
  CAST(NULL AS SMALLINT) AS config,
  CAST(s.short_desc AS NVARCHAR(255)) AS comment,
  CAST(
    CASE
      WHEN s.context = 'postmaster' OR s.context = 'internal' THEN 0
      ELSE 1
    END AS SMALLINT
  ) AS status
FROM pg_settings s;

-- sys.sysconfigures
CREATE OR REPLACE VIEW sys.sysconfigures AS
SELECT
  CAST(s.setting AS SQL_VARIANT) AS value,
  CAST(NULL AS INT) AS config,
  CAST(s.short_desc AS NVARCHAR(255)) AS comment,
  CAST(
    CASE
      WHEN s.context = 'postmaster' OR s.context = 'internal' THEN 0
      ELSE 1
    END AS SMALLINT
  ) AS status
FROM pg_settings s;

-- sys.check_constraints
CREATE OR REPLACE VIEW sys.check_constraints AS
SELECT
  con.conname AS name,
  con.oid AS object_id,
  CAST(NULL AS INT) AS principal_id,
  con.connamespace AS schema_id,
  con.conrelid AS parent_object_id,
  CAST('C' AS char(2)) AS type,
  CAST('CHECK_CONSTRAINT' AS NVARCHAR(60)) AS type_desc,
  CAST(NULL AS TIMESTAMP) AS create_date,
  CAST(NULL AS TIMESTAMP) AS modify_date,
  CAST(0 AS BIT) AS is_ms_shipped,
  CAST(0 AS BIT) AS is_published,
  CAST(0 AS BIT) AS is_schema_published,
  CAST(CAST(con.condisable AS INT) AS BIT) AS is_disabled,
  CAST(0 AS BIT) AS is_not_for_replication,
  CAST(CAST(NOT con.convalidated AS INT) AS BIT) AS is_not_trusted,
  CAST(CASE WHEN ARRAY_LENGTH(con.conkey, 1) != 1 THEN 0 ELSE con.conkey[1] END AS INT) AS parent_column_id,
  con.consrc AS definition,
  CAST(1 AS BIT) AS uses_database_collation,
  CAST(1 AS BIT) AS is_system_named
FROM pg_constraint con
INNER JOIN pg_class c ON c.oid = con.conrelid
INNER JOIN pg_namespace s ON s.oid = con.connamespace
WHERE con.contype = 'c'
AND s.nspname NOT IN ('information_schema', 'pg_catalog', 'sys', 'information_schema_tsql')
AND (pg_catalog.pg_has_role(c.relowner, 'USAGE')
OR pg_catalog.has_table_privilege(c.oid, 'INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER')
OR pg_catalog.has_any_column_privilege(c.oid, 'INSERT, UPDATE, REFERENCES'));

-- sys.index_columns
CREATE OR REPLACE VIEW sys.index_columns AS
SELECT
  i.indrelid AS object_id,
  i.indexrelid AS index_id,
  CAST(idx AS INT) AS index_column_id,
  CAST(i.indkey[idx - 1] AS INT) AS column_id,
  CAST(
    (CASE
      WHEN index_column_id <= i.indnkeyatts THEN index_column_id
      ELSE 0
    END)
    AS TINYINT) AS key_ordinal,
  CAST(0 AS TINYINT) AS partition_ordinal,
  CAST(
    (CASE
      WHEN i.indoption[index_column_id - 1] & 1 = 1 THEN 1
      ELSE 0
    END)
    AS BIT) AS is_descending_key,
  CAST(
    (CASE
      WHEN idx > i.indnkeyatts THEN 1
      ELSE 0
    END)
    AS BIT) AS is_included_column
FROM
    pg_index i
    INNER JOIN pg_class c ON i.indrelid = c.oid and c.parttype = 'n'
    INNER JOIN pg_namespace nsp ON nsp.oid = c.relnamespace
    INNER JOIN generate_series(1, array_length(i.indkey, 1)) AS idx ON TRUE
WHERE
    pg_catalog.pg_has_role(c.relowner, 'USAGE')
    OR pg_catalog.has_table_privilege(c.oid, 'INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER')
    OR pg_catalog.has_any_column_privilege(c.oid, 'INSERT, UPDATE, REFERENCES')
UNION ALL
SELECT
  i.indrelid AS object_id,
  i.indexrelid AS index_id,
  CAST(idx AS INT) AS index_column_id,
  CAST(i.indkey[idx - 1] AS INT) AS column_id,
  CAST(
    (CASE
      WHEN index_column_id <= i.indnkeyatts THEN index_column_id
      ELSE 0
    END)
    AS TINYINT) AS key_ordinal,
  CAST(
    (CASE
      WHEN array_position(string_to_array(pp.partkey::TEXT, ' ')::int2[], i.indkey[idx - 1]) IS NULL THEN 0
      ELSE array_position(string_to_array(pp.partkey::TEXT, ' ')::int2[], i.indkey[idx - 1])
    END)
    AS TINYINT) AS partition_ordinal,
  CAST(
    (CASE
      WHEN i.indoption[index_column_id - 1] & 1 = 1 THEN 1
      ELSE 0
    END)
    AS BIT) AS is_descending_key,
  CAST(
    (CASE
      WHEN idx > i.indnkeyatts AND i.indkey[idx - 1] > 0 THEN 1
      ELSE 0
    END)
    AS BIT) AS is_included_column
FROM
    pg_index i
    INNER JOIN pg_class c ON i.indrelid = c.oid and c.parttype != 'n'
    INNER JOIN pg_namespace nsp ON nsp.oid = c.relnamespace
    INNER JOIN pg_partition pp ON pp.parentid = c.oid AND pp.relname = c.relname
    INNER JOIN generate_series(1, array_length(i.indkey, 1)) AS idx ON TRUE
WHERE
    pg_catalog.pg_has_role(c.relowner, 'USAGE')
    OR pg_catalog.has_table_privilege(c.oid, 'INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER')
    OR pg_catalog.has_any_column_privilege(c.oid, 'INSERT, UPDATE, REFERENCES');

-- sys.system_objects
CREATE OR REPLACE VIEW sys.system_objects AS
SELECT o.* FROM sys.all_objects o
INNER JOIN pg_namespace s ON o.schema_id = s.oid
AND s.nspname IN ('information_schema', 'pg_catalog', 'sys', 'information_schema_tsql')
UNION ALL
SELECT
  t.relname AS name,
  t.oid AS object_id,
  CAST(CASE s.nspowner WHEN t.relowner THEN NULL ELSE t.relowner END AS OID) AS principal_id,
  s.oid AS schema_id,
  CAST(0 AS OID) AS parent_object_id,
  CAST('V' AS CHAR(2)) AS type,
  CAST('VIEW' AS NVARCHAR(60)) AS type_desc,
  CAST(o.ctime AS TIMESTAMP) AS create_date,
  CAST(o.mtime AS TIMESTAMP) AS modify_date,
  CAST(0 AS bit) AS is_ms_shipped,
  CAST(0 AS bit) AS is_published,
  CAST(0 AS bit) AS is_schema_published
FROM pg_class t
INNER JOIN pg_namespace s ON t.relnamespace = s.oid
LEFT JOIN pg_object o ON o.object_oid = t.oid
WHERE t.relkind IN ('v', 'm')
AND s.nspname = 'dbe_perf'
AND (pg_catalog.pg_has_role(t.relowner, 'USAGE')
  OR pg_catalog.has_table_privilege(t.oid, 'SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER')
  OR pg_catalog.has_any_column_privilege(t.oid, 'SELECT, INSERT, UPDATE, REFERENCES'));

-- sys.database_principals
CREATE OR REPLACE VIEW sys.database_principals AS
SELECT
rolname AS name,
oid AS principal_id,
CAST('R' AS CHAR(1)) AS type,
CAST('DATABASE_ROLE' AS NVARCHAR(60)) AS type_desc,
CAST(NULL AS NAME) AS default_schema_name,
CAST(NULL AS TIMESTAMP) AS create_date,
CAST(NULL AS TIMESTAMP) AS modify_date,
10 AS owning_principal_id,
CAST(CAST(oid AS INT) AS VARBINARY(85)) AS SID,
CAST(CASE WHEN oid < 16384 THEN 1 ELSE 0 END AS BIT) AS is_fixed_role,
0 as authentication_type,
CAST('NONE' AS NVARCHAR(60)) as authentication_type_desc,
CAST(NULL AS NAME) AS default_language_name,
CAST(NULL AS INT) AS default_language_lcid,
CAST(0 AS BIT) AS allow_encrypted_value_modifications
from pg_roles;

-- sys.sysprocesses
CREATE OR REPLACE VIEW sys.sysprocesses AS
SELECT
  blocked_activity.pid AS spid,
  CAST(NULL AS SMALLINT) AS kpid,
  (SELECT blocking_activity.pid
     FROM pg_locks blocked
     JOIN pg_locks blocking
       ON blocking.locktype = blocked.locktype
      AND blocking.database IS NOT DISTINCT FROM blocked.database
      AND blocking.relation IS NOT DISTINCT FROM blocked.relation
      AND blocking.page IS NOT DISTINCT FROM blocked.page
      AND blocking.tuple IS NOT DISTINCT FROM blocked.tuple
      AND blocking.virtualxid IS NOT DISTINCT FROM blocked.virtualxid
      AND blocking.transactionid IS NOT DISTINCT FROM blocked.transactionid
      AND blocking.classid IS NOT DISTINCT FROM blocked.classid
      AND blocking.objid IS NOT DISTINCT FROM blocked.objid
      AND blocking.objsubid IS NOT DISTINCT FROM blocked.objsubid
      AND blocking.pid != blocked.pid
      AND blocking.granted = true
      AND blocked.granted = false
     JOIN pg_stat_activity blocking_activity ON blocking_activity.pid = blocking.pid
     WHERE blocked.pid = blocked_activity.pid
     LIMIT 1
  ) AS blocked,
  CAST(NULL AS VARBINARY(2)) AS waittype,
  CAST(0 AS BIGINT) AS waittime,
  CAST(NULL AS NCHAR(32)) AS lastwaittype,
  CAST(NULL AS NCHAR(256)) AS waitresource,
  blocked_activity.datid AS dbid,
  blocked_activity.usesysid AS uid,
  0 AS cpu,
  CAST(0 AS BIGINT) AS physical_io,
  0 AS memusage,
  blocked_activity.backend_start AS login_time,
  blocked_activity.query_start AS last_batch,
  CAST(0 AS SMALLINT) AS ecid,
  CAST(0 AS SMALLINT) AS open_tran,
  CAST(blocked_activity.state AS NCHAR(30)) AS status,
  CAST(CAST(blocked_activity.usesysid AS INT) AS VARBINARY(86)) AS sid,
  CAST(blocked_activity.client_hostname AS NCHAR(128)) AS hostname,
  CAST(blocked_activity.application_name AS NCHAR(128)) AS program_name,
  CAST(NULL AS NCHAR(10)) AS hostprocess,
  blocked_activity.query AS cmd,
  CAST(NULL AS NCHAR(128)) AS nt_domain,
  CAST(NULL AS NCHAR(128)) AS nt_username,
  CAST(NULL AS NCHAR(12)) AS net_address,
  CAST(NULL AS NCHAR(12)) AS net_library,
  CAST(blocked_activity.usename AS NCHAR(128)) AS loginame,
  CAST(NULL AS VARBINARY(128)) AS context_info,
  CAST(NULL AS VARBINARY(20)) AS sql_handle,
  0 AS stmt_start,
  0 AS stmt_end,
  blocked_activity.query_id AS request_id
FROM pg_stat_activity blocked_activity;

-- sys.sequences
CREATE OR REPLACE VIEW sys.sequences AS
SELECT
  c.relname AS name,
  c.oid AS object_id,
  CAST(CASE s.nspowner WHEN c.relowner THEN NULL ELSE c.relowner END AS OID) AS principal_id,
  s.oid AS schema_id,
  CAST(0 AS OID) AS parent_object_id,
  CAST('SO' AS CHAR(2)) AS type,
  CAST('SEQUENCE_OBJECT' AS NVARCHAR(60)) AS type_desc,
  CAST(o.ctime AS TIMESTAMP) AS create_date,
  CAST(o.mtime AS TIMESTAMP) AS modify_date,
  CAST(0 AS BIT) AS is_ms_shipped,
  CAST(0 AS BIT) AS is_published,
  CAST(0 AS BIT) AS is_schema_published,
  CAST(
          (pg_catalog.pg_sequence_all_parameters(
              quote_ident(s.nspname) || '.' || quote_ident(c.relname)
          )).start_value
      AS SQL_VARIANT
  ) AS start_value,
  CAST(
          (pg_catalog.pg_sequence_all_parameters(
              quote_ident(s.nspname) || '.' || quote_ident(c.relname)
          )).increment
      AS SQL_VARIANT
  ) AS increment,
  CAST(
          (pg_catalog.pg_sequence_all_parameters(
              quote_ident(s.nspname) || '.' || quote_ident(c.relname)
          )).minimum_value
      AS SQL_VARIANT
  ) AS minimum_value,
  CAST(
          (pg_catalog.pg_sequence_all_parameters(
              quote_ident(s.nspname) || '.' || quote_ident(c.relname)
          )).maximum_value
      AS SQL_VARIANT
  ) AS maximum_value,
  CAST(
      CAST(
          (pg_catalog.pg_sequence_all_parameters(
              quote_ident(s.nspname) || '.' || quote_ident(c.relname)
          )).cycle_option
          AS INT
      )
      AS BIT
  ) AS is_cycling,
  CAST(1 AS BIT) AS is_cached,
  CAST(
          (pg_catalog.pg_sequence_all_parameters(
              quote_ident(s.nspname) || '.' || quote_ident(c.relname)
          )).cache_size
      AS SQL_VARIANT
  ) AS cache_size,
  CAST(
    CASE relkind
      WHEN 'S' THEN 20
      WHEN 'L' THEN 34
      WHEN 'z' THEN 20
      WHEN 'Z' THEN 34
      ELSE -1
    END AS TINYINT
  ) AS system_type_id,
  CAST(
    CASE relkind
      WHEN 'S' THEN 20
      WHEN 'L' THEN 34
      WHEN 'z' THEN 20
      WHEN 'Z' THEN 34
      ELSE -1
    END AS INT
  ) AS user_type_id,
  CAST(
    CASE relkind
      WHEN 'S' THEN 19
      WHEN 'L' THEN 39
      WHEN 'z' THEN 19
      WHEN 'Z' THEN 39
      ELSE -1
    END AS TINYINT
  ) AS precision,
  CAST(0 AS TINYINT) AS scale,
  CAST(
          (pg_catalog.pg_sequence_all_parameters(
              quote_ident(s.nspname) || '.' || quote_ident(c.relname)
          )).last_value
      AS SQL_VARIANT
  ) AS current_value,
  CAST(
      CAST(
          (pg_catalog.pg_sequence_all_parameters(
              quote_ident(s.nspname) || '.' || quote_ident(c.relname)
          )).is_exhausted
          AS INT
      )
      AS BIT
  ) AS is_exhausted,
  CAST(
          (pg_catalog.pg_sequence_all_parameters(
              quote_ident(s.nspname) || '.' || quote_ident(c.relname)
          )).last_used_value
      AS SQL_VARIANT
  ) AS last_used_value
FROM pg_class c
INNER JOIN pg_namespace s ON s.oid = c.relnamespace
INNER JOIN pg_object o ON o.object_oid = c.oid
WHERE relkind IN ('S', 'L', 'z', 'Z')
AND s.nspname NOT IN ('information_schema', 'pg_catalog', 'sys', 'information_schema_tsql')
AND (pg_catalog.pg_has_role(c.relowner, 'USAGE') OR pg_catalog.has_sequence_privilege(c.oid, 'SELECT, UPDATE, USAGE'));




-- sys.object_name
CREATE OR REPLACE FUNCTION sys.object_name(IN object_id int, IN database_id int DEFAULT NULL) RETURNS nvarchar AS '$libdir/shark', 'object_name' LANGUAGE C STABLE;
CREATE OR REPLACE FUNCTION sys.object_name(IN object_id bigint, IN database_id int DEFAULT NULL) RETURNS nvarchar LANGUAGE SQL STABLE as 'select sys.object_name($1::int, $2::int)';
CREATE OR REPLACE FUNCTION sys.object_name(IN object_id bit, IN database_id int DEFAULT NULL) RETURNS nvarchar LANGUAGE SQL STABLE as 'select sys.object_name($1::int, $2::int)';
CREATE OR REPLACE FUNCTION sys.object_name(IN object_id Oid, IN database_id int DEFAULT NULL) RETURNS nvarchar LANGUAGE SQL STABLE as 'select sys.object_name($1::int, $2::int)';
CREATE OR REPLACE FUNCTION sys.object_name(IN object_id int, IN database_id Oid) RETURNS nvarchar LANGUAGE SQL STABLE as 'select sys.object_name($1::int, $2::int)';
CREATE OR REPLACE FUNCTION sys.object_name(IN object_id bigint, IN database_id Oid) RETURNS nvarchar LANGUAGE SQL STABLE as 'select sys.object_name($1::int)';
CREATE OR REPLACE FUNCTION sys.object_name(IN object_id bit, IN database_id Oid) RETURNS nvarchar LANGUAGE SQL STABLE as 'select sys.object_name($1::int, $2::int)';
CREATE OR REPLACE FUNCTION sys.object_name(IN object_id Oid, IN database_id Oid) RETURNS nvarchar LANGUAGE SQL STABLE as 'select sys.object_name($1::int, $2::int)';


-- sys.object_schema_name
CREATE OR REPLACE FUNCTION sys.object_schema_name(IN object_id INT, IN database_id INT DEFAULT NULL) RETURNS text AS '$libdir/shark', 'object_schema_name' LANGUAGE C STABLE;
CREATE OR REPLACE FUNCTION sys.object_schema_name(IN object_id bigint, IN database_id int DEFAULT NULL) RETURNS text LANGUAGE SQL STABLE as 'select sys.object_schema_name($1::int, $2::int)';
CREATE OR REPLACE FUNCTION sys.object_schema_name(IN object_id bit, IN database_id int DEFAULT NULL) RETURNS text LANGUAGE SQL STABLE as 'select sys.object_schema_name($1::int, $2::int)';
CREATE OR REPLACE FUNCTION sys.object_schema_name(IN object_id Oid, IN database_id int DEFAULT NULL) RETURNS text LANGUAGE SQL STABLE as 'select sys.object_schema_name($1::int, $2::int)';
CREATE OR REPLACE FUNCTION sys.object_schema_name(IN object_id int, IN database_id Oid) RETURNS text LANGUAGE SQL STABLE as 'select sys.object_schema_name($1::int, $2::int)';
CREATE OR REPLACE FUNCTION sys.object_schema_name(IN object_id bigint, IN database_id Oid) RETURNS text LANGUAGE SQL STABLE as 'select sys.object_schema_name($1::int)';
CREATE OR REPLACE FUNCTION sys.object_schema_name(IN object_id bit, IN database_id Oid) RETURNS text LANGUAGE SQL STABLE as 'select sys.object_schema_name($1::int, $2::int)';
CREATE OR REPLACE FUNCTION sys.object_schema_name(IN object_id Oid, IN database_id Oid) RETURNS text LANGUAGE SQL STABLE as 'select sys.object_schema_name($1::int, $2::int)';


-- sys.object_definition
CREATE OR REPLACE FUNCTION sys.object_definition(IN object_id int) RETURNS varchar AS '$libdir/shark', 'object_define' LANGUAGE C STABLE STRICT;
CREATE OR REPLACE FUNCTION sys.object_definition(IN object_id bigint) RETURNS nvarchar LANGUAGE SQL STABLE as 'select sys.object_definition($1::int)';
CREATE OR REPLACE FUNCTION sys.object_definition(IN object_id bit) RETURNS nvarchar LANGUAGE SQL STABLE as 'select sys.object_definition($1::int)';
CREATE OR REPLACE FUNCTION sys.object_definition(IN object_id Oid) RETURNS nvarchar LANGUAGE SQL STABLE as 'select sys.object_definition($1::int)';


-- sys.objectpropertyex
CREATE OR REPLACE FUNCTION sys.objectpropertyex(id INT, property VARCHAR) RETURNS SQL_VARIANT
AS $$
    BEGIN
        property := PG_CATALOG.RTRIM(LOWER(COALESCE(property, '')));
        IF NOT EXISTS(SELECT ao.object_id FROM sys.all_objects ao WHERE object_id = id)
        THEN
            RETURN NULL;
        END IF;
        IF property = 'basetype' COLLATE "C" -- BaseType
        THEN
            RETURN (SELECT CAST(ao.type AS SYS.SQL_VARIANT) FROM sys.all_objects ao WHERE ao.object_id = id LIMIT 1);
        END IF;
        RETURN CAST(OBJECTPROPERTY(id, property) AS SYS.SQL_VARIANT);
    EXCEPTION
        WHEN OTHERS THEN
            RETURN NULL;
    END
$$
LANGUAGE plpgsql STABLE;

CREATE OR REPLACE FUNCTION sys.objectpropertyex(IN object_id bigint, property VARCHAR) RETURNS SQL_VARIANT LANGUAGE SQL STABLE as 'select sys.objectpropertyex($1::int, $2)';
CREATE OR REPLACE FUNCTION sys.objectpropertyex(IN object_id bit, property VARCHAR) RETURNS SQL_VARIANT LANGUAGE SQL STABLE as 'select sys.objectpropertyex($1::int, $2)';
CREATE OR REPLACE FUNCTION sys.objectpropertyex(IN object_id Oid, property VARCHAR) RETURNS SQL_VARIANT LANGUAGE SQL STABLE as 'select sys.objectpropertyex($1::int, $2)';
CREATE OR REPLACE FUNCTION sys.objectpropertyex(IN object_id varbinary, property VARCHAR) RETURNS SQL_VARIANT LANGUAGE SQL STABLE as 'select sys.objectpropertyex($1::int, $2)';
CREATE OR REPLACE FUNCTION sys.objectpropertyex(id INT, property bit) RETURNS SQL_VARIANT LANGUAGE SQL STABLE as 'select sys.objectpropertyex($1, $2::varchar)';
CREATE OR REPLACE FUNCTION sys.objectpropertyex(id INT, property varbinary) RETURNS SQL_VARIANT LANGUAGE SQL STABLE as 'select sys.objectpropertyex($1, $2::varchar)';


--sys.col_length
CREATE OR REPLACE FUNCTION sys.type_max_length_helper_ext(IN type TEXT, IN typelen INT, IN typemod INT, IN for_sys_types boolean DEFAULT false, IN used_typmod_array boolean DEFAULT false)
RETURNS SMALLINT
AS $$
DECLARE
	max_length SMALLINT;
	precision INT;
	v_type TEXT COLLATE "default" := type;
BEGIN
	-- unknown tsql type
	IF v_type IS NULL THEN
		RETURN CAST(typelen as SMALLINT);
	END IF;

	-- if using typmod_array from pg_proc.probin
	IF used_typmod_array THEN
		IF v_type = 'sysname' THEN
			RETURN 256;
		ELSIF (v_type in ('char', 'bpchar', 'varchar', 'binary', 'varbinary', 'nchar', 'nvarchar'))
		THEN
			IF typemod < 0 THEN -- max value. 
				RETURN -1;
			ELSIF v_type in ('nchar', 'nvarchar') THEN
				RETURN (2 * typemod);
			ELSE
				RETURN typemod;
			END IF;
		END IF;
	END IF;

	IF typelen != -1 THEN
		CASE v_type 
            WHEN 'tinyint' THEN max_length = 1;
            WHEN 'date' THEN max_length = 3;
            WHEN 'smalldatetime' THEN max_length = 4;
            WHEN 'datetime2' THEN
                IF typemod = -1 THEN max_length = 8;
                ELSIF typemod <= 2 THEN max_length = 6;
                ELSIF typemod <= 4 THEN max_length = 7;
	            ELSEIF typemod <= 7 THEN max_length = 8;
			-- typemod = 7 is not possible for datetime2 in Babel
			END IF;
		WHEN 'datetimeoffset' THEN
			IF typemod = -1 THEN max_length = 10;
			ELSIF typemod <= 2 THEN max_length = 8;
			ELSIF typemod <= 4 THEN max_length = 9;
			ELSIF typemod <= 7 THEN max_length = 10;
			-- typemod = 7 is not possible for datetimeoffset in Babel
			END IF;
		WHEN 'time' THEN
			IF typemod = -1 THEN max_length = 5;
			ELSIF typemod <= 2 THEN max_length = 3;
			ELSIF typemod <= 4 THEN max_length = 4;
			ELSIF typemod <= 7 THEN max_length = 5;
			END IF;
		WHEN 'timestamp' THEN max_length = 8;
		WHEN 'vector' THEN max_length = -1; -- dummy as varchar max
		WHEN 'halfvec' THEN max_length = -1; -- dummy as varchar max
		WHEN 'sparsevec' THEN max_length = -1; -- dummy as varchar max
		ELSE max_length = typelen;
		END CASE;
		RETURN max_length;
	END IF;

	IF typemod = -1 THEN
		CASE 
		WHEN v_type in ('image', 'text', 'ntext') THEN max_length = 16;
		WHEN v_type = 'sql_variant' THEN max_length = 8016;
		WHEN v_type in ('varbinary', 'varchar', 'nvarchar') THEN 
			IF for_sys_types THEN max_length = 8000;
			ELSE max_length = -1;
			END IF;
		WHEN v_type in ('binary', 'char', 'bpchar', 'nchar') THEN max_length = 8000;
		WHEN v_type in ('decimal', 'numeric') THEN max_length = 17;
		WHEN v_type in ('geometry', 'geography') THEN max_length = -1;
		ELSE max_length = typemod;
		END CASE;
		RETURN max_length;
	END IF;

	CASE
	WHEN v_type in ('char', 'bpchar', 'varchar', 'binary', 'varbinary') THEN max_length = typemod - 4;
	WHEN v_type in ('nchar', 'nvarchar') THEN max_length = (typemod - 4) * 2;
	WHEN v_type = 'sysname' THEN max_length = (typemod - 4) * 2;
	WHEN v_type in ('numeric', 'decimal') THEN
		precision = ((typemod - 4) >> 16) & 65535;
		IF precision >= 1 and precision <= 9 THEN max_length = 5;
		ELSIF precision <= 19 THEN max_length = 9;
		ELSIF precision <= 28 THEN max_length = 13;
		ELSIF precision <= 38 THEN max_length = 17;
	ELSE max_length = typelen;
	END IF;
	ELSE
		max_length = typemod;
	END CASE;
	RETURN max_length;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION sys.remove_delimiter_pair(IN name TEXT)
RETURNS TEXT AS
$BODY$
BEGIN
    IF name collate "default" IN('[', ']', '"') THEN
        RETURN NULL;

    ELSIF length(name) >= 2 AND left(name, 1) = '[' collate "default" AND right(name, 1) = ']' collate "default" THEN
        IF length(name) = 2 THEN
            RETURN '';
        ELSE
            RETURN substring(name from 2 for length(name)-2);
        END IF;
    ELSIF length(name) >= 2 AND left(name, 1) = '[' collate "default" AND right(name, 1) != ']' collate "default" THEN
        RETURN NULL;
    ELSIF length(name) >= 2 AND left(name, 1) != '[' collate "default" AND right(name, 1) = ']' collate "default" THEN
        RETURN NULL;

    ELSIF length(name) >= 2 AND left(name, 1) = '"' collate "default" AND right(name, 1) = '"' collate "default" THEN
        IF length(name) = 2 THEN
            RETURN '';
        ELSE
            RETURN substring(name from 2 for length(name)-2);
        END IF;
    ELSIF length(name) >= 2 AND left(name, 1) = '"' collate "default" AND right(name, 1) != '"' collate "default" THEN
        RETURN NULL;
    ELSIF length(name) >= 2 AND left(name, 1) != '"' collate "default" AND right(name, 1) = '"' collate "default" THEN
        RETURN NULL;

    END IF;
    RETURN name;
END;
$BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION sys.truncate_identifier(IN object_name TEXT) RETURNS text AS '$libdir/shark', 'truncate_identifier' LANGUAGE C IMMUTABLE STRICT;
CREATE OR REPLACE FUNCTION sys.translate_pg_type_to_tsql(pgoid oid) RETURNS TEXT AS '$libdir/shark', 'translate_pg_type_to_tsql' LANGUAGE C IMMUTABLE;


CREATE OR REPLACE FUNCTION sys.col_length(IN object_name TEXT, IN column_name TEXT)
RETURNS SMALLINT AS $BODY$
DECLARE
    col_name TEXT;
    object_id oid;
    column_id INT;
    column_length SMALLINT;
    column_data_type TEXT;
    typeid oid;
    typelen INT;
    typemod INT;
BEGIN
    -- Get the object ID for the provided object_name
    object_id := sys.OBJECT_ID(object_name, 'U');
    IF object_id IS NULL THEN
        RETURN NULL;
    END IF;

    -- Truncate and normalize the column name
    col_name := sys.truncate_identifier(sys.remove_delimiter_pair(pg_catalog.lower(column_name)));

    -- Get the column ID, typeid, length, and typmod for the provided column_name
    SELECT attnum, a.atttypid, a.attlen, a.atttypmod
    INTO column_id, typeid, typelen, typemod
    FROM pg_attribute a
    WHERE attrelid = object_id AND pg_catalog.lower(attname) = col_name COLLATE "default";

    IF column_id IS NULL THEN
        RETURN NULL;
    END IF;

    -- Get the correct data type
    column_data_type := sys.translate_pg_type_to_tsql(typeid);

    IF column_data_type = 'sysname' THEN
        column_length := 256;
    ELSIF column_data_type IS NULL THEN

        -- Check if it ia user-defined data type
        SELECT sys.translate_pg_type_to_tsql(typbasetype), typlen, typtypmod 
        INTO column_data_type, typelen, typemod
        FROM pg_type
        WHERE oid = typeid;

        IF column_data_type = 'sysname' THEN
            column_length := 256;
        ELSE 
            -- Calculate column length based on base type information
            column_length := sys.type_max_length_helper_ext(column_data_type, typelen, typemod);
        END IF;
    ELSE
        -- Calculate column length based on base type information
        column_length := sys.type_max_length_helper_ext(column_data_type, typelen, typemod);
    END IF;
    RETURN column_length;
EXCEPTION
    WHEN OTHERS THEN
	  return NULL;
END;
$BODY$
LANGUAGE plpgsql
IMMUTABLE
STRICT;

CREATE OR REPLACE FUNCTION sys.col_length(IN object_name varbinary, IN column_name TEXT) RETURNS SMALLINT LANGUAGE SQL STABLE STRICT as 'select sys.col_length($1::text, $2)';
CREATE OR REPLACE FUNCTION sys.col_length(IN object_name text, IN column_name varbinary) RETURNS SMALLINT LANGUAGE SQL STABLE STRICT as 'select sys.col_length($1, $2::text)';

-- sys.col_name
CREATE OR REPLACE FUNCTION sys.COL_NAME(IN table_id INT, IN column_id INT)
RETURNS text AS $$
    DECLARE
        column_name TEXT;
    BEGIN
        SELECT case attisdropped when true then NULL else attname end  INTO STRICT column_name
        FROM pg_attribute
        WHERE attrelid = table_id AND attnum = column_id AND attnum > 0;
        RETURN column_name::text;
    EXCEPTION
        WHEN OTHERS THEN
            RETURN NULL;
    END;
$$
LANGUAGE plpgsql IMMUTABLE
STRICT;


CREATE OR REPLACE FUNCTION sys.COL_NAME(IN table_id bigint, IN column_id INT) RETURNS text LANGUAGE SQL STABLE as 'select sys.COL_NAME($1::int, $2::int)';
CREATE OR REPLACE FUNCTION sys.COL_NAME(IN table_id bit, IN column_id INT) RETURNS text LANGUAGE SQL STABLE as 'select sys.COL_NAME($1::int, $2::int)';
CREATE OR REPLACE FUNCTION sys.COL_NAME(IN table_id Oid, IN column_id INT) RETURNS text LANGUAGE SQL STABLE as 'select sys.COL_NAME($1::int, $2::int)';
CREATE OR REPLACE FUNCTION sys.COL_NAME(IN table_id INT, IN column_id bigint) RETURNS text LANGUAGE SQL STABLE as 'select sys.COL_NAME($1::int, $2::int)';
CREATE OR REPLACE FUNCTION sys.COL_NAME(IN table_id INT, IN column_id bit) RETURNS text LANGUAGE SQL STABLE as 'select sys.COL_NAME($1::int, $2::int)';
CREATE OR REPLACE FUNCTION sys.COL_NAME(IN table_id INT, IN column_id Oid) RETURNS text LANGUAGE SQL STABLE as 'select sys.COL_NAME($1::int, $2::int)';
CREATE OR REPLACE FUNCTION sys.COL_NAME(IN table_id text, IN column_id text) RETURNS text LANGUAGE SQL STABLE as 'select sys.COL_NAME($1::int, $2::int)';


-- sys.columnproperty
CREATE OR REPLACE FUNCTION sys.columnproperty(object_id OID, property TEXT, property_name TEXT) RETURNS INTEGER AS '$libdir/shark', 'columnproperty' LANGUAGE C STABLE STRICT;


-- sys.year
CREATE OR REPLACE FUNCTION sys.year(input ANYELEMENT) RETURNS INTEGER AS
$BODY$
SELECT sys.datepart('year', input);
$BODY$
STRICT
LANGUAGE SQL IMMUTABLE;


CREATE OR REPLACE FUNCTION sys.year(input text) RETURNS INTEGER AS
$BODY$
SELECT sys.datepart('year', input::timestamp without time zone);
$BODY$
STRICT
LANGUAGE SQL IMMUTABLE;


CREATE OR REPLACE FUNCTION sys.year(input bigint) RETURNS INTEGER AS
$BODY$
SELECT sys.datepart('year', input::int);
$BODY$
STRICT
LANGUAGE SQL IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.year(input bit) RETURNS INTEGER AS
$BODY$
SELECT sys.datepart('year', input::int);
$BODY$
STRICT
LANGUAGE SQL IMMUTABLE;



-- sys.month
CREATE OR REPLACE FUNCTION sys.month(input date) RETURNS INTEGER AS
$BODY$
SELECT sys.datepart('month', input);
$BODY$
STRICT
LANGUAGE SQL IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.month(input timestamp without time zone) RETURNS INTEGER AS
$BODY$
SELECT sys.datepart('month', input);
$BODY$
STRICT
LANGUAGE SQL IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.month(input timestamp with time zone) RETURNS INTEGER AS
$BODY$
SELECT sys.datepart('month', input);
$BODY$
STRICT
LANGUAGE SQL IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.month(input time without time zone) RETURNS INTEGER AS
$BODY$
SELECT sys.datepart('month', input);
$BODY$
STRICT
LANGUAGE SQL IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.month(input time with time zone) RETURNS INTEGER AS
$BODY$
SELECT sys.datepart('month', input);
$BODY$
STRICT
LANGUAGE SQL IMMUTABLE;


CREATE OR REPLACE FUNCTION sys.month(input tinyint) RETURNS INTEGER AS
$BODY$
SELECT sys.datepart('month', input::int);
$BODY$
STRICT
LANGUAGE SQL IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.month(input int) RETURNS INTEGER AS
$BODY$
SELECT sys.datepart('month', input);
$BODY$
STRICT
LANGUAGE SQL IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.month(input bigint) RETURNS INTEGER AS
$BODY$
SELECT sys.datepart('month', input::int);
$BODY$
STRICT
LANGUAGE SQL IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.month(input bit) RETURNS INTEGER AS
$BODY$
SELECT sys.datepart('month', input::int);
$BODY$
STRICT
LANGUAGE SQL IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.month(input text) RETURNS INTEGER AS
$BODY$
SELECT sys.datepart('month', input::date);
$BODY$
STRICT
LANGUAGE SQL IMMUTABLE;

-- sys.day
drop function if exists sys.day(timestamptz);;
drop function if exists sys.day(abstime);
drop function if exists sys.day(date);
drop function if exists sys.day(timestamp(0) with time zone);
create function sys.day(timestamptz) RETURNS int LANGUAGE SQL IMMUTABLE STRICT as 'select pg_catalog.date_part(''day'', $1)::int';
create function sys.day(abstime) RETURNS int LANGUAGE SQL IMMUTABLE STRICT as 'select pg_catalog.date_part(''day'', $1)::int';
create function sys.day(date) RETURNS int LANGUAGE SQL IMMUTABLE STRICT as 'select pg_catalog.date_part(''day'', $1)::int';

CREATE OR REPLACE FUNCTION sys.day(input ANYELEMENT) RETURNS INTEGER AS
$BODY$
SELECT sys.datepart('day', input)::int;
$BODY$
STRICT
LANGUAGE SQL IMMUTABLE;


CREATE OR REPLACE FUNCTION sys.day(input bigint) RETURNS INTEGER AS
$BODY$
SELECT sys.datepart('day', input::int)::int;
$BODY$
STRICT
LANGUAGE SQL IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.day(input bit) RETURNS INTEGER AS
$BODY$
SELECT sys.datepart('day', input::int)::int;
$BODY$
STRICT
LANGUAGE SQL IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.day(input text) RETURNS INTEGER AS
$BODY$
SELECT sys.datepart('day', input::date)::int;
$BODY$
STRICT
LANGUAGE SQL IMMUTABLE;


-- sys.isdate
CREATE OR REPLACE FUNCTION sys.isdate(text) RETURNS int AS '$libdir/shark', 'is_date' LANGUAGE C STABLE;	

create or replace function sys.isdate(v int) returns integer as
$body$
begin
    if v is NULL THEN
        return 0;
    else
        perform v::text::date;
        return 1;
    end if;
    EXCEPTION WHEN others THEN
    RETURN 0;
end
$body$
language 'plpgsql' STABLE;

-- sys.EOMONTH
CREATE OR REPLACE FUNCTION sys.eomonth(date, int DEFAULT 0) RETURNS date AS '$libdir/shark', 'eomonth' LANGUAGE C STABLE STRICT;
CREATE OR REPLACE FUNCTION sys.eomonth(timestamp without time zone, int DEFAULT 0) RETURNS date LANGUAGE SQL STABLE STRICT as 'select sys.eomonth($1::date, $2)';
CREATE OR REPLACE FUNCTION sys.eomonth(timestamp with time zone, int DEFAULT 0) RETURNS date LANGUAGE SQL STABLE STRICT as 'select sys.eomonth($1::date, $2)';
CREATE OR REPLACE FUNCTION sys.eomonth(timestamp without time zone, numeric DEFAULT 0) RETURNS date LANGUAGE SQL STABLE STRICT as 'select sys.eomonth($1::date, trunc($2)::int)';
CREATE OR REPLACE FUNCTION sys.eomonth(timestamp with time zone, numeric DEFAULT 0) RETURNS date LANGUAGE SQL STABLE STRICT as 'select sys.eomonth($1::date, trunc($2)::int)';
CREATE OR REPLACE FUNCTION sys.eomonth(text, numeric DEFAULT 0) RETURNS date LANGUAGE SQL STABLE STRICT as 'select sys.eomonth($1::date, trunc($2)::int)';

-- sys.sysdatetime
CREATE OR REPLACE FUNCTION sys.sysdatetime() RETURNS timestamptz AS '$libdir/shark', 'sysdatetime' LANGUAGE C STABLE STRICT;

--sys.square
create or replace function sys.square(in x double precision) returns double precision AS
$BODY$
DECLARE
        res double precision;
BEGIN
        res = pow(x, 2::float);
        return res;
END;
$BODY$
LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION sys.square(in x bit) RETURNS double precision LANGUAGE SQL STABLE as 'select sys.square($1::varchar::double precision)';
CREATE OR REPLACE FUNCTION sys.square(in x text) RETURNS double precision LANGUAGE SQL STABLE as 'select sys.square($1::double precision)';

-- sys.isnumeric
CREATE OR REPLACE FUNCTION sys.isnumeric(IN expr ANYELEMENT) RETURNS INTEGER AS '$libdir/shark', 'is_numeric' LANGUAGE C STABLE STRICT;
CREATE OR REPLACE FUNCTION sys.isnumeric(IN expr TEXT) RETURNS INTEGER AS '$libdir/shark', 'is_numeric' LANGUAGE C STABLE STRICT;

-- sys.sql_variant_property
CREATE OR REPLACE FUNCTION sys.sql_variant_property(sys.SQL_VARIANT, VARCHAR(20)) RETURNS sys.SQL_VARIANT AS '$libdir/shark', 'sql_variant_property' LANGUAGE C STABLE STRICT;
CREATE OR REPLACE FUNCTION sys.sql_variant_property(text, VARCHAR(20)) RETURNS sys.SQL_VARIANT LANGUAGE SQL STABLE as 'select sys.sql_variant_property($1::sys.SQL_VARIANT, $2)';
CREATE OR REPLACE FUNCTION sys.sql_variant_property(time, VARCHAR(20)) RETURNS sys.SQL_VARIANT LANGUAGE SQL STABLE as 'select sys.sql_variant_property($1::sys.SQL_VARIANT, $2)';



-- sys.patindex
create or replace function sys.patindex(in pattern varchar, in expression varchar) returns bigint as
$body$
declare
  v_find_result VARCHAR;
  v_pos bigint;
  v_regexp_pattern VARCHAR;
  start_offset boolean;
  end_offset boolean;
begin
  if expression is null then
    raise exception 'pattern cannot be null.';
  end if;
  
  if pattern is null then
    return null;
  end if;
  
  if pattern = '%' or pattern = '%%' then
    return 1;
  end if;
  if PG_CATALOG.left(pattern, 1) = '%' collate "default" then
    v_regexp_pattern := regexp_replace(pattern, '^%', '%#"', 'i'::pg_catalog.TEXT);
    start_offset := true;
  else
    v_regexp_pattern := '#"' || pattern;
    start_offset := false;
  end if;

  if PG_CATALOG.right(pattern, 1) = '%' collate "default" then
    v_regexp_pattern := regexp_replace(v_regexp_pattern, '%$', '#"%', 'i'::pg_catalog.TEXT);
    end_offset := true;
  else
   v_regexp_pattern := v_regexp_pattern || '#"';
   end_offset := false;
  end if;
  v_find_result := substring(expression, v_regexp_pattern, '#');
  if v_find_result <> '' collate "default" then
    if start_offset and not end_offset then
      v_pos := LENGTH(expression) - STRPOS(REVERSE(expression), REVERSE(v_find_result)) + 2 - LENGTH(v_find_result);
    else
      v_pos := strpos(expression, v_find_result);
    end if;
  else
    v_pos := 0;
  end if;
  return v_pos;
end;
$body$
language plpgsql IMMUTABLE;


create or replace function sys.patindex(in pattern bit, in expression varchar) returns bigint LANGUAGE SQL STABLE as 'select sys.patindex(pattern::varchar, expression)';
create or replace function sys.patindex(in pattern time, in expression varchar) returns bigint LANGUAGE SQL STABLE as 'select sys.patindex(pattern::varchar, expression)';
create or replace function sys.patindex(in pattern varbinary, in expression varchar) returns bigint LANGUAGE SQL STABLE as 'select sys.patindex(pattern::varchar, expression)';
create or replace function sys.patindex(in pattern varchar, in expression bit) returns bigint LANGUAGE SQL STABLE as 'select sys.patindex(pattern, expression::varchar)';
create or replace function sys.patindex(in pattern varchar, in expression time) returns bigint LANGUAGE SQL STABLE as 'select sys.patindex(pattern, expression::varchar)';
create or replace function sys.patindex(in pattern varchar, in expression varbinary) returns bigint LANGUAGE SQL STABLE as 'select sys.patindex(pattern, expression::varchar)';
create or replace function sys.patindex(in pattern text, in expression text) returns bigint LANGUAGE SQL STABLE as 'select sys.patindex(pattern::varchar, expression::varchar)';

-- sys.stuff
CREATE OR REPLACE FUNCTION sys.stuff(expr VARBINARY, start INTEGER, length INTEGER, replace_expr VARCHAR)
RETURNS VARBINARY
AS
$BODY$
BEGIN
    IF start IS NULL OR expr IS NULL OR length IS NULL THEN
        RETURN NULL;
    END IF;
    IF start <= 0 OR start > sys.len(expr) OR length < 0 THEN
        RETURN NULL;
    END IF;
    IF replace_expr IS NULL THEN
        RETURN (SELECT (overlay (expr::VARCHAR placing '' from start for length))::VARCHAR)::VARBINARY;
    END IF;
    RETURN (SELECT (overlay (expr::VARCHAR placing replace_expr::VARCHAR from start for length))::VARCHAR)::VARBINARY;
END;
$BODY$
LANGUAGE plpgsql IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION sys.stuff(expr VARCHAR, start INTEGER, length INTEGER, replace_expr VARCHAR)
RETURNS VARCHAR
AS
$BODY$
BEGIN
    IF start IS NULL OR expr IS NULL OR length IS NULL THEN
        RETURN NULL;
    END IF;
    IF start <= 0 OR start > length(expr) OR length < 0 THEN
        RETURN NULL;
    END IF;
    IF replace_expr IS NULL THEN
        RETURN (SELECT overlay (expr placing '' from start for length));
    END IF;
    RETURN (SELECT overlay (expr placing replace_expr from start for length));
END;
$BODY$
LANGUAGE plpgsql IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION sys.stuff(expr NVARCHAR, start INTEGER, length INTEGER, replace_expr NVARCHAR)
RETURNS NVARCHAR
AS
$BODY$
BEGIN
    IF start IS NULL OR expr IS NULL OR length IS NULL THEN
        RETURN NULL;
    END IF;
    IF start <= 0 OR start > length(expr) OR length < 0 THEN
        RETURN NULL;
    END IF;
    IF replace_expr IS NULL THEN
        RETURN (SELECT overlay (expr placing '' from start for length));
    END IF;
    RETURN (SELECT overlay (expr placing replace_expr from start for length));
END;
$BODY$
LANGUAGE plpgsql IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION sys.stuff(expr TEXT, start INTEGER, length INTEGER, replace_expr TEXT)
RETURNS VARCHAR
AS
$BODY$
BEGIN
    IF start IS NULL OR expr IS NULL OR length IS NULL THEN
        RETURN NULL;
    END IF;
    IF start <= 0 OR start > length(expr) OR length < 0 THEN
        RETURN NULL;
    END IF;
    IF replace_expr IS NULL THEN
        RETURN (SELECT overlay (expr placing '' from start for length));
    END IF;
    RETURN (SELECT overlay (expr placing replace_expr from start for length));
END;
$BODY$
LANGUAGE plpgsql IMMUTABLE STRICT;


create or replace function sys.stuff(expr bit, start INTEGER, length INTEGER, replace_expr varchar) returns varchar LANGUAGE SQL STABLE STRICT as 'select sys.stuff($1::varchar, $2, $3, $4)';
create or replace function sys.stuff(expr varchar, start INTEGER, length INTEGER, replace_expr bit) returns varchar LANGUAGE SQL STABLE STRICT as 'select sys.stuff($1::varchar, $2, $3, $4::varchar)';
create or replace function sys.stuff(expr varchar, start INTEGER, length INTEGER, replace_expr varbinary) returns varchar LANGUAGE SQL STABLE STRICT as 'select sys.stuff($1::varchar, $2, $3, $4::varchar)';


-- sys.str
CREATE OR REPLACE FUNCTION sys.str(IN float_expression NUMERIC, IN length INTEGER DEFAULT 10, IN decimal_point INTEGER DEFAULT 0) RETURNS VARCHAR
AS '$libdir/shark', 'float_str' LANGUAGE C STABLE STRICT;

create or replace function sys.str(IN float_expression bit, IN length INTEGER DEFAULT 10, IN decimal_point INTEGER DEFAULT 0) RETURNS VARCHAR LANGUAGE SQL STABLE STRICT as 'select sys.str($1::varchar::NUMERIC, $2, $3)';

create or replace function sys.str(IN float_expression varchar, IN length INTEGER DEFAULT 10, IN decimal_point INTEGER DEFAULT 0) RETURNS VARCHAR LANGUAGE SQL STABLE STRICT as 'select sys.str($1::NUMERIC, $2, $3)';



-- sys.replicate
CREATE OR REPLACE FUNCTION sys.replicate(string NVARCHAR, i INTEGER) RETURNS NVARCHAR AS
$BODY$
BEGIN
    IF i < 0 THEN
        RETURN NULL;
    END IF;

    RETURN PG_CATALOG.repeat(string, i);
END;
$BODY$
LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION sys.replicate(string VARCHAR, i INTEGER) RETURNS VARCHAR AS
$BODY$
BEGIN
    IF i < 0 THEN
        RETURN NULL;
    END IF;

    RETURN PG_CATALOG.repeat(string, i);
END;
$BODY$
LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION sys.replicate(string TEXT, i INTEGER) RETURNS VARCHAR AS
$BODY$
BEGIN
    IF i < 0 THEN
        RETURN NULL;
    END IF;

    RETURN PG_CATALOG.repeat(string, i);
END;
$BODY$
LANGUAGE plpgsql IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION sys.replicate(string bit, i INTEGER) RETURNS VARCHAR LANGUAGE SQL STABLE as 'select sys.replicate($1::text, $2)';
CREATE OR REPLACE FUNCTION sys.replicate(string TEXT, i bigint) RETURNS VARCHAR LANGUAGE SQL STABLE as 'select sys.replicate($1, $2::int)';
CREATE OR REPLACE FUNCTION sys.replicate(string TEXT, i bit) RETURNS VARCHAR LANGUAGE SQL STABLE as 'select sys.replicate($1, $2::int)';


-- sys.string_split
CREATE OR REPLACE FUNCTION sys.string_split(IN string VARCHAR, IN separator VARCHAR, OUT value VARCHAR) RETURNS SETOF VARCHAR AS
$body$
BEGIN
  if separator is null or length(separator) != 1 then
    RAISE EXCEPTION 'Invalid separator: %', separator USING HINT =
      'Separator must be length 1';
  else
    RETURN QUERY(SELECT cast(unnest(string_to_array(string, separator)) as varchar));
end if;
END
$body$
LANGUAGE plpgsql IMMUTABLE;

-- sys.quotename
CREATE OR REPLACE FUNCTION sys.quotename(IN input_string VARCHAR, IN delimiter char default '[') RETURNS varchar AS '$libdir/shark', 'quotename' LANGUAGE C STABLE STRICT;
CREATE OR REPLACE FUNCTION sys.quotename(IN input_string bit, IN delimiter char default '[') RETURNS varchar LANGUAGE SQL STABLE as 'select sys.quotename($1::varchar, $2)';
CREATE OR REPLACE FUNCTION sys.quotename(IN input_string varbinary, IN delimiter char default '[') RETURNS varchar LANGUAGE SQL STABLE as 'select sys.quotename($1::varchar, $2)';

-- sys.trim
CREATE OR REPLACE FUNCTION pg_catalog.btrim(IN input_string varbinary) RETURNS varchar LANGUAGE SQL STABLE as 'select pg_catalog.btrim($1::varchar::text)';
