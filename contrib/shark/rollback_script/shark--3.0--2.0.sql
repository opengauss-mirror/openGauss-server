SET LOCAL d_format_behavior_compat_options = '';

-- rollback rebuild some views in verion before shark 3.0
drop view if exists sys.sysobjects;
drop view if exists sys.objects;
drop view if exists sys.views;
drop view if exists sys.procedures;
drop view if exists sys.all_objects;
drop view if exists sys.tables;
drop function if exists sys.ts_procedure_object_internal;
drop function if exists sys.ts_tables_obj_internal;

drop view if exists sys.all_columns;
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
and has_column_privilege(quote_ident(s.nspname) ||'.'||quote_ident(c.relname), a.attname, 'SELECT');

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
        else 'U' end as char(2)),
  cast(case s.nspname
        when 'information_schema' then 'SYSTEM_TABLE'
        when 'pg_catalog' then 'SYSTEM_TABLE'
        else 'USER_TABLE' end as nvarchar(60)),
  cast(o.ctime as timestamp), 
  cast(o.mtime as timestamp),
  cast(case s.nspname
        when 'information_schema' then 1
        when 'pg_catalog' then 1
        else 0 end as bit),
  ts_is_publication_helper(t.oid),
  cast(0 as bit)
from pg_class t
inner join pg_namespace s on s.oid = t.relnamespace
inner join pg_object o on o.object_oid = t.oid
where t.relpersistence in ('p', 'u', 't')
and (t.relkind = 'r' or t.relkind = 'f')
and has_table_privilege(quote_ident(s.nspname) ||'.'||quote_ident(t.relname), 'SELECT');
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
inner join pg_object o on o.object_oid = p.oid
where has_function_privilege(p.oid, 'EXECUTE');
end $$
language plpgsql;

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
where ti.out_type = 'U' and ti.out_schema_name not in ('cstore', 'pg_toast');

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
inner join pg_object o on o.object_oid = c.oid
where relkind in ('S', 'L')
and has_table_privilege(quote_ident(s.nspname) ||'.'||quote_ident(c.relname), 'SELECT')
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
inner join pg_object o on o.object_oid = c.oid 
where c.relkind in ('v', 'm')
and has_table_privilege(quote_ident(s.nspname) ||'.'||quote_ident(c.relname), 'SELECT')
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
and has_table_privilege(quote_ident(s.nspname) ||'.'||quote_ident(c.relname), 'SELECT')
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
where has_table_privilege(quote_ident(s.nspname) ||'.'||quote_ident(c.relname), 'SELECT')
union all
select
  cast(null as name) as name,
  ad.oid as object_id,
  cast(null as oid) as principal_id,
  c.relnamespace as schema_id,
  ad.adrelid as parent_object_id,
  cast('D' as char(2)) as type,
  cast('DEFAULT' as nvarchar(2)) as type_desc,
  cast(o.ctime as timestamp) as create_date,
  cast(o.mtime as timestamp) as modify_date,
  cast(0 as bit) as is_ms_shipped,
  cast(0 as bit) as is_published,
  cast(0 as bit) as is_schema_published
from pg_attrdef ad
inner join pg_class c on c.oid = ad.adrelid
inner join pg_namespace s on s.oid = c.relnamespace
inner join pg_object o on o.object_oid = ad.adrelid
where has_table_privilege(quote_ident(s.nspname) ||'.'||quote_ident(c.relname), 'SELECT')
union all
select
  syn.synname as name,
  syn.oid as object_id,
  cast(case s.nspowner when syn.synowner then null else syn.synowner end as oid) as principal_id,
  syn.synnamespace as schema_id,
  cast(null as oid) as parent_object_id,
  cast('SN' as char(2)) as type,
  cast('SYNONYM' as nvarchar(60)) as type_desc,
  cast(null as timestamp) as create_date,
  cast(null as timestamp) as modify_date,
  cast(0 as bit) as is_ms_shipped,
  cast(0 as bit) as is_published,
  cast(0 as bit) as is_schema_published
from pg_synonym syn
inner join pg_namespace s on s.oid = syn.synnamespace;

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
and pi.out_scheam not in ('pg_catalog', 'information_schema');

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
  cast(0 as bit) as is_date_correlation_view
from pg_class t
inner join pg_namespace s on t.relnamespace = s.oid
inner join pg_object o on o.object_oid = t.oid 
where t.relkind in ('v', 'm')
and has_table_privilege(quote_ident(s.nspname) ||'.'||quote_ident(t.relname), 'SELECT')
and s.nspname not in ('information_schema', 'pg_catalog', 'dbe_perf');

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
where relkind in ('S', 'L')
and s.nspname not in ('information_schema', 'pg_catalog')
and has_table_privilege(quote_ident(s.nspname) ||'.'||quote_ident(c.relname), 'SELECT')
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
and has_table_privilege(quote_ident(s.nspname) ||'.'||quote_ident(c.relname), 'SELECT')
and s.nspname not in ('information_schema', 'pg_catalog')
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
where has_table_privilege(quote_ident(s.nspname) ||'.'||quote_ident(c.relname), 'SELECT')
and s.nspname not in ('information_schema', 'pg_catalog')
union all
select
  cast(null as name) as name,
  ad.oid as object_id,
  cast(null as oid) as principal_id,
  c.relnamespace as schema_id,
  ad.adrelid as parent_object_id,
  cast('D' as char(2)) as type,
  cast('DEFAULT' as nvarchar(2)) as type_desc,
  cast(o.ctime as timestamp) as create_date,
  cast(o.mtime as timestamp) as modify_date,
  cast(0 as bit) as is_ms_shipped,
  cast(0 as bit) as is_published,
  cast(0 as bit) as is_schema_published
from pg_attrdef ad
inner join pg_class c on c.oid = ad.adrelid
inner join pg_namespace s on s.oid = c.relnamespace
inner join pg_object o on o.object_oid = ad.adrelid
where has_table_privilege(quote_ident(s.nspname) ||'.'||quote_ident(c.relname), 'SELECT')
and s.nspname not in ('information_schema', 'pg_catalog')
union all
select
  syn.synname as name,
  syn.oid as object_id,
  cast(case s.nspowner when syn.synowner then null else syn.synowner end as oid) as principal_id,
  syn.synnamespace as schema_id,
  cast(null as oid) as parent_object_id,
  cast('SN' as char(2)) as type,
  cast('Synonym' as nvarchar(60)) as type_desc,
  cast(null as timestamp) as create_date,
  cast(null as timestamp) as modify_date,
  cast(0 as bit) as is_ms_shipped,
  cast(0 as bit) as is_published,
  cast(0 as bit) as is_schema_published
from pg_synonym syn
inner join pg_namespace s on s.oid = syn.synnamespace
where s.nspname not in ('information_schema', 'pg_catalog');

create or replace view sys.sysobjects as
select
  cast(t.relname as name) as name,
  cast(t.oid as oid) as id,
  cast(case t.relkind 
  	when 'r' then
      case s.nspname 
        when 'information_schema' then 'S'
        when 'pg_catalog' then 'S'
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
and t.relkind in ('r', 'v', 'm', 'S')
and has_table_privilege(quote_ident(s.nspname) ||'.'||quote_ident(t.relname), 'SELECT')
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
and has_table_privilege(quote_ident(s.nspname) ||'.'||quote_ident(t.relname), 'SELECT')
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
and has_table_privilege(quote_ident(s.nspname) ||'.'||quote_ident(c.relname), 'SELECT')
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
where has_table_privilege(quote_ident(s.nspname) ||'.'||quote_ident(c.relname), 'SELECT,TRIGGER')
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

drop function if exists sys.newid();

drop function if exists sys.get_sequence_start_value;
drop function if exists sys.get_sequence_increment_value;
drop function if exists sys.get_sequence_last_value;

drop view if exists sys.identity_columns;
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
  cast(0 as bit) as is_identity,
  cast(case when d.adgencol = 's' then 1 else 0 end as bit) as is_computed,
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
and has_column_privilege(quote_ident(s.nspname) ||'.'||quote_ident(c.relname), a.attname, 'SELECT')
and s.nspname not in ('information_schema', 'pg_catalog', 'dbe_pldeveloper', 'coverage', 'dbe_perf', 'cstore', 'db4ai');

drop view if exists sys.server_principals;

drop function if exists sys.datepart(cstring, int);
drop function if exists sys.datepart(text, text);

drop view if exists sys.database_principals;
drop view if exists sys.foreign_key_columns;
drop view if exists sys.foreign_keys;
drop view if exists sys.key_constraints;
drop view if exists sys.sysforeignkeys;
drop view if exists sys.check_constraints;
drop view if exists sys.index_columns;
drop view if exists sys.sysprocesses;
drop view if exists sys.synonyms;
drop view if exists sys.system_objects;
drop view if exists sys.all_views;
drop view if exists sys.triggers;
drop view if exists sys.trigger_events;
drop view if exists sys.configurations;
drop view if exists sys.syscurconfigs;
drop view if exists sys.sysconfigures;
drop view if exists sys.sequences;

drop cast if exists (int16 as sys.SQL_VARIANT) cascade;
drop function if exists sys.int16_sqlvariant(int16, int);

-- sys.trim
drop function if exists pg_catalog.btrim(IN input_string varbinary);

-- sys.quotename
drop function if exists sys.quotename(IN input_string VARCHAR, IN delimiter char);
drop function if exists sys.quotename(IN input_string bit, IN delimiter char);
drop function if exists sys.quotename(IN input_string varbinary, IN delimiter char);

-- sys.string_split
drop function if exists sys.string_split(IN string VARCHAR, IN separator VARCHAR, OUT value VARCHAR);

-- sys.replicate
drop function if exists sys.replicate(string bit, i INTEGER);
drop function if exists sys.replicate(string TEXT, i bigint);
drop function if exists sys.replicate(string TEXT, i bit);
drop function if exists sys.replicate(string TEXT, i INTEGER);
drop function if exists sys.replicate(string VARCHAR, i INTEGER);
drop function if exists sys.replicate(string NVARCHAR, i INTEGER);

-- sys.str
drop function if exists sys.str(IN float_expression varchar, IN length INTEGER, IN decimal_point INTEGER);
drop function if exists sys.str(IN float_expression bit, IN length INTEGER, IN decimal_point INTEGER);
drop function if exists sys.str(IN float_expression NUMERIC, IN length INTEGER, IN decimal_point INTEGER);

-- sys.stuff
drop function if exists sys.stuff(expr bit, start INTEGER, length INTEGER, replace_expr varchar);
drop function if exists sys.stuff(expr varchar, start INTEGER, length INTEGER, replace_expr bit);
drop function if exists sys.stuff(expr varchar, start INTEGER, length INTEGER, replace_expr varbinary);
drop function if exists sys.stuff(expr TEXT, start INTEGER, length INTEGER, replace_expr TEXT);
drop function if exists sys.stuff(expr NVARCHAR, start INTEGER, length INTEGER, replace_expr NVARCHAR);
drop function if exists sys.stuff(expr VARCHAR, start INTEGER, length INTEGER, replace_expr VARCHAR);
drop function if exists sys.stuff(expr VARBINARY, start INTEGER, length INTEGER, replace_expr VARCHAR);

-- sys.patindex
drop function if exists sys.patindex(in pattern text, in expression text);
drop function if exists sys.patindex(in pattern varchar, in expression varbinary);
drop function if exists sys.patindex(in pattern varchar, in expression time);
drop function if exists sys.patindex(in pattern varchar, in expression bit);
drop function if exists sys.patindex(in pattern varbinary, in expression varchar);
drop function if exists sys.patindex(in pattern time, in expression varchar);
drop function if exists sys.patindex(in pattern bit, in expression varchar);
drop function if exists sys.patindex(in pattern varchar, in expression varchar);

-- sys.sql_variant_property
drop function if exists sys.sql_variant_property(sys.SQL_VARIANT, VARCHAR(20));
drop function if exists sys.sql_variant_property(text, VARCHAR(20));
drop function if exists sys.sql_variant_property(time, VARCHAR(20));

-- sys.isnumeric
drop function if exists sys.isnumeric(IN expr TEXT);
drop function if exists sys.isnumeric(IN expr ANYELEMENT);

--sys.square
drop function if exists sys.square(in x text);
drop function if exists sys.square(in x bit);
drop function if exists sys.square(in x double precision);

-- sys.sysdatetime
drop function if exists sys.sysdatetime();

-- sys.EOMONTH
drop function if exists sys.eomonth(text, numeric);
drop function if exists sys.eomonth(timestamp with time zone, numeric);
drop function if exists sys.eomonth(timestamp without time zone, numeric);
drop function if exists sys.eomonth(timestamp with time zone, int);
drop function if exists sys.eomonth(timestamp without time zone, int);
drop function if exists sys.eomonth(date,int);


-- sys.isdate
drop function if exists sys.isdate(v int);
drop function if exists sys.isdate(text);

-- sys.day
drop function if exists sys.day(input text);
drop function if exists sys.day(input bit);
drop function if exists sys.day(input bigint);
drop function if exists sys.day(input ANYELEMENT);
drop function if exists sys.day(timestamptz);
drop function if exists sys.day(abstime);
drop function if exists sys.day(date);
drop function if exists sys.day(timestamp(0) with time zone);
CREATE OR REPLACE FUNCTION sys.day (timestamptz) RETURNS float8 LANGUAGE SQL IMMUTABLE STRICT as 'select pg_catalog.date_part(''day'', $1)';
CREATE OR REPLACE FUNCTION sys.day (abstime) RETURNS float8 LANGUAGE SQL IMMUTABLE STRICT as 'select pg_catalog.date_part(''day'', $1)';
CREATE OR REPLACE FUNCTION sys.day (date) RETURNS float8 LANGUAGE SQL IMMUTABLE STRICT as 'select pg_catalog.date_part(''day'', $1)';
CREATE OR REPLACE FUNCTION sys.day (timestamp(0) with time zone) RETURNS float8 LANGUAGE SQL IMMUTABLE STRICT as 'select pg_catalog.date_part(''day'', $1)';

-- sys.month
drop function if exists sys.month(input text);
drop function if exists sys.month(input bit);
drop function if exists sys.month(input bigint);
drop function if exists sys.month(input int);
drop function if exists sys.month(input tinyint);
drop function if exists sys.month(input time with time zone);
drop function if exists sys.month(input time without time zone);
drop function if exists sys.month(input timestamp with time zone);
drop function if exists sys.month(input timestamp without time zone);
drop function if exists sys.month(input date);

-- sys.year
drop function if exists sys.year(input bit);
drop function if exists sys.year(input bigint);
drop function if exists sys.year(input text);
drop function if exists sys.year(input ANYELEMENT);


-- sys.columnproperty
drop function if exists sys.columnproperty(object_id OID, property TEXT, property_name TEXT);

-- sys.col_name
drop function if exists sys.COL_NAME(IN table_id INT, IN column_id Oid);
drop function if exists sys.COL_NAME(IN table_id INT, IN column_id bit);
drop function if exists sys.COL_NAME(IN table_id INT, IN column_id bigint);
drop function if exists sys.COL_NAME(IN table_id Oid, IN column_id INT);
drop function if exists sys.COL_NAME(IN table_id bit, IN column_id INT);
drop function if exists sys.COL_NAME(IN table_id bigint, IN column_id INT);
drop function if exists sys.COL_NAME(IN table_id INT, IN column_id INT);
drop function if exists sys.COL_NAME(IN table_id text, IN column_id text);


-- sys.col_length
drop function if exists sys.col_length(IN object_name text, IN column_name varbinary);
drop function if exists sys.col_length(IN object_name varbinary, IN column_name TEXT);
drop function if exists sys.col_length(IN object_name TEXT, IN column_name TEXT);
drop function if exists sys.translate_pg_type_to_tsql(pgoid oid);
drop function if exists sys.truncate_identifier(IN object_name TEXT);
drop function if exists sys.remove_delimiter_pair(IN name TEXT);
drop function if exists sys.type_max_length_helper_ext(IN type TEXT, IN typelen INT, IN typemod INT, IN for_sys_types boolean, IN used_typmod_array boolean);

-- sys.objectpropertyex
drop function if exists sys.objectpropertyex(id INT, property varbinary);
drop function if exists sys.objectpropertyex(id INT, property bit);
drop function if exists sys.objectpropertyex(IN object_id varbinary, property VARCHAR);
drop function if exists sys.objectpropertyex(IN object_id Oid, property VARCHAR);
drop function if exists sys.objectpropertyex(IN object_id bit, property VARCHAR);
drop function if exists sys.objectpropertyex(IN object_id bigint, property VARCHAR);
drop function if exists sys.objectpropertyex(id INT, property VARCHAR);

-- sys.object_definition
drop function if exists sys.object_definition(IN object_id Oid);
drop function if exists sys.object_definition(IN object_id bit);
drop function if exists sys.object_definition(IN object_id bigint);
drop function if exists sys.object_definition(IN object_id int);


-- sys.object_schema_name
drop function if exists sys.object_schema_name(IN object_id Oid, IN database_id Oid);
drop function if exists sys.object_schema_name(IN object_id bit, IN database_id Oid);
drop function if exists sys.object_schema_name(IN object_id bigint, IN database_id Oid);
drop function if exists sys.object_schema_name(IN object_id int, IN database_id Oid);
drop function if exists sys.object_schema_name(IN object_id Oid, IN database_id int);
drop function if exists sys.object_schema_name(IN object_id bit, IN database_id int);
drop function if exists sys.object_schema_name(IN object_id bigint, IN database_id int);
drop function if exists sys.object_schema_name(IN object_id INT, IN database_id INT);

-- sys.object_name
drop function if exists sys.object_name(IN object_id Oid, IN database_id Oid);
drop function if exists sys.object_name(IN object_id bit, IN database_id Oid);
drop function if exists sys.object_name(IN object_id bigint, IN database_id Oid);
drop function if exists sys.object_name(IN object_id int, IN database_id Oid);
drop function if exists sys.object_name(IN object_id Oid, IN database_id int);
drop function if exists sys.object_name(IN object_id bit, IN database_id int);
drop function if exists sys.object_name(IN object_id bigint, IN database_id int);
drop function if exists sys.object_name(IN object_id int, IN database_id int);
