create schema sys;
grant usage on schema sys to public;
set search_path = 'sys';

create function sys.pltsql_call_handler()
    returns language_handler as 'MODULE_PATHNAME' language C;

create function sys.pltsql_validator(oid)
    returns void as 'MODULE_PATHNAME' language C;

create function sys.pltsql_inline_handler(internal)
    returns void as 'MODULE_PATHNAME' language C;

create trusted language pltsql
    handler sys.pltsql_call_handler
    inline sys.pltsql_inline_handler
    validator sys.pltsql_validator;

grant usage on language pltsql to public;

CREATE OR REPLACE FUNCTION sys.day (timestamptz) RETURNS float8 LANGUAGE SQL IMMUTABLE STRICT as 'select pg_catalog.date_part(''day'', $1)';
CREATE OR REPLACE FUNCTION sys.day (abstime) RETURNS float8 LANGUAGE SQL IMMUTABLE STRICT as 'select pg_catalog.date_part(''day'', $1)';
CREATE OR REPLACE FUNCTION sys.day (date) RETURNS float8 LANGUAGE SQL IMMUTABLE STRICT as 'select pg_catalog.date_part(''day'', $1)';
CREATE OR REPLACE FUNCTION sys.day (timestamp(0) with time zone) RETURNS float8 LANGUAGE SQL IMMUTABLE STRICT as 'select pg_catalog.date_part(''day'', $1)';

CREATE OR REPLACE FUNCTION sys.rand()
returns double precision       
as 
$$
begin
   return (select random());
end;
$$
language plpgsql;

CREATE OR REPLACE FUNCTION sys.rand(int) returns double precision LANGUAGE C volatile STRICT as  '$libdir/shark', 'rand_seed';
CREATE OR REPLACE FUNCTION sys.rand(smallint) returns double precision LANGUAGE SQL volatile STRICT as 'select rand($1::int)';
CREATE OR REPLACE FUNCTION sys.rand(tinyint) returns double precision LANGUAGE SQL volatile STRICT as 'select rand($1::int)';


-- Return the object ID given the object name. Can specify optional type.
CREATE OR REPLACE FUNCTION sys.object_id(IN object_name VARCHAR, IN object_type VARCHAR DEFAULT '')
RETURNS integer AS '$libdir/shark', 'object_id_internal'
LANGUAGE C STABLE STRICT;

CREATE OR REPLACE FUNCTION objectproperty(
    id INT,
    property VARCHAR
    )
RETURNS INT AS
'$libdir/shark', 'objectproperty_internal'
LANGUAGE C STABLE;

-- sys view: sysobjects
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

create or replace function sys.tsql_type_max_length_helper(in type text, in typelen smallint, in typemod int)
returns smallint
as $$
declare
	max_length smallint;
	precision int;
begin
	max_length := -1;

	if typelen != -1 then
		return typelen;
	end if;

	if typemod != -1 then
    if lower(type) in ('numeric', 'decimal') then
      precision := ((typemod - 4) >> 16) & 65535;
      /* Each four bits (decimal bits) takes up two bytes and then adds an additional overhead of eight bytes to the entire data. */
      max_length := (ceil((precision / 4 + 1) * 2 + 8))::smallint;
      return max_length;
    end if;
		max_length = typemod::smallint;
	end if;

	return max_length;
end;
$$ language plpgsql immutable strict;

create or replace function sys.tsql_type_precision_helper(in type text, in typemod int) returns smallint
as $$
declare
  precision int := -1;
begin
	if type is null then 
		return -1;
	end if;

	if typemod = -1 then
		case lower(type)
		  when 'int1' then precision := 3;
		  when 'int2' then precision := 5;
		  when 'int4' then precision := 10;
		  when 'int8' then precision := 19;
		  when 'bit' then precision := 1;
		  when 'date' then precision := 10;
		  when 'time' then precision := 15;
		  when 'smalldatetime' then precision := 16;
		  when 'timestamp' then precision := 26;
		  when 'real' then precision := 24;
		  when 'float' then precision := 53;
		  when 'money' then precision := 19;
		  else precision := 0;
		  end case;
		return precision;
	end if;

	case lower(type)
	  when 'numeric' then precision := ((typemod - 4) >> 16) & 65535;
	  when 'decimal' then precision := ((typemod - 4) >> 16) & 65535;
	  when 'smalldatetime' then precision := 16;
	  when 'timestamp' then 
	  	case typemod 
	  	  when 0 then precision := 19;
	  	  when 1 then precision := 21;
	  	  when 2 then precision := 22;
	  	  when 3 then precision := 23;
	  	  when 4 then precision := 24;
	  	  when 5 then precision := 25;
	  	  when 6 then precision := 26;
	  	end case;
	  when 'time' then
	  	case typemod
	  	  when 0 then precision := 8;
	  	  when 1 then precision := 10;
	  	  when 2 then precision := 11;
	  	  when 3 then precision := 12;
	  	  when 4 then precision := 13;
	  	  when 5 then precision := 14;
	  	  when 6 then precision := 15;
	  	end case;
	  else precision := 0;
	end case;
	return precision;
end;
$$ language plpgsql immutable strict;

create or replace function sys.tsql_type_scale_helper(in type text, in typemod int) returns int
as $$
begin
	if type is null then 
		return null;
	end if;

  if typemod = -1 then
    return null;
  end if;
	
  if lower(type) in ('numeric', 'decimal') then
    return (typemod - 4) & 65535;
  end if;

  return typemod;
end;
$$ language plpgsql immutable strict;

-- sys view: syscolumns
create or replace view sys.syscolumns as
select 
  cast(a.attname as name) as name,
  cast(c.oid as oid) as id,
  cast(t.oid as oid) as xtype,
  cast(0 as tinyint) as typestat,
  cast(t.oid as oid) as xusertype,
  cast(sys.tsql_type_max_length_helper(t.typname, a.attlen, a.atttypmod) as smallint) as length,
  cast(0 as tinyint) as xprec,
  cast(0 as tinyint) as xscale,
  cast(a.attnum as smallint) as colid,
  cast(0 as smallint) as xoffset,
  cast(0 as tinyint) as bitpos,
  cast(0 as tinyint) as reserved,
  cast(0 as smallint) as colstat,
  cast(d.oid as int) as cdefault,
  cast(coalesce((select oid from pg_constraint where conrelid = c.oid
                 and contype = 'c' and a.attnum = any(conkey) limit 1), 0)
      as int) as domain,
  cast(0 as smallint) as number,
  cast(0 as smallint) as colorder,
  cast(null as bytea) as autoval,
  cast(a.attnum as smallint) as offset,
  cast(case when a.attcollation = 0 then null else a.attcollation end as oid) as collationid,
  cast(case when not a.attnotnull then 8 else 0 end as tinyint) as status,
  cast(t.oid as oid) as type,
  cast(t.oid as oid) as usertype,
  cast(null as varchar(255)) as printfmt,
  cast(sys.tsql_type_precision_helper(t.typname, a.atttypmod) as smallint) as prec,
  cast(sys.tsql_type_scale_helper(t.typname, a.atttypmod) as int) as scale,
  cast(case when d.adgencol = 's' then 1 else 0 end as int) as iscomputed,
  cast(0 as int) as isoutparam,
  cast(a.attnotnull as int) as isnullable,
  cast(coll.collname as name) as collation
from pg_attribute a
inner join pg_class c on c.oid = a.attrelid
inner join pg_type t on t.oid = a.atttypid
inner join pg_namespace sch on c.relnamespace = sch.oid 
left join pg_attrdef d on c.oid = d.adrelid and a.attnum = d.adnum
left join pg_collation coll on coll.oid = a.attcollation
where not a.attisdropped
and a.attnum > 0
and c.relkind in ('r', 'v', 'm', 'f', 'p')
and c.parttype = 'n'
and has_column_privilege(a.attrelid, a.attname, 'select')
union all
select
  cast(pgproc.proname as name) as name,
  cast(pgproc.oid as oid) as id,
  cast(case when pgproc.proallargtypes is null then split_part(pgproc.proargtypes::varchar, ' ', params.ordinal_position)
    else split_part(btrim(pgproc.proallargtypes::text,'{}'), ',', params.ordinal_position) end AS oid) as xtype,
  cast(0 as tinyint) as typestat,
  cast(xtype as oid) as xusertype,
  cast(0 as smallint) as length,
  cast(0 as tinyint) as xprec,
  cast(0 as tinyint) as xscale,
  cast(params.ordinal_position as smallint) as colid,
  cast(0 as smallint) as offset,
  cast(0 as tinyint) as bitpos,
  cast(0 as tinyint) as reserved,
  cast(0 as smallint) as colstat,
  cast(null as int) as cdefault,
  cast(null as int) as domain,
  cast(0 as smallint) as number,
  cast(0 as smallint) as colorder,
  cast(null as bytea) as autoval,
  cast(0 as smallint) as offset,
  cast(case when params.collation_name is null then null else coll.oid end as oid) as collationid,
  cast(case params.parameter_mode when 'OUT' then 64 when 'INOUT' then 64 else 0 end as tinyint) as status,
  cast(case when pgproc.proallargtypes is null then split_part(pgproc.proargtypes::varchar, ' ', params.ordinal_position)
    else split_part(btrim(pgproc.proallargtypes::text,'{}'), ',', params.ordinal_position) end AS oid) as type,
  cast(type as oid) as usertype,
  cast(null as varchar(255)) as printfmt,
  cast(params.numeric_precision as smallint) as prec,
  cast(params.numeric_scale as int) as scale,
  cast(0 as int) as iscomputed,
  cast(case params.parameter_mode when 'OUT' then 1 when 'INOUT' then 1 else 0 end as int) as iscomputed,
  cast(1 as int) as isnullable,
  cast(params.collation_name as name) as collation
from information_schema.routines routine
left join information_schema.parameters params
  on routine.specific_schema = params.specific_schema
  and routine.specific_name = params.specific_name
left join pg_collation coll on coll.collname = params.collation_name
/* routine.specific_name is constructed by concatenating procedure name and oid */
left join pg_proc pgproc on routine.specific_name = concat(pgproc.proname, '_', pgproc.oid)
left join pg_namespace sch on sch.oid = pgproc.pronamespace
where has_function_privilege(pgproc.oid, 'EXECUTE');
grant select on sys.syscolumns to public;

create or replace function sys.tsql_relation_reloptions_helper(in reloptions text[], in targetKey text)
returns text as $$
	select split_part(entry, '=', 2)
  from unnest(reloptions) as entry
  where split_part(entry, '=', 1) = lower(targetKey)
  limit 1;
$$ language sql;

-- sys.sysindexes
create or replace view sys.sysindexes as
select
  cast(i.indrelid as oid) as id,
  cast(0 as int) as status,
  cast(null as bytea) as first,
  cast(i.indexrelid as oid) as indid,
  cast(null as bytea) as root,
  cast(0 as smallint) as minlen,
  cast(0 as smallint) as keycnt,
  cast(0 as smallint) as groupid,
  cast(0 as int) as dpages,
  cast(0 as int) as reserved,
  cast(0 as int) as used,
  cast(0 as bigint) as rowcnt,
  cast(0 as int) as rowmodctr,
  cast(0 as int) as reserved3,
  cast(0 as int) as reserved4,
  cast(0 as int) as xmaxlen,
  cast(0 as int) as maxirow,
  cast(case
		    when sys.tsql_relation_reloptions_helper(c.reloptions, 'fillfactor') is null then '0'
		    else sys.tsql_relation_reloptions_helper(c.reloptions, 'fillfactor')
		    end as int) as OrigFillFactor,
  cast(0 as tinyint) as StatVersion,
  cast(0 as int) as reserved2,
  cast(null as bytea) as FirstIAM,
  cast(0 as smallint) as impid,
  cast(0 as smallint) as lockflags,
  cast(0 as int) as pgmodctr,
  cast(null as bytea) as keys,
  cast(c.relname as name) as name,
  cast(null as bytea) as statblob,
  cast(0 as int) as maxlen,
  cast(0 as int) as rows
from pg_class c
inner join pg_namespace s on s.oid = c.relnamespace
inner join pg_index i on i.indexrelid = c.oid
where c.relkind = 'i' and i.indisenable and i.indisvalid and c.parttype = 'n'
and has_table_privilege(quote_ident(s.nspname) ||'.'||quote_ident(c.relname), 'SELECT');
grant select on sys.sysindexes to public;

-- sys.indexkey
create or replace view sys.sysindexkeys as 
select
  cast(i.indrelid as oid) as id,
  cast(i.indexrelid as oid) as indid,
  cast(i.indkey[idx.pos] as smallint) as colid,
  cast((idx.pos + 1) as smallint) as keyno
from pg_index as i
inner join pg_class c_ind on c_ind.oid = i.indexrelid
inner join pg_class c_tab on c_tab.oid = i.indrelid
inner join pg_namespace s on s.oid = c_ind.relnamespace
join pg_class c on i.indexrelid = c.oid,
lateral (
    select generate_series(0, array_length(i.indkey::int2[], 1) - 1) as pos
) as idx
where has_table_privilege(quote_ident(s.nspname) ||'.'||quote_ident(c_tab.relname), 'SELECT');
grant select on sys.sysindexkeys to public;

create or replace function sys.ts_is_mot_table_helper(in reloid oid)
returns bit as $$
  select case (
	select w.fdwname from pg_foreign_table t 
	left join pg_foreign_server s on t.ftserver = s.oid
	left join pg_foreign_data_wrapper w on s.srvfdw = w.oid
    where t.ftrelid = reloid) 
    when 'mot_fdw' then 1::bit
    else 0::bit end;
$$ language sql;

create or replace function sys.ts_is_publication_helper(in relid oid)
returns bit as $$
	select case 
            when (select 1 from pg_publication_rel where prrelid = relid) = 1 then 1::bit
            else 0::bit end;
$$ language sql;

create or replace function sys.ts_graph_type_helper(in relid oid, in typ text)
returns boolean as $$
begin
	if not (select exists (select 1 from pg_extension where extname = 'age')) then
		return false;
	end if;

	return (select exists(
        select 1 from ag_catalog.ag_label ag inner join pg_class c on c.oid = ag.relation where c.oid = relid and ag.kind = typ
    ));
end
$$ language plpgsql;

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
and t.relkind = 'r'
and has_table_privilege(quote_ident(s.nspname) ||'.'||quote_ident(t.relname), 'SELECT');
end $$
language plpgsql;

reset search_path;
