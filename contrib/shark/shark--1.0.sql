create schema sys;
grant usage on schema sys to public;

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

CREATE FUNCTION dbcc_check_ident_no_reseed(varchar, boolean, boolean) RETURNS varchar as 'MODULE_PATHNAME', 'dbcc_check_ident_no_reseed' LANGUAGE C STRICT STABLE;
CREATE FUNCTION dbcc_check_ident_reseed(varchar, int16, boolean) RETURNS varchar as 'MODULE_PATHNAME', 'dbcc_check_ident_reseed' LANGUAGE C STABLE;
    
create function fetch_status()
    returns int as 'MODULE_PATHNAME' language C;

create function rowcount()
    returns int as 'MODULE_PATHNAME' language C;

create function rowcount_big()
    returns bigint as 'MODULE_PATHNAME' language C;

create function spid()
    returns bigint language sql as $$ select pg_current_sessid() $$;

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

create or replace function sys.ts_numeric_precision_helper(in typname text, in typmod int)
returns smallint as $$
declare
	prec smallint := 0;
begin
    if typmod = -1 then
		return 0;
	end if;

	case lower(typname)
		when 'numeric' then prec := ((typmod - 4) >> 16) & 65535;
		when 'decimal' then prec := ((typmod - 4) >> 16) & 65535;
		else prec := 0;
	end case;

	return prec;
end;
$$ language plpgsql;

create or replace function sys.ts_numeric_scale_helper(in typname text, in typmod int)
returns smallint as $$
declare
	scale smallint := 0;
begin
    if typmod = -1 then
		return 0;
	end if;

	case lower(typname)
		when 'numeric' then scale := (typmod - 4) & 65535;
		when 'decimal' then scale := (typmod - 4) & 65535;
		else scale := 0;
	end case;

	return scale;
end;
$$ language plpgsql;

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

create or replace function sys.ts_index_type_helper(in indexid oid, in reloptions text[])
returns tinyint as $$
declare 
	tab_type text;
	ind_typ tinyint;
begin
	select sys.tsql_relation_reloptions_helper(reloptions, 'orientation') into tab_type;

	case (select amname from pg_am where oid = (select relam from pg_class where oid = indexid))
		when 'hash' then ind_typ := 7;
		else ind_typ := (case tab_type when 'row' then 2 else 6 end);
	end case;
	
	return ind_typ;
end;
$$ language plpgsql;

create or replace view sys.indexes as
select
  ind.indrelid as object_id,
  i.relname as name,
  ind.indexrelid as index_id,
  sys.ts_index_type_helper(ind.indexrelid, t.reloptions) as type,
  cast(case sys.ts_index_type_helper(ind.indexrelid, t.reloptions)
  	when 2 then 'NONCLUSTERED'
  	when 6 then 'NONCLUSTERED COLUMNSTORE'
  	else 'NONCLUSTERED HASH' end as nvarchar(60)) as type_desc,
  cast(case ind.indisunique when 't' then 1 else 0 end as bit) as is_unique,
  t.reltablespace as data_space_id,
  cast(0 as bit) as ignore_dup_key,
  cast(case ind.indisprimary when 't' then 1 else 0 end as bit) as is_primary_key,
  cast(case when const.oid is null then 0 else 1 end as bit) as is_unique_constraint,
  cast(case
       	when sys.tsql_relation_reloptions_helper(i.reloptions, 'fillfactory') is null then '0'
        else sys.tsql_relation_reloptions_helper(i.reloptions, 'fillfactory')
        end as tinyint) as fill_factor,
  cast(0 as bit) as is_padded,
  cast(case ind.indisenable when 't' then 0 else 1 end as bit) as is_disabled,
  cast(0 as bit) as is_hypothetical,
  cast(1 as bit) as allow_row_locks,
  cast(1 as bit) as allow_page_locks,
  cast(case when ind.indpred is null then 0 else 1 end as bit) as has_filter,
  cast(ind.indpred as varchar) as filter_definition,
  cast(0 as int) as compression_delay,
  cast(0 as bit) as suppress_dup_key_messages
from pg_index ind
inner join pg_class i on ind.indexrelid = i.oid
inner join pg_class t on ind.indrelid = t.oid
left join pg_constraint const on const.conindid = i.oid and const.contype = 'u'
where ind.indisvalid;

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

reset search_path;
