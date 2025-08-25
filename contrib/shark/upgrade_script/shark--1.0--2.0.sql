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
CREATE OR REPLACE FUNCTION sys.object_id(IN object_name VARCHAR)
RETURNS integer AS '$libdir/shark', 'object_id_internal'
LANGUAGE C STABLE STRICT;

CREATE OR REPLACE FUNCTION sys.object_id(IN object_name VARCHAR, IN object_type VARCHAR)
RETURNS integer AS '$libdir/shark', 'object_id_internal'
LANGUAGE C STABLE STRICT;

CREATE OR REPLACE FUNCTION sys.objectproperty(
    id INT,
    property VARCHAR
    )
RETURNS INT AS
'$libdir/shark', 'objectproperty_internal'
LANGUAGE C STABLE;

CREATE FUNCTION sys.dbcc_check_ident_no_reseed(varchar, boolean, boolean) RETURNS varchar as 'MODULE_PATHNAME', 'dbcc_check_ident_no_reseed' LANGUAGE C STRICT STABLE;
CREATE FUNCTION sys.dbcc_check_ident_reseed(varchar, int16, boolean) RETURNS varchar as 'MODULE_PATHNAME', 'dbcc_check_ident_reseed' LANGUAGE C STABLE;
    
create function sys.fetch_status()
    returns int as 'MODULE_PATHNAME' language C;

create function sys.rowcount()
    returns int as 'MODULE_PATHNAME' language C;

create function sys.rowcount_big()
    returns bigint as 'MODULE_PATHNAME' language C;

create function sys.spid()
    returns bigint language sql as $$ select pg_current_sessid() $$;

create function sys.procid()
    returns bigint as 'MODULE_PATHNAME' language C;

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
and (t.relkind = 'r' or t.relkind = 'f')
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

CREATE SCHEMA information_schema_tsql;
GRANT USAGE ON SCHEMA information_schema_tsql TO PUBLIC;

CREATE OR REPLACE VIEW information_schema_tsql.check_constraints AS
SELECT 
	  cast(current_database() as nvarchar(128)) AS constraint_catalog,
    cast(n.nspname as nvarchar(128)) AS constraint_schema,  
    cast(c.conname as name) AS constraint_name,
    cast(pg_get_constraintdef(c.oid) as nvarchar(4000)) AS check_clause
FROM 
    pg_constraint c
    JOIN pg_class t ON c.conrelid = t.oid
    JOIN pg_namespace n ON t.relnamespace = n.oid
WHERE 
    c.contype = 'c';  -- 筛选 CHECK 约束

CREATE FUNCTION information_schema_tsql._pg_char_max_length(type text, typmod int4) RETURNS integer
    LANGUAGE sql
	IMMUTABLE
	RETURNS NULL ON NULL INPUT
	AS
$$SELECT
	CASE WHEN type IN ('char', 'nchar', 'varchar', 'nvarchar')
		THEN CASE WHEN typmod = -1
			THEN -1
			ELSE typmod - 4
			END
		WHEN type IN ('text')
		THEN 2147483647
		WHEN type = 'name'
		THEN 64
		WHEN type IN ('xml', 'vector', 'sparsevec')
		THEN -1
		ELSE null
	END$$;

CREATE FUNCTION information_schema_tsql._pg_char_octet_length(type text, typmod int4) RETURNS integer
	LANGUAGE sql
	IMMUTABLE
	RETURNS NULL ON NULL INPUT
	AS
$$SELECT
	CASE WHEN type IN ('char', 'varchar')
		THEN CASE WHEN typmod = -1 /* default typmod */
			THEN -1
			ELSE typmod - 4
			END
		WHEN type IN ('nchar', 'nvarchar')
		THEN CASE WHEN typmod = -1 /* default typmod */
			THEN -1
			ELSE (typmod - 4) * 2
			END
		WHEN type IN ('text')
		THEN 2147483647 /* 2^30 + 1 */
		WHEN type = 'name'
		THEN 128
		WHEN type IN ('xml', 'vector', 'sparsevec')
		THEN -1
	   ELSE null
  END$$;

CREATE OR REPLACE FUNCTION information_schema_tsql._pgtsql_numeric_precision(type text, typid oid, typmod int4) RETURNS integer
	LANGUAGE sql
	IMMUTABLE
	RETURNS NULL ON NULL INPUT
	AS
$$
	SELECT
	CASE typid
		WHEN 21 /*int2*/ THEN 5
		WHEN 23 /*int4*/ THEN 10
		WHEN 20 /*int8*/ THEN 19
		WHEN 1700 /*numeric*/ THEN
			CASE WHEN typmod = -1 THEN null
				ELSE ((typmod - 4) >> 16) & 65535
			END
		WHEN 700 /*float4*/ THEN 24
		WHEN 701 /*float8*/ THEN 53
		ELSE
			CASE WHEN type = 'tinyint' THEN 3
				WHEN type = 'money' THEN 19
				WHEN type = 'decimal'	THEN
					CASE WHEN typmod = -1 THEN null
						ELSE ((typmod - 4) >> 16) & 65535
					END
				ELSE null
			END
	END
$$;

CREATE OR REPLACE FUNCTION information_schema_tsql._pgtsql_numeric_precision_radix(type text, typid oid, typmod int4) RETURNS integer
	LANGUAGE sql
	IMMUTABLE
	RETURNS NULL ON NULL INPUT
	AS
$$SELECT
	CASE WHEN typid IN (700, 701) THEN 2
		WHEN typid IN (20, 21, 23, 1700) THEN 10
		WHEN type IN ('tinyint', 'money') THEN 10
		ELSE null
	END$$;

CREATE OR REPLACE FUNCTION information_schema_tsql._pgtsql_numeric_scale(type text, typid oid, typmod int4) RETURNS integer
	LANGUAGE sql
	IMMUTABLE
	RETURNS NULL ON NULL INPUT
	AS
$$
	SELECT
	CASE WHEN typid IN (21, 23, 20) THEN 0
		WHEN typid IN (1700) THEN
			CASE WHEN typmod = -1 THEN null
				ELSE (typmod - 4) & 65535
			END
		WHEN type = 'tinyint' THEN 0
		WHEN type IN ('money') THEN 4
		WHEN type = 'decimal' THEN
			CASE WHEN typmod = -1 THEN NULL
				ELSE (typmod - 4) & 65535
			END
		ELSE null
	END
$$;

CREATE OR REPLACE FUNCTION information_schema_tsql._pgtsql_datetime_precision(type text, typmod int4) RETURNS integer
	LANGUAGE sql
	IMMUTABLE
	RETURNS NULL ON NULL INPUT
	AS
$$SELECT
  CASE WHEN type = 'date'
		   THEN 0
	  WHEN type IN ('time', 'smalldatetime')
			THEN CASE WHEN typmod < 0 THEN 6 ELSE typmod END
	  ELSE null
  END
$$;

CREATE OR REPLACE FUNCTION information_schema_tsql.is_d_format_schema(nspoid oid, nspname name) RETURNS boolean
  LANGUAGE sql
  IMMUTABLE
  RETURNS NULL ON NULL INPUT
	AS
$$SELECT
  CASE WHEN nspname <> 'pg_catalog' AND nspname <> 'sys' AND nspname <> 'dbe_pldeveloper' AND nspname <> 'coverage' 
    AND nspname <> 'dbe_perf' AND nspname <> 'information_schema' AND nspname <> 'db4ai' AND nspname <> 'public' AND nspname <> 'information_schema_tsql'
	  AND (NOT pg_catalog.pg_is_other_temp_schema(nspoid))
    THEN true
    ELSE false
  END
$$;

CREATE OR REPLACE VIEW information_schema_tsql.columns AS
SELECT 
    CAST(current_database() AS nvarchar(128)) AS table_catalog,
    CAST(nc.nspname AS nvarchar(128)) AS table_schema,
    CAST(c.relname AS nvarchar(128)) AS table_name,
    CAST(a.attname AS nvarchar(128)) AS column_name,
	CAST(
        CASE WHEN t.typtype = 'd' THEN t.typbasetype ELSE a.atttypid END 
        AS int
    ) AS ordinal_position,
	CAST(pg_get_expr(ad.adbin, ad.adrelid) AS nvarchar(4000)) AS column_default,
	CAST(
        CASE 
            WHEN a.attnotnull THEN 'NO'
            ELSE 'YES' 
        END AS information_schema.yes_or_no
    ) AS is_nullable,
      CAST(pg_catalog.format_type(a.atttypid, a.atttypmod) AS varchar(128)) AS data_type,
	CAST(
		 information_schema_tsql._pg_char_max_length(t.typname, a.atttypmod)
		 AS int)
		 AS character_maximum_length,
	CAST(
		 information_schema_tsql._pg_char_octet_length(t.typname, a.atttypmod)
		 AS int)
		 AS character_octet_length,
	CAST(information_schema_tsql._pgtsql_numeric_precision(t.typname, a.atttypid, a.atttypmod) AS tinyint) AS numeric_precision,
	CAST(information_schema_tsql._pgtsql_numeric_precision_radix(t.typname, a.atttypid, a.atttypmod) AS smallint) AS numeric_precision_radix,
	CAST(information_schema_tsql._pgtsql_numeric_scale(t.typname, a.atttypid, a.atttypmod) AS int) AS numeric_scale,
	CAST(information_schema_tsql._pgtsql_datetime_precision(t.typname, a.atttypmod) AS smallint) AS datetime_precision,
  CAST(null AS nvarchar(128)) AS character_set_catalog,
  CAST(null AS nvarchar(128)) AS character_set_schema,
	CAST(pg_encoding_to_char(co.collencoding) AS nvarchar(128)) AS character_set_name,
	CAST(null as nvarchar(128)) as collation_catalog,
	CAST(null as nvarchar(128)) as collation_schema,
	CAST(co.collname AS nvarchar(128)) AS collation_name,
	CAST(CASE WHEN t.typtype = 'd' AND nc.nspname <> 'pg_catalog' AND nc.nspname <> 'sys' THEN pg_catalog.current_database() ELSE null END
		AS nvarchar(128)) AS domain_catalog,
	CAST(CASE WHEN t.typtype = 'd' AND nc.nspname <> 'pg_catalog' AND nc.nspname <> 'sys' THEN nc.nspname ELSE null END
		AS nvarchar(128)) AS domain_schema,
	CAST(CASE WHEN t.typtype = 'd' AND nc.nspname <> 'pg_catalog' AND nc.nspname <> 'sys' THEN t.typname ELSE null END
		AS nvarchar(128)) AS domain_name
FROM 
    pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace nc ON c.relnamespace = nc.oid
    JOIN pg_catalog.pg_attribute a ON c.oid = a.attrelid
    LEFT JOIN pg_catalog.pg_attrdef ad ON (a.attrelid, a.attnum) = (ad.adrelid, ad.adnum)
    LEFT JOIN pg_catalog.pg_type t ON a.atttypid = t.oid
    LEFT JOIN pg_catalog.pg_collation co ON t.typcollation = co.oid
WHERE information_schema_tsql.is_d_format_schema(nc.oid, nc.nspname)
    AND c.relkind IN ('r', 'v', 'm', 'f')  -- 表/视图/物化视图/外表
    AND a.attnum > 0 
    AND NOT a.attisdropped
    AND (pg_has_role(c.relowner, 'USAGE')
			OR has_column_privilege(c.oid, a.attnum,
			'SELECT, INSERT, UPDATE, REFERENCES'));

CREATE OR REPLACE VIEW information_schema_tsql.tables AS
SELECT CAST(pg_catalog.current_database() AS nvarchar(128)) AS table_catalog,
           CAST(nc.nspname AS nvarchar(128)) AS table_schema,
           CAST(c.relname AS name) AS table_name,

           CAST(
             CASE WHEN nc.oid = pg_catalog.pg_my_temp_schema() THEN 'LOCAL TEMPORARY'
                  WHEN c.relkind = 'r' THEN 'BASE TABLE'
                  WHEN c.relkind = 'v' THEN 'VIEW'
                  ELSE null END
             AS varchar(10)) AS table_type
FROM pg_namespace nc JOIN pg_class c ON (nc.oid = c.relnamespace)
	   LEFT JOIN (pg_type t JOIN pg_namespace nt ON (t.typnamespace = nt.oid)) ON (c.reloftype = t.oid)
WHERE c.relkind IN ('r', 'm', 'v', 'f')
    AND information_schema_tsql.is_d_format_schema(nc.oid, nc.nspname)
	  AND (pg_catalog.pg_has_role(c.relowner, 'USAGE')
		   OR pg_catalog.has_table_privilege(c.oid, 'SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER')
		   OR pg_catalog.has_any_column_privilege(c.oid, 'SELECT, INSERT, UPDATE, REFERENCES') );

CREATE OR REPLACE VIEW information_schema_tsql.views AS
SELECT CAST(pg_catalog.current_database() AS nvarchar(128)) AS table_catalog,
           CAST(nc.nspname AS nvarchar(128)) AS table_schema,
           CAST(c.relname AS nvarchar(128)) AS table_name,

           CAST(
             CASE WHEN pg_catalog.pg_has_role(c.relowner, 'USAGE')
                  THEN pg_catalog.pg_get_viewdef(c.oid)
                  ELSE null END
             AS nvarchar(4000)) AS view_definition,

           CAST(
             CASE WHEN 'check_option=cascaded' = ANY (c.reloptions)
                  THEN 'CASCADED'
                  ELSE 'NONE' END
             AS varchar(7)) AS check_option,
           CAST(CASE WHEN pg_relation_is_updatable(c.oid, false) & 4 = 4
                  THEN 'YES' ELSE 'NO' END
                AS varchar(3)) AS is_updatable
    FROM pg_namespace nc, pg_class c
    WHERE c.relnamespace = nc.oid
          AND c.relkind = 'v'
          AND information_schema_tsql.is_d_format_schema(nc.oid, nc.nspname)
          AND (pg_catalog.pg_has_role(c.relowner, 'USAGE')
               OR pg_catalog.has_table_privilege(c.oid, 'SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER')
               OR pg_catalog.has_any_column_privilege(c.oid, 'SELECT, INSERT, UPDATE, REFERENCES') );

CREATE OR REPLACE VIEW sys.sysdatabases AS
SELECT
    CAST(d.datname as name) AS name,         -- 数据库名称
    CAST(d.oid as oid) AS dbid,             -- 数据库唯一标识符（OID）
    CAST(d.datdba as oid) AS sid,           -- 数据库所有者用户 OID
    CAST(0 as smallint) as mode,
	  CAST(0 as integer) as status,
	  CAST(0 as integer) as status2,
	  CAST('1900-01-01 00:00:00.000' as timestamp) as crdate,
	  CAST('1900-01-01 00:00:00.000' as timestamp) as reserved,
    CAST(
		  CASE 
		  WHEN EXISTS (SELECT 1 FROM pg_subscription WHERE subdbid = d.oid) 
		  THEN 2 ELSE 0 
	    END as integer)
      AS category,
	  CAST(0 as tinyint) as cmplevel,
	  CAST(NULL as nvarchar(260)) as filename,
	  CAST(NULL as smallint) as version
FROM pg_database d;

CREATE OR REPLACE VIEW sys.schemas AS
SELECT 
	CAST(n.nspname as name) AS name,
	CAST(n.oid as integer) AS schema_id,
	CAST(n.nspowner AS integer) AS principal_id
    FROM pg_namespace n
    WHERE pg_catalog.pg_has_role(n.nspowner, 'USAGE');

-- 模拟 SQL Server 的 sys.sysusers
CREATE OR REPLACE VIEW sys.sysusers AS
SELECT 
	CAST(r.oid AS integer) AS uid,           -- 用户/角色唯一标识符
	CAST(0 AS smallint) AS status,
	CAST(r.rolname AS name) AS name,         -- 用户/角色名称
	CAST(NULL AS bytea) AS sid,
	CAST(NULL AS bytea) AS roles,
	CAST(NULL AS date) AS createdate,
	CAST(NULL AS date) AS updatedate,
	CAST(0 AS smallint) AS altuid,
	CAST(NULL AS bytea) AS password,
	CAST(0 AS smallint) AS gid,
	CAST(NULL AS varchar(255)) AS environ,
	CAST(
		CASE WHEN has_database_privilege(name, current_database(), 'CONNECT')
		THEN 1
		ELSE 0
		END AS integer)
	AS hasdbaccess,
	CAST(r.rolcanlogin AS integer) AS islogin,
	CAST(0 AS integer) AS isntname,
	CAST(0 AS integer) AS isntgroup,
	CAST(0 AS integer) AS isntuser,
	CAST(r.rolcanlogin AS integer) AS issqluser,
	CAST(0 AS integer) AS isaliased,
	CAST(NOT r.rolcanlogin AS integer) AS issqlrole,
	CAST(0 AS integer) AS isapprole
FROM pg_roles r
WHERE pg_has_role(r.rolname, 'USAGE'); -- 仅显示当前用户有权查看的角色

-- 模拟 SQL Server 的 sys.databases
CREATE OR REPLACE VIEW sys.databases AS
SELECT 
	CAST(d.datname AS VARCHAR(128)) AS name,
	CAST(d.oid AS OID) AS database_id,
	CAST(NULL AS INTEGER) AS source_database_id,
	CAST(
	CASE WHEN d.datdba > 0 THEN d.datdba ELSE NULL END
	AS OID) AS owner_sid,
	CAST(NULL AS TIMESTAMP) AS create_date,
	CAST(NULL AS TINYINT) AS compatibility_level,
	CAST(pg_catalog.getdatabaseencoding() AS NAME) AS collation_name,
	CAST(0 AS TINYINT) AS user_access,
	CAST('MULTI_USER' AS NVARCHAR(60)) AS user_access_desc,
	CAST(0 AS BIT) AS is_read_onliy,
	CAST(0 AS BIT) AS is_auto_close_on,
	CAST(0 AS BIT) AS is_auto_shrink_on,
	CAST(0 AS TINYINT) AS state,
	CAST('ONLINE' AS NVARCHAR(60)) AS state_desc,
	CAST(
	CASE WHEN pg_is_in_recovery() THEN 1
	ELSE 0 END
	AS BIT) AS is_in_standby,
	CAST(0 AS BIT) AS is_cleanly_shutdown,
	CAST(0 AS BIT) AS is_supplemental_logging_enabled,
	CAST(1 AS TINYINT) AS snapshot_isolation_state,
	CAST('ON' AS NVARCHAR(60)) AS snapshot_isolation_state_desc,
	CAST(
	CASE WHEN current_setting('default_transaction_isolation') = 'read committed'
	THEN 1
	ELSE 0
	END AS BIT) AS is_read_committed_snapshot_on,
	CAST(1 AS TINYINT) AS recovery_model,
	CAST('FULL' AS NVARCHAR(60)) AS recovery_model_desc,
	CAST(0 AS TINYINT) AS page_verify_option,
	CAST(NULL AS NVARCHAR(60)) AS page_verify_option_desc,
	CAST(1 AS BIT) AS is_auto_create_stats_on,
	CAST(0 AS BIT) AS is_auto_create_stats_incremental_on,
	CAST(0 AS BIT) AS is_auto_update_stats_on,
	CAST(0 AS BIT) AS is_auto_update_stats_async_on,
	CAST(1 AS BIT) AS is_ansi_null_default_on,
	CAST(1 AS BIT) AS is_ansi_nulls_on,
	CAST(0 AS BIT) AS is_ansi_padding_on,
	CAST(0 AS BIT) AS is_ansi_warnings_on,
	CAST(1 AS BIT) AS is_arithabort_on,
	CAST(1 AS BIT) AS is_concat_null_yields_null_on,
	CAST(0 AS BIT) AS is_numeric_roundabort_on,
	CAST(1 AS BIT) AS is_quoted_identifier_on,
	CAST(0 AS BIT) AS is_recursive_triggers_on,
	CAST(0 AS BIT) AS is_cursor_close_on_commit_on,
	CAST(0 AS BIT) AS is_local_cursor_default,
	CAST(0 AS BIT) AS is_fulltext_enabled,
	CAST(0 AS BIT) AS is_trustworthy_on,
	CAST(0 AS BIT) AS is_db_chaining_on,
	CAST(0 AS BIT) AS is_parameterization_forced,
	CAST(0 AS BIT) AS is_master_key_encrypted_by_server,
	CAST(0 AS BIT) AS is_query_store_on,
	CAST(0 AS BIT) AS is_published,
	CAST(0 AS BIT) AS is_subscribed,
	CAST(0 AS BIT) AS is_merge_published,
	CAST(0 AS BIT) AS is_distributor,
	CAST(0 AS BIT) AS is_sync_with_backup,
	CAST(NULL AS OID) AS service_broker_guid,
	CAST(0 AS BIT) AS is_broker_enabled,
	CAST(0 AS TINYINT) AS log_reuse_wait,
	CAST('NOTHING' AS NVARCHAR(60)) as log_reuse_wait_desc,
	CAST(0 AS BIT) AS is_date_correlation_on,
	CAST(0 AS BIT) AS is_cdc_enabled,
	CAST(0 AS BIT) AS is_encrypted,
	CAST(0 AS BIT) AS is_honor_broker_priority_on,
	CAST(NULL AS OID) AS replica_id,
	CAST(NULL AS OID) AS group_database_id,
	CAST(NULL AS INTEGER) AS resource_pool_id,
	CAST(NULL AS SMALLINT) AS default_language_lcid,
	CAST(NULL AS VARCHAR(128)) AS default_language_name,
	CAST(NULL AS INTEGER) AS default_fulltext_language_lcid,
	CAST(NULL AS VARCHAR(128)) AS default_fulltext_language_name,
	CAST(NULL AS BIT) AS is_nested_triggers_on,
	CAST(NULL AS BIT) AS is_transform_noise_words_on,
	CAST(NULL AS SMALLINT) AS two_digit_year_cutoff,
	CAST(0 AS TINYINT) AS containment,
	CAST('NONE' AS VARCHAR(60)) AS containment_desc,
	CAST(0 AS INTEGER) AS target_recovery_time_in_seconds,
	CAST(0 AS INTEGER) AS delayed_durability,
	CAST(NULL AS VARCHAR(60)) AS delayed_durability_desc,
	CAST(0 AS BIT) AS is_memory_optimized_elevate_to_snapshot_on,
	CAST(0 AS BIT) AS is_federation_member,
	CAST(0 AS BIT) AS is_remote_data_archive_enabled,
	CAST(0 AS BIT) AS is_mixed_page_allocation_on,
	CAST(0 AS BIT) AS is_temporal_history_retention_enabled,
	CAST(0 AS BIT) AS catalog_collation_type,
	CAST('Not Applicable' AS NVARCHAR(60)) as catalog_collation_type_desc,
	CAST(NULL AS NVARCHAR(128)) as physical_database_name,
	CAST(0 AS BIT) as is_result_set_caching_on,
	CAST(0 AS BIT) as is_accelerated_database_recovery_on,
	CAST(0 AS BIT) as is_tempdb_spill_to_remote_store,
	CAST(0 AS BIT) as is_stale_page_detection_on,
	CAST(0 AS BIT) as is_memory_optimized_enabled,
	CAST(0 AS BIT) as is_ledger_on,
	CAST(0 AS BIT) as is_change_feed_enabled,
	CAST(0 AS BIT) as is_vorder_enable
FROM pg_database d, pg_settings s
WHERE
  s.name = 'wal_level';

-- varbinary.sql
-- VARBINARY
CREATE TYPE sys.VARBINARY;

CREATE FUNCTION sys.varbinaryin(cstring, oid, integer)
RETURNS sys.VARBINARY
AS '$libdir/shark', 'varbinaryin'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION sys.varbinaryout(sys.VARBINARY)
RETURNS cstring
AS '$libdir/shark', 'varbinaryout'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION sys.varbinaryrecv(internal, oid, integer)
RETURNS sys.VARBINARY
AS '$libdir/shark', 'varbinaryrecv'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION sys.varbinarysend(sys.VARBINARY)
RETURNS bytea
AS '$libdir/shark', 'varbinarysend'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION sys.varbinarytypmodin(cstring[])
RETURNS integer
AS '$libdir/shark', 'varbinarytypmodin'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION sys.varbinarytypmodout(integer)
RETURNS cstring
AS '$libdir/shark', 'varbinarytypmodout'
LANGUAGE C IMMUTABLE STRICT;

CREATE TYPE sys.VARBINARY (
    INPUT          = sys.varbinaryin,
    OUTPUT         = sys.varbinaryout,
    RECEIVE        = sys.varbinaryrecv,
    SEND           = sys.varbinarysend,
    TYPMOD_IN      = sys.varbinarytypmodin,
    TYPMOD_OUT     = sys.varbinarytypmodout,
    INTERNALLENGTH = VARIABLE,
    ALIGNMENT      = 'int4',
    STORAGE        = 'extended',
    CATEGORY       = 'U',
    PREFERRED      = false,
    COLLATABLE     = false
);

CREATE OR REPLACE FUNCTION sys.varbinary(sys.VARBINARY, integer, boolean)
RETURNS sys.VARBINARY
AS '$libdir/shark', 'varbinary'
LANGUAGE C IMMUTABLE STRICT;

-- typmod cast for sys.VARBINARY
CREATE CAST (sys.VARBINARY AS sys.VARBINARY)
WITH FUNCTION sys.varbinary(sys.VARBINARY, integer, BOOLEAN) AS ASSIGNMENT;

CREATE OR REPLACE FUNCTION sys.byteavarbinary(pg_catalog.BYTEA, integer, boolean)
RETURNS sys.VARBINARY
AS '$libdir/shark', 'byteavarbinary'
LANGUAGE C IMMUTABLE STRICT;

CREATE CAST (pg_catalog.BYTEA AS sys.VARBINARY)
WITH FUNCTION sys.byteavarbinary(pg_catalog.BYTEA, integer, boolean) AS ASSIGNMENT;

CREATE OR REPLACE FUNCTION sys.varbinarybytea(sys.VARBINARY, integer, boolean)
RETURNS pg_catalog.BYTEA
AS '$libdir/shark', 'byteavarbinary'
LANGUAGE C IMMUTABLE STRICT;

CREATE CAST (sys.VARBINARY AS pg_catalog.BYTEA)
WITH FUNCTION sys.varbinarybytea(sys.VARBINARY, integer, boolean) AS ASSIGNMENT;


CREATE OR REPLACE FUNCTION sys.varcharvarbinary(pg_catalog.VARCHAR, integer, boolean)
RETURNS sys.VARBINARY
AS '$libdir/shark', 'varcharvarbinary'
LANGUAGE C IMMUTABLE STRICT;

CREATE CAST (pg_catalog.VARCHAR AS sys.VARBINARY)
WITH FUNCTION sys.varcharvarbinary (pg_catalog.VARCHAR, integer, boolean);

CREATE OR REPLACE FUNCTION sys.bpcharvarbinary(pg_catalog.BPCHAR, integer, boolean)
RETURNS sys.VARBINARY
AS '$libdir/shark', 'bpcharvarbinary'
LANGUAGE C IMMUTABLE STRICT;

CREATE CAST (pg_catalog.BPCHAR AS sys.VARBINARY)
WITH FUNCTION sys.bpcharvarbinary (pg_catalog.BPCHAR, integer, boolean);

CREATE OR REPLACE FUNCTION sys.varbinarybpchar(sys.VARBINARY, integer, boolean)
RETURNS pg_catalog.BPCHAR
AS '$libdir/shark', 'varbinarybpchar'
LANGUAGE C IMMUTABLE STRICT;

CREATE CAST (sys.VARBINARY AS pg_catalog.BPCHAR)
WITH FUNCTION sys.varbinarybpchar (sys.VARBINARY, integer, boolean) AS ASSIGNMENT;

CREATE OR REPLACE FUNCTION sys.varbinaryvarchar(sys.VARBINARY, integer, boolean)
RETURNS pg_catalog.VARCHAR
AS '$libdir/shark', 'varbinaryvarchar'
LANGUAGE C IMMUTABLE STRICT;

CREATE CAST (sys.VARBINARY AS pg_catalog.VARCHAR)
WITH FUNCTION sys.varbinaryvarchar (sys.VARBINARY, integer, boolean) AS ASSIGNMENT;

CREATE OR REPLACE FUNCTION sys.int2varbinary(INT2, integer, boolean)
RETURNS sys.VARBINARY
AS '$libdir/shark', 'int2varbinary'
LANGUAGE C IMMUTABLE STRICT;

CREATE CAST (INT2 AS sys.VARBINARY)
WITH FUNCTION sys.int2varbinary (INT2, integer, boolean) AS ASSIGNMENT;

CREATE OR REPLACE FUNCTION sys.int4varbinary(INT4, integer, boolean)
RETURNS sys.VARBINARY
AS '$libdir/shark', 'int4varbinary'
LANGUAGE C IMMUTABLE STRICT;

CREATE CAST (INT4 AS sys.VARBINARY)
WITH FUNCTION sys.int4varbinary (INT4, integer, boolean) AS ASSIGNMENT;

CREATE OR REPLACE FUNCTION sys.int8varbinary(INT8, integer, boolean)
RETURNS sys.VARBINARY
AS '$libdir/shark', 'int8varbinary'
LANGUAGE C IMMUTABLE STRICT;

CREATE CAST (INT8 AS sys.VARBINARY)
WITH FUNCTION sys.int8varbinary (INT8, integer, boolean) AS ASSIGNMENT;

CREATE OR REPLACE FUNCTION sys.float4varbinary(REAL, integer, boolean)
RETURNS sys.VARBINARY
AS '$libdir/shark', 'float4varbinary'
LANGUAGE C IMMUTABLE STRICT;

CREATE CAST (REAL AS sys.VARBINARY)
WITH FUNCTION sys.float4varbinary (REAL, integer, boolean) AS ASSIGNMENT;

CREATE OR REPLACE FUNCTION sys.float8varbinary(DOUBLE PRECISION, integer, boolean)
RETURNS sys.VARBINARY
AS '$libdir/shark', 'float8varbinary'
LANGUAGE C IMMUTABLE STRICT;

CREATE CAST (DOUBLE PRECISION AS sys.VARBINARY)
WITH FUNCTION sys.float8varbinary (DOUBLE PRECISION, integer, boolean) AS ASSIGNMENT;

CREATE OR REPLACE FUNCTION sys.varbinaryint2(sys.VARBINARY)
RETURNS INT2
AS '$libdir/shark', 'varbinaryint2'
LANGUAGE C IMMUTABLE STRICT;

CREATE CAST (sys.VARBINARY as INT2)
WITH FUNCTION sys.varbinaryint2 (sys.VARBINARY) AS ASSIGNMENT;

CREATE OR REPLACE FUNCTION sys.varbinaryint4(sys.VARBINARY)
RETURNS INT4
AS '$libdir/shark', 'varbinaryint4'
LANGUAGE C IMMUTABLE STRICT;

CREATE CAST (sys.VARBINARY as INT4)
WITH FUNCTION sys.varbinaryint4 (sys.VARBINARY) AS ASSIGNMENT;

CREATE OR REPLACE FUNCTION sys.varbinaryint8(sys.VARBINARY)
RETURNS INT8
AS '$libdir/shark', 'varbinaryint8'
LANGUAGE C IMMUTABLE STRICT;

CREATE CAST (sys.VARBINARY as INT8)
WITH FUNCTION sys.varbinaryint8 (sys.VARBINARY) AS ASSIGNMENT;

-- Add support for varbinary and binary with operators
-- Support equals
CREATE FUNCTION sys.varbinary_eq(leftarg sys.varbinary, rightarg sys.varbinary)
RETURNS boolean
AS '$libdir/shark', 'varbinary_eq'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR sys.= (
    LEFTARG = sys.varbinary,
    RIGHTARG = sys.varbinary,
    PROCEDURE = sys.varbinary_eq,
    COMMUTATOR = =,
    RESTRICT = eqsel
);

-- Support not equals
CREATE FUNCTION sys.varbinary_neq(leftarg sys.varbinary, rightarg sys.varbinary)
RETURNS boolean
AS '$libdir/shark', 'varbinary_neq'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR sys.<> (
    LEFTARG = sys.varbinary,
    RIGHTARG = sys.varbinary,
    PROCEDURE = sys.varbinary_neq,
    COMMUTATOR = <>
);

-- Support greater than
CREATE FUNCTION sys.varbinary_gt(leftarg sys.varbinary, rightarg sys.varbinary)
RETURNS boolean
AS '$libdir/shark', 'varbinary_gt'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR sys.> (
    LEFTARG = sys.varbinary,
    RIGHTARG = sys.varbinary,
    PROCEDURE = sys.varbinary_gt,
    COMMUTATOR = <
);

-- Support greater than equals
CREATE FUNCTION sys.varbinary_geq(leftarg sys.varbinary, rightarg sys.varbinary)
RETURNS boolean
AS '$libdir/shark', 'varbinary_geq'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR sys.>= (
    LEFTARG = sys.varbinary,
    RIGHTARG = sys.varbinary,
    PROCEDURE = sys.varbinary_geq,
    COMMUTATOR = <=
);

-- Support less than
CREATE FUNCTION sys.varbinary_lt(leftarg sys.varbinary, rightarg sys.varbinary)
RETURNS boolean
AS '$libdir/shark', 'varbinary_lt'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR sys.< (
    LEFTARG = sys.varbinary,
    RIGHTARG = sys.varbinary,
    PROCEDURE = sys.varbinary_lt,
    COMMUTATOR = >
);

-- Support less than equals
CREATE FUNCTION sys.varbinary_leq(leftarg sys.varbinary, rightarg sys.varbinary)
RETURNS boolean
AS '$libdir/shark', 'varbinary_leq'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR sys.<= (
    LEFTARG = sys.varbinary,
    RIGHTARG = sys.varbinary,
    PROCEDURE = sys.varbinary_leq,
    COMMUTATOR = >=
);


CREATE FUNCTION sys.varbinary_cmp(sys.varbinary, sys.varbinary)
RETURNS int
AS '$libdir/shark', 'varbinary_cmp'
LANGUAGE C IMMUTABLE STRICT;


CREATE OPERATOR CLASS sys.varbinary_ops
DEFAULT FOR TYPE sys.varbinary USING btree AS
    OPERATOR    1   <  (sys.varbinary, sys.varbinary),
    OPERATOR    2   <= (sys.varbinary, sys.varbinary),
    OPERATOR    3   =  (sys.varbinary, sys.varbinary),
    OPERATOR    4   >= (sys.varbinary, sys.varbinary),
    OPERATOR    5   >  (sys.varbinary, sys.varbinary),
    FUNCTION    1   sys.varbinary_cmp(sys.varbinary, sys.varbinary);

-- varbinary.sql end

-- sql_variant
set search_path = 'sys';
create type sys.sql_variant;
CREATE OR REPLACE FUNCTION sys.sql_variantin(cstring)
 RETURNS sys.sql_variant
 LANGUAGE C
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
as '$libdir/shark', 'sql_variantin';

CREATE OR REPLACE FUNCTION sys.sql_variantout(sys.sql_variant)
 RETURNS cstring
 LANGUAGE C
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
as '$libdir/shark', 'sql_variantout';

CREATE OR REPLACE FUNCTION sys.sql_variantsend(sys.sql_variant)
 RETURNS bytea
 LANGUAGE C
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
as '$libdir/shark', 'sql_variantsend';

CREATE OR REPLACE FUNCTION sys.sql_variantrecv(internal)
 RETURNS sys.sql_variant
 LANGUAGE C
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
as '$libdir/shark', 'sql_variantrecv';

CREATE TYPE sys.SQL_VARIANT (
    INPUT          = sys.sql_variantin,
    OUTPUT         = sys.sql_variantout,
    RECEIVE        = sys.sql_variantrecv,
    SEND           = sys.sql_variantsend,
    INTERNALLENGTH = VARIABLE,
    ALIGNMENT      = 'int4',
    STORAGE        = 'extended',
    CATEGORY       = 'U',
    PREFERRED      = false,
    COLLATABLE     = true
);

CREATE OR REPLACE FUNCTION sys.sql_variantcmp(sys.sql_variant, sys.sql_variant)
 RETURNS integer
 LANGUAGE C
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
as '$libdir/shark', 'sql_variantcmp';

CREATE OR REPLACE FUNCTION sys.sql_varianteq(sys.sql_variant, sys.sql_variant)
 RETURNS boolean
 LANGUAGE C
 IMMUTABLE STRICT LEAKPROOF NOT FENCED NOT SHIPPABLE
as '$libdir/shark', 'sql_varianteq';

CREATE OR REPLACE FUNCTION sys.sql_variantge(sys.sql_variant, sys.sql_variant)
 RETURNS boolean
 LANGUAGE C
 IMMUTABLE STRICT LEAKPROOF NOT FENCED NOT SHIPPABLE
as '$libdir/shark', 'sql_variantge';

CREATE OR REPLACE FUNCTION sys.sql_variantgt(sys.sql_variant, sys.sql_variant)
 RETURNS boolean
 LANGUAGE C
 IMMUTABLE STRICT LEAKPROOF NOT FENCED NOT SHIPPABLE
as '$libdir/shark', 'sql_variantgt';

CREATE OR REPLACE FUNCTION sys.sql_variantle(sys.sql_variant, sys.sql_variant)
 RETURNS boolean
 LANGUAGE C
 IMMUTABLE STRICT LEAKPROOF NOT FENCED NOT SHIPPABLE
as '$libdir/shark', 'sql_variantle';

CREATE OR REPLACE FUNCTION sys.sql_variantlt(sys.sql_variant, sys.sql_variant)
 RETURNS boolean
 LANGUAGE C
 IMMUTABLE STRICT LEAKPROOF NOT FENCED NOT SHIPPABLE
as '$libdir/shark', 'sql_variantlt';

CREATE OR REPLACE FUNCTION sys.sql_variantne(sys.sql_variant, sys.sql_variant)
 RETURNS boolean
 LANGUAGE C
 IMMUTABLE STRICT LEAKPROOF NOT FENCED NOT SHIPPABLE
as '$libdir/shark', 'sql_variantne';

CREATE OPERATOR sys.= (
    LEFTARG    = sys.SQL_VARIANT,
    RIGHTARG   = sys.SQL_VARIANT,
    COMMUTATOR = =,
    NEGATOR    = <>,
    PROCEDURE  = sys.sql_varianteq,
    RESTRICT   = eqsel,
    JOIN       = eqjoinsel,
    MERGES
);

CREATE OPERATOR sys.<> (
    LEFTARG    = sys.SQL_VARIANT,
    RIGHTARG   = sys.SQL_VARIANT,
    NEGATOR    = =,
    COMMUTATOR = <>,
    PROCEDURE  = sys.sql_variantne,
    RESTRICT   = neqsel,
    JOIN       = neqjoinsel
);

CREATE OPERATOR sys.< (
    LEFTARG    = sys.SQL_VARIANT,
    RIGHTARG   = sys.SQL_VARIANT,
    NEGATOR    = >=,
    COMMUTATOR = >,
    PROCEDURE  = sys.sql_variantlt,
    RESTRICT   = scalarltsel,
    JOIN       = scalarltjoinsel
);

CREATE OPERATOR sys.<= (
    LEFTARG    = sys.SQL_VARIANT,
    RIGHTARG   = sys.SQL_VARIANT,
    NEGATOR    = >,
    COMMUTATOR = >=,
    PROCEDURE  = sys.sql_variantle,
    RESTRICT   = scalarltsel,
    JOIN       = scalarltjoinsel
);

CREATE OPERATOR sys.> (
    LEFTARG    = sys.SQL_VARIANT,
    RIGHTARG   = sys.SQL_VARIANT,
    NEGATOR    = <=,
    COMMUTATOR = <,
    PROCEDURE  = sys.sql_variantgt,
    RESTRICT   = scalargtsel,
    JOIN       = scalargtjoinsel
);

CREATE OPERATOR sys.>= (
    LEFTARG    = sys.SQL_VARIANT,
    RIGHTARG   = sys.SQL_VARIANT,
    NEGATOR    = <,
    COMMUTATOR = <=,
    PROCEDURE  = sys.sql_variantge,
    RESTRICT   = scalargtsel,
    JOIN       = scalargtjoinsel
);

CREATE OPERATOR CLASS sys.sqlvariant_ops
DEFAULT FOR TYPE sys.SQL_VARIANT USING btree AS
    OPERATOR    1   <  (sys.SQL_VARIANT, sys.SQL_VARIANT),
    OPERATOR    2   <= (sys.SQL_VARIANT, sys.SQL_VARIANT),
    OPERATOR    3   =  (sys.SQL_VARIANT, sys.SQL_VARIANT),
    OPERATOR    4   >= (sys.SQL_VARIANT, sys.SQL_VARIANT),
    OPERATOR    5   >  (sys.SQL_VARIANT, sys.SQL_VARIANT),
    FUNCTION    1   sql_variantcmp(sys.SQL_VARIANT, sys.SQL_VARIANT);

-- CAST FUNCTIONS to SQL_VARIANT
CREATE OR REPLACE FUNCTION sys.smalldatetime_sqlvariant(SMALLDATETIME, int)
RETURNS sys.SQL_VARIANT
AS '$libdir/shark', 'smalldatetime2sqlvariant'
LANGUAGE C VOLATILE STRICT ;

CREATE CAST (SMALLDATETIME AS sys.SQL_VARIANT)
WITH FUNCTION sys.smalldatetime_sqlvariant (SMALLDATETIME, int) AS IMPLICIT;

CREATE OR REPLACE FUNCTION sys.date_sqlvariant(DATE, int)
RETURNS sys.SQL_VARIANT
AS '$libdir/shark', 'date2sqlvariant'
LANGUAGE C IMMUTABLE STRICT ;

CREATE CAST (DATE AS sys.SQL_VARIANT)
WITH FUNCTION sys.date_sqlvariant (DATE, int) AS IMPLICIT;

CREATE OR REPLACE FUNCTION sys.time_sqlvariant(TIME, int)
RETURNS sys.SQL_VARIANT
AS '$libdir/shark', 'time2sqlvariant'
LANGUAGE C IMMUTABLE STRICT ;

CREATE CAST (TIME AS sys.SQL_VARIANT)
WITH FUNCTION sys.time_sqlvariant (TIME, int) AS IMPLICIT;

CREATE OR REPLACE FUNCTION sys.float_sqlvariant(FLOAT, int)
RETURNS sys.SQL_VARIANT
AS '$libdir/shark', 'float2sqlvariant'
LANGUAGE C IMMUTABLE STRICT ;

CREATE CAST (FLOAT AS sys.SQL_VARIANT)
WITH FUNCTION sys.float_sqlvariant (FLOAT, int) AS IMPLICIT;

CREATE OR REPLACE FUNCTION sys.real_sqlvariant(REAL, int)
RETURNS sys.SQL_VARIANT
AS '$libdir/shark', 'real2sqlvariant'
LANGUAGE C IMMUTABLE STRICT ;

CREATE CAST (REAL AS sys.SQL_VARIANT)
WITH FUNCTION sys.real_sqlvariant (REAL, int) AS IMPLICIT;

CREATE OR REPLACE FUNCTION sys.numeric_sqlvariant(NUMERIC, int)
RETURNS sys.SQL_VARIANT
AS '$libdir/shark', 'numeric2sqlvariant'
LANGUAGE C IMMUTABLE STRICT ;

CREATE CAST (NUMERIC AS sys.SQL_VARIANT)
WITH FUNCTION sys.numeric_sqlvariant (NUMERIC, int) AS IMPLICIT;

CREATE OR REPLACE FUNCTION sys.money_sqlvariant(money, int)
RETURNS sys.SQL_VARIANT
AS '$libdir/shark', 'money2sqlvariant'
LANGUAGE C IMMUTABLE STRICT ;

CREATE CAST (money AS sys.SQL_VARIANT)
WITH FUNCTION sys.money_sqlvariant (money, int) AS IMPLICIT;

CREATE OR REPLACE FUNCTION sys.bigint_sqlvariant(BIGINT, int)
RETURNS sys.SQL_VARIANT
AS '$libdir/shark', 'bigint2sqlvariant'
LANGUAGE C IMMUTABLE STRICT ;

CREATE CAST (BIGINT AS sys.SQL_VARIANT)
WITH FUNCTION sys.bigint_sqlvariant (BIGINT, int) AS IMPLICIT;

CREATE OR REPLACE FUNCTION sys.int_sqlvariant(INT, int)
RETURNS sys.SQL_VARIANT
AS '$libdir/shark', 'int2sqlvariant'
LANGUAGE C IMMUTABLE STRICT ;

CREATE CAST (INT AS sys.SQL_VARIANT)
WITH FUNCTION sys.int_sqlvariant (INT, int) AS IMPLICIT;

CREATE OR REPLACE FUNCTION sys.smallint_sqlvariant(smallint, int)
RETURNS sys.SQL_VARIANT
AS '$libdir/shark', 'smallint2sqlvariant'
LANGUAGE C IMMUTABLE STRICT ;

CREATE CAST (smallint AS sys.SQL_VARIANT)
WITH FUNCTION sys.smallint_sqlvariant (smallint, int) AS IMPLICIT;

CREATE OR REPLACE FUNCTION sys.tinyint_sqlvariant(tinyint, int)
RETURNS sys.SQL_VARIANT
AS '$libdir/shark', 'tinyint2sqlvariant'
LANGUAGE C IMMUTABLE STRICT ;

CREATE CAST (tinyint AS sys.SQL_VARIANT)
WITH FUNCTION sys.tinyint_sqlvariant (tinyint, int) AS IMPLICIT;

CREATE OR REPLACE FUNCTION sys.bit_sqlvariant(BIT, int)
RETURNS sys.SQL_VARIANT
AS '$libdir/shark', 'bit2sqlvariant'
LANGUAGE C IMMUTABLE STRICT ;

CREATE CAST (BIT AS sys.SQL_VARIANT)
WITH FUNCTION sys.bit_sqlvariant (BIT, int) AS IMPLICIT;

CREATE OR REPLACE FUNCTION sys.varchar_sqlvariant(varchar, int)
RETURNS sys.SQL_VARIANT
AS '$libdir/shark', 'varchar2sqlvariant'
LANGUAGE C IMMUTABLE STRICT ;

CREATE CAST (VARCHAR AS sys.SQL_VARIANT)
WITH FUNCTION sys.varchar_sqlvariant (VARCHAR, int) AS IMPLICIT;

CREATE OR REPLACE FUNCTION sys.nvarchar_sqlvariant(nvarchar, int)
RETURNS sys.SQL_VARIANT
AS '$libdir/shark', 'nvarchar2sqlvariant'
LANGUAGE C IMMUTABLE STRICT ;

CREATE CAST (NVARCHAR AS sys.SQL_VARIANT)
WITH FUNCTION sys.nvarchar_sqlvariant (NVARCHAR, int) AS IMPLICIT;

CREATE OR REPLACE FUNCTION sys.char_sqlvariant(CHAR, int)
RETURNS sys.SQL_VARIANT
AS '$libdir/shark', 'char2sqlvariant'
LANGUAGE C IMMUTABLE STRICT ;

CREATE CAST (CHAR AS sys.SQL_VARIANT)
WITH FUNCTION sys.char_sqlvariant (CHAR, int) AS IMPLICIT;

-- CAST functions from SQL_VARIANT
CREATE OR REPLACE FUNCTION sys.sqlvariant_smalldatetime(sys.SQL_VARIANT)
RETURNS SMALLDATETIME
AS '$libdir/shark', 'sqlvariant2smalldatetime'
LANGUAGE C VOLATILE STRICT ;

CREATE CAST (sys.SQL_VARIANT AS SMALLDATETIME)
WITH FUNCTION sys.sqlvariant_smalldatetime (sys.SQL_VARIANT);

CREATE OR REPLACE FUNCTION sys.sqlvariant_date(sys.SQL_VARIANT)
RETURNS DATE
AS '$libdir/shark', 'sqlvariant2date'
LANGUAGE C VOLATILE STRICT ;

CREATE CAST (sys.SQL_VARIANT AS DATE)
WITH FUNCTION sys.sqlvariant_date (sys.SQL_VARIANT);

CREATE OR REPLACE FUNCTION sys.sqlvariant_time(sys.SQL_VARIANT)
RETURNS TIME
AS '$libdir/shark', 'sqlvariant2time'
LANGUAGE C VOLATILE STRICT ;

CREATE CAST (sys.SQL_VARIANT AS TIME)
WITH FUNCTION sys.sqlvariant_time (sys.SQL_VARIANT);

CREATE OR REPLACE FUNCTION sys.sqlvariant_float(sys.SQL_VARIANT)
RETURNS FLOAT
AS '$libdir/shark', 'sqlvariant2float'
LANGUAGE C VOLATILE STRICT ;

CREATE CAST (sys.SQL_VARIANT AS FLOAT)
WITH FUNCTION sys.sqlvariant_float (sys.SQL_VARIANT);

CREATE OR REPLACE FUNCTION sys.sqlvariant_real(sys.SQL_VARIANT)
RETURNS REAL
AS '$libdir/shark', 'sqlvariant2real'
LANGUAGE C VOLATILE STRICT ;

CREATE CAST (sys.SQL_VARIANT AS REAL)
WITH FUNCTION sys.sqlvariant_real (sys.SQL_VARIANT);

CREATE OR REPLACE FUNCTION sys.sqlvariant_numeric(sys.SQL_VARIANT)
RETURNS NUMERIC
AS '$libdir/shark', 'sqlvariant2numeric'
LANGUAGE C VOLATILE STRICT ;

CREATE CAST (sys.SQL_VARIANT AS NUMERIC)
WITH FUNCTION sys.sqlvariant_numeric (sys.SQL_VARIANT);

CREATE OR REPLACE FUNCTION sys.sqlvariant_money(sys.SQL_VARIANT)
RETURNS MONEY
AS '$libdir/shark', 'sqlvariant2money'
LANGUAGE C VOLATILE STRICT ;

CREATE CAST (sys.SQL_VARIANT AS MONEY)
WITH FUNCTION sys.sqlvariant_money (sys.SQL_VARIANT);

CREATE OR REPLACE FUNCTION sys.sqlvariant_bigint(sys.SQL_VARIANT)
RETURNS BIGINT
AS '$libdir/shark', 'sqlvariant2bigint'
LANGUAGE C VOLATILE STRICT ;

CREATE CAST (sys.SQL_VARIANT AS BIGINT)
WITH FUNCTION sys.sqlvariant_bigint (sys.SQL_VARIANT);

CREATE OR REPLACE FUNCTION sys.sqlvariant_int(sys.SQL_VARIANT)
RETURNS INT
AS '$libdir/shark', 'sqlvariant2int'
LANGUAGE C VOLATILE STRICT ;

CREATE CAST (sys.SQL_VARIANT AS INT)
WITH FUNCTION sys.sqlvariant_int (sys.SQL_VARIANT);

CREATE OR REPLACE FUNCTION sys.sqlvariant_smallint(sys.SQL_VARIANT)
RETURNS SMALLINT
AS '$libdir/shark', 'sqlvariant2smallint'
LANGUAGE C VOLATILE STRICT ;

CREATE CAST (sys.SQL_VARIANT AS SMALLINT)
WITH FUNCTION sys.sqlvariant_smallint (sys.SQL_VARIANT);

CREATE OR REPLACE FUNCTION sys.sqlvariant_tinyint(sys.SQL_VARIANT)
RETURNS TINYINT
AS '$libdir/shark', 'sqlvariant2smallint'
LANGUAGE C VOLATILE STRICT ;

CREATE CAST (sys.SQL_VARIANT AS TINYINT)
WITH FUNCTION sys.sqlvariant_tinyint (sys.SQL_VARIANT);

CREATE OR REPLACE FUNCTION sys.sqlvariant_bit(sys.SQL_VARIANT)
RETURNS BIT
AS '$libdir/shark', 'sqlvariant2bit'
LANGUAGE C VOLATILE STRICT ;

CREATE CAST (sys.SQL_VARIANT AS BIT)
WITH FUNCTION sys.sqlvariant_bit (sys.SQL_VARIANT);

CREATE OR REPLACE FUNCTION sys.sqlvariant_sysvarchar(sys.SQL_VARIANT)
RETURNS VARCHAR
AS '$libdir/shark', 'sqlvariant2varchar'
LANGUAGE C VOLATILE STRICT ;

CREATE CAST (sys.SQL_VARIANT AS VARCHAR)
WITH FUNCTION sys.sqlvariant_sysvarchar (sys.SQL_VARIANT);

CREATE OR REPLACE FUNCTION sys.sqlvariant_nvarchar(sys.SQL_VARIANT)
RETURNS NVARCHAR
AS '$libdir/shark', 'sqlvariant2varchar'
LANGUAGE C VOLATILE STRICT ;

CREATE CAST (sys.SQL_VARIANT AS NVARCHAR)
WITH FUNCTION sys.sqlvariant_sysvarchar (sys.SQL_VARIANT);

CREATE OR REPLACE FUNCTION sys.sqlvariant_char(sys.SQL_VARIANT)
RETURNS CHAR
AS '$libdir/shark', 'sqlvariant2char'
LANGUAGE C VOLATILE STRICT ;

CREATE CAST (sys.SQL_VARIANT AS CHAR)
WITH FUNCTION sys.sqlvariant_char (sys.SQL_VARIANT);

CREATE OR REPLACE FUNCTION sys.error_number()
RETURNS INT
AS '$libdir/shark', 'error_number'
LANGUAGE C VOLATILE STRICT ;

CREATE OR REPLACE FUNCTION sys.error_severity()
RETURNS INT
AS '$libdir/shark', 'error_severity'
LANGUAGE C VOLATILE STRICT ;

CREATE OR REPLACE FUNCTION sys.error_state()
RETURNS INT
AS '$libdir/shark', 'error_state'
LANGUAGE C VOLATILE STRICT ;

CREATE OR REPLACE FUNCTION sys.error_procedure()
RETURNS TEXT
AS '$libdir/shark', 'error_procedure'
LANGUAGE C VOLATILE STRICT ;

CREATE OR REPLACE FUNCTION sys.error_message()
RETURNS TEXT
AS '$libdir/shark', 'error_message'
LANGUAGE C VOLATILE STRICT ;

CREATE OR REPLACE FUNCTION sys.error_line()
RETURNS INT
AS '$libdir/shark', 'error_line'
LANGUAGE C VOLATILE STRICT ;
reset search_path;

-- sys.databasepropertyex
create or replace function sys.databasepropertyex (nvarchar(128), nvarchar(128))
returns sys.SQL_VARIANT AS
'$libdir/shark', 'databasepropertyex'
language C IMMUTABLE STRICT;

-- sys.suser_id
create or replace function sys.suser_id_internal(IN login nvarchar(256))
RETURNS OID AS
'$libdir/shark', 'suser_id'
LANGUAGE C IMMUTABLE;

create or replace function sys.suser_id(IN login nvarchar(256))
returns OID as $$
    select case
        when login IS NULL THEN NULL
        else sys.suser_id_internal(login)
    end;
$$
language sql IMMUTABLE STRICT;

create or replace function sys.suser_id()
returns OID as $$
    select sys.suser_id_internal(NULL);
$$
language sql IMMUTABLE;

-- sys.suser_name
create or replace function sys.suser_name_internal(IN server_user_id OID)
RETURNS nvarchar(128) AS
'$libdir/shark', 'suser_name'
LANGUAGE C IMMUTABLE;

create or replace function sys.suser_name(IN server_user_id OID)
returns nvarchar(128) as $$
    select sys.suser_name_internal(server_user_id);
$$
language sql IMMUTABLE STRICT;

create or replace function sys.suser_name()
returns nvarchar(128) as $$
    select sys.suser_name_internal(sys.suser_id());
$$
language sql IMMUTABLE;

-- sys.suser_sname
-- Since openGauss currently does not support SIDs, this function ultimately behaves the same as suser_name, but
-- with a different input parameter type.
create or replace function sys.suser_sname(IN server_user_sid varbinary(85))
returns nvarchar(128) as $$
    select sys.suser_name(cast(server_user_sid as int));
$$
language sql IMMUTABLE;

create or replace function sys.suser_sname()
returns nvarchar(128) as $$
    select sys.suser_name();
$$
language sql IMMUTABLE;

-- sys.scope_identity
create or replace function sys.get_scope_identity()
returns int16 AS
'$libdir/shark', 'get_scope_identity'
language C STABLE STRICT;

create or replace function sys.scope_identity()
returns numeric(38, 0) as $$
    select sys.get_scope_identity()::numeric(38, 0);
$$
language sql STABLE;

-- sys.ident_current
create or replace function sys.get_ident_current(IN tablename nvarchar(128))
RETURNS int16 AS
'$libdir/shark', 'get_ident_current'
LANGUAGE C STRICT;

create or replace function sys.ident_current(IN tablename nvarchar(128))
returns numeric(38, 0) as $$
    select sys.get_ident_current(tablename)::numeric(38, 0);
$$
language sql;

CREATE TABLE sys.shark_syslanguages (
    lang_id SMALLINT,
    lang_name_pg VARCHAR(30),
    lang_alias_pg VARCHAR(30),
    lang_name_mssql VARCHAR(30),
    lang_alias_mssql VARCHAR(30),
    territory VARCHAR(50),
    spec_culture VARCHAR(10),
    lang_data_json JSON
) WITH (OIDS = FALSE);
GRANT SELECT ON sys.shark_syslanguages TO PUBLIC;

/* Tsql DMLs*/
INSERT INTO sys.shark_syslanguages
     VALUES (1,
             'ENGLISH',
             'ENGLISH (UNITED STATES)',
             'US_ENGLISH',
             'ENGLISH',
             'UNITED STATES',
             'EN_US',
             json_build_object('date_format', 'MDY',
                                'date_first', 7,
                                'months_names', json_build_array('January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'),
                                'months_shortnames', json_build_array('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'),
                                'days_names', json_build_array('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'),
                                'days_shortnames', json_build_array('Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun')));

INSERT INTO sys.shark_syslanguages
     VALUES (2,
             'CHINESE (TRADITIONAL)',
             'CHINESE (TRADITIONAL, CHINA)',
             '繁體中文',
             'TRADITIONAL CHINESE',
             'CHINA',
             'ZH_TW',
             json_build_object('date_format', 'YMD',
                                'date_first', 7,
                                'months_names', json_build_array('一月', '二月', '三月', '四月', '五月', '六月', '七月', '八月', '九月', '十月', '十一月', '十二月'),
                                'months_shortnames', json_build_array('01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'),
                                'months_extranames', json_build_array('January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'),
                                'months_extrashortnames', json_build_array('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'),
                                'days_names', json_build_array('星期一', '星期二', '星期三', '星期四', '星期五', '星期六', '星期日'),
                                'days_shortnames', json_build_array('Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'),
                                'days_extrashortnames', json_build_array('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday')));


INSERT INTO sys.shark_syslanguages
     VALUES (3,
             'CHINESE (SIMPLIFIED)',
             'CHINESE (SIMPLIFIED, CHINA)',
             '简体中文',
             'SIMPLIFIED CHINESE',
             'CHINA',
             'ZH_CN',
             json_build_object('date_format', 'YMD',
                                'date_first', 7,
                                'months_names', json_build_array('01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'),
                                'months_shortnames', json_build_array('01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'),
                                'months_extranames', json_build_array('January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'),
                                'months_extrashortnames', json_build_array('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'),
                                'days_names', json_build_array('星期一', '星期二', '星期三', '星期四', '星期五', '星期六', '星期日'),
                                'days_shortnames', json_build_array('Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'),
                                'days_extrashortnames', json_build_array('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday')));

INSERT INTO sys.shark_syslanguages
     VALUES (4,
             'HIJRI',
             'HIJRI (ISLAMIC)',
             'HIJRI',
             'ISLAMIC',
             'ISLAMIC',
             'HI_IS',
             json_build_object('date_format', 'DMY',
                                'date_first', 1,
                                'months_names', json_build_array('محرم', 'صفر', 'ربيع الاول', 'ربيع الثاني', 'جمادى الاولى', 'جمادى الثانية', 'رجب', 'شعبان', 'رمضان', 'شوال', 'ذو القعدة', 'ذو الحجة'),
                                'months_shortnames', json_build_array('محرم', 'صفر', 'ربيع الاول', 'ربيع الثاني', 'جمادى الاولى', 'جمادى الثانية', 'رجب', 'شعبان', 'رمضان', 'شوال', 'ذو القعدة', 'ذو الحجة'),
                                'months_extranames', json_build_array('January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'),
                                'months_extrashortnames', json_build_array('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'),
                                'days_names', json_build_array('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'),
                                'days_shortnames', json_build_array('Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun')));


-- LOG10 implmentation
CREATE OR REPLACE FUNCTION sys.log10(IN arg1 double precision)
RETURNS double precision  AS '$libdir/shark','numeric_log10' LANGUAGE C IMMUTABLE STRICT;

-- ATN2 implmentation
CREATE OR REPLACE FUNCTION sys.atn2(IN x double precision, IN y double precision) RETURNS double precision
AS
$$
DECLARE
    res double precision;
BEGIN
    IF x = 0 AND y = 0 THEN
        RAISE EXCEPTION 'An invalid floating point operation occurred.';
    ELSE
        res = PG_CATALOG.atan2(x, y);
        RETURN res;
    END IF;
END;
$$
LANGUAGE plpgsql IMMUTABLE RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION sys.atn2(IN x money, IN y money) RETURNS double precision
AS
$$
BEGIN
    RETURN sys.atn2(x::double precision, y::double precision);
END;
$$
LANGUAGE plpgsql IMMUTABLE RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION sys.atn2(IN x varchar, IN y varchar) RETURNS double precision
AS
$$
BEGIN
    RETURN sys.atn2(x::double precision, y::double precision);
END;
$$
LANGUAGE plpgsql IMMUTABLE RETURNS NULL ON NULL INPUT;

-- ISNULL implmentation
CREATE FUNCTION sys.isnull(text,text) RETURNS text AS $$
  SELECT COALESCE($1,$2);
$$
LANGUAGE SQL STABLE;

CREATE FUNCTION sys.isnull(boolean,boolean) RETURNS boolean AS $$
  SELECT COALESCE($1,$2);
$$
LANGUAGE SQL STABLE;

CREATE FUNCTION sys.isnull(smallint,smallint) RETURNS smallint AS $$
  SELECT COALESCE($1,$2);
$$
LANGUAGE SQL STABLE;

CREATE FUNCTION sys.isnull(integer,integer) RETURNS integer AS $$
  SELECT COALESCE($1,$2);
$$
LANGUAGE SQL STABLE;

CREATE FUNCTION sys.isnull(bigint,bigint) RETURNS bigint AS $$
  SELECT COALESCE($1,$2);
$$
LANGUAGE SQL STABLE;

CREATE FUNCTION sys.isnull(real,real) RETURNS real AS $$
  SELECT COALESCE($1,$2);
$$
LANGUAGE SQL STABLE;

CREATE FUNCTION sys.isnull(double precision, double precision) RETURNS double precision AS $$
  SELECT COALESCE($1,$2);
$$
LANGUAGE SQL STABLE;

CREATE FUNCTION sys.isnull(numeric,numeric) RETURNS numeric AS $$
  SELECT COALESCE($1,$2);
$$
LANGUAGE SQL STABLE;

CREATE FUNCTION sys.isnull(date, date) RETURNS date AS $$
  SELECT COALESCE($1,$2);
$$
LANGUAGE SQL STABLE;

CREATE FUNCTION sys.isnull(timestamp,timestamp) RETURNS timestamp AS $$
  SELECT COALESCE($1,$2);
$$
LANGUAGE SQL STABLE;

CREATE FUNCTION sys.isnull(timestamp with time zone,timestamp with time zone) RETURNS timestamp with time zone AS $$
  SELECT COALESCE($1,$2);
$$
LANGUAGE SQL STABLE;

CREATE FUNCTION sys.isnull(text,smallint) RETURNS text AS $$
  SELECT COALESCE($1,$2::text);
$$
LANGUAGE SQL STABLE;

CREATE FUNCTION sys.isnull(text,integer) RETURNS text AS $$
  SELECT COALESCE($1,$2::text);
$$
LANGUAGE SQL STABLE;

CREATE FUNCTION sys.isnull(text,bigint) RETURNS text AS $$
  SELECT COALESCE($1,$2::text);
$$
LANGUAGE SQL STABLE;

CREATE FUNCTION sys.isnull(text,real) RETURNS text AS $$
  SELECT COALESCE($1,$2::text);
$$
LANGUAGE SQL STABLE;

CREATE FUNCTION sys.isnull(text,double precision) RETURNS text AS $$
  SELECT COALESCE($1,$2::text);
$$
LANGUAGE SQL STABLE;

CREATE FUNCTION sys.isnull(text,numeric) RETURNS text AS $$
  SELECT COALESCE($1,$2::text);
$$
LANGUAGE SQL STABLE;

CREATE OR REPLACE FUNCTION sys.charindex(expressionToFind PG_CATALOG.TEXT,
										 expressionToSearch PG_CATALOG.TEXT,
										 start_location INTEGER DEFAULT 0)
RETURNS INTEGER AS
$BODY$
SELECT
CASE
WHEN expressionToFind = '' THEN
    0
WHEN start_location <= 0 THEN
	strpos(expressionToSearch, expressionToFind)
ELSE
	CASE
	WHEN strpos(substr(expressionToSearch, start_location), expressionToFind) = 0 THEN
		0
	ELSE
		strpos(substr(expressionToSearch, start_location), expressionToFind) + start_location - 1
	END
END;
$BODY$
STRICT
LANGUAGE SQL IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.shark_try_conv_string_to_varbinary(IN arg VARCHAR,
                                                                      IN p_style NUMERIC DEFAULT 0)
RETURNS sys.varbinary
AS
$BODY$
BEGIN
    RETURN sys.shark_conv_string_to_varbinary(arg, p_style);
    EXCEPTION
        WHEN OTHERS THEN
            RETURN NULL;
END;
$BODY$
LANGUAGE plpgsql
IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.shark_conv_helper_to_varbinary(IN typmod INTEGER,
                                                                  IN arg anyelement,
                                                                  IN p_try BOOL,
                                                                  IN p_style NUMERIC DEFAULT 0)
RETURNS sys.varbinary
AS
$BODY$
DECLARE result sys.varbinary;
BEGIN
    IF p_try THEN
        RETURN sys.shark_try_conv_to_varbinary(typmod, arg, p_style);
    ELSE
        IF pg_typeof(arg) IN ('text'::regtype, 'nvarchar2'::regtype, 'bpchar'::regtype) THEN
            RETURN sys.shark_conv_string_to_varbinary(arg, p_style);
        ELSE
            IF typmod = -1 THEN
                RETURN CAST(arg as sys.varbinary);
            ELSE
                EXECUTE format('SELECT CAST($1 as sys.varbinary(%s))', typmod) INTO result USING arg;
                RETURN result;
            END IF;
        END IF;
    END IF;
END;
$BODY$
LANGUAGE plpgsql
IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.shark_conv_helper_to_varbinary(IN typmod INTEGER,
                                                                  IN arg VARCHAR,
                                                                  IN p_try BOOL,
                                                                  IN p_style NUMERIC DEFAULT 0)
RETURNS sys.varbinary
AS
$BODY$
BEGIN
    IF p_try THEN
        RETURN sys.shark_try_conv_string_to_varbinary(arg, p_style);
    ELSE
        RETURN sys.shark_conv_string_to_varbinary(arg, p_style);
    END IF;
END;
$BODY$
LANGUAGE plpgsql
IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.shark_conv_string_to_varbinary(IN input_value VARCHAR, IN style NUMERIC DEFAULT 0)
RETURNS sys.varbinary
AS
$BODY$
DECLARE
    result bytea;
BEGIN
    IF style = 0 THEN
        RETURN CAST(input_value AS sys.varbinary);
    ELSIF style = 1 THEN
        -- Handle hexadecimal conversion
        IF (PG_CATALOG.left(input_value, 2) = '0x' COLLATE "C" AND PG_CATALOG.length(input_value) % 2 = 0) THEN
            result := decode(substring(input_value from 3), 'hex');
        ELSE
            RAISE EXCEPTION 'Error converting data type varchar to varbinary.';
        END IF;
    ELSIF style = 2 THEN
        IF PG_CATALOG.left(input_value, 2) = '0x' COLLATE "C" THEN
            RAISE EXCEPTION 'Error converting data type varchar to varbinary.';
        ELSE
            result := decode(input_value, 'hex');
        END IF;
    ELSE
        RAISE EXCEPTION 'The style % is not supported for conversions from varchar to varbinary.', style;
    END IF;

    RETURN CAST(result AS sys.varbinary);
END;
$BODY$
LANGUAGE plpgsql
IMMUTABLE
STRICT;

CREATE OR REPLACE FUNCTION sys.datetimefromparts(IN p_year NUMERIC,
                                                               IN p_month NUMERIC,
                                                               IN p_day NUMERIC,
                                                               IN p_hour NUMERIC,
                                                               IN p_minute NUMERIC,
                                                               IN p_seconds NUMERIC,
                                                               IN p_milliseconds NUMERIC)
RETURNS TIMESTAMP WITHOUT TIME ZONE
AS
$BODY$
DECLARE
    v_err_message VARCHAR;
    v_calc_seconds NUMERIC;
    v_milliseconds SMALLINT;
    v_resdatetime TIMESTAMP WITHOUT TIME ZONE;
BEGIN
    -- Check if arguments are out of range
    IF ((floor(p_year)::SMALLINT NOT BETWEEN 1753 AND 9999) OR
        (floor(p_month)::SMALLINT NOT BETWEEN 1 AND 12) OR
        (floor(p_day)::SMALLINT NOT BETWEEN 1 AND 31) OR
        (floor(p_hour)::SMALLINT NOT BETWEEN 0 AND 23) OR
        (floor(p_minute)::SMALLINT NOT BETWEEN 0 AND 59) OR
        (floor(p_seconds)::SMALLINT NOT BETWEEN 0 AND 59) OR
        (floor(p_milliseconds)::SMALLINT NOT BETWEEN 0 AND 999))
    THEN
        RAISE invalid_datetime_format;
    END IF;

    v_milliseconds := sys.shark_round_fractseconds(p_milliseconds::INTEGER);

    v_calc_seconds := pg_catalog.format('%s.%s',
                             floor(p_seconds)::SMALLINT,
                             CASE v_milliseconds
                                WHEN 1000 THEN '0'
                                ELSE lpad(v_milliseconds::VARCHAR, 3, '0')
                             END)::NUMERIC;

    v_resdatetime := make_timestamp(floor(p_year)::SMALLINT,
                                    floor(p_month)::SMALLINT,
                                    floor(p_day)::SMALLINT,
                                    floor(p_hour)::SMALLINT,
                                    floor(p_minute)::SMALLINT,
                                    v_calc_seconds);
    RETURN CASE
              WHEN (v_milliseconds != 1000) THEN v_resdatetime
              ELSE v_resdatetime + INTERVAL '1 second'
           END;
EXCEPTION
    WHEN invalid_datetime_format THEN
        RAISE USING MESSAGE := 'Cannot construct data type timestamp with time zone, some of the arguments have values which are not valid.',
                    DETAIL := 'Possible use of incorrect value of date or time part (which lies outside of valid range).',
                    HINT := 'Check each input argument belongs to the valid range and try again.';

    WHEN numeric_value_out_of_range THEN
        GET STACKED DIAGNOSTICS v_err_message = MESSAGE_TEXT;
        v_err_message := pg_catalog.upper(split_part(v_err_message, ' ', 1));

        RAISE USING MESSAGE := pg_catalog.format('Error while trying to cast to %s data type.', v_err_message),
                    DETAIL := pg_catalog.format('Source value is out of %s data type range.', v_err_message),
                    HINT := pg_catalog.format('Correct the source value you are trying to cast to %s data type and try again.',
                                   v_err_message);
END;
$BODY$
LANGUAGE plpgsql
IMMUTABLE
RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION sys.datetimefromparts(IN p_year TEXT,
                                                               IN p_month TEXT,
                                                               IN p_day TEXT,
                                                               IN p_hour TEXT,
                                                               IN p_minute TEXT,
                                                               IN p_seconds TEXT,
                                                               IN p_milliseconds TEXT)
RETURNS TIMESTAMP WITHOUT TIME ZONE
AS
$BODY$
DECLARE
    v_err_message VARCHAR;
BEGIN
    RETURN sys.datetimefromparts(p_year::NUMERIC, p_month::NUMERIC, p_day::NUMERIC,
                                               p_hour::NUMERIC, p_minute::NUMERIC,
                                               p_seconds::NUMERIC, p_milliseconds::NUMERIC);
EXCEPTION
    WHEN invalid_text_representation THEN
        GET STACKED DIAGNOSTICS v_err_message = MESSAGE_TEXT;
        v_err_message := substring(pg_catalog.lower(v_err_message), 'numeric\:\s\"(.*)\"');

        RAISE USING MESSAGE := pg_catalog.format('Error while trying to convert "%s" value to NUMERIC data type.', v_err_message),
                    DETAIL := 'Supplied string value contains illegal characters.',
                    HINT := 'Correct supplied value, remove all illegal characters and try again.';
END;
$BODY$
LANGUAGE plpgsql
IMMUTABLE
RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION sys.shark_round_fractseconds(IN p_fractseconds NUMERIC)
RETURNS INTEGER
AS
$BODY$
DECLARE
   v_modpart INTEGER;
   v_decpart INTEGER;
   v_fractseconds INTEGER;
BEGIN
    v_fractseconds := floor(p_fractseconds)::INTEGER;
    v_modpart := v_fractseconds % 10;
    v_decpart := v_fractseconds - v_modpart;

    RETURN CASE
              WHEN (v_modpart BETWEEN 0 AND 1) THEN v_decpart
              WHEN (v_modpart BETWEEN 2 AND 4) THEN v_decpart + 3
              WHEN (v_modpart BETWEEN 5 AND 8) THEN v_decpart + 7
              ELSE v_decpart + 10
           END;
END;
$BODY$
LANGUAGE plpgsql
IMMUTABLE
RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION sys.shark_round_fractseconds(IN p_fractseconds TEXT)
RETURNS INTEGER
AS
$BODY$
BEGIN
    RETURN sys.shark_round_fractseconds(p_fractseconds::NUMERIC);
EXCEPTION
    WHEN invalid_text_representation THEN
        RAISE USING MESSAGE := pg_catalog.format('Error while trying to convert "%s" value to NUMERIC data type.', pg_catalog.btrim(p_fractseconds)),
                    DETAIL := 'Passed argument value contains illegal characters.',
                    HINT := 'Correct passed argument value, remove all illegal characters.';


END;
$BODY$
LANGUAGE plpgsql
IMMUTABLE
RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION sys.shark_conv_greg_to_hijri(IN p_day NUMERIC,
                                                                IN p_month NUMERIC,
                                                                IN p_year NUMERIC)
RETURNS DATE
AS
$BODY$
DECLARE
    v_day SMALLINT;
    v_month SMALLINT;
    v_year INTEGER;
    v_jdnum DOUBLE PRECISION;
    v_lnum DOUBLE PRECISION;
    v_inum DOUBLE PRECISION;
    v_nnum DOUBLE PRECISION;
    v_jnum DOUBLE PRECISION;
BEGIN
    v_day := floor(p_day)::SMALLINT;
    v_month := floor(p_month)::SMALLINT;
    v_year := floor(p_year)::INTEGER;

    IF ((sign(v_day) = -1) OR (sign(v_month) = -1) OR (sign(v_year) = -1))
    THEN
        RAISE invalid_character_value_for_cast;
    ELSIF (v_year = 0) THEN
        RAISE null_value_not_allowed;
    END IF;

    IF ((p_year > 1582) OR ((p_year = 1582) AND (p_month > 10)) OR ((p_year = 1582) AND (p_month = 10) AND (p_day > 14)))
    THEN
        v_jdnum := sys.shark_get_int_part((1461 * (p_year + 4800 + sys.shark_get_int_part((p_month - 14) / 12))) / 4) +
                   sys.shark_get_int_part((367 * (p_month - 2 - 12 * (sys.shark_get_int_part((p_month - 14) / 12)))) / 12) -
                   sys.shark_get_int_part((3 * (sys.shark_get_int_part((p_year + 4900 +
                   sys.shark_get_int_part((p_month - 14) / 12)) / 100))) / 4) + p_day - 32075;
    ELSE
        v_jdnum := 367 * p_year - sys.shark_get_int_part((7 * (p_year + 5001 +
                   sys.shark_get_int_part((p_month - 9) / 7))) / 4) +
                   sys.shark_get_int_part((275 * p_month) / 9) + p_day + 1729777;
    END IF;

    v_lnum := v_jdnum - 1948440 + 10632;
    v_nnum := sys.shark_get_int_part((v_lnum - 1) / 10631);
    v_lnum := v_lnum - 10631 * v_nnum + 354;
    v_jnum := (sys.shark_get_int_part((10985 - v_lnum) / 5316)) * (sys.shark_get_int_part((50 * v_lnum) / 17719)) +
              (sys.shark_get_int_part(v_lnum / 5670)) * (sys.shark_get_int_part((43 * v_lnum) / 15238));
    v_lnum := v_lnum - (sys.shark_get_int_part((30 - v_jnum) / 15)) * (sys.shark_get_int_part((17719 * v_jnum) / 50)) -
              (sys.shark_get_int_part(v_jnum / 16)) * (sys.shark_get_int_part((15238 * v_jnum) / 43)) + 29;

    v_month := sys.shark_get_int_part((24 * v_lnum) / 709);
    v_day := v_lnum - sys.shark_get_int_part((709 * v_month) / 24);
    v_year := 30 * v_nnum + v_jnum - 30;

    RETURN to_date(pg_catalog.concat_ws('.', v_day, v_month, v_year), 'DD.MM.YYYY');
EXCEPTION
    WHEN invalid_character_value_for_cast THEN
        RAISE USING MESSAGE := 'Could not convert Gregorian to Hijri date if any part of the date is negative.',
                    DETAIL := 'Some of the supplied date parts (day, month, year) is negative.',
                    HINT := 'Change the value of the date part (day, month, year) wich was found to be negative.';

    WHEN null_value_not_allowed THEN
        RAISE USING MESSAGE := 'Could not convert Gregorian to Hijri date if year value is equal to zero.',
                    DETAIL := 'Supplied year value is equal to zero.',
                    HINT := 'Change the value of the year so that it is greater than zero.';
END;
$BODY$
LANGUAGE plpgsql
STABLE
RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION sys.shark_conv_greg_to_hijri(IN p_datetimeval TIMESTAMP WITHOUT TIME ZONE)
RETURNS TIMESTAMP WITHOUT TIME ZONE
AS
$BODY$
DECLARE
    v_hijri_date DATE;
BEGIN
    v_hijri_date := sys.shark_conv_greg_to_hijri(extract(day from p_datetimeval)::SMALLINT,
                                                         extract(month from p_datetimeval)::SMALLINT,
                                                         extract(year from p_datetimeval)::INTEGER);

    RETURN to_timestamp(pg_catalog.format('%s %s', to_char(v_hijri_date, 'DD.MM.YYYY'),
                                        to_char(p_datetimeval, ' HH24:MI:SS.US')),
                        'DD.MM.YYYY HH24:MI:SS.US');
END;
$BODY$
LANGUAGE plpgsql
STABLE
RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION sys.shark_conv_greg_to_hijri(IN p_day TEXT,
                                                                IN p_month TEXT,
                                                                IN p_year TEXT)
RETURNS DATE
AS
$BODY$
BEGIN
    RETURN sys.shark_conv_greg_to_hijri(p_day::NUMERIC,
                                                p_month::NUMERIC,
                                                p_year::NUMERIC);
END;
$BODY$
LANGUAGE plpgsql
STABLE
RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION sys.shark_conv_greg_to_hijri(IN p_dateval DATE)
RETURNS DATE
AS
$BODY$
BEGIN
    RETURN sys.shark_conv_greg_to_hijri(extract(day from p_dateval)::NUMERIC,
                                                extract(month from p_dateval)::NUMERIC,
                                                extract(year from p_dateval)::NUMERIC);
END;
$BODY$
LANGUAGE plpgsql
STABLE
RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION sys.shark_get_microsecs_from_fractsecs_v2(IN p_fractsecs TEXT,
                                                                          IN p_scale NUMERIC DEFAULT 7)
RETURNS VARCHAR
AS
$BODY$
DECLARE
    v_scale SMALLINT;
    v_decplaces INTEGER;
    v_fractsecs VARCHAR COLLATE "C";
    v_pureplaces VARCHAR COLLATE "C";
    v_rnd_fractsecs INTEGER;
    v_fractsecs_len INTEGER;
    v_pureplaces_len INTEGER;
    v_err_message VARCHAR COLLATE "C";
BEGIN
    v_fractsecs := pg_catalog.btrim(p_fractsecs);
    v_fractsecs_len := char_length(v_fractsecs);
    v_scale := floor(p_scale)::SMALLINT;

    IF (v_fractsecs_len < 7) THEN
        v_fractsecs := rpad(v_fractsecs, 7, '0');
        v_fractsecs_len := char_length(v_fractsecs);
    END IF;

    v_pureplaces := trim(leading '0' from v_fractsecs);
    v_pureplaces_len := char_length(v_pureplaces);

    v_decplaces := v_fractsecs_len - v_pureplaces_len;

    v_rnd_fractsecs := round(v_fractsecs::INTEGER, (v_pureplaces_len - (v_scale - v_decplaces)) * (-1));

    IF (char_length(v_rnd_fractsecs::TEXT) > v_fractsecs_len) THEN
        RETURN '-1';
    END IF;

    v_fractsecs := lpad(v_rnd_fractsecs::TEXT, v_fractsecs_len, '0');

    RETURN substring(v_fractsecs, 1, CASE
                                        WHEN (v_scale >= 7) THEN 6
                                        ELSE v_scale
                                     END);
EXCEPTION
    WHEN invalid_text_representation THEN
        GET STACKED DIAGNOSTICS v_err_message = MESSAGE_TEXT;
        v_err_message := substring(pg_catalog.lower(v_err_message), 'integer\:\s\"(.*)\"');

        RAISE USING MESSAGE := pg_catalog.format('Error while trying to convert "%s" value to SMALLINT data type.', v_err_message),
                    DETAIL := 'Supplied value contains illegal characters.',
                    HINT := 'Correct supplied value, remove all illegal characters.';
END;
$BODY$
LANGUAGE plpgsql
IMMUTABLE
RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION sys.shark_get_int_part(IN p_srcnumber DOUBLE PRECISION)
RETURNS DOUBLE PRECISION
AS
$BODY$
BEGIN
    RETURN CASE
              WHEN (p_srcnumber < -0.0000001) THEN ceil(p_srcnumber - 0.0000001)
              ELSE floor(p_srcnumber + 0.0000001)
           END;
END;
$BODY$
LANGUAGE plpgsql
IMMUTABLE
RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION sys.shark_get_full_year(IN p_short_year TEXT,
                                                           IN p_base_century TEXT DEFAULT '',
                                                           IN p_year_cutoff NUMERIC DEFAULT 49)
RETURNS VARCHAR
AS
$BODY$
DECLARE
    v_err_message VARCHAR;
    v_full_year SMALLINT;
    v_short_year SMALLINT;
    v_base_century SMALLINT;
    v_result_param_set JSON;
    v_full_year_res_json JSON;
BEGIN
    v_short_year := p_short_year::SMALLINT;

    BEGIN
        v_full_year_res_json := nullif(current_setting('sys.full_year_res_json'), '')::JSON;
    EXCEPTION
        WHEN undefined_object THEN
        v_full_year_res_json := NULL;
    END;

    SELECT result
      INTO v_full_year
      FROM json_to_recordset(v_full_year_res_json, true) AS result_set (param1 SMALLINT,
                                                                    param2 TEXT,
                                                                    param3 NUMERIC,
                                                                    result VARCHAR)
     WHERE param1 = v_short_year
       AND param2 = p_base_century
       AND param3 = p_year_cutoff;

    IF (v_full_year IS NULL)
    THEN
        IF (v_short_year <= 99)
        THEN
            v_base_century := CASE
                                 WHEN (p_base_century ~ '^\s*([1-9]{1,2})\s*$') THEN pg_catalog.concat(pg_catalog.btrim(p_base_century), '00')::SMALLINT
                                 ELSE trunc(extract(year from current_date)::NUMERIC, -2)
                              END;

            v_full_year = v_base_century + v_short_year;
            v_full_year = CASE
                             WHEN (v_short_year::NUMERIC > p_year_cutoff) THEN v_full_year - 100
                             ELSE v_full_year
                          END;
        ELSE v_full_year := v_short_year;
        END IF;

        v_result_param_set := json_build_object('param1', v_short_year,
                                                 'param2', p_base_century,
                                                 'param3', p_year_cutoff,
                                                 'result', v_full_year);
        v_full_year_res_json := CASE
                                    WHEN (v_full_year_res_json IS NULL) THEN json_build_array(v_result_param_set)
                                    ELSE v_full_year_res_json || v_result_param_set
                                 END;

        PERFORM set_config('sys.full_year_res_json',
                           v_full_year_res_json::TEXT,
                           FALSE);
    END IF;

    RETURN v_full_year;
EXCEPTION
    WHEN invalid_text_representation THEN
        GET STACKED DIAGNOSTICS v_err_message = MESSAGE_TEXT;
        v_err_message := substring(pg_catalog.lower(v_err_message), 'integer\:\s\"(.*)\"');

        RAISE USING MESSAGE := pg_catalog.format('Error while trying to convert "%s" value to SMALLINT data type.',
                                      v_err_message),
                    DETAIL := 'Supplied value contains illegal characters.',
                    HINT := 'Correct supplied value, remove all illegal characters.';
END;
$BODY$
LANGUAGE plpgsql
IMMUTABLE
RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION sys.shark_get_monthnum_by_name(IN p_monthname TEXT,
                                                                  IN p_lang_metadata_json JSON)
RETURNS VARCHAR
AS
$BODY$
DECLARE
    v_monthname TEXT;
    v_monthnum SMALLINT;
BEGIN
    v_monthname := pg_catalog.lower(pg_catalog.btrim(p_monthname));

    v_monthnum := array_position(ARRAY(SELECT pg_catalog.lower(json_array_elements_text(p_lang_metadata_json -> 'months_shortnames'))), v_monthname);

    v_monthnum := coalesce(v_monthnum,
                           array_position(ARRAY(SELECT pg_catalog.lower(json_array_elements_text(p_lang_metadata_json -> 'months_names'))), v_monthname));

    v_monthnum := coalesce(v_monthnum,
                           array_position(ARRAY(SELECT pg_catalog.lower(json_array_elements_text(p_lang_metadata_json -> 'months_extrashortnames'))), v_monthname));

    v_monthnum := coalesce(v_monthnum,
                           array_position(ARRAY(SELECT pg_catalog.lower(json_array_elements_text(p_lang_metadata_json -> 'months_extranames'))), v_monthname));

    IF (v_monthnum IS NULL) THEN
        RAISE datetime_field_overflow;
    END IF;

    RETURN v_monthnum;
EXCEPTION
    WHEN datetime_field_overflow THEN
        RAISE USING MESSAGE := pg_catalog.format('Can not convert value "%s" to a correct month number.',
                                      pg_catalog.btrim(p_monthname)),
                    DETAIL := 'Supplied month name is not valid.',
                    HINT := 'Correct supplied month name value and try again.';
END;
$BODY$
LANGUAGE plpgsql
IMMUTABLE
RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION sys.shark_get_timeunit_from_string(IN p_timepart TEXT,
                                                                      IN p_timeunit TEXT)
RETURNS VARCHAR
AS
$BODY$
DECLARE
    v_hours VARCHAR COLLATE "C";
    v_minutes VARCHAR COLLATE "C";
    v_seconds VARCHAR COLLATE "C";
    v_fractsecs VARCHAR COLLATE "C";
    v_sign VARCHAR COLLATE "C";
    v_offhours VARCHAR COLLATE "C";
    v_offminutes VARCHAR COLLATE "C";
    v_daypart VARCHAR COLLATE "C";
    v_timepart VARCHAR COLLATE "C";
    v_offset VARCHAR COLLATE "C";
    v_timeunit VARCHAR COLLATE "C";
    v_err_message VARCHAR COLLATE "C";
    v_timeunit_mask VARCHAR COLLATE "C";
    v_regmatch_groups TEXT[];
    AMPM_REGEXP CONSTANT VARCHAR COLLATE "C" := '\s*([AP]M)';
    TIMEUNIT_REGEXP CONSTANT VARCHAR COLLATE "C" := '\s*(\d{1,2})\s*';
    FRACTSECS_REGEXP CONSTANT VARCHAR COLLATE "C" := '\s*(\d{1,9})';
    TIME_OFFSET_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('((\-|\+)', TIMEUNIT_REGEXP, '\:', TIMEUNIT_REGEXP, '|Z)');
    HHMMSSFS_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', TIMEUNIT_REGEXP,
                                               '\:', TIMEUNIT_REGEXP,
                                               '\:', TIMEUNIT_REGEXP,
                                               '(?:\.|\:)', FRACTSECS_REGEXP, '$');
    HHMMSS_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', TIMEUNIT_REGEXP, '\:', TIMEUNIT_REGEXP, '\:', TIMEUNIT_REGEXP, '$');
    HHMMFS_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', TIMEUNIT_REGEXP, '\:', TIMEUNIT_REGEXP, '\.', FRACTSECS_REGEXP, '$');
    HHMM_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', TIMEUNIT_REGEXP, '\:', TIMEUNIT_REGEXP, '$');
    HH_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', TIMEUNIT_REGEXP, '$');
BEGIN
    v_timepart := pg_catalog.upper(pg_catalog.btrim(p_timepart));
    v_timeunit := pg_catalog.upper(pg_catalog.btrim(p_timeunit));

    v_daypart := substring(v_timepart, AMPM_REGEXP);
    v_offset := substring(v_timepart, TIME_OFFSET_REGEXP);
    v_timepart := pg_catalog.btrim(regexp_replace(v_timepart, AMPM_REGEXP, ''));
    v_timepart := pg_catalog.btrim(regexp_replace(v_timepart, TIME_OFFSET_REGEXP, ''));

    v_timeunit_mask :=
        CASE
           WHEN (v_timepart ~* HHMMSSFS_REGEXP) THEN HHMMSSFS_REGEXP
           WHEN (v_timepart ~* HHMMSS_REGEXP) THEN HHMMSS_REGEXP
           WHEN (v_timepart ~* HHMMFS_REGEXP) THEN HHMMFS_REGEXP
           WHEN (v_timepart ~* HHMM_REGEXP) THEN HHMM_REGEXP
           WHEN (v_timepart ~* HH_REGEXP) THEN HH_REGEXP
        END;

    v_regmatch_groups := regexp_matches(v_timepart, v_timeunit_mask, 'gi');

    v_hours := v_regmatch_groups[1];
    v_minutes := v_regmatch_groups[2];

    IF (v_timepart ~* HHMMFS_REGEXP) THEN
        v_fractsecs := v_regmatch_groups[3];
    ELSE
        v_seconds := v_regmatch_groups[3];
        v_fractsecs := v_regmatch_groups[4];
    END IF;

    v_regmatch_groups := regexp_matches(v_offset, TIME_OFFSET_REGEXP, 'gi');

    v_sign := coalesce(v_regmatch_groups[2], '+');
    v_offhours := coalesce(v_regmatch_groups[3], '0');
    v_offminutes := coalesce(v_regmatch_groups[4], '0');

    IF (v_timeunit = 'HOURS' AND v_daypart IS NOT NULL)
    THEN
        IF ((v_daypart = 'AM' AND v_hours::SMALLINT NOT BETWEEN 0 AND 12) OR
            (v_daypart = 'PM' AND v_hours::SMALLINT NOT BETWEEN 1 AND 23))
        THEN
            RAISE numeric_value_out_of_range;
        ELSIF (v_daypart = 'PM' AND v_hours::SMALLINT < 12) THEN
            v_hours := (v_hours::SMALLINT + 12)::VARCHAR;
        ELSIF (v_daypart = 'AM' AND v_hours::SMALLINT = 12) THEN
            v_hours := (v_hours::SMALLINT - 12)::VARCHAR;
        END IF;
    END IF;

    RETURN CASE v_timeunit
              WHEN 'HOURS' THEN v_hours
              WHEN 'MINUTES' THEN v_minutes
              WHEN 'SECONDS' THEN v_seconds
              WHEN 'FRACTSECONDS' THEN v_fractsecs
              WHEN 'OFFHOURS' THEN v_offhours
              WHEN 'OFFMINUTES' THEN v_offminutes
              WHEN 'OFFSIGN' THEN v_sign
           END;
EXCEPTION
    WHEN numeric_value_out_of_range THEN
        RAISE USING MESSAGE := 'Could not extract correct hour value due to it''s inconsistency with AM|PM day part mark.',
                    DETAIL := 'Extracted hour value doesn''t fall in correct day part mark range: 0..12 for "AM" or 1..23 for "PM".',
                    HINT := 'Correct a hour value in the source string or remove AM|PM day part mark out of it.';

    WHEN invalid_text_representation THEN
        GET STACKED DIAGNOSTICS v_err_message = MESSAGE_TEXT;
        v_err_message := substring(pg_catalog.lower(v_err_message), 'integer\:\s\"(.*)\"');

        RAISE USING MESSAGE := pg_catalog.format('Error while trying to convert "%s" value to SMALLINT data type.', v_err_message),
                    DETAIL := 'Supplied value contains illegal characters.',
                    HINT := 'Correct supplied value, remove all illegal characters.';
END;
$BODY$
LANGUAGE plpgsql
IMMUTABLE
RETURNS NULL ON NULL INPUT;

-- datediff implmentation
CREATE OR REPLACE FUNCTION sys.datediff_internal(IN datepart PG_CATALOG.TEXT, IN startdate anyelement, IN enddate anyelement)
RETURNS INT AS
'$libdir/shark', 'shark_timestamp_diff'
STRICT
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.datediff_internal_big(IN datepart PG_CATALOG.TEXT, IN startdate anyelement, IN enddate anyelement)
RETURNS BIGINT AS
'$libdir/shark', 'shark_timestamp_diff_big'
STRICT
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.datediff(IN datepart PG_CATALOG.TEXT, IN startdate PG_CATALOG.date, IN enddate PG_CATALOG.date) RETURNS INTEGER
AS
$body$
BEGIN
    return sys.datediff_internal(datepart, startdate::TIMESTAMP, enddate::TIMESTAMP);
END
$body$
LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.datediff(IN datepart PG_CATALOG.TEXT, IN startdate PG_CATALOG.timestamp, IN enddate PG_CATALOG.timestamp) RETURNS INTEGER
AS
$body$
BEGIN
    return sys.datediff_internal(datepart, startdate, enddate);
END
$body$
LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.datediff(IN datepart PG_CATALOG.TEXT, IN startdate PG_CATALOG.timestamptz, IN enddate PG_CATALOG.timestamptz) RETURNS INTEGER
AS
$body$
BEGIN
    return sys.datediff_internal(datepart, startdate::TIMESTAMP, enddate::TIMESTAMP);
END
$body$
LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.datediff(IN datepart PG_CATALOG.TEXT, IN startdate PG_CATALOG.smalldatetime, IN enddate PG_CATALOG.smalldatetime) RETURNS INTEGER
AS
$body$
BEGIN
    return sys.datediff_internal(datepart, startdate::TIMESTAMP, enddate::TIMESTAMP);
END
$body$
LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.datediff(IN datepart PG_CATALOG.TEXT, IN startdate PG_CATALOG.time, IN enddate PG_CATALOG.time) RETURNS INTEGER
AS
$body$
BEGIN
    return sys.datediff_internal(datepart, startdate, enddate);
END
$body$
LANGUAGE plpgsql IMMUTABLE;

-- datediff_big implmentation
CREATE OR REPLACE FUNCTION sys.datediff_big(IN datepart PG_CATALOG.TEXT, IN startdate PG_CATALOG.date, IN enddate PG_CATALOG.date) RETURNS BIGINT
AS
$body$
BEGIN
    return sys.datediff_internal_big(datepart, startdate::TIMESTAMP, enddate::TIMESTAMP);
END
$body$
LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.datediff_big(IN datepart PG_CATALOG.TEXT, IN startdate PG_CATALOG.time, IN enddate PG_CATALOG.time) RETURNS BIGINT
AS
$body$
BEGIN
    return sys.datediff_internal_big(datepart, startdate, enddate);
END
$body$
LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.datediff_big(IN datepart PG_CATALOG.TEXT, IN startdate PG_CATALOG.timestamp, IN enddate PG_CATALOG.timestamp) RETURNS INTEGER
AS
$body$
BEGIN
    return sys.datediff_internal_big(datepart, startdate, enddate);
END
$body$
LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.datediff_big(IN datepart PG_CATALOG.TEXT, IN startdate PG_CATALOG.timestamptz, IN enddate PG_CATALOG.timestamptz) RETURNS INTEGER
AS
$body$
BEGIN
    return sys.datediff_internal_big(datepart, startdate::TIMESTAMP, enddate::TIMESTAMP);
END
$body$
LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.datediff_big(IN datepart PG_CATALOG.TEXT, IN startdate PG_CATALOG.smalldatetime, IN enddate PG_CATALOG.smalldatetime) RETURNS INTEGER
AS
$body$
BEGIN
    return sys.datediff_internal_big(datepart, startdate::TIMESTAMP, enddate::TIMESTAMP);
END
$body$
LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.shark_get_lang_metadata_json(IN p_lang_spec_culture TEXT)
RETURNS JSON
AS
$BODY$
DECLARE
    v_locale_parts TEXT[] COLLATE "C";
    v_lang_data_json JSON;
    v_lang_spec_culture VARCHAR COLLATE "C";
    v_is_cached BOOLEAN := FALSE;
BEGIN
    v_lang_spec_culture := pg_catalog.upper(pg_catalog.btrim(p_lang_spec_culture));

    IF (char_length(v_lang_spec_culture) > 0)
    THEN
        BEGIN
            v_lang_data_json := nullif(current_setting(format('sys.lang_metadata_json.%s',
                                                               v_lang_spec_culture)), '')::JSON;
        EXCEPTION
            WHEN undefined_object THEN
            v_lang_data_json := NULL;
        END;

        IF (v_lang_data_json IS NULL)
        THEN
            v_lang_spec_culture := pg_catalog.upper(regexp_replace(v_lang_spec_culture, '-\s*', '_', 'gi'));
            IF (v_lang_spec_culture IN ('AR', 'FI') OR
                v_lang_spec_culture ~ '_')
            THEN
                SELECT lang_data_json
                  INTO STRICT v_lang_data_json
                  FROM sys.shark_syslanguages
                 WHERE spec_culture = v_lang_spec_culture;
            ELSE
                SELECT lang_data_json
                  INTO STRICT v_lang_data_json
                  FROM sys.shark_syslanguages
                 WHERE lang_name_mssql = v_lang_spec_culture
                    OR lang_alias_mssql = v_lang_spec_culture;
            END IF;
        ELSE
            v_is_cached := TRUE;
        END IF;
    ELSE
        v_lang_spec_culture := current_setting('LC_TIME');

        v_lang_spec_culture := CASE
                                  WHEN (v_lang_spec_culture !~ '\.') THEN v_lang_spec_culture
                                  ELSE substring(v_lang_spec_culture, '(.*)(?:\.)')
                               END;

        v_lang_spec_culture := pg_catalog.upper(regexp_replace(v_lang_spec_culture, ',\s*', '_', 'gi'));

        BEGIN
            v_lang_data_json := nullif(current_setting(format('sys.lang_metadata_json.%s',
                                                               v_lang_spec_culture)), '')::JSON;
        EXCEPTION
            WHEN undefined_object THEN
            v_lang_data_json := NULL;
        END;

        IF (v_lang_data_json IS NULL)
        THEN
            BEGIN
                IF (char_length(v_lang_spec_culture) = 5)
                THEN
                    SELECT lang_data_json
                      INTO STRICT v_lang_data_json
                      FROM sys.shark_syslanguages
                     WHERE spec_culture = v_lang_spec_culture;
                ELSE
                    v_locale_parts := string_to_array(v_lang_spec_culture, '-');

                    SELECT lang_data_json
                      INTO STRICT v_lang_data_json
                      FROM sys.shark_syslanguages
                     WHERE lang_name_pg = v_locale_parts[1]
                       AND territory = v_locale_parts[2];
                END IF;
            EXCEPTION
                WHEN OTHERS THEN
                    v_lang_spec_culture := 'EN_US';

                    SELECT lang_data_json
                      INTO v_lang_data_json
                      FROM sys.shark_syslanguages
                     WHERE spec_culture = v_lang_spec_culture;
            END;
        ELSE
            v_is_cached := TRUE;
        END IF;
    END IF;

    IF (NOT v_is_cached) THEN
        BEGIN
            PERFORM set_config(format('sys.lang_metadata_json.%s',
                                                v_lang_spec_culture),
                                        v_lang_data_json::TEXT,
                                        FALSE);
        EXCEPTION
            WHEN invalid_transaction_state THEN
            -- This exception will only occur when we are trying to set config in parallel mode
            -- we can ignore this error as we cannot store this config during a parallel operation
        END;
    END IF;

    RETURN v_lang_data_json;
EXCEPTION
    WHEN invalid_text_representation THEN
        RAISE USING MESSAGE := pg_catalog.format('The language metadata JSON value extracted from chache is not a valid JSON object.',
                                      p_lang_spec_culture),
                    HINT := 'Drop the current session, fix the appropriate record in "sys.shark_syslanguages" table, and try again after reconnection.';

    WHEN OTHERS THEN
        RAISE USING MESSAGE := pg_catalog.format('"%s" is not a valid special culture or language name parameter.',
                                      p_lang_spec_culture),
                    DETAIL := 'Use of incorrect "lang_spec_culture" parameter value during conversion process.',
                    HINT := 'Change "lang_spec_culture" parameter to the proper value and try again.';
END;
$BODY$
LANGUAGE plpgsql
STABLE;

-- CAST and related functions.
-- Duplicate functions with arg TEXT since ANYELEMNT cannot handle type unknown.

CREATE OR REPLACE FUNCTION sys.shark_cast_floor_smallint(IN arg TEXT)
RETURNS SMALLINT
AS $BODY$ BEGIN
    RETURN CAST(arg AS SMALLINT);
END; $BODY$
LANGUAGE plpgsql
STABLE;


CREATE OR REPLACE FUNCTION sys.shark_cast_floor_smallint(IN arg ANYELEMENT)
RETURNS SMALLINT
AS $BODY$ BEGIN
    CASE pg_typeof(arg)
        WHEN 'numeric'::regtype, 'double precision'::regtype, 'real'::regtype THEN
            RETURN CAST(TRUNC(arg) AS SMALLINT);
        ELSE
            RETURN CAST(arg AS SMALLINT);
    END CASE;
END; $BODY$
LANGUAGE plpgsql
STABLE;

CREATE OR REPLACE FUNCTION sys.shark_cast_floor_int(IN arg TEXT)
RETURNS INT
AS $BODY$ BEGIN
    RETURN CAST(arg AS INT);
END; $BODY$
LANGUAGE plpgsql
STABLE;


CREATE OR REPLACE FUNCTION sys.shark_cast_floor_int(IN arg ANYELEMENT)
RETURNS INT
AS $BODY$ BEGIN
    CASE pg_typeof(arg)
        WHEN 'numeric'::regtype, 'double precision'::regtype, 'real'::regtype THEN
            RETURN CAST(TRUNC(arg) AS INT);
        ELSE
            RETURN CAST(arg AS INT);
    END CASE;
END; $BODY$
LANGUAGE plpgsql
STABLE;

CREATE OR REPLACE FUNCTION sys.shark_cast_floor_bigint(IN arg TEXT)
RETURNS BIGINT
AS $BODY$ BEGIN
    RETURN CAST(arg AS BIGINT);
END; $BODY$
LANGUAGE plpgsql
STABLE;


CREATE OR REPLACE FUNCTION sys.shark_cast_floor_bigint(IN arg ANYELEMENT)
RETURNS BIGINT
AS $BODY$ BEGIN
    CASE pg_typeof(arg)
        WHEN 'numeric'::regtype, 'double precision'::regtype, 'real'::regtype THEN
            RETURN CAST(TRUNC(arg) AS BIGINT);
        ELSE
            RETURN CAST(arg AS BIGINT);
    END CASE;
END; $BODY$
LANGUAGE plpgsql
STABLE;

-- TRY_CAST helper functions
CREATE OR REPLACE FUNCTION sys.shark_try_cast_floor_smallint(IN arg TEXT) RETURNS SMALLINT
AS $BODY$ BEGIN
    RETURN sys.shark_cast_floor_smallint(arg);
    EXCEPTION WHEN OTHERS THEN RETURN NULL;
END; $BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION sys.shark_try_cast_floor_smallint(IN arg ANYELEMENT) RETURNS SMALLINT
AS $BODY$ BEGIN
    RETURN sys.shark_cast_floor_smallint(arg);
    EXCEPTION WHEN OTHERS THEN RETURN NULL;
END; $BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION sys.shark_try_cast_floor_int(IN arg TEXT) RETURNS INT
AS $BODY$ BEGIN
    RETURN sys.shark_cast_floor_int(arg);
    EXCEPTION WHEN OTHERS THEN RETURN NULL;
END; $BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION sys.shark_try_cast_floor_int(IN arg ANYELEMENT) RETURNS INT
AS $BODY$ BEGIN
    RETURN sys.shark_cast_floor_int(arg);
    EXCEPTION WHEN OTHERS THEN RETURN NULL;
END; $BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION sys.shark_try_cast_floor_bigint(IN arg TEXT) RETURNS BIGINT
AS $BODY$ BEGIN
    RETURN sys.shark_cast_floor_bigint(arg);
    EXCEPTION WHEN OTHERS THEN RETURN NULL;
END; $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION sys.shark_try_cast_floor_bigint(IN arg ANYELEMENT) RETURNS BIGINT
AS $BODY$ BEGIN
    RETURN sys.shark_cast_floor_bigint(arg);
    EXCEPTION WHEN OTHERS THEN RETURN NULL;
END; $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION sys.shark_try_cast_to_any(IN arg SMALLINT, INOUT output ANYELEMENT, IN typmod INT)
RETURNS ANYELEMENT
AS $BODY$ BEGIN
    EXECUTE pg_catalog.format('SELECT CAST(CAST(%L AS %s) AS %s)', arg, format_type(pg_typeof(arg), NULL), format_type(pg_typeof(output), typmod)) INTO output;
    EXCEPTION
        WHEN cannot_coerce THEN
            RAISE USING MESSAGE := pg_catalog.format('cannot cast type %s to %s.', pg_typeof(arg),
                                      pg_typeof(output));
        WHEN OTHERS THEN
            -- Do nothing. Output carries NULL.
END; $BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION sys.shark_try_cast_to_any(IN arg TINYINT, INOUT output ANYELEMENT, IN typmod INT)
RETURNS ANYELEMENT
AS $BODY$ BEGIN
    EXECUTE pg_catalog.format('SELECT CAST(CAST(%L AS %s) AS %s)', arg, format_type(pg_typeof(arg), NULL), format_type(pg_typeof(output), typmod)) INTO output;
    EXCEPTION
        WHEN cannot_coerce THEN
            RAISE USING MESSAGE := pg_catalog.format('cannot cast type %s to %s.', pg_typeof(arg),
                                      pg_typeof(output));
        WHEN OTHERS THEN
            -- Do nothing. Output carries NULL.
END; $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION sys.shark_try_cast_to_any(IN arg INT, INOUT output ANYELEMENT, IN typmod INT)
RETURNS ANYELEMENT
AS $BODY$ BEGIN
    EXECUTE pg_catalog.format('SELECT CAST(CAST(%L AS %s) AS %s)', arg, format_type(pg_typeof(arg), NULL), format_type(pg_typeof(output), typmod)) INTO output;
    EXCEPTION
        WHEN cannot_coerce THEN
            RAISE USING MESSAGE := pg_catalog.format('cannot cast type %s to %s.', pg_typeof(arg),
                                      pg_typeof(output));
        WHEN OTHERS THEN
            -- Do nothing. Output carries NULL.
END; $BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION sys.shark_try_cast_to_any(IN arg BIGINT, INOUT output ANYELEMENT, IN typmod INT)
RETURNS ANYELEMENT
AS $BODY$ BEGIN
    EXECUTE pg_catalog.format('SELECT CAST(CAST(%L AS %s) AS %s)', arg, format_type(pg_typeof(arg), NULL), format_type(pg_typeof(output), typmod)) INTO output;
    EXCEPTION
        WHEN cannot_coerce THEN
            RAISE USING MESSAGE := pg_catalog.format('cannot cast type %s to %s.', pg_typeof(arg),
                                      pg_typeof(output));
        WHEN OTHERS THEN
            -- Do nothing. Output carries NULL.
END; $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION sys.shark_try_cast_to_any(IN arg NUMERIC, INOUT output ANYELEMENT, IN typmod INT)
RETURNS ANYELEMENT
AS $BODY$ BEGIN
    EXECUTE pg_catalog.format('SELECT CAST(CAST(%L AS %s) AS %s)', arg, format_type(pg_typeof(arg), NULL), format_type(pg_typeof(output), typmod)) INTO output;
    EXCEPTION
        WHEN cannot_coerce THEN
            RAISE USING MESSAGE := pg_catalog.format('cannot cast type %s to %s.', pg_typeof(arg),
                                      pg_typeof(output));
        WHEN OTHERS THEN
            -- Do nothing. Output carries NULL.
END; $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION sys.shark_try_cast_to_any(IN arg TEXT, INOUT output ANYELEMENT, IN typmod INT)
RETURNS ANYELEMENT
AS $BODY$ BEGIN
    EXECUTE pg_catalog.format('SELECT CAST(CAST(%L AS %s) AS %s)', arg, format_type(pg_typeof(arg), NULL), format_type(pg_typeof(output), typmod)) INTO output;
    EXCEPTION
        WHEN cannot_coerce THEN
            RAISE USING MESSAGE := pg_catalog.format('cannot cast type %s to %s.', pg_typeof(arg),
                                      pg_typeof(output));
        WHEN OTHERS THEN
            -- Do nothing. Output carries NULL.
END; $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION sys.shark_try_cast_to_any(IN arg VARCHAR, INOUT output ANYELEMENT, IN typmod INT)
RETURNS ANYELEMENT
AS $BODY$ BEGIN
    EXECUTE pg_catalog.format('SELECT CAST(CAST(%L AS %s) AS %s)', arg, format_type(pg_typeof(arg), NULL), format_type(pg_typeof(output), typmod)) INTO output;
    EXCEPTION
        WHEN cannot_coerce THEN
            RAISE USING MESSAGE := pg_catalog.format('cannot cast type %s to %s.', pg_typeof(arg),
                                      pg_typeof(output));
        WHEN OTHERS THEN
END; $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION sys.shark_try_cast_to_any(IN arg BPCHAR, INOUT output ANYELEMENT, IN typmod INT)
RETURNS ANYELEMENT
AS $BODY$ BEGIN
    EXECUTE pg_catalog.format('SELECT CAST(CAST(%L AS %s) AS %s)', arg, format_type(pg_typeof(arg), NULL), format_type(pg_typeof(output), typmod)) INTO output;
    EXCEPTION
        WHEN cannot_coerce THEN
            RAISE USING MESSAGE := pg_catalog.format('cannot cast type %s to %s.', pg_typeof(arg),
                                      pg_typeof(output));
        WHEN OTHERS THEN
END; $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION sys.shark_try_cast_to_any(IN arg NVARCHAR2, INOUT output ANYELEMENT, IN typmod INT)
RETURNS ANYELEMENT
AS $BODY$ BEGIN
    EXECUTE pg_catalog.format('SELECT CAST(CAST(%L AS %s) AS %s)', arg, format_type(pg_typeof(arg), NULL), format_type(pg_typeof(output), typmod)) INTO output;
    EXCEPTION
        WHEN cannot_coerce THEN
            RAISE USING MESSAGE := pg_catalog.format('cannot cast type %s to %s.', pg_typeof(arg),
                                      pg_typeof(output));
        WHEN OTHERS THEN
END; $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION sys.shark_try_cast_to_any(IN arg CHAR, INOUT output ANYELEMENT, IN typmod INT)
RETURNS ANYELEMENT
AS $BODY$ BEGIN
    EXECUTE pg_catalog.format('SELECT CAST(CAST(%L AS %s) AS %s)', arg, format_type(pg_typeof(arg), NULL), format_type(pg_typeof(output), typmod)) INTO output;
    EXCEPTION
        WHEN cannot_coerce THEN
            RAISE USING MESSAGE := pg_catalog.format('cannot cast type %s to %s.', pg_typeof(arg),
                                      pg_typeof(output));
        WHEN OTHERS THEN
END; $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION sys.shark_try_cast_to_any(IN arg REAL, INOUT output ANYELEMENT, IN typmod INT)
RETURNS ANYELEMENT
AS $BODY$ BEGIN
    EXECUTE pg_catalog.format('SELECT CAST(CAST(%L AS %s) AS %s)', arg, format_type(pg_typeof(arg), NULL), format_type(pg_typeof(output), typmod)) INTO output;
    EXCEPTION
        WHEN cannot_coerce THEN
            RAISE USING MESSAGE := pg_catalog.format('cannot cast type %s to %s.', pg_typeof(arg),
                                      pg_typeof(output));
        WHEN OTHERS THEN
END; $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION sys.shark_try_cast_to_any(IN arg DOUBLE PRECISION, INOUT output ANYELEMENT, IN typmod INT)
RETURNS ANYELEMENT
AS $BODY$ BEGIN
    EXECUTE pg_catalog.format('SELECT CAST(CAST(%L AS %s) AS %s)', arg, format_type(pg_typeof(arg), NULL), format_type(pg_typeof(output), typmod)) INTO output;
    EXCEPTION
        WHEN cannot_coerce THEN
            RAISE USING MESSAGE := pg_catalog.format('cannot cast type %s to %s.', pg_typeof(arg),
                                      pg_typeof(output));
        WHEN OTHERS THEN
END; $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION sys.shark_try_cast_to_any(IN arg BOOL, INOUT output ANYELEMENT, IN typmod INT)
RETURNS ANYELEMENT
AS $BODY$ BEGIN
    EXECUTE pg_catalog.format('SELECT CAST(CAST(%L AS %s) AS %s)', arg, format_type(pg_typeof(arg), NULL), format_type(pg_typeof(output), typmod)) INTO output;
    EXCEPTION
        WHEN cannot_coerce THEN
            RAISE USING MESSAGE := pg_catalog.format('cannot cast type %s to %s.', pg_typeof(arg),
                                      pg_typeof(output));
        WHEN OTHERS THEN
END; $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION sys.shark_try_cast_to_any(IN arg TIME, INOUT output ANYELEMENT, IN typmod INT)
RETURNS ANYELEMENT
AS $BODY$ BEGIN
    EXECUTE pg_catalog.format('SELECT CAST(CAST(%L AS %s) AS %s)', arg, format_type(pg_typeof(arg), NULL), format_type(pg_typeof(output), typmod)) INTO output;
    EXCEPTION
        WHEN cannot_coerce THEN
            RAISE USING MESSAGE := pg_catalog.format('cannot cast type %s to %s.', pg_typeof(arg),
                                      pg_typeof(output));
        WHEN OTHERS THEN
END; $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION sys.shark_try_cast_to_any(IN arg TIMETZ, INOUT output ANYELEMENT, IN typmod INT)
RETURNS ANYELEMENT
AS $BODY$ BEGIN
    EXECUTE pg_catalog.format('SELECT CAST(CAST(%L AS %s) AS %s)', arg, format_type(pg_typeof(arg), NULL), format_type(pg_typeof(output), typmod)) INTO output;
    EXCEPTION
        WHEN cannot_coerce THEN
            RAISE USING MESSAGE := pg_catalog.format('cannot cast type %s to %s.', pg_typeof(arg),
                                      pg_typeof(output));
        WHEN OTHERS THEN
END; $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION sys.shark_try_cast_to_any(IN arg TIMESTAMP, INOUT output ANYELEMENT, IN typmod INT)
RETURNS ANYELEMENT
AS $BODY$ BEGIN
    EXECUTE pg_catalog.format('SELECT CAST(CAST(%L AS %s) AS %s)', arg, format_type(pg_typeof(arg), NULL), format_type(pg_typeof(output), typmod)) INTO output;
    EXCEPTION
        WHEN cannot_coerce THEN
            RAISE USING MESSAGE := pg_catalog.format('cannot cast type %s to %s.', pg_typeof(arg),
                                      pg_typeof(output));
        WHEN OTHERS THEN
END; $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION sys.shark_try_cast_to_any(IN arg TIMESTAMPTZ, INOUT output ANYELEMENT, IN typmod INT)
RETURNS ANYELEMENT
AS $BODY$ BEGIN
    EXECUTE pg_catalog.format('SELECT CAST(CAST(%L AS %s) AS %s)', arg, format_type(pg_typeof(arg), NULL), format_type(pg_typeof(output), typmod)) INTO output;
    EXCEPTION
        WHEN cannot_coerce THEN
            RAISE USING MESSAGE := pg_catalog.format('cannot cast type %s to %s.', pg_typeof(arg),
                                      pg_typeof(output));
        WHEN OTHERS THEN
END; $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION sys.shark_try_cast_to_any(IN arg DATE, INOUT output ANYELEMENT, IN typmod INT)
RETURNS ANYELEMENT
AS $BODY$ BEGIN
    EXECUTE pg_catalog.format('SELECT CAST(CAST(%L AS %s) AS %s)', arg, format_type(pg_typeof(arg), NULL), format_type(pg_typeof(output), typmod)) INTO output;
    EXCEPTION
        WHEN cannot_coerce THEN
            RAISE USING MESSAGE := pg_catalog.format('cannot cast type %s to %s.', pg_typeof(arg),
                                      pg_typeof(output));
        WHEN OTHERS THEN
END; $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION sys.shark_try_cast_to_any(IN arg SMALLDATETIME, INOUT output ANYELEMENT, IN typmod INT)
RETURNS ANYELEMENT
AS $BODY$ BEGIN
    EXECUTE pg_catalog.format('SELECT CAST(CAST(%L AS %s) AS %s)', arg, format_type(pg_typeof(arg), NULL), format_type(pg_typeof(output), typmod)) INTO output;
    EXCEPTION
        WHEN cannot_coerce THEN
            RAISE USING MESSAGE := pg_catalog.format('cannot cast type %s to %s.', pg_typeof(arg),
                                      pg_typeof(output));
        WHEN OTHERS THEN
END; $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION sys.shark_conv_date_to_string(IN p_datatype TEXT,
                                                                 IN p_dateval DATE,
                                                                 IN p_style NUMERIC DEFAULT 20)
RETURNS TEXT
AS
$BODY$
DECLARE
    v_day VARCHAR COLLATE "C";
    v_dateval DATE;
    v_style SMALLINT;
    v_month SMALLINT;
    v_resmask VARCHAR COLLATE "C";
    v_datatype VARCHAR COLLATE "C";
    v_language VARCHAR COLLATE "C";
    v_monthname VARCHAR COLLATE "C";
    v_resstring VARCHAR COLLATE "C";
    v_lengthexpr VARCHAR COLLATE "C";
    v_maxlength SMALLINT;
    v_res_length SMALLINT;
    v_err_message VARCHAR COLLATE "C";
    v_res_datatype VARCHAR COLLATE "C";
    v_lang_metadata_json JSON;
    VARCHAR_MAX CONSTANT SMALLINT := 8000;
    NVARCHAR_MAX CONSTANT SMALLINT := 4000;
    CONVERSION_LANG CONSTANT VARCHAR COLLATE "C" := '';
    DATATYPE_REGEXP CONSTANT VARCHAR COLLATE "C" := '^\s*(CHAR|BPCHAR|NCHAR|CHARACTER|NVARCHAR|NVARCHAR2|VARCHAR|CHARACTER VARYING)\s*$';
    DATATYPE_MASK_REGEXP CONSTANT VARCHAR COLLATE "C" := '^\s*(?:CHAR|BPCHAR|NCHAR|CHARACTER|NVARCHAR|NVARCHAR2|VARCHAR|CHARACTER VARYING)\s*\(\s*(\d+|MAX)\s*\)\s*$';
BEGIN
    v_datatype := pg_catalog.upper(pg_catalog.btrim(p_datatype));
    v_style := floor(p_style)::SMALLINT;

    IF (scale(p_style) > 0) THEN
        RAISE most_specific_type_mismatch;
    ELSIF (NOT ((v_style BETWEEN 0 AND 13) OR
                (v_style BETWEEN 20 AND 25) OR
                (v_style BETWEEN 100 AND 113) OR
                v_style IN (120, 121, 126, 127, 130, 131)))
    THEN
        RAISE invalid_parameter_value;
    ELSIF (v_style IN (8, 24, 108)) THEN
        RAISE invalid_datetime_format;
    END IF;

    IF (v_datatype ~* DATATYPE_MASK_REGEXP) THEN
        v_res_datatype := PG_CATALOG.rtrim(split_part(v_datatype, '(', 1));
        v_maxlength := CASE
                          WHEN (v_res_datatype IN ('CHAR', 'VARCHAR')) THEN VARCHAR_MAX
                          ELSE NVARCHAR_MAX
                       END;

        v_lengthexpr := substring(v_datatype, DATATYPE_MASK_REGEXP);

        IF (v_lengthexpr <> 'MAX' AND char_length(v_lengthexpr) > 4) THEN
            RAISE interval_field_overflow;
        END IF;

        v_res_length := CASE v_lengthexpr
                           WHEN 'MAX' THEN v_maxlength
                           ELSE v_lengthexpr::SMALLINT
                        END;
    ELSIF (v_datatype ~* DATATYPE_REGEXP) THEN
        v_res_datatype := v_datatype;
    ELSE
        RAISE datatype_mismatch;
    END IF;

    v_dateval := CASE
                    WHEN (v_style NOT IN (130, 131)) THEN p_dateval
                    ELSE sys.shark_conv_greg_to_hijri(p_dateval) + 1
                 END;

    v_day := PG_CATALOG.ltrim(to_char(v_dateval, 'DD'), '0');
    v_month := to_char(v_dateval, 'MM')::SMALLINT;

    v_language := CASE
                     WHEN (v_style IN (130, 131)) THEN 'HIJRI'
                     ELSE CONVERSION_LANG
                  END;
    BEGIN
        v_lang_metadata_json := sys.shark_get_lang_metadata_json(v_language);
    EXCEPTION
        WHEN OTHERS THEN
        RAISE invalid_character_value_for_cast;
    END;

    v_monthname := (v_lang_metadata_json -> 'months_shortnames') ->> v_month - 1;

    v_resmask := CASE
                    WHEN (v_style IN (1, 22)) THEN 'MM/DD/YY'
                    WHEN (v_style = 101) THEN 'MM/DD/YYYY'
                    WHEN (v_style = 2) THEN 'YY.MM.DD'
                    WHEN (v_style = 102) THEN 'YYYY.MM.DD'
                    WHEN (v_style = 3) THEN 'DD/MM/YY'
                    WHEN (v_style = 103) THEN 'DD/MM/YYYY'
                    WHEN (v_style = 4) THEN 'DD.MM.YY'
                    WHEN (v_style = 104) THEN 'DD.MM.YYYY'
                    WHEN (v_style = 5) THEN 'DD-MM-YY'
                    WHEN (v_style = 105) THEN 'DD-MM-YYYY'
                    WHEN (v_style = 6) THEN 'DD $mnme$ YY'
                    WHEN (v_style IN (13, 106, 113)) THEN 'DD $mnme$ YYYY'
                    WHEN (v_style = 7) THEN '$mnme$ DD, YY'
                    WHEN (v_style = 107) THEN '$mnme$ DD, YYYY'
                    WHEN (v_style = 10) THEN 'MM-DD-YY'
                    WHEN (v_style = 110) THEN 'MM-DD-YYYY'
                    WHEN (v_style = 11) THEN 'YY/MM/DD'
                    WHEN (v_style = 111) THEN 'YYYY/MM/DD'
                    WHEN (v_style = 12) THEN 'YYMMDD'
                    WHEN (v_style = 112) THEN 'YYYYMMDD'
                    WHEN (v_style IN (20, 21, 23, 25, 120, 121, 126, 127)) THEN 'YYYY-MM-DD'
                    WHEN (v_style = 130) THEN 'DD $mnme$ YYYY'
                    WHEN (v_style = 131) THEN pg_catalog.format('%s/MM/YYYY', lpad(v_day, 2, ' '))
                    WHEN (v_style IN (0, 9, 100, 109)) THEN pg_catalog.format('$mnme$ %s YYYY', lpad(v_day, 2, ' '))
                 END;

    v_resstring := to_char(v_dateval, v_resmask);
    v_resstring := pg_catalog.replace(v_resstring, '$mnme$', v_monthname);
    v_resstring := substring(v_resstring, 1, coalesce(v_res_length, char_length(v_resstring)));
    v_res_length := coalesce(v_res_length,
                             CASE v_res_datatype
                                WHEN 'CHAR' THEN 30
                                ELSE 60
                             END);
    RETURN CASE
              WHEN (v_res_datatype NOT IN ('CHAR', 'NCHAR')) THEN v_resstring
              ELSE rpad(v_resstring, v_res_length, ' ')
           END;
EXCEPTION
    WHEN most_specific_type_mismatch THEN
        RAISE USING MESSAGE := 'Argument data type NUMERIC is invalid for argument 3 of convert function.',
                    DETAIL := 'Use of incorrect "style" parameter value during conversion process.',
                    HINT := 'Change "style" parameter to the proper value and try again.';

    WHEN invalid_parameter_value THEN
        RAISE USING MESSAGE := pg_catalog.format('%s is not a valid style number when converting from DATE to a character string.', v_style),
                    DETAIL := 'Use of incorrect "style" parameter value during conversion process.',
                    HINT := 'Change "style" parameter to the proper value and try again.';

    WHEN invalid_datetime_format THEN
        RAISE USING MESSAGE := pg_catalog.format('Error converting data type DATE to %s.', pg_catalog.btrim(p_datatype)),
                    DETAIL := 'Incorrect using of pair of input parameters values during conversion process.',
                    HINT := 'Check the input parameters values, correct them if needed, and try again.';

   WHEN interval_field_overflow THEN
       RAISE USING MESSAGE := pg_catalog.format('The size (%s) given to the convert specification ''%s'' exceeds the maximum allowed for any data type (%s).',
                                     v_lengthexpr,
                                     pg_catalog.lower(v_res_datatype),
                                     v_maxlength),
                   DETAIL := 'Use of incorrect size value of data type parameter during conversion process.',
                   HINT := 'Change size component of data type parameter to the allowable value and try again.';

    WHEN datatype_mismatch THEN
        RAISE USING MESSAGE := 'Data type should be one of these values: ''CHAR(n|MAX)'', ''NCHAR(n|MAX)'', ''VARCHAR(n|MAX)'', ''NVARCHAR(n|MAX)''.',
                    DETAIL := 'Use of incorrect "datatype" parameter value during conversion process.',
                    HINT := 'Change "datatype" parameter to the proper value and try again.';

    WHEN invalid_character_value_for_cast THEN
        RAISE USING MESSAGE := pg_catalog.format('Invalid CONVERSION_LANG constant value - ''%s''. Allowed values are: ''English'', ''Deutsch'', etc.',
                                      CONVERSION_LANG),
                    DETAIL := 'Compiled incorrect CONVERSION_LANG constant value in function''s body.',
                    HINT := 'Correct CONVERSION_LANG constant value in function''s body, recompile it and try again.';

    WHEN invalid_text_representation THEN
        GET STACKED DIAGNOSTICS v_err_message = MESSAGE_TEXT;
        v_err_message := substring(pg_catalog.lower(v_err_message), 'integer\:\s\"(.*)\"');

        RAISE USING MESSAGE := pg_catalog.format('Error while trying to convert "%s" value to SMALLINT (or INTEGER) data type.',
                                      v_err_message),
                    DETAIL := 'Supplied value contains illegal characters.',
                    HINT := 'Correct supplied value, remove all illegal characters.';
END;
$BODY$
LANGUAGE plpgsql
STABLE
RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION sys.shark_conv_time_to_string(IN p_datatype TEXT,
                                                                 IN p_src_datatype TEXT,
                                                                 IN p_timeval TIME(6) WITHOUT TIME ZONE,
                                                                 IN p_style NUMERIC DEFAULT 25)
RETURNS TEXT
AS
$BODY$
DECLARE
    v_hours VARCHAR COLLATE "C";
    v_style SMALLINT;
    v_scale SMALLINT;
    v_resmask VARCHAR COLLATE "C";
    v_fseconds VARCHAR COLLATE "C";
    v_datatype VARCHAR COLLATE "C";
    v_resstring VARCHAR COLLATE "C";
    v_lengthexpr VARCHAR COLLATE "C";
    v_res_length SMALLINT;
    v_res_datatype VARCHAR COLLATE "C";
    v_src_datatype VARCHAR COLLATE "C";
    v_res_maxlength SMALLINT;
    VARCHAR_MAX CONSTANT SMALLINT := 8000;
    NVARCHAR_MAX CONSTANT SMALLINT := 4000;
    -- We use the regex below to make sure input p_datatype is one of them
    DATATYPE_REGEXP CONSTANT VARCHAR COLLATE "C" := '^\s*(CHAR|BPCHAR|NCHAR|CHARACTER|NVARCHAR|NVARCHAR2|VARCHAR|CHARACTER VARYING)\s*$';
    -- We use the regex below to get the length of the datatype, if specified
    -- For example, to get the '10' out of 'varchar(10)'
    DATATYPE_MASK_REGEXP CONSTANT VARCHAR COLLATE "C" := '^\s*(?:CHAR|BPCHAR|NCHAR|CHARACTER|NVARCHAR|NVARCHAR2|VARCHAR|CHARACTER VARYING)\s*\(\s*(\d+|MAX)\s*\)\s*$';
    SRCDATATYPE_MASK_REGEXP VARCHAR COLLATE "C" := '^\s*(?:TIME)\s*(?:\s*\(\s*(\d+)\s*\)\s*)?\s*$';
BEGIN
    v_datatype := pg_catalog.upper(pg_catalog.btrim(p_datatype));
    v_src_datatype := pg_catalog.upper(pg_catalog.btrim(p_src_datatype));
    v_style := floor(p_style)::SMALLINT;

    IF (v_src_datatype ~* SRCDATATYPE_MASK_REGEXP)
    THEN
        v_scale := coalesce(substring(v_src_datatype, SRCDATATYPE_MASK_REGEXP)::SMALLINT, 7);
        IF (v_scale NOT BETWEEN 0 AND 7) THEN
            RAISE invalid_regular_expression;
        END IF;
    ELSE
        RAISE most_specific_type_mismatch;
    END IF;

    IF (v_datatype ~* DATATYPE_MASK_REGEXP)
    THEN
        v_res_datatype := PG_CATALOG.rtrim(split_part(v_datatype, '(', 1));

        v_res_maxlength := CASE
                              WHEN (v_res_datatype IN ('CHAR', 'VARCHAR')) THEN VARCHAR_MAX
                              ELSE NVARCHAR_MAX
                           END;

        v_lengthexpr := substring(v_datatype, DATATYPE_MASK_REGEXP);

        IF (v_lengthexpr <> 'MAX' AND char_length(v_lengthexpr) > 4) THEN
            RAISE interval_field_overflow;
        END IF;

        v_res_length := CASE v_lengthexpr
                           WHEN 'MAX' THEN v_res_maxlength
                           ELSE v_lengthexpr::SMALLINT
                        END;
    ELSIF (v_datatype ~* DATATYPE_REGEXP) THEN
        v_res_datatype := v_datatype;
    ELSE
        RAISE datatype_mismatch;
    END IF;

    IF (scale(p_style) > 0) THEN
        RAISE escape_character_conflict;
    ELSIF (NOT ((v_style BETWEEN 0 AND 14) OR
                (v_style BETWEEN 20 AND 25) OR
                (v_style BETWEEN 100 AND 114) OR
                v_style IN (120, 121, 126, 127, 130, 131)))
    THEN
        RAISE invalid_parameter_value;
    ELSIF ((v_style BETWEEN 1 AND 7) OR
           (v_style BETWEEN 10 AND 12) OR
           (v_style BETWEEN 101 AND 107) OR
           (v_style BETWEEN 110 AND 112) OR
           v_style = 23)
    THEN
        RAISE invalid_datetime_format;
    END IF;

    v_hours := PG_CATALOG.ltrim(to_char(p_timeval, 'HH12'), '0');
    v_fseconds := sys.shark_get_microsecs_from_fractsecs_v2(to_char(p_timeval, 'US'), v_scale);

    -- Following condition will handle overflow of fractsecs
    IF (v_fseconds::INTEGER < 0) THEN
        v_fseconds := PG_CATALOG.repeat('0', LEAST(v_scale, 6));
        p_timeval := p_timeval + INTERVAL '1 second';
    END IF;

    IF (v_scale = 7) THEN
        v_fseconds := pg_catalog.concat(v_fseconds, '0');
    END IF;

    IF (v_style IN (0, 100))
    THEN
        v_resmask := pg_catalog.concat(v_hours, ':MIAM');
    ELSIF (v_style IN (8, 20, 24, 108, 120))
    THEN
        v_resmask := 'HH24:MI:SS';
    ELSIF (v_style IN (9, 109))
    THEN
        v_resmask := CASE
                        WHEN (char_length(v_fseconds) = 0) THEN pg_catalog.concat(v_hours, ':MI:SSAM')
                        ELSE pg_catalog.format('%s:MI:SS.%sAM', v_hours, v_fseconds)
                     END;
    ELSIF (v_style IN (13, 14, 21, 25, 113, 114, 121, 126, 127))
    THEN
        v_resmask := CASE
                        WHEN (char_length(v_fseconds) = 0) THEN 'HH24:MI:SS'
                        ELSE pg_catalog.concat('HH24:MI:SS.', v_fseconds)
                     END;
    ELSIF (v_style = 22)
    THEN
        v_resmask := pg_catalog.format('%s:MI:SS AM', lpad(v_hours, 2, ' '));
    ELSIF (v_style IN (130, 131))
    THEN
        v_resmask := CASE
                        WHEN (char_length(v_fseconds) = 0) THEN pg_catalog.concat(lpad(v_hours, 2, ' '), ':MI:SSAM')
                        ELSE pg_catalog.format('%s:MI:SS.%sAM', lpad(v_hours, 2, ' '), v_fseconds)
                     END;
    END IF;

    v_resstring := to_char(p_timeval, v_resmask);

    v_resstring := substring(v_resstring, 1, coalesce(v_res_length, char_length(v_resstring)));
    v_res_length := coalesce(v_res_length,
                             CASE v_res_datatype
                                WHEN 'CHAR' THEN 30
                                ELSE 60
                             END);
    RETURN CASE
              WHEN (v_res_datatype NOT IN ('CHAR', 'NCHAR')) THEN v_resstring
              ELSE rpad(v_resstring, v_res_length, ' ')
           END;
EXCEPTION
    WHEN most_specific_type_mismatch THEN
        RAISE USING MESSAGE := 'Source data type should be ''TIME'' or ''TIME(n)''.',
                    DETAIL := 'Use of incorrect "src_datatype" parameter value during conversion process.',
                    HINT := 'Change "src_datatype" parameter to the proper value and try again.';

   WHEN invalid_regular_expression THEN
       RAISE USING MESSAGE := pg_catalog.format('The source data type scale (%s) given to the convert specification exceeds the maximum allowable value (7).',
                                     v_scale),
                   DETAIL := 'Use of incorrect scale value of source data type parameter during conversion process.',
                   HINT := 'Change scale component of source data type parameter to the allowable value and try again.';

   WHEN interval_field_overflow THEN
       RAISE USING MESSAGE := pg_catalog.format('The size (%s) given to the convert specification ''%s'' exceeds the maximum allowed for any data type (%s).',
                                     v_lengthexpr, pg_catalog.lower(v_res_datatype), v_res_maxlength),
                   DETAIL := 'Use of incorrect size value of target data type parameter during conversion process.',
                   HINT := 'Change size component of data type parameter to the allowable value and try again.';

    WHEN escape_character_conflict THEN
        RAISE USING MESSAGE := 'Argument data type NUMERIC is invalid for argument 4 of convert function.',
                    DETAIL := 'Use of incorrect "style" parameter value during conversion process.',
                    HINT := 'Change "style" parameter to the proper value and try again.';

    WHEN invalid_parameter_value THEN
        RAISE USING MESSAGE := pg_catalog.format('%s is not a valid style number when converting from TIME to a character string.', v_style),
                    DETAIL := 'Use of incorrect "style" parameter value during conversion process.',
                    HINT := 'Change "style" parameter to the proper value and try again.';

    WHEN datatype_mismatch THEN
        RAISE USING MESSAGE := 'Data type should be one of these values: ''CHAR(n|MAX)'', ''NCHAR(n|MAX)'', ''VARCHAR(n|MAX)'', ''NVARCHAR(n|MAX)''.',
                    DETAIL := 'Use of incorrect "datatype" parameter value during conversion process.',
                    HINT := 'Change "datatype" parameter to the proper value and try again.';

    WHEN invalid_datetime_format THEN
        RAISE USING MESSAGE := pg_catalog.format('Error converting data type TIME to %s.',
                                      PG_CATALOG.rtrim(split_part(pg_catalog.btrim(p_datatype), '(', 1))),
                    DETAIL := 'Incorrect using of pair of input parameters values during conversion process.',
                    HINT := 'Check the input parameters values, correct them if needed, and try again.';
END;
$BODY$
LANGUAGE plpgsql
STABLE
RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION sys.shark_conv_string_to_datetime_v2(IN p_datatype TEXT,
                                                                     IN p_datetimestring TEXT,
                                                                     IN p_style NUMERIC DEFAULT 0)
RETURNS TIMESTAMP WITHOUT TIME ZONE
AS
$BODY$
DECLARE
    v_day VARCHAR COLLATE "C";
    v_year VARCHAR COLLATE "C";
    v_month VARCHAR COLLATE "C";
    v_style SMALLINT;
    v_scale SMALLINT;
    v_hours VARCHAR COLLATE "C";
    v_hijridate DATE;
    v_minutes VARCHAR COLLATE "C";
    v_seconds VARCHAR COLLATE "C";
    v_fseconds VARCHAR COLLATE "C";
    v_datatype VARCHAR COLLATE "C";
    v_timepart VARCHAR COLLATE "C";
    v_leftpart VARCHAR COLLATE "C";
    v_middlepart VARCHAR COLLATE "C";
    v_rightpart VARCHAR COLLATE "C";
    v_datestring VARCHAR COLLATE "C";
    v_err_message VARCHAR COLLATE "C";
    v_date_format VARCHAR COLLATE "C";
    v_res_datatype VARCHAR COLLATE "C";
    v_datetimestring VARCHAR COLLATE "C";
    v_datatype_groups TEXT[];
    v_regmatch_groups TEXT[];
    v_lang_metadata_json JSON;
    v_compmonth_regexp VARCHAR COLLATE "C";
    v_resdatetime TIMESTAMP(6) WITHOUT TIME ZONE;
    v_language VARCHAR COLLATE "C";
    CONVERSION_LANG CONSTANT VARCHAR COLLATE "C" := '';
    DATE_FORMAT CONSTANT VARCHAR COLLATE "C" := '';
    DAYMM_REGEXP CONSTANT VARCHAR COLLATE "C" := '(\d{1,2})';
    FULLYEAR_REGEXP CONSTANT VARCHAR COLLATE "C" := '(\d{4})';
    SHORTYEAR_REGEXP CONSTANT VARCHAR COLLATE "C" := '(\d{1,2})';
    COMPYEAR_REGEXP CONSTANT VARCHAR COLLATE "C" := '(\d{1,2}|\d{4})';
    AMPM_REGEXP CONSTANT VARCHAR COLLATE "C" := '(?:[AP]M)';
    MASKSEP_REGEXP CONSTANT VARCHAR COLLATE "C" := '(?:\.|-|/)';
    TIMEUNIT_REGEXP CONSTANT VARCHAR COLLATE "C" := '\s*\d{1,2}\s*';
    FRACTSECS_REGEXP CONSTANT VARCHAR COLLATE "C" := '\s*\d{1,3}\s*';
    DATATYPE_REGEXP CONSTANT VARCHAR COLLATE "C" := '^(TIMESTAMP WITHOUT TIME ZONE|SMALLDATETIME)\s*(?:\()?\s*((?:-)?\d+)?\s*(?:\))?$';
    HHMMSSFS_PART_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat( TIMEUNIT_REGEXP, AMPM_REGEXP, '|',
                                                    TIMEUNIT_REGEXP, '\:', TIMEUNIT_REGEXP, AMPM_REGEXP, '?|',
                                                    TIMEUNIT_REGEXP, '\:', TIMEUNIT_REGEXP, '\.', FRACTSECS_REGEXP, AMPM_REGEXP, '?|',
                                                    TIMEUNIT_REGEXP, '\:', TIMEUNIT_REGEXP, '\:', TIMEUNIT_REGEXP, AMPM_REGEXP, '?|',
                                                    TIMEUNIT_REGEXP, '\:', TIMEUNIT_REGEXP, '\:', TIMEUNIT_REGEXP, '(?:\.|\:)', FRACTSECS_REGEXP, AMPM_REGEXP, '?');
    HHMMSSFS_DOT_PART_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat( TIMEUNIT_REGEXP, AMPM_REGEXP, '|',
                                                        TIMEUNIT_REGEXP, '\:', TIMEUNIT_REGEXP, AMPM_REGEXP, '?|',
                                                        TIMEUNIT_REGEXP, '\:', TIMEUNIT_REGEXP, '\.', FRACTSECS_REGEXP, AMPM_REGEXP, '?|',
                                                        TIMEUNIT_REGEXP, '\:', TIMEUNIT_REGEXP, '\:', TIMEUNIT_REGEXP, AMPM_REGEXP, '?|',
                                                        TIMEUNIT_REGEXP, '\:', TIMEUNIT_REGEXP, '\:', TIMEUNIT_REGEXP, '(?:\.)', FRACTSECS_REGEXP, AMPM_REGEXP, '?');
    HHMMSSFS_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^(', HHMMSSFS_PART_REGEXP, ')$');
    DEFMASK1_0_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^(', '(', HHMMSSFS_PART_REGEXP, ')', '\s+)?\s*',
                                                 MASKSEP_REGEXP, '?\s*($comp_month$)\s*,?\s*', DAYMM_REGEXP, '\s+', COMPYEAR_REGEXP,
                                                 '\s*(\s+', '(', HHMMSSFS_PART_REGEXP, ')', ')?$');
    DEFMASK1_1_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', MASKSEP_REGEXP, '?\s*($comp_month$)\s*,?\s*', DAYMM_REGEXP, '\s+', COMPYEAR_REGEXP, '$');
    DEFMASK1_2_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', MASKSEP_REGEXP, '\s*($comp_month$)\s*,?\s*', DAYMM_REGEXP, '\s+', COMPYEAR_REGEXP, '$');
    DEFMASK1_3_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', MASKSEP_REGEXP, '?\s*($comp_month$)\s*,\s*', DAYMM_REGEXP, '\s+', COMPYEAR_REGEXP, '$');
    DEFMASK2_0_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^(', '(', HHMMSSFS_PART_REGEXP, ')', '\s+)?\s*',
                                                 DAYMM_REGEXP, '\s*(?:,|', MASKSEP_REGEXP, ')', '?\s*($comp_month$)\s*,?\s*', COMPYEAR_REGEXP,
                                                 '\s*(\s+', '(', HHMMSSFS_PART_REGEXP, ')', ')?$');
    DEFMASK2_1_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', DAYMM_REGEXP, '\s*(?:,|', MASKSEP_REGEXP, ')', '?\s*($comp_month$)\s*,?\s*', COMPYEAR_REGEXP, '$');
    DEFMASK2_2_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', DAYMM_REGEXP, '\s*(?:,|', MASKSEP_REGEXP, ')', '\s*($comp_month$)\s*,?\s*', COMPYEAR_REGEXP, '$');
    DEFMASK3_0_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^(', '(', HHMMSSFS_PART_REGEXP, ')', '\s+)?\s*',
                                                 FULLYEAR_REGEXP, '\s*', MASKSEP_REGEXP, '?\s*($comp_month$)\s*,?\s*', DAYMM_REGEXP,
                                                 '\s*(\s+', '(', HHMMSSFS_PART_REGEXP, ')', ')?$');
    DEFMASK3_1_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', FULLYEAR_REGEXP, '\s*', MASKSEP_REGEXP, '?\s*($comp_month$)\s*,?\s*', DAYMM_REGEXP, '$');
    DEFMASK3_2_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', FULLYEAR_REGEXP, '\s*', MASKSEP_REGEXP, '\s*($comp_month$)\s*,?\s*', DAYMM_REGEXP, '$');
    DEFMASK3_3_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', FULLYEAR_REGEXP, '\s*', MASKSEP_REGEXP, '?\s*($comp_month$)\s*,\s*', DAYMM_REGEXP, '$');
    DEFMASK4_0_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^(', '(', HHMMSSFS_PART_REGEXP, ')', '\s+)?\s*',
                                                 FULLYEAR_REGEXP, '\s+', DAYMM_REGEXP, '\s*', '\s*(?:,|', MASKSEP_REGEXP, ')', '?\s*($comp_month$)',
                                                 '\s*(', HHMMSSFS_PART_REGEXP, ')?$');
    DEFMASK4_1_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', FULLYEAR_REGEXP, '\s+', DAYMM_REGEXP, '\s*(?:,|', MASKSEP_REGEXP, ')', '?\s*($comp_month$)$');
    DEFMASK4_2_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', FULLYEAR_REGEXP, '\s+', DAYMM_REGEXP, '\s*(?:,|', MASKSEP_REGEXP, ')', '\s*($comp_month$)$');
    DEFMASK5_0_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^(', '(', HHMMSSFS_PART_REGEXP, ')', '\s+)?\s*',
                                                 DAYMM_REGEXP, '(?:\s+|\s*,\s*)', COMPYEAR_REGEXP, '\s*', MASKSEP_REGEXP, '?\s*($comp_month$)',
                                                 '\s*(', HHMMSSFS_PART_REGEXP, ')?$');
    DEFMASK5_1_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', DAYMM_REGEXP, '(?:\s+|\s*,\s*)', COMPYEAR_REGEXP, '\s*', MASKSEP_REGEXP, '?\s*($comp_month$)$');
    DEFMASK5_2_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', DAYMM_REGEXP, '(?:\s+|\s*,\s*)', COMPYEAR_REGEXP, '\s*', MASKSEP_REGEXP, '\s*($comp_month$)$');
    DEFMASK5_3_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', DAYMM_REGEXP, '(?:\s*,\s*)', COMPYEAR_REGEXP, '\s*', MASKSEP_REGEXP, '?\s*($comp_month$)$');
    DEFMASK6_0_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^(', HHMMSSFS_PART_REGEXP, ')?\s*',
                                                 MASKSEP_REGEXP, '?\s*($comp_month$)\s*,?\s*', FULLYEAR_REGEXP, '\s+', DAYMM_REGEXP,
                                                 '\s*(\s+', '(', HHMMSSFS_PART_REGEXP, ')', ')?$');
    DEFMASK6_1_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', MASKSEP_REGEXP, '?\s*($comp_month$)\s*,?\s*', FULLYEAR_REGEXP, '\s+', DAYMM_REGEXP, '$');
    DEFMASK6_2_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', MASKSEP_REGEXP, '\s*($comp_month$)\s*,?\s*', FULLYEAR_REGEXP, '\s+', DAYMM_REGEXP, '$');
    DEFMASK6_3_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', MASKSEP_REGEXP, '?\s*($comp_month$)\s*,\s*', FULLYEAR_REGEXP, '\s+', DAYMM_REGEXP, '$');
    DEFMASK7_0_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^(', HHMMSSFS_PART_REGEXP, ')?\s*',
                                                 MASKSEP_REGEXP, '?\s*($comp_month$)\s*,?\s*', DAYMM_REGEXP, '\s*,\s*', COMPYEAR_REGEXP,
                                                 '\s*(\s+', '(', HHMMSSFS_PART_REGEXP, ')', ')?$');
    DEFMASK7_1_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', MASKSEP_REGEXP, '?\s*($comp_month$)\s*,?\s*', DAYMM_REGEXP, '\s*,\s*', COMPYEAR_REGEXP, '$');
    DEFMASK7_2_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', MASKSEP_REGEXP, '\s*($comp_month$)\s*,?\s*', DAYMM_REGEXP, '\s*,\s*', COMPYEAR_REGEXP, '$');
    DEFMASK7_3_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', MASKSEP_REGEXP, '?\s*($comp_month$)\s*,\s*', DAYMM_REGEXP, '\s*,\s*', COMPYEAR_REGEXP, '$');
    DEFMASK8_0_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^(', '(', HHMMSSFS_PART_REGEXP, ')', '\s+)?\s*',
                                                 FULLYEAR_REGEXP, '\s*', MASKSEP_REGEXP, '?\s*($comp_month$)',
                                                 '\s*(', HHMMSSFS_PART_REGEXP, ')?$');
    DEFMASK8_1_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', FULLYEAR_REGEXP, '\s*', MASKSEP_REGEXP, '?\s*($comp_month$)$');
    DEFMASK8_2_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', FULLYEAR_REGEXP, '\s*', MASKSEP_REGEXP, '\s*($comp_month$)$');
    DEFMASK9_0_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^(', HHMMSSFS_PART_REGEXP, ')?\s*',
                                                 MASKSEP_REGEXP, '?\s*($comp_month$)\s*,?\s*', FULLYEAR_REGEXP,
                                                 '\s*(\s+', '(', HHMMSSFS_PART_REGEXP, ')', ')?$');
    DEFMASK9_1_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', MASKSEP_REGEXP, '?\s*($comp_month$)\s*,?\s*', FULLYEAR_REGEXP, '$');
    DEFMASK9_2_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', MASKSEP_REGEXP, '\s*($comp_month$)\s*,?\s*', FULLYEAR_REGEXP, '$');
    DEFMASK10_0_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^(', '(', HHMMSSFS_PART_REGEXP, ')', '\s+)?\s*',
                                                  DAYMM_REGEXP, '\s*', MASKSEP_REGEXP, '\s*($comp_month$)\s*', MASKSEP_REGEXP, '\s*', COMPYEAR_REGEXP,
                                                  '\s*(\s+', '(', HHMMSSFS_PART_REGEXP, ')', ')?$');
    DEFMASK10_1_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', DAYMM_REGEXP, '\s*', MASKSEP_REGEXP, '\s*($comp_month$)\s*', MASKSEP_REGEXP, '\s*', COMPYEAR_REGEXP, '$');
    DEFMASK10_2_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', DAYMM_REGEXP, '\s*-\s*($comp_month$)\s*-\s*', COMPYEAR_REGEXP, '$');
    DEFMASK10_3_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', DAYMM_REGEXP, '\s*\/\s*($comp_month$)\s*\/\s*', COMPYEAR_REGEXP, '$');
    DEFMASK10_4_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', DAYMM_REGEXP, '\s*\.\s*($comp_month$)\s*\.\s*', COMPYEAR_REGEXP, '$');
    DOT_SLASH_DASH_COMPYEAR1_0_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^(', '(', HHMMSSFS_PART_REGEXP, ')', '\s+)?\s*',
                                                                 DAYMM_REGEXP, '\s*(?:\.|/|-)\s*', DAYMM_REGEXP, '\s*(?:\.|/|-)\s*', COMPYEAR_REGEXP,
                                                                 '\s*(\s+', '(', HHMMSSFS_PART_REGEXP, ')', ')?$');
    DOT_SLASH_DASH_COMPYEAR1_1_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', DAYMM_REGEXP, '\s*', MASKSEP_REGEXP, '\s*', DAYMM_REGEXP, '\s*', MASKSEP_REGEXP, '\s*', COMPYEAR_REGEXP, '$');
    DOT_SLASH_DASH_SHORTYEAR_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', DAYMM_REGEXP, '\s*', MASKSEP_REGEXP, '\s*', DAYMM_REGEXP, '\s*', MASKSEP_REGEXP, '\s*', SHORTYEAR_REGEXP, '$');
    DOT_SLASH_DASH_FULLYEAR1_0_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^(', '(', HHMMSSFS_PART_REGEXP, ')', '\s+)?\s*',
                                                                 DAYMM_REGEXP, '\s*(?:\.|/|-)\s*', DAYMM_REGEXP, '\s*(?:\.|/|-)\s*', FULLYEAR_REGEXP,
                                                                 '\s*(\s+', '(', HHMMSSFS_PART_REGEXP, ')', ')?$');
    DOT_SLASH_DASH_FULLYEAR1_1_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', DAYMM_REGEXP, '\s*', MASKSEP_REGEXP, '\s*', DAYMM_REGEXP, '\s*', MASKSEP_REGEXP, '\s*', FULLYEAR_REGEXP, '$');

    FULLYEAR_DOT_SLASH_DASH1_0_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^(', '(', HHMMSSFS_PART_REGEXP, ')', '\s+)?\s*',
                                                                 FULLYEAR_REGEXP, '\s*', MASKSEP_REGEXP, '\s*', DAYMM_REGEXP, '\s*', MASKSEP_REGEXP, '\s*', DAYMM_REGEXP,
                                                                 '\s*(\s+', '(', HHMMSSFS_PART_REGEXP, ')', ')?$');
    FULLYEAR_DOT_SLASH_DASH1_1_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', FULLYEAR_REGEXP, '\s*', MASKSEP_REGEXP, '\s*', DAYMM_REGEXP, '\s*', MASKSEP_REGEXP, '\s*', DAYMM_REGEXP, '$');

    DOT_SLASH_DASH_FULLYEAR_DOT_SLASH_DASH1_0_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^(', '(', HHMMSSFS_PART_REGEXP, ')', '\s+)?\s*',
                                                                 DAYMM_REGEXP, '\s*', MASKSEP_REGEXP, '\s*', FULLYEAR_REGEXP, '\s*', MASKSEP_REGEXP, '\s*', DAYMM_REGEXP,
                                                                 '\s*(\s+', '(', HHMMSSFS_PART_REGEXP, ')', ')?$');
    DOT_SLASH_DASH_FULLYEAR_DOT_SLASH_DASH1_1_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', DAYMM_REGEXP, '\s*', MASKSEP_REGEXP, '\s*', FULLYEAR_REGEXP, '\s*', MASKSEP_REGEXP, '\s*', DAYMM_REGEXP, '$');

    FULLYEAR_DIGITMASK1_0_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^(', '(', HHMMSSFS_PART_REGEXP, ')', '\s+)?\s*\d{4}\s*(\s+', '(', HHMMSSFS_PART_REGEXP, ')', ')?$');
    SHORT_DIGITMASK1_0_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^(', '(', HHMMSSFS_PART_REGEXP, ')', '\s+)?\s*\d{6}\s*(\s+', '(', HHMMSSFS_PART_REGEXP, ')', ')?$');
    FULL_DIGITMASK1_0_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^(', '(', HHMMSSFS_PART_REGEXP, ')', '\s+)?\s*\d{8}\s*(\s+', '(', HHMMSSFS_PART_REGEXP, ')', ')?$');
    ISO_8601_DATETIME_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', FULLYEAR_REGEXP, '-', '(\d{2})', '-', '(\d{2})', 'T', '\d{2}', '\:', '\d{2}', '\:', '\d{2}', '(?:\.', FRACTSECS_REGEXP, ')?','$');
BEGIN
    v_datatype := pg_catalog.btrim(p_datatype);
    v_datetimestring := pg_catalog.upper(pg_catalog.btrim(p_datetimestring));
    v_style := floor(p_style)::SMALLINT;

    v_datatype_groups := regexp_matches(v_datatype, DATATYPE_REGEXP, 'gi');

    v_res_datatype := pg_catalog.upper(v_datatype_groups[1]);
    v_scale := v_datatype_groups[2]::SMALLINT;

    IF (v_res_datatype IS NULL) THEN
        RAISE datatype_mismatch;
    ELSIF (v_scale IS NOT NULL)
    THEN
        RAISE invalid_indicator_parameter_value;
    ELSIF (v_scale IS NULL) THEN
        v_scale := 7;
    END IF;

    IF (scale(p_style) > 0) THEN
        RAISE most_specific_type_mismatch;
    END IF;

    v_timepart := pg_catalog.btrim(substring(v_datetimestring, PG_CATALOG.concat('(', HHMMSSFS_PART_REGEXP, ')')));
    v_datestring := pg_catalog.btrim(regexp_replace(v_datetimestring, PG_CATALOG.concat('T?', '(', HHMMSSFS_PART_REGEXP, ')'), '', 'gi'));

    v_language := CASE
                    WHEN (v_style IN (130, 131)) THEN 'HIJRI'
                    ELSE CONVERSION_LANG
                  END;

    BEGIN
        v_lang_metadata_json := sys.shark_get_lang_metadata_json(v_language);
    EXCEPTION
        WHEN OTHERS THEN
        RAISE invalid_escape_sequence;
    END;

    v_date_format := coalesce(nullif(DATE_FORMAT, ''), v_lang_metadata_json ->> 'date_format');

    v_compmonth_regexp := array_to_string(array_cat(ARRAY(SELECT json_array_elements_text(v_lang_metadata_json -> 'months_shortnames')),
                                                    ARRAY(SELECT json_array_elements_text(v_lang_metadata_json -> 'months_names'))), '|');

    IF (v_datetimestring ~* pg_catalog.replace(DEFMASK1_0_REGEXP, '$comp_month$', v_compmonth_regexp) OR
        v_datetimestring ~* pg_catalog.replace(DEFMASK2_0_REGEXP, '$comp_month$', v_compmonth_regexp) OR
        v_datetimestring ~* pg_catalog.replace(DEFMASK3_0_REGEXP, '$comp_month$', v_compmonth_regexp) OR
        v_datetimestring ~* pg_catalog.replace(DEFMASK4_0_REGEXP, '$comp_month$', v_compmonth_regexp) OR
        v_datetimestring ~* pg_catalog.replace(DEFMASK5_0_REGEXP, '$comp_month$', v_compmonth_regexp) OR
        v_datetimestring ~* pg_catalog.replace(DEFMASK6_0_REGEXP, '$comp_month$', v_compmonth_regexp) OR
        v_datetimestring ~* pg_catalog.replace(DEFMASK7_0_REGEXP, '$comp_month$', v_compmonth_regexp) OR
        v_datetimestring ~* pg_catalog.replace(DEFMASK8_0_REGEXP, '$comp_month$', v_compmonth_regexp) OR
        v_datetimestring ~* pg_catalog.replace(DEFMASK9_0_REGEXP, '$comp_month$', v_compmonth_regexp) OR
        v_datetimestring ~* pg_catalog.replace(DEFMASK10_0_REGEXP, '$comp_month$', v_compmonth_regexp))
    THEN
        IF (v_style = 127) THEN
            RAISE invalid_datetime_format;
        END IF;

        IF (v_datestring ~* pg_catalog.replace(DEFMASK1_1_REGEXP, '$comp_month$', v_compmonth_regexp))
        THEN
            v_regmatch_groups := regexp_matches(v_datestring, pg_catalog.replace(DEFMASK1_1_REGEXP, '$comp_month$', v_compmonth_regexp), 'gi');
            v_day := v_regmatch_groups[2];
            v_month := sys.shark_get_monthnum_by_name(v_regmatch_groups[1], v_lang_metadata_json);

            -- For v_style 130 and 131, 1 or 2 digit year is out of range
            IF (v_style IN (130, 131) AND v_regmatch_groups[3]::SMALLINT <= 99)
            THEN
                RAISE invalid_character_value_for_cast;
            END IF;
            v_year := sys.shark_get_full_year(v_regmatch_groups[3]);

        ELSIF (v_datestring ~* pg_catalog.replace(DEFMASK2_1_REGEXP, '$comp_month$', v_compmonth_regexp))
        THEN
            v_regmatch_groups := regexp_matches(v_datestring, pg_catalog.replace(DEFMASK2_1_REGEXP, '$comp_month$', v_compmonth_regexp), 'gi');
            v_day := v_regmatch_groups[1];
            v_month := sys.shark_get_monthnum_by_name(v_regmatch_groups[2], v_lang_metadata_json);

            -- For v_style 130 and 131, 1 or 2 digit year is out of range
            IF (v_style IN (130, 131) AND v_regmatch_groups[3]::SMALLINT <= 99)
            THEN
                RAISE invalid_character_value_for_cast;
            END IF;
            v_year := sys.shark_get_full_year(v_regmatch_groups[3]);

        ELSIF (v_datestring ~* pg_catalog.replace(DEFMASK3_1_REGEXP, '$comp_month$', v_compmonth_regexp))
        THEN
            v_regmatch_groups := regexp_matches(v_datestring, pg_catalog.replace(DEFMASK3_1_REGEXP, '$comp_month$', v_compmonth_regexp), 'gi');
            v_day := v_regmatch_groups[3];
            v_month := sys.shark_get_monthnum_by_name(v_regmatch_groups[2], v_lang_metadata_json);
            v_year := v_regmatch_groups[1];

        ELSIF (v_datestring ~* pg_catalog.replace(DEFMASK4_1_REGEXP, '$comp_month$', v_compmonth_regexp))
        THEN
            v_regmatch_groups := regexp_matches(v_datestring, pg_catalog.replace(DEFMASK4_1_REGEXP, '$comp_month$', v_compmonth_regexp), 'gi');
            v_day := v_regmatch_groups[2];
            v_month := sys.shark_get_monthnum_by_name(v_regmatch_groups[3], v_lang_metadata_json);
            v_year := v_regmatch_groups[1];

        ELSIF (v_datestring ~* pg_catalog.replace(DEFMASK5_1_REGEXP, '$comp_month$', v_compmonth_regexp))
        THEN
            v_regmatch_groups := regexp_matches(v_datestring, pg_catalog.replace(DEFMASK5_1_REGEXP, '$comp_month$', v_compmonth_regexp), 'gi');
            v_day := v_regmatch_groups[1];
            v_month := sys.shark_get_monthnum_by_name(v_regmatch_groups[3], v_lang_metadata_json);

            -- For v_style 130 and 131, 1 or 2 digit year is out of range
            IF (v_style IN (130, 131) AND v_regmatch_groups[2]::SMALLINT <= 99)
            THEN
                RAISE invalid_character_value_for_cast;
            END IF;
            v_year := sys.shark_get_full_year(v_regmatch_groups[2]);

        ELSIF (v_datestring ~* pg_catalog.replace(DEFMASK6_1_REGEXP, '$comp_month$', v_compmonth_regexp))
        THEN
            v_regmatch_groups := regexp_matches(v_datestring, pg_catalog.replace(DEFMASK6_1_REGEXP, '$comp_month$', v_compmonth_regexp), 'gi');
            v_day := v_regmatch_groups[3];
            v_month := sys.shark_get_monthnum_by_name(v_regmatch_groups[1], v_lang_metadata_json);
            v_year := v_regmatch_groups[2];

        ELSIF (v_datestring ~* pg_catalog.replace(DEFMASK7_1_REGEXP, '$comp_month$', v_compmonth_regexp))
        THEN
            v_regmatch_groups := regexp_matches(v_datestring, pg_catalog.replace(DEFMASK7_1_REGEXP, '$comp_month$', v_compmonth_regexp), 'gi');
            v_day := v_regmatch_groups[2];
            v_month := sys.shark_get_monthnum_by_name(v_regmatch_groups[1], v_lang_metadata_json);

            -- For v_style 130 and 131, 1 or 2 digit year is out of range
            IF (v_style IN (130, 131) AND v_regmatch_groups[3]::SMALLINT <= 99)
            THEN
                RAISE invalid_character_value_for_cast;
            END IF;
            v_year := sys.shark_get_full_year(v_regmatch_groups[3]);

        ELSIF (v_datestring ~* pg_catalog.replace(DEFMASK8_1_REGEXP, '$comp_month$', v_compmonth_regexp))
        THEN
            v_regmatch_groups := regexp_matches(v_datestring, pg_catalog.replace(DEFMASK8_1_REGEXP, '$comp_month$', v_compmonth_regexp), 'gi');
            v_day := '01';
            v_month := sys.shark_get_monthnum_by_name(v_regmatch_groups[2], v_lang_metadata_json);
            v_year := v_regmatch_groups[1];

        ELSIF (v_datestring ~* pg_catalog.replace(DEFMASK9_1_REGEXP, '$comp_month$', v_compmonth_regexp))
        THEN
            v_regmatch_groups := regexp_matches(v_datestring, pg_catalog.replace(DEFMASK9_1_REGEXP, '$comp_month$', v_compmonth_regexp), 'gi');
            v_day := '01';
            v_month := sys.shark_get_monthnum_by_name(v_regmatch_groups[1], v_lang_metadata_json);
            v_year := v_regmatch_groups[2];

        ELSIF (v_datestring ~* pg_catalog.replace(DEFMASK10_1_REGEXP, '$comp_month$', v_compmonth_regexp))
        THEN
            v_regmatch_groups := regexp_matches(v_datestring, pg_catalog.replace(DEFMASK10_1_REGEXP, '$comp_month$', v_compmonth_regexp), 'gi');
            v_day := v_regmatch_groups[1];
            v_month := sys.shark_get_monthnum_by_name(v_regmatch_groups[2], v_lang_metadata_json);

            -- For v_style 130 and 131, 1 or 2 digit year is out of range
            IF (v_style IN (130, 131) AND v_regmatch_groups[3]::SMALLINT <= 99)
            THEN
                RAISE invalid_character_value_for_cast;
            END IF;
            v_year := sys.shark_get_full_year(v_regmatch_groups[3]);
        ELSE
            RAISE invalid_character_value_for_cast;
        END IF;
    ELSIF (v_datetimestring ~* DOT_SLASH_DASH_COMPYEAR1_0_REGEXP)
    THEN
        v_regmatch_groups := regexp_matches(v_datestring, DOT_SLASH_DASH_COMPYEAR1_1_REGEXP, 'gi');
        v_leftpart := v_regmatch_groups[1];
        v_middlepart := v_regmatch_groups[2];
        v_rightpart := v_regmatch_groups[3];

        IF (v_datestring ~* DOT_SLASH_DASH_SHORTYEAR_REGEXP)
        THEN
            IF (v_style NOT IN (0, 1, 2, 3, 4, 5, 10, 11))
            THEN
                RAISE invalid_datetime_format;
            END IF;

            IF ((v_style IN (1, 10)) OR
                ((v_style IS NULL OR v_style = 0) AND v_date_format = 'MDY'))
            THEN
                v_day := v_middlepart;
                v_month := v_leftpart;
                v_year := sys.shark_get_full_year(v_rightpart);

            ELSIF ((v_style IN (2, 11)) OR
                   ((v_style IS NULL OR v_style = 0) AND v_date_format = 'YMD'))
            THEN
                v_day := v_rightpart;
                v_month := v_middlepart;
                v_year := sys.shark_get_full_year(v_leftpart);

            ELSIF ((v_style IN (3, 4, 5)) OR
                   ((v_style IS NULL OR v_style = 0) AND v_date_format = 'DMY'))
            THEN
                v_day := v_leftpart;
                v_month := v_middlepart;
                v_year := sys.shark_get_full_year(v_rightpart);

            ELSIF ((v_style IS NULL OR v_style = 0) AND v_date_format = 'DYM')
            THEN
                v_day = v_leftpart;
                v_month = v_rightpart;
                v_year = sys.shark_get_full_year(v_middlepart);

            ELSIF ((v_style IS NULL OR v_style = 0) AND v_date_format = 'MYD')
            THEN
                v_day := v_rightpart;
                v_month := v_leftpart;
                v_year = sys.shark_get_full_year(v_middlepart);

            ELSIF ((v_style IS NULL OR v_style = 0) AND v_date_format = 'YDM')
            THEN
                v_day := v_middlepart;
                v_month := v_rightpart;
                v_year := sys.shark_get_full_year(v_leftpart);
            END IF;
        ELSIF (v_datestring ~* DOT_SLASH_DASH_FULLYEAR1_1_REGEXP)
        THEN
            IF (v_style NOT IN (0, 20, 21, 101, 102, 103, 104, 105, 110, 111, 120, 121, 130, 131))
            THEN
                RAISE invalid_datetime_format;
            END IF;

            v_year := v_rightpart;
            IF ((v_style IN (103, 104, 105, 130, 131)) OR
                ((v_style IS NULL OR v_style = 0) AND v_date_format IN ('DMY', 'DYM', 'YDM')))
            THEN
                v_day := v_leftpart;
                v_month := v_middlepart;

            ELSIF ((v_style IN (20, 21, 101, 102, 110, 111, 120, 121)) OR
                    ((v_style IS NULL OR v_style = 0) AND v_date_format IN ('MDY', 'MYD', 'YMD')))
            THEN
                v_day := v_middlepart;
                v_month := v_leftpart;
            END IF;
        END IF;
    ELSIF (v_datetimestring ~* FULLYEAR_DOT_SLASH_DASH1_0_REGEXP)
    THEN
        IF (v_style NOT IN (0, 20, 21, 101, 102, 103, 104, 105, 110, 111, 120, 121, 130, 131))
        THEN
            RAISE invalid_datetime_format;
        ELSIF (v_style IN (130, 131) AND v_res_datatype = 'SMALLDATETIME')
        THEN
            RAISE invalid_character_value_for_cast;
        END IF;

        v_regmatch_groups := regexp_matches(v_datestring, FULLYEAR_DOT_SLASH_DASH1_1_REGEXP, 'gi');
        v_year := v_regmatch_groups[1];
        v_middlepart := v_regmatch_groups[2];
        v_rightpart := v_regmatch_groups[3];

        IF ((v_style IN (20, 21, 101, 102, 110, 111, 120, 121)) OR
            ((v_style IS NULL OR v_style = 0) AND v_date_format IN ('MDY', 'MYD', 'YMD')))
        THEN
            v_day := v_rightpart;
            v_month := v_middlepart;

        ELSIF ((v_style IN (103, 104, 105, 130, 131)) OR
               ((v_style IS NULL OR v_style = 0) AND v_date_format IN ('DMY', 'DYM', 'YDM')))
        THEN
            v_day := v_middlepart;
            v_month := v_rightpart;
        END IF;
    ELSIF (v_datetimestring ~* DOT_SLASH_DASH_FULLYEAR_DOT_SLASH_DASH1_0_REGEXP)
    THEN
        IF (v_style IS NOT NULL AND v_style != 0)
        THEN
            RAISE invalid_datetime_format;
        ELSIF (v_style IN (130, 131) AND v_res_datatype = 'SMALLDATETIME')
        THEN
            RAISE invalid_character_value_for_cast;
        END IF;

        v_regmatch_groups := regexp_matches(v_datestring, DOT_SLASH_DASH_FULLYEAR_DOT_SLASH_DASH1_1_REGEXP, 'gi');
        v_leftpart := v_regmatch_groups[1];
        v_year := v_regmatch_groups[2];
        v_rightpart := v_regmatch_groups[3];

        IF (v_date_format IN ('MYD', 'MDY', 'YMD'))
        THEN
            v_day := v_rightpart;
            v_month := v_leftpart;
        ELSIF (v_date_format IN ('DYM', 'DMY', 'YDM'))
        THEN
            v_day := v_leftpart;
            v_month := v_rightpart;
        ELSE
            RAISE invalid_datetime_format;
        END IF;
    ELSIF (v_datetimestring ~* FULLYEAR_DIGITMASK1_0_REGEXP OR
           v_datetimestring ~* SHORT_DIGITMASK1_0_REGEXP OR
           v_datetimestring ~* FULL_DIGITMASK1_0_REGEXP)
    THEN
        IF (v_style = 127 AND v_datestring !~* '^\d{4}$')
        THEN
            RAISE invalid_datetime_format;
        ELSIF (v_style IN (130, 131) AND v_res_datatype = 'SMALLDATETIME')
        THEN
            RAISE invalid_character_value_for_cast;
        END IF;

        IF (v_datestring ~* '^\d{4}$')
        THEN
            v_day := '01';
            v_month := '01';
            v_year := substr(v_datestring, 1, 4);

        ELSIF (v_datestring ~* '^\d{6}$')
        THEN
            v_day := substr(v_datestring, 5, 2);
            v_month := substr(v_datestring, 3, 2);
            v_year := sys.shark_get_full_year(substr(v_datestring, 1, 2));

        ELSIF (v_datestring ~* '^\d{8}$')
        THEN
            v_day := substr(v_datestring, 7, 2);
            v_month := substr(v_datestring, 5, 2);
            v_year := substr(v_datestring, 1, 4);
        END IF;
    ELSIF (v_datetimestring ~* HHMMSSFS_REGEXP OR length(v_datetimestring) = 0)
    THEN
        v_day := '01';
        v_month := '01';
        v_year := '1900';
    ELSIF (v_datetimestring ~* ISO_8601_DATETIME_REGEXP)
    THEN
        IF (v_style IN (130, 131))
        THEN
            RAISE invalid_character_value_for_cast;
        END IF;
        v_regmatch_groups := regexp_matches(v_datetimestring, ISO_8601_DATETIME_REGEXP, 'gi');

        v_day := v_regmatch_groups[3];
        v_month := v_regmatch_groups[2];
        v_year := v_regmatch_groups[1];
    ELSE
        RAISE invalid_datetime_format;
    END IF;

    IF ((SELECT COUNT(*) FROM regexp_matches(v_datetimestring, HHMMSSFS_PART_REGEXP)) > 1)
    THEN
        RAISE invalid_character_value_for_cast;
    END IF;

    IF ((v_datetimestring !~* HHMMSSFS_REGEXP AND
         length(v_datetimestring) != 0) AND
        v_style IN (130, 131))
    THEN
        -- validate date according to hijri date format
        IF ((v_month::SMALLINT NOT BETWEEN 1 AND 12) OR
            (v_day::SMALLINT NOT BETWEEN 1 AND 30) OR
            ((MOD(v_month::SMALLINT, 2) = 0 AND v_month::SMALLINT != 12) AND v_day::SMALLINT = 30))
        THEN
            RAISE invalid_character_value_for_cast;
        END IF;

        -- for hijri leap year
        IF (v_month::SMALLINT = 12)
        THEN
            -- check for a leap year
            IF (MOD(v_year::SMALLINT, 30) IN (2, 5, 7, 10, 13, 16, 18, 21, 24, 26, 29))
            THEN
                IF (v_day::SMALLINT NOT BETWEEN 1 AND 30)
                THEN
                    RAISE invalid_character_value_for_cast;
                END IF;
            ELSE
                IF (v_day::SMALLINT NOT BETWEEN 1 AND 29)
                THEN
                    RAISE invalid_character_value_for_cast;
                END IF;
            END IF;
        END IF;

        v_hijridate := sys.shark_conv_hijri_to_greg(v_day, v_month, v_year) - 1;
        v_day = to_char(v_hijridate, 'DD');
        v_month = to_char(v_hijridate, 'MM');
        v_year = to_char(v_hijridate, 'YYYY');
    END IF;

    BEGIN
        v_hours := coalesce(sys.shark_get_timeunit_from_string(v_timepart, 'HOURS'), '0');
        v_minutes := coalesce(sys.shark_get_timeunit_from_string(v_timepart, 'MINUTES'), '0');
        v_seconds := coalesce(sys.shark_get_timeunit_from_string(v_timepart, 'SECONDS'), '0');
        v_fseconds := coalesce(sys.shark_get_timeunit_from_string(v_timepart, 'FRACTSECONDS'), '0');
    EXCEPTION
        WHEN OTHERS THEN
            RAISE invalid_character_value_for_cast;
    END;

    -- validate time
    IF ((v_hours::SMALLINT NOT BETWEEN 0 AND 23) OR
        (v_minutes::SMALLINT NOT BETWEEN 0 AND 59) OR
        (v_seconds::SMALLINT NOT BETWEEN 0 AND 59))
    THEN
        RAISE invalid_character_value_for_cast;
    END IF;

    -- validate date according to gregorian date format
    IF ((v_year::SMALLINT NOT BETWEEN 1 AND 9999) OR
        (v_month::SMALLINT NOT BETWEEN 1 AND 12) OR
        ((v_month::SMALLINT IN (1,3,5,7,8,10,12)) AND (v_day::SMALLINT NOT BETWEEN 1 AND 31)) OR
        ((v_month::SMALLINT IN (4,6,9,11)) AND (v_day::SMALLINT NOT BETWEEN 1 AND 30)) OR
        ((v_res_datatype = 'TIMESTAMP WITHOUT TIME ZONE') AND (v_year::SMALLINT NOT BETWEEN 1753 AND 9999)) OR
        ((v_res_datatype = 'SMALLDATETIME') AND
            ((v_year::SMALLINT NOT BETWEEN 1900 AND 2079) OR
             (v_year::SMALLINT = 2079 AND
                ((v_month::SMALLINT NOT BETWEEN 1 AND 6) OR (v_day::SMALLINT NOT BETWEEN 1 AND 6))))))
    THEN
        RAISE invalid_character_value_for_cast;
    ELSIF (v_month::SMALLINT = 2)
    THEN
        -- check for a leap year
        IF ((v_year::SMALLINT % 4 = 0) AND ((v_year::SMALLINT % 100 <> 0) or (v_year::SMALLINT % 400 = 0)))
        THEN
            IF (v_day::SMALLINT NOT BETWEEN 1 AND 29)
            THEN
                RAISE invalid_character_value_for_cast;
            END IF;
        ELSE
            IF (v_day::SMALLINT NOT BETWEEN 1 AND 28)
            THEN
                RAISE invalid_character_value_for_cast;
            END IF;
        END IF;
    END IF;

    -- validate boundary condition for date and time
    IF ((v_res_datatype = 'SMALLDATETIME' AND
         (v_year::SMALLINT = 2079 AND v_month::SMALLINT = 6 AND v_day::SMALLINT = 6 AND
            v_hours::SMALLINT = 23 AND v_minutes::SMALLINT = 59 AND
                (v_seconds::SMALLINT > 29 OR (v_seconds::SMALLINT = 29 AND v_fseconds::SMALLINT > 998)))) OR
        (v_res_datatype = 'TIMESTAMP WITHOUT TIME ZONE' AND
         (v_year::SMALLINT = 9999 AND v_month::SMALLINT = 12 AND v_day::SMALLINT = 31 AND
            v_hours::SMALLINT = 23 AND v_minutes::SMALLINT = 59 AND v_seconds::SMALLINT = 59 AND v_fseconds::SMALLINT > 998))
        )
    THEN
        RAISE invalid_character_value_for_cast;
    END IF;

    IF (v_timepart ~* PG_CATALOG.concat('^(', HHMMSSFS_DOT_PART_REGEXP, ')$')) THEN
        -- if before fractional seconds there is a '.'
        v_resdatetime := sys.datetimefromparts(v_year, v_month, v_day,
                                                            v_hours, v_minutes, v_seconds,
                                                            rpad(v_fseconds, 3, '0'));
    ELSE
        -- if before fractional seconds there is a ':'
        v_resdatetime := sys.datetimefromparts(v_year, v_month, v_day,
                                                            v_hours, v_minutes, v_seconds,
                                                            lpad(v_fseconds, 3, '0'));
    END IF;

    IF (v_res_datatype = 'SMALLDATETIME' AND
        to_char(v_resdatetime, 'SS') <> '00')
    THEN
        IF (to_char(v_resdatetime, 'SS')::SMALLINT >= 30) THEN
            v_resdatetime := v_resdatetime + INTERVAL '1 minute';
        END IF;

        v_resdatetime := to_timestamp(to_char(v_resdatetime, 'DD.MM.YYYY.HH24.MI'), 'DD.MM.YYYY.HH24.MI');
    END IF;

    RETURN v_resdatetime;
EXCEPTION
    WHEN most_specific_type_mismatch THEN
        RAISE USING MESSAGE := 'Argument data type numeric is invalid for argument 3 of convert function.',
                    DETAIL := 'Use of incorrect "style" parameter value during conversion process.',
                    HINT := 'Change "style" parameter to the proper value and try again.';

    WHEN invalid_parameter_value THEN
        RAISE USING MESSAGE := pg_catalog.format('The style %s is not supported for conversions from varchar to %s.', v_style, PG_CATALOG.lower(v_res_datatype)),
                    DETAIL := 'Use of incorrect "style" parameter value during conversion process.',
                    HINT := 'Change "style" parameter to the proper value and try again.';

    WHEN invalid_regular_expression THEN
        RAISE USING MESSAGE := pg_catalog.format('The input character string does not follow style %s, either change the input character string or use a different style.', v_style),
                    DETAIL := 'Selected "style" param value isn''t valid for conversion of passed character string.',
                    HINT := 'Either change the input character string or use a different style.';

    WHEN datatype_mismatch THEN
        RAISE USING MESSAGE := 'Data type should be one of these values: ''TIMESTAMP WITHOUT TIME ZONE'', ''SMALLDATETIME''.',
                    DETAIL := 'Use of incorrect "datatype" parameter value during conversion process.',
                    HINT := 'Change "datatype" parameter to the proper value and try again.';

    WHEN invalid_indicator_parameter_value THEN
        RAISE USING MESSAGE := pg_catalog.format('CAST or CONVERT: invalid attributes specified for type ''%s''', PG_CATALOG.lower(v_res_datatype)),
                    DETAIL := 'Use of incorrect scale value, which is not corresponding to specified data type.',
                    HINT := 'Change data type scale component or select different data type and try again.';

    WHEN interval_field_overflow THEN
        RAISE USING MESSAGE := pg_catalog.format('Specified scale %s is invalid.', v_scale),
                    DETAIL := 'Use of incorrect data type scale value during conversion process.',
                    HINT := 'Change scale component of data type parameter to be in range [0..7] and try again.';

    WHEN invalid_datetime_format THEN
        RAISE USING MESSAGE := CASE v_res_datatype
                                  WHEN 'SMALLDATETIME' THEN 'Conversion failed when converting character string to smalldatetime data type.'
                                  ELSE 'Conversion failed when converting date and/or time from character string.'
                               END,
                    DETAIL := 'Incorrect using of pair of input parameters values during conversion process.',
                    HINT := 'Check the input parameters values, correct them if needed, and try again.';

    WHEN invalid_character_value_for_cast THEN
        RAISE USING MESSAGE := pg_catalog.format('The conversion of a varchar data type to a %s data type resulted in an out-of-range value.', PG_CATALOG.lower(v_res_datatype)),
                    DETAIL := 'Use of incorrect pair of input parameter values during conversion process.',
                    HINT := 'Check input parameter values, correct them if needed, and try again.';

    WHEN invalid_escape_sequence THEN
        RAISE USING MESSAGE := pg_catalog.format('Invalid CONVERSION_LANG constant value - ''%s''. Allowed values are: ''English'', ''Deutsch'', etc.',
                                      CONVERSION_LANG),
                    DETAIL := 'Compiled incorrect CONVERSION_LANG constant value in function''s body.',
                    HINT := 'Correct CONVERSION_LANG constant value in function''s body, recompile it and try again.';

    WHEN invalid_text_representation THEN
        GET STACKED DIAGNOSTICS v_err_message = MESSAGE_TEXT;
        v_err_message := substring(pg_catalog.lower(v_err_message), 'integer\:\s\"(.*)\"');

        RAISE USING MESSAGE := pg_catalog.format('Error while trying to convert "%s" value to SMALLINT data type.',
                                      v_err_message),
                    DETAIL := 'Passed argument value contains illegal characters.',
                    HINT := 'Correct passed argument value, remove all illegal characters.';
END;
$BODY$
LANGUAGE plpgsql
IMMUTABLE
RETURNS NULL ON NULL INPUT;

-- conversion to smalldatetime
CREATE OR REPLACE FUNCTION sys.shark_try_conv_string_to_datetime_v2(IN p_datatype TEXT,
                                                                         IN p_datetimestring TEXT,
                                                                         IN p_style NUMERIC DEFAULT 0)
RETURNS TIMESTAMP WITHOUT TIME ZONE
AS
$BODY$
BEGIN
    RETURN sys.shark_conv_string_to_datetime_v2(p_datatype,
                                                     p_datetimestring ,
                                                     p_style);
EXCEPTION
    WHEN OTHERS THEN
        RETURN NULL;
END;
$BODY$
LANGUAGE plpgsql
IMMUTABLE
RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION sys.shark_conv_string_to_datetimeoffset(IN p_datatype TEXT,
                                                                     IN p_datetimestring TEXT,
                                                                     IN p_style NUMERIC DEFAULT 0)
RETURNS TIMESTAMP WITH TIME ZONE
AS
$BODY$
DECLARE
    v_sign VARCHAR COLLATE "C";
    v_offhours VARCHAR COLLATE "C";
    v_offminutes VARCHAR COLLATE "C";
    v_timepart VARCHAR COLLATE "C";
    v_datestring VARCHAR COLLATE "C";
    v_datetimestring VARCHAR COLLATE "C";
    v_regmatch_groups TEXT[];
    v_resdatetime TIMESTAMP(6) WITHOUT TIME ZONE;
    v_resdatetime_string VARCHAR COLLATE "C";
    DAYMM_REGEXP CONSTANT VARCHAR COLLATE "C" := '(\d{1,2})';
    FULLYEAR_REGEXP CONSTANT VARCHAR COLLATE "C" := '(\d{4})';
    AMPM_REGEXP CONSTANT VARCHAR COLLATE "C" := '(?:[AP]M)';
    TIMEUNIT_REGEXP CONSTANT VARCHAR COLLATE "C" := '\s*\d{1,2}\s*';
    FRACTSECS_REGEXP CONSTANT VARCHAR COLLATE "C" := '\s*\d{1,9}\s*';
    TIME_OFFSET_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('\s*((\-|\+)\s*(', TIMEUNIT_REGEXP, ')\s*\:\s*(', TIMEUNIT_REGEXP, ')|Z)\s*');
    HHMMSSFSOFF_PART_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('(', TIMEUNIT_REGEXP, AMPM_REGEXP, '|',
                                                    TIMEUNIT_REGEXP, '\:', TIMEUNIT_REGEXP, AMPM_REGEXP, '?|',
                                                    TIMEUNIT_REGEXP, '\:', TIMEUNIT_REGEXP, '\:', TIMEUNIT_REGEXP, AMPM_REGEXP, '?|',
                                                    TIMEUNIT_REGEXP, '\:', TIMEUNIT_REGEXP, '\:', TIMEUNIT_REGEXP, '(?:\.|\:)', FRACTSECS_REGEXP, AMPM_REGEXP, '?)(', TIME_OFFSET_REGEXP, ')?');
    W3C_XML_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', FULLYEAR_REGEXP, '-', DAYMM_REGEXP, '-', DAYMM_REGEXP, '(', '(\-|\+)', '\s*(\d{2})\s*', '\:', '\s*(\d{2})\s*', '|', 'Z', ')','$');
    W3C_XML_Z_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', FULLYEAR_REGEXP, '-', DAYMM_REGEXP, '-', DAYMM_REGEXP, 'Z','$');
BEGIN
    v_datetimestring := pg_catalog.upper(pg_catalog.btrim(p_datetimestring));
    v_resdatetime := sys.shark_conv_string_to_datetime2(p_datatype, p_datetimestring, p_style);

    v_timepart := pg_catalog.btrim(substring(v_datetimestring, PG_CATALOG.concat('(', HHMMSSFSOFF_PART_REGEXP, ')')));
    v_datestring := pg_catalog.btrim(regexp_replace(v_datetimestring, PG_CATALOG.concat('T?', '(', HHMMSSFSOFF_PART_REGEXP, ')'), '', 'gi'));

    -- Get the time offset value
    IF (v_datetimestring ~* W3C_XML_REGEXP)
    THEN
        v_regmatch_groups := regexp_matches(v_datetimestring, W3C_XML_REGEXP, 'gi');

        IF (v_datetimestring !~* W3C_XML_Z_REGEXP)
        THEN
            v_sign := v_regmatch_groups[5];
            v_offhours := v_regmatch_groups[6];
            v_offminutes := v_regmatch_groups[7];
        ELSE
            v_sign := '+';
            v_offhours := '0';
            v_offminutes := '0';
        END IF;
    ELSE
        BEGIN
            v_sign := coalesce(sys.shark_get_timeunit_from_string(v_timepart, 'OFFSIGN'), '+');
            v_offhours := coalesce(sys.shark_get_timeunit_from_string(v_timepart, 'OFFHOURS'), '0');
            v_offminutes := coalesce(sys.shark_get_timeunit_from_string(v_timepart, 'OFFMINUTES'), '0');
        EXCEPTION
            WHEN OTHERS THEN
                RAISE invalid_character_value_for_cast;
        END;
    END IF;

    -- validate offset
    IF ((v_offhours::SMALLINT NOT BETWEEN 0 AND 14) OR
        (v_offminutes::SMALLINT NOT BETWEEN 0 AND 59) OR
        (v_offhours::SMALLINT = 14 AND v_offminutes::SMALLINT != 0))
    THEN
        RAISE invalid_character_value_for_cast;
    END IF;

    v_resdatetime_string := PG_CATALOG.concat(v_resdatetime::PG_CATALOG.TEXT,v_sign,v_offhours,':',v_offminutes);

    RETURN CAST(v_resdatetime_string AS TIMESTAMP WITH TIME ZONE);
EXCEPTION
    WHEN invalid_character_value_for_cast THEN
        RAISE USING MESSAGE := 'The conversion of a varchar data type to a timestamp with time zone data type resulted in an out-of-range value.',
                    DETAIL := 'Use of incorrect pair of input parameter values during conversion process.',
                    HINT := 'Check input parameter values, correct them if needed, and try again.';
END;
$BODY$
LANGUAGE plpgsql
IMMUTABLE
RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION sys.shark_try_conv_string_to_datetimeoffset(IN p_datatype TEXT,
                                                                         IN p_datetimestring TEXT,
                                                                         IN p_style NUMERIC DEFAULT 0)
RETURNS TIMESTAMP WITH TIME ZONE
AS
$BODY$
BEGIN
    RETURN sys.shark_conv_string_to_datetimeoffset(p_datatype,
                                                        p_datetimestring ,
                                                        p_style);
EXCEPTION
    WHEN OTHERS THEN
        RETURN NULL;
END;
$BODY$
LANGUAGE plpgsql
IMMUTABLE
RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION sys.shark_conv_helper_to_datetimeoffset(IN typmod INTEGER,
                                                            IN arg TEXT,
                                                            IN p_try BOOL,
													        IN p_style NUMERIC DEFAULT 0)
RETURNS TIMESTAMP WITH TIME ZONE
AS
$BODY$
DECLARE
    v_res_datatype TEXT COLLATE "C";
BEGIN
    IF (typmod = -1) THEN
        v_res_datatype := 'TIMESTAMP WITH TIME ZONE';
    ELSE
        v_res_datatype := PG_CATALOG.format('TIMESTAMP WITH TIME ZONE(%s)', typmod);
    END IF;

    IF p_try THEN
	    RETURN sys.shark_try_conv_string_to_datetimeoffset(v_res_datatype, arg, p_style);
    ELSE
        RETURN sys.shark_conv_string_to_datetimeoffset(v_res_datatype, arg, p_style);
    END IF;
END;
$BODY$
LANGUAGE plpgsql
IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.shark_conv_helper_to_datetimeoffset(IN typmod INTEGER,
                                                            IN arg VARCHAR,
                                                            IN p_try BOOL,
													        IN p_style NUMERIC DEFAULT 0)
RETURNS TIMESTAMP WITH TIME ZONE
AS
$BODY$
BEGIN
    RETURN sys.shark_conv_helper_to_datetimeoffset(typmod, arg::TEXT, p_try, p_style);
END;
$BODY$
LANGUAGE plpgsql
IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.shark_conv_helper_to_datetimeoffset(IN typmod INTEGER,
                                                            IN arg NVARCHAR2,
                                                            IN p_try BOOL,
													        IN p_style NUMERIC DEFAULT 0)
RETURNS TIMESTAMP WITH TIME ZONE
AS
$BODY$
BEGIN
    RETURN sys.shark_conv_helper_to_datetimeoffset(typmod, arg::TEXT, p_try, p_style);
END;
$BODY$
LANGUAGE plpgsql
IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.shark_conv_helper_to_datetimeoffset(IN typmod INTEGER,
                                                            IN arg BPCHAR,
                                                            IN p_try BOOL,
													        IN p_style NUMERIC DEFAULT 0)
RETURNS TIMESTAMP WITH TIME ZONE
AS
$BODY$
BEGIN
    RETURN sys.shark_conv_helper_to_datetimeoffset(typmod, arg::TEXT, p_try, p_style);
END;
$BODY$
LANGUAGE plpgsql
IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.shark_conv_helper_to_smalldatetime(IN typmod INTEGER,
                                                            IN arg TEXT,
                                                            IN p_try BOOL,
													        IN p_style NUMERIC DEFAULT 0)
RETURNS SMALLDATETIME
AS
$BODY$
DECLARE
    v_res_datatype TEXT COLLATE "C";
BEGIN
    IF (typmod = -1) THEN
        v_res_datatype := 'SMALLDATETIME';
    ELSE
        v_res_datatype := PG_CATALOG.format('SMALLDATETIME(%s)', typmod);
    END IF;

    IF p_try THEN
	    RETURN sys.shark_try_conv_string_to_datetime_v2(v_res_datatype, arg, p_style);
    ELSE
        RETURN sys.shark_conv_string_to_datetime_v2(v_res_datatype, arg, p_style);
    END IF;
END;
$BODY$
LANGUAGE plpgsql
IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.shark_conv_helper_to_smalldatetime(IN typmod INTEGER,
                                                            IN arg VARCHAR,
                                                            IN p_try BOOL,
													        IN p_style NUMERIC DEFAULT 0)
RETURNS SMALLDATETIME
AS
$BODY$
BEGIN
    RETURN sys.shark_conv_helper_to_smalldatetime(typmod, arg::TEXT, p_try, p_style);
END;
$BODY$
LANGUAGE plpgsql
IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.shark_conv_helper_to_smalldatetime(IN typmod INTEGER,
                                                            IN arg NVARCHAR2,
                                                            IN p_try BOOL,
													        IN p_style NUMERIC DEFAULT 0)
RETURNS SMALLDATETIME
AS
$BODY$
BEGIN
    RETURN sys.shark_conv_helper_to_smalldatetime(typmod, arg::TEXT, p_try, p_style);
END;
$BODY$
LANGUAGE plpgsql
IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.shark_conv_helper_to_smalldatetime(IN typmod INTEGER,
                                                            IN arg BPCHAR,
                                                            IN p_try BOOL,
													        IN p_style NUMERIC DEFAULT 0)
RETURNS SMALLDATETIME
AS
$BODY$
BEGIN
    RETURN sys.shark_conv_helper_to_smalldatetime(typmod, arg::TEXT, p_try, p_style);
END;
$BODY$
LANGUAGE plpgsql
IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.shark_conv_datetime_to_string(IN p_datatype TEXT,
                                                                     IN p_src_datatype TEXT,
                                                                     IN p_datetimeval TIMESTAMP(6) WITHOUT TIME ZONE,
                                                                     IN p_style NUMERIC DEFAULT -1)
RETURNS TEXT
AS
$BODY$
DECLARE
    v_day VARCHAR COLLATE "C";
    v_hour VARCHAR COLLATE "C";
    v_month SMALLINT;
    v_style SMALLINT;
    v_scale SMALLINT;
    v_resmask VARCHAR COLLATE "C";
    v_language VARCHAR COLLATE "C";
    v_datatype VARCHAR COLLATE "C";
    v_fseconds VARCHAR COLLATE "C";
    v_fractsep VARCHAR COLLATE "C";
    v_monthname VARCHAR COLLATE "C";
    v_resstring VARCHAR COLLATE "C";
    v_lengthexpr VARCHAR COLLATE "C";
    v_maxlength SMALLINT;
    v_res_length SMALLINT;
    v_err_message VARCHAR COLLATE "C";
    v_src_datatype VARCHAR COLLATE "C";
    v_res_datatype VARCHAR COLLATE "C";
    v_lang_metadata_json JSON;
    VARCHAR_MAX CONSTANT SMALLINT := 8000;
    NVARCHAR_MAX CONSTANT SMALLINT := 4000;
    CONVERSION_LANG CONSTANT VARCHAR COLLATE "C" := '';
    DATATYPE_REGEXP CONSTANT VARCHAR COLLATE "C" := '^\s*(CHAR|BPCHAR|NCHAR|CHARACTER|NVARCHAR|NVARCHAR2|VARCHAR|CHARACTER VARYING)\s*$';
    SRCDATATYPE_MASK_REGEXP VARCHAR COLLATE "C" := '^(?:TIMESTAMP WITHOUT TIME ZONE|SMALLDATETIME)\s*(?:\s*\(\s*(\d+)\s*\)\s*)?$';
    DATATYPE_MASK_REGEXP CONSTANT VARCHAR COLLATE "C" := '^\s*(?:CHAR|BPCHAR|NCHAR|CHARACTER|NVARCHAR|NVARCHAR2|VARCHAR|CHARACTER VARYING)\s*\(\s*(\d+|MAX)\s*\)\s*$';
    v_datetimeval TIMESTAMP(6) WITHOUT TIME ZONE;
BEGIN
    v_datatype := pg_catalog.upper(pg_catalog.btrim(p_datatype));
    v_src_datatype := pg_catalog.upper(pg_catalog.btrim(p_src_datatype));
    v_style := floor(p_style)::SMALLINT;

    IF (v_src_datatype ~* SRCDATATYPE_MASK_REGEXP)
    THEN
        v_scale := substring(v_src_datatype, SRCDATATYPE_MASK_REGEXP)::SMALLINT;

        v_src_datatype := PG_CATALOG.rtrim(split_part(v_src_datatype, '(', 1));

        IF (v_src_datatype <> 'TIMESTAMP WITHOUT TIME ZONE' AND v_scale IS NOT NULL) THEN
            RAISE invalid_indicator_parameter_value;
        ELSIF (v_scale NOT BETWEEN 0 AND 7) THEN
            RAISE invalid_regular_expression;
        END IF;

        v_scale := coalesce(v_scale, 7);
    ELSE
        RAISE most_specific_type_mismatch;
    END IF;

    IF (scale(p_style) > 0) THEN
        RAISE escape_character_conflict;
    ELSIF (NOT ((v_style BETWEEN 0 AND 14) OR
                (v_style BETWEEN 20 AND 25) OR
                (v_style BETWEEN 100 AND 114) OR
                v_style IN (-1, 120, 121, 126, 127, 130, 131)))
    THEN
        RAISE invalid_parameter_value;
    END IF;

    IF (v_datatype ~* DATATYPE_MASK_REGEXP) THEN
        v_res_datatype := PG_CATALOG.rtrim(split_part(v_datatype, '(', 1));

        v_maxlength := CASE
                          WHEN (v_res_datatype IN ('CHAR', 'VARCHAR')) THEN VARCHAR_MAX
                          ELSE NVARCHAR_MAX
                       END;

        v_lengthexpr := substring(v_datatype, DATATYPE_MASK_REGEXP);

        IF (v_lengthexpr <> 'MAX' AND char_length(v_lengthexpr) > 4)
        THEN
            RAISE interval_field_overflow;
        END IF;

        v_res_length := CASE v_lengthexpr
                           WHEN 'MAX' THEN v_maxlength
                           ELSE v_lengthexpr::SMALLINT
                        END;
    ELSIF (v_datatype ~* DATATYPE_REGEXP) THEN
        v_res_datatype := v_datatype;
    ELSE
        RAISE datatype_mismatch;
    END IF;

    v_datetimeval := CASE
                        WHEN (v_style NOT IN (130, 131)) THEN p_datetimeval
                        ELSE sys.shark_conv_greg_to_hijri(p_datetimeval) + INTERVAL '1 day'
                     END;

    v_day := PG_CATALOG.ltrim(to_char(v_datetimeval, 'DD'), '0');
    v_hour := PG_CATALOG.ltrim(to_char(v_datetimeval, 'HH12'), '0');
    v_month := to_char(v_datetimeval, 'MM')::SMALLINT;

    v_language := CASE
                     WHEN (v_style IN (130, 131)) THEN 'HIJRI'
                     ELSE CONVERSION_LANG
                  END;
    BEGIN
        v_lang_metadata_json := sys.shark_get_lang_metadata_json(v_language);
    EXCEPTION
        WHEN OTHERS THEN
        RAISE invalid_character_value_for_cast;
    END;

    v_monthname := (v_lang_metadata_json -> 'months_shortnames') ->> v_month - 1;

    IF (v_src_datatype IN ('TIMESTAMP WITHOUT TIME ZONE', 'SMALLDATETIME')) THEN
        v_fseconds := sys.shark_round_fractseconds(to_char(v_datetimeval, 'MS'));

        IF (v_fseconds::INTEGER = 1000) THEN
            v_fseconds := '000';
            v_datetimeval := v_datetimeval + INTERVAL '1 second';
        ELSE
            v_fseconds := lpad(v_fseconds, 3, '0');
        END IF;
    ELSE
        v_fseconds := sys.shark_get_microsecs_from_fractsecs_v2(to_char(v_datetimeval, 'US'), v_scale);

        -- Following condition will handle overflow of fractsecs
        IF (v_fseconds::INTEGER < 0) THEN
            v_fseconds := PG_CATALOG.repeat('0', LEAST(v_scale, 6));
            v_datetimeval := v_datetimeval + INTERVAL '1 second';
        END IF;

        IF (v_scale = 7) THEN
            v_fseconds := pg_catalog.concat(v_fseconds, '0');
        END IF;
    END IF;

    v_fractsep := CASE v_src_datatype
                     WHEN 'TIMESTAMP WITHOUT TIME ZONE' THEN '.'
                     ELSE ':'
                  END;

    IF ((v_style = -1 AND v_src_datatype <> 'TIMESTAMP WITHOUT TIME ZONE') OR
        v_style IN (0, 9, 100, 109))
    THEN
        v_resmask := pg_catalog.format('$mnme$ %s YYYY %s:MI%s',
                            lpad(v_day, 2, ' '),
                            lpad(v_hour, 2, ' '),
                            CASE
                               WHEN (v_style IN (-1, 0, 100)) THEN 'AM'
                               ELSE pg_catalog.format(':SS:%sAM', v_fseconds)
                            END);
    ELSIF (v_style = 1) THEN
        v_resmask := 'MM/DD/YY';
    ELSIF (v_style = 101) THEN
        v_resmask := 'MM/DD/YYYY';
    ELSIF (v_style = 2) THEN
        v_resmask := 'YY.MM.DD';
    ELSIF (v_style = 102) THEN
        v_resmask := 'YYYY.MM.DD';
    ELSIF (v_style = 3) THEN
        v_resmask := 'DD/MM/YY';
    ELSIF (v_style = 103) THEN
        v_resmask := 'DD/MM/YYYY';
    ELSIF (v_style = 4) THEN
        v_resmask := 'DD.MM.YY';
    ELSIF (v_style = 104) THEN
        v_resmask := 'DD.MM.YYYY';
    ELSIF (v_style = 5) THEN
        v_resmask := 'DD-MM-YY';
    ELSIF (v_style = 105) THEN
        v_resmask := 'DD-MM-YYYY';
    ELSIF (v_style = 6) THEN
        v_resmask := 'DD $mnme$ YY';
    ELSIF (v_style = 106) THEN
        v_resmask := 'DD $mnme$ YYYY';
    ELSIF (v_style = 7) THEN
        v_resmask := '$mnme$ DD, YY';
    ELSIF (v_style = 107) THEN
        v_resmask := '$mnme$ DD, YYYY';
    ELSIF (v_style IN (8, 24, 108)) THEN
        v_resmask := 'HH24:MI:SS';
    ELSIF (v_style = 10) THEN
        v_resmask := 'MM-DD-YY';
    ELSIF (v_style = 110) THEN
        v_resmask := 'MM-DD-YYYY';
    ELSIF (v_style = 11) THEN
        v_resmask := 'YY/MM/DD';
    ELSIF (v_style = 111) THEN
        v_resmask := 'YYYY/MM/DD';
    ELSIF (v_style = 12) THEN
        v_resmask := 'YYMMDD';
    ELSIF (v_style = 112) THEN
        v_resmask := 'YYYYMMDD';
    ELSIF (v_style IN (13, 113)) THEN
        v_resmask := pg_catalog.format('DD $mnme$ YYYY HH24:MI:SS%s%s', v_fractsep, v_fseconds);
    ELSIF (v_style IN (14, 114)) THEN
        v_resmask := pg_catalog.format('HH24:MI:SS%s%s', v_fractsep, v_fseconds);
    ELSIF (v_style IN (20, 120)) THEN
        v_resmask := 'YYYY-MM-DD HH24:MI:SS';
    ELSIF ((v_style = -1 AND v_src_datatype = 'TIMESTAMP WITHOUT TIME ZONE') OR
           v_style IN (21, 25, 121))
    THEN
        v_resmask := pg_catalog.format('YYYY-MM-DD HH24:MI:SS.%s', v_fseconds);
    ELSIF (v_style = 22) THEN
        v_resmask := pg_catalog.format('MM/DD/YY %s:MI:SS AM', lpad(v_hour, 2, ' '));
    ELSIF (v_style = 23) THEN
        v_resmask := 'YYYY-MM-DD';
    ELSIF (v_style IN (126, 127)) THEN
        v_resmask := CASE v_src_datatype
                        WHEN 'SMALLDATETIME' THEN 'YYYY-MM-DDT$rem$HH24:MI:SS'
                        ELSE pg_catalog.format('YYYY-MM-DDT$rem$HH24:MI:SS.%s', v_fseconds)
                     END;
    ELSIF (v_style IN (130, 131)) THEN
        v_resmask := pg_catalog.concat(CASE p_style
                               WHEN 131 THEN pg_catalog.format('%s/MM/YYYY ', lpad(v_day, 2, ' '))
                               ELSE pg_catalog.format('%s $mnme$ YYYY ', lpad(v_day, 2, ' '))
                            END,
                            pg_catalog.format('%s:MI:SS%s%sAM', lpad(v_hour, 2, ' '), v_fractsep, v_fseconds));
    END IF;

    v_resstring := to_char(v_datetimeval, v_resmask);
    v_resstring := pg_catalog.replace(v_resstring, '$mnme$', v_monthname);
    v_resstring := pg_catalog.replace(v_resstring, '$rem$', '');

    v_resstring := substring(v_resstring, 1, coalesce(v_res_length, char_length(v_resstring)));
    v_res_length := coalesce(v_res_length,
                             CASE v_res_datatype
                                WHEN 'CHAR' THEN 30
                                ELSE 60
                             END);
    RETURN CASE
              WHEN (v_res_datatype NOT IN ('CHAR', 'NCHAR')) THEN v_resstring
              ELSE rpad(v_resstring, v_res_length, ' ')
           END;
EXCEPTION
    WHEN most_specific_type_mismatch THEN
        RAISE USING MESSAGE := 'Source data type should be one of these values: ''TIMESTAMP WITHOUT TIME ZONE'', ''SMALLDATETIME''.',
                    DETAIL := 'Use of incorrect "src_datatype" parameter value during conversion process.',
                    HINT := 'Change "srcdatatype" parameter to the proper value and try again.';

   WHEN invalid_regular_expression THEN
       RAISE USING MESSAGE := pg_catalog.format('The source data type scale (%s) given to the convert specification exceeds the maximum allowable value (7).',
                                     v_scale),
                   DETAIL := 'Use of incorrect scale value of source data type parameter during conversion process.',
                   HINT := 'Change scale component of source data type parameter to the allowable value and try again.';

    WHEN invalid_indicator_parameter_value THEN
        RAISE USING MESSAGE := pg_catalog.format('Invalid attributes specified for data type %s.', v_src_datatype),
                    DETAIL := 'Use of incorrect scale value, which is not corresponding to specified data type.',
                    HINT := 'Change data type scale component or select different data type and try again.';

    WHEN escape_character_conflict THEN
        RAISE USING MESSAGE := 'Argument data type NUMERIC is invalid for argument 4 of convert function.',
                    DETAIL := 'Use of incorrect "style" parameter value during conversion process.',
                    HINT := 'Change "style" parameter to the proper value and try again.';

    WHEN invalid_parameter_value THEN
        RAISE USING MESSAGE := pg_catalog.format('%s is not a valid style number when converting from %s to a character string.',
                                      v_style, v_src_datatype),
                    DETAIL := 'Use of incorrect "style" parameter value during conversion process.',
                    HINT := 'Change "style" parameter to the proper value and try again.';

    WHEN interval_field_overflow THEN
        RAISE USING MESSAGE := pg_catalog.format('The size (%s) given to the convert specification ''%s'' exceeds the maximum allowed for any data type (%s).',
                                      v_lengthexpr, pg_catalog.lower(v_res_datatype), v_maxlength),
                    DETAIL := 'Use of incorrect size value of data type parameter during conversion process.',
                    HINT := 'Change size component of data type parameter to the allowable value and try again.';

    WHEN datatype_mismatch THEN
        RAISE USING MESSAGE := 'Data type should be one of these values: ''CHAR(n|MAX)'', ''NCHAR(n|MAX)'', ''VARCHAR(n|MAX)'', ''NVARCHAR(n|MAX)''.',
                    DETAIL := 'Use of incorrect "datatype" parameter value during conversion process.',
                    HINT := 'Change "datatype" parameter to the proper value and try again.';

    WHEN invalid_character_value_for_cast THEN
        RAISE USING MESSAGE := pg_catalog.format('Invalid CONVERSION_LANG constant value - ''%s''. Allowed values are: ''English'', ''Deutsch'', etc.',
                                      CONVERSION_LANG),
                    DETAIL := 'Compiled incorrect CONVERSION_LANG constant value in function''s body.',
                    HINT := 'Correct CONVERSION_LANG constant value in function''s body, recompile it and try again.';

    WHEN invalid_text_representation THEN
        GET STACKED DIAGNOSTICS v_err_message = MESSAGE_TEXT;
        v_err_message := substring(pg_catalog.lower(v_err_message), 'integer\:\s\"(.*)\"');

        RAISE USING MESSAGE := pg_catalog.format('Error while trying to convert "%s" value to SMALLINT data type.',
                                      v_err_message),
                    DETAIL := 'Supplied value contains illegal characters.',
                    HINT := 'Correct supplied value, remove all illegal characters.';
END;
$BODY$
LANGUAGE plpgsql
STABLE
RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION sys.shark_conv_string_to_datetime2(IN p_datatype TEXT,
                                                                        IN p_datetimestring TEXT,
                                                                        IN p_style NUMERIC DEFAULT 0)
RETURNS TIMESTAMP WITHOUT TIME ZONE
AS
$BODY$
DECLARE
    v_day VARCHAR COLLATE "C";
    v_year VARCHAR COLLATE "C";
    v_month VARCHAR COLLATE "C";
    v_style SMALLINT;
    v_scale SMALLINT;
    v_hours VARCHAR COLLATE "C";
    v_hijridate DATE;
    v_minutes VARCHAR COLLATE "C";
    v_seconds VARCHAR COLLATE "C";
    v_fseconds VARCHAR COLLATE "C";
    v_sign VARCHAR COLLATE "C" = NULL::VARCHAR;
    v_offhours VARCHAR COLLATE "C" = NULL::VARCHAR;
    v_offminutes VARCHAR COLLATE "C" = NULL::VARCHAR;
    v_datatype VARCHAR COLLATE "C";
    v_timepart VARCHAR COLLATE "C";
    v_leftpart VARCHAR COLLATE "C";
    v_middlepart VARCHAR COLLATE "C";
    v_rightpart VARCHAR COLLATE "C";
    v_datestring VARCHAR COLLATE "C";
    v_err_message VARCHAR COLLATE "C";
    v_date_format VARCHAR COLLATE "C";
    v_res_datatype VARCHAR COLLATE "C";
    v_datetimestring VARCHAR COLLATE "C";
    v_datatype_groups TEXT[];
    v_regmatch_groups TEXT[];
    v_lang_metadata_json JSON;
    v_compmonth_regexp VARCHAR COLLATE "C";
    v_resdatetime TIMESTAMP(6) WITHOUT TIME ZONE;
    v_language VARCHAR COLLATE "C";
    CONVERSION_LANG CONSTANT VARCHAR COLLATE "C" := '';
    DATE_FORMAT CONSTANT VARCHAR COLLATE "C" := '';
    DAYMM_REGEXP CONSTANT VARCHAR COLLATE "C" := '(\d{1,2})';
    FULLYEAR_REGEXP CONSTANT VARCHAR COLLATE "C" := '(\d{4})';
    SHORTYEAR_REGEXP CONSTANT VARCHAR COLLATE "C" := '(\d{1,2})';
    COMPYEAR_REGEXP CONSTANT VARCHAR COLLATE "C" := '(\d{1,2}|\d{4})';
    AMPM_REGEXP CONSTANT VARCHAR COLLATE "C" := '(?:[AP]M)';
    MASKSEP_REGEXP CONSTANT VARCHAR COLLATE "C" := '(?:\.|-|/)';
    TIMEUNIT_REGEXP CONSTANT VARCHAR COLLATE "C" := '\s*\d{1,2}\s*';
    FRACTSECS_REGEXP CONSTANT VARCHAR COLLATE "C" := '\s*\d{1,9}\s*';
    DATATYPE_REGEXP CONSTANT VARCHAR COLLATE "C" := '^(DATE|TIME|TIMESTAMP WITHOUT TIME ZONE|TIMESTAMP WITH TIME ZONE)\s*(?:\()?\s*((?:-)?\d+)?\s*(?:\))?$';
    TIME_OFFSET_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('\s*((\-|\+)\s*(', TIMEUNIT_REGEXP, ')\s*\:\s*(', TIMEUNIT_REGEXP, ')|Z)\s*');
    HHMMSSFSOFF_PART_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('(', TIMEUNIT_REGEXP, AMPM_REGEXP, '|',
                                                    TIMEUNIT_REGEXP, '\:', TIMEUNIT_REGEXP, AMPM_REGEXP, '?|',
                                                    TIMEUNIT_REGEXP, '\:', TIMEUNIT_REGEXP, '\:', TIMEUNIT_REGEXP, AMPM_REGEXP, '?|',
                                                    TIMEUNIT_REGEXP, '\:', TIMEUNIT_REGEXP, '\:', TIMEUNIT_REGEXP, '(?:\.|\:)', FRACTSECS_REGEXP, AMPM_REGEXP, '?)(', TIME_OFFSET_REGEXP, ')?');
    HHMMSSFSOFF_DOT_PART_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('(', TIMEUNIT_REGEXP, AMPM_REGEXP, '|',
                                                        TIMEUNIT_REGEXP, '\:', TIMEUNIT_REGEXP, AMPM_REGEXP, '?|',
                                                        TIMEUNIT_REGEXP, '\:', TIMEUNIT_REGEXP, '\:', TIMEUNIT_REGEXP, AMPM_REGEXP, '?|',
                                                        TIMEUNIT_REGEXP, '\:', TIMEUNIT_REGEXP, '\:', TIMEUNIT_REGEXP, '(?:\.)', FRACTSECS_REGEXP, AMPM_REGEXP, '?)(', TIME_OFFSET_REGEXP, ')?');
    HHMMSSFSOFF_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^(', HHMMSSFSOFF_PART_REGEXP, ')$');
    DEFMASK1_0_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', '($comp_month$)\s*', DAYMM_REGEXP, '\s+', COMPYEAR_REGEXP, '\s*(\s+', '(', HHMMSSFSOFF_PART_REGEXP, ')', ')?$');
    DEFMASK1_1_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', '($comp_month$)\s*', DAYMM_REGEXP, '\s+', COMPYEAR_REGEXP, '$');
    DEFMASK2_0_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', DAYMM_REGEXP, '\s*($comp_month$)\s*,?\s*', COMPYEAR_REGEXP, '\s*(\s+', '(', HHMMSSFSOFF_PART_REGEXP, ')', ')?$');
    DEFMASK2_1_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', DAYMM_REGEXP, '\s*($comp_month$)\s*,?\s*', COMPYEAR_REGEXP, '$');
    DEFMASK3_0_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', FULLYEAR_REGEXP, '\s*($comp_month$)\s*', DAYMM_REGEXP, '\s*(\s+', '(', HHMMSSFSOFF_PART_REGEXP, ')', ')?$');
    DEFMASK3_1_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', FULLYEAR_REGEXP, '\s*($comp_month$)\s*', DAYMM_REGEXP, '$');
    DEFMASK4_0_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', FULLYEAR_REGEXP, '\s+', DAYMM_REGEXP, '\s*($comp_month$)', '\s*(', HHMMSSFSOFF_PART_REGEXP, ')?$');
    DEFMASK4_1_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', FULLYEAR_REGEXP, '\s+', DAYMM_REGEXP, '\s*($comp_month$)$');
    DEFMASK5_0_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', DAYMM_REGEXP, '\s+', COMPYEAR_REGEXP, '\s*($comp_month$)', '\s*(', HHMMSSFSOFF_PART_REGEXP, ')?$');
    DEFMASK5_1_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', DAYMM_REGEXP, '\s+', COMPYEAR_REGEXP, '\s*($comp_month$)$');
    DEFMASK6_0_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', '($comp_month$)\s*', FULLYEAR_REGEXP, '\s+', DAYMM_REGEXP, '\s*(\s+', '(', HHMMSSFSOFF_PART_REGEXP, ')', ')?$');
    DEFMASK6_1_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', '($comp_month$)\s*', FULLYEAR_REGEXP, '\s+', DAYMM_REGEXP, '$');
    DEFMASK7_0_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', '($comp_month$)\s*', DAYMM_REGEXP, '\s*,\s*', COMPYEAR_REGEXP, '\s*(\s+', '(', HHMMSSFSOFF_PART_REGEXP, ')', ')?$');
    DEFMASK7_1_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', '($comp_month$)\s*', DAYMM_REGEXP, '\s*,\s*', COMPYEAR_REGEXP, '$');
    DEFMASK8_0_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', COMPYEAR_REGEXP, '\s*($comp_month$)', '\s*(', HHMMSSFSOFF_PART_REGEXP, ')?$');
    DEFMASK8_1_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', COMPYEAR_REGEXP, '\s*($comp_month$)$');
    DEFMASK8_2_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', SHORTYEAR_REGEXP, '\s*($comp_month$)$');
    DEFMASK9_0_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', '($comp_month$)\s*,?\s*', COMPYEAR_REGEXP, '\s*(\s+', '(', HHMMSSFSOFF_PART_REGEXP, ')', ')?$');
    DEFMASK9_1_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', '($comp_month$)\s*\,?\s*', COMPYEAR_REGEXP, '$');
    DEFMASK9_2_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', '($comp_month$)\s*', SHORTYEAR_REGEXP, '$');
    DEFMASK9_3_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', '($comp_month$)\s*\,?\s*', FULLYEAR_REGEXP, '$');
    DEFMASK10_0_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', DAYMM_REGEXP, '\s*', MASKSEP_REGEXP, '\s*($comp_month$)\s*', MASKSEP_REGEXP, '\s*', COMPYEAR_REGEXP, '\s*(\s+', '(', HHMMSSFSOFF_PART_REGEXP, ')', ')?$');
    DEFMASK10_1_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', DAYMM_REGEXP, '\s*', MASKSEP_REGEXP, '\s*($comp_month$)\s*', MASKSEP_REGEXP, '\s*', COMPYEAR_REGEXP, '$');
    DEFMASK10_2_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', DAYMM_REGEXP, '\s*-\s*($comp_month$)\s*-\s*', COMPYEAR_REGEXP, '$');
    DEFMASK10_3_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', DAYMM_REGEXP, '\s*\/\s*($comp_month$)\s*\/\s*', COMPYEAR_REGEXP, '$');
    DEFMASK10_4_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', DAYMM_REGEXP, '\s*\.\s*($comp_month$)\s*\.\s*', COMPYEAR_REGEXP, '$');
    DOT_SLASH_DASH_COMPYEAR1_0_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', DAYMM_REGEXP, '\s*', MASKSEP_REGEXP, '\s*', DAYMM_REGEXP, '\s*', MASKSEP_REGEXP, '\s*', COMPYEAR_REGEXP, '\s*(\s+', '(', HHMMSSFSOFF_PART_REGEXP, ')', ')?$');
    DOT_SLASH_DASH_COMPYEAR1_1_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', DAYMM_REGEXP, '\s*', MASKSEP_REGEXP, '\s*', DAYMM_REGEXP, '\s*', MASKSEP_REGEXP, '\s*', COMPYEAR_REGEXP, '$');
    DASH_COMPYEAR1_1_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', DAYMM_REGEXP, '\s*-\s*', DAYMM_REGEXP, '\s*-\s*', COMPYEAR_REGEXP, '$');
    SLASH_COMPYEAR1_1_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', DAYMM_REGEXP, '\s*\/\s*', DAYMM_REGEXP, '\s*\/\s*', COMPYEAR_REGEXP, '$');
    DOT_COMPYEAR1_1_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', DAYMM_REGEXP, '\s*\.\s*', DAYMM_REGEXP, '\s*\.\s*', COMPYEAR_REGEXP, '$');
    DOT_SLASH_DASH_SHORTYEAR_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', DAYMM_REGEXP, '\s*', MASKSEP_REGEXP, '\s*', DAYMM_REGEXP, '\s*', MASKSEP_REGEXP, '\s*', SHORTYEAR_REGEXP, '$');
    DOT_SLASH_DASH_FULLYEAR1_0_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', DAYMM_REGEXP, '\s*', MASKSEP_REGEXP, '\s*', DAYMM_REGEXP, '\s*', MASKSEP_REGEXP, '\s*', FULLYEAR_REGEXP, '\s*(\s+', '(', HHMMSSFSOFF_PART_REGEXP, ')', ')?$');
    DOT_SLASH_DASH_FULLYEAR1_1_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', DAYMM_REGEXP, '\s*', MASKSEP_REGEXP, '\s*', DAYMM_REGEXP, '\s*', MASKSEP_REGEXP, '\s*', FULLYEAR_REGEXP, '$');
    FULLYEAR_DOT_SLASH_DASH1_0_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', FULLYEAR_REGEXP, '\s*', MASKSEP_REGEXP, '\s*', DAYMM_REGEXP, '\s*', MASKSEP_REGEXP, '\s*', DAYMM_REGEXP, '\s*(\s+', '(', HHMMSSFSOFF_PART_REGEXP, ')', ')?$');
    FULLYEAR_DOT_SLASH_DASH1_1_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', FULLYEAR_REGEXP, '\s*', MASKSEP_REGEXP, '\s*', DAYMM_REGEXP, '\s*', MASKSEP_REGEXP, '\s*', DAYMM_REGEXP, '$');
    FULLYEAR_DASH1_1_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', FULLYEAR_REGEXP, '\s*-\s*', DAYMM_REGEXP, '\s*-\s*', DAYMM_REGEXP, '$');
    FULLYEAR_SLASH1_1_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', FULLYEAR_REGEXP, '\s*\/\s*', DAYMM_REGEXP, '\s*\/\s*', DAYMM_REGEXP, '$');
    FULLYEAR_DOT1_1_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', FULLYEAR_REGEXP, '\s*\.\s*', DAYMM_REGEXP, '\s*\.\s*', DAYMM_REGEXP, '$');
    DOT_SLASH_DASH_FULLYEAR_DOT_SLASH_DASH1_0_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', DAYMM_REGEXP, '\s*', MASKSEP_REGEXP, '\s*', FULLYEAR_REGEXP, '\s*', MASKSEP_REGEXP, '\s*', DAYMM_REGEXP, '\s*(\s+', '(', HHMMSSFSOFF_PART_REGEXP, ')', ')?$');
    DOT_SLASH_DASH_FULLYEAR_DOT_SLASH_DASH1_1_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', DAYMM_REGEXP, '\s*', MASKSEP_REGEXP, '\s*', FULLYEAR_REGEXP, '\s*', MASKSEP_REGEXP, '\s*', DAYMM_REGEXP, '$');
    DASH_FULLYEAR_DASH1_1_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', DAYMM_REGEXP, '\s*-\s*', FULLYEAR_REGEXP, '\s*-\s*', DAYMM_REGEXP, '$');
    SLASH_FULLYEAR_SLASH1_1_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', DAYMM_REGEXP, '\s*\/\s*', FULLYEAR_REGEXP, '\s*\/\s*', DAYMM_REGEXP, '$');
    DOT_FULLYEAR_DOT1_1_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', DAYMM_REGEXP, '\s*\.\s*', FULLYEAR_REGEXP, '\s*\.\s*', DAYMM_REGEXP, '$');
    FULLYEAR_DIGITMASK1_0_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', '\s*\d{4}', '(\s+(', HHMMSSFSOFF_PART_REGEXP, '))?$');
    SHORT_DIGITMASK1_0_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', '\s*\d{6}', '(\s+(', HHMMSSFSOFF_PART_REGEXP, '))?$');
    FULL_DIGITMASK1_0_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', '\s*\d{8}', '(\s+(', HHMMSSFSOFF_PART_REGEXP, '))?$');
    W3C_XML_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', FULLYEAR_REGEXP, '-', DAYMM_REGEXP, '-', DAYMM_REGEXP, '(', '(\-|\+)', '(\d{2})', '\:', '(\d{2})', '|', 'Z', ')','$');
    W3C_XML_Z_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', FULLYEAR_REGEXP, '-', DAYMM_REGEXP, '-', DAYMM_REGEXP, 'Z','$');
    ISO_8601_DATETIMEOFFSET_REGEXP CONSTANT VARCHAR COLLATE "C" := pg_catalog.concat('^', FULLYEAR_REGEXP, '-', DAYMM_REGEXP, '-', DAYMM_REGEXP, 'T', '\d{2}', '\:', '\d{1,2}', '\:', '\d{1,2}', '(?:\.', '\d{1,9}', ')?', '((\-|\+)', '\d{2}', '\:', '\d{2}','|Z)?$');
BEGIN
    v_datatype := pg_catalog.btrim(p_datatype);
    v_datetimestring := pg_catalog.upper(pg_catalog.btrim(p_datetimestring));
    v_style := floor(p_style)::SMALLINT;

    v_datatype_groups := regexp_matches(v_datatype, DATATYPE_REGEXP, 'gi');

    v_res_datatype := pg_catalog.upper(v_datatype_groups[1]);
    v_scale := v_datatype_groups[2]::SMALLINT;

    IF (v_res_datatype IS NULL) THEN
        RAISE datatype_mismatch;
    ELSIF (v_res_datatype = 'DATE' AND v_scale IS NOT NULL)
    THEN
        RAISE invalid_indicator_parameter_value;
    ELSIF (coalesce(v_scale, 0) NOT BETWEEN 0 AND 7)
    THEN
        RAISE interval_field_overflow;
    ELSIF (v_scale IS NULL) THEN
        v_scale := 6;
    END IF;

    IF (scale(p_style) > 0) THEN
        RAISE most_specific_type_mismatch;
    ELSIF (NOT ((v_style BETWEEN 0 AND 14) OR
             (v_style BETWEEN 20 AND 25) OR
             (v_style BETWEEN 100 AND 114) OR
             (v_style IN (120, 121, 126, 127, 130, 131))))
    THEN
        RAISE invalid_parameter_value;
    END IF;

    IF (v_datetimestring ~* W3C_XML_REGEXP)
    THEN
        v_timepart := NULL;
        v_datestring := NULL;
    ELSE
        v_timepart := pg_catalog.btrim(substring(v_datetimestring, PG_CATALOG.concat('(', HHMMSSFSOFF_PART_REGEXP, ')')));
        v_datestring := pg_catalog.btrim(regexp_replace(v_datetimestring, PG_CATALOG.concat('T?', '(', HHMMSSFSOFF_PART_REGEXP, ')'), '', 'gi'));
    END IF;

    v_language := CASE
                    WHEN (v_style IN (130, 131)) THEN 'HIJRI'
                    ELSE CONVERSION_LANG
                  END;


    BEGIN
        v_lang_metadata_json := sys.shark_get_lang_metadata_json(v_language);
    EXCEPTION
        WHEN OTHERS THEN
        RAISE invalid_escape_sequence;
    END;

    v_date_format := coalesce(nullif(DATE_FORMAT, ''), v_lang_metadata_json ->> 'date_format');

    v_compmonth_regexp := array_to_string(array_cat(ARRAY(SELECT json_array_elements_text(v_lang_metadata_json -> 'months_shortnames')),
                                                    ARRAY(SELECT json_array_elements_text(v_lang_metadata_json -> 'months_names'))), '|');

    IF (v_datetimestring ~* pg_catalog.concat(AMPM_REGEXP, 'Z'))
    THEN
        RAISE invalid_datetime_format;
    END IF;

    IF (v_datetimestring ~* pg_catalog.replace(DEFMASK1_0_REGEXP, '$comp_month$', v_compmonth_regexp) OR
        v_datetimestring ~* pg_catalog.replace(DEFMASK2_0_REGEXP, '$comp_month$', v_compmonth_regexp) OR
        v_datetimestring ~* pg_catalog.replace(DEFMASK3_0_REGEXP, '$comp_month$', v_compmonth_regexp) OR
        v_datetimestring ~* pg_catalog.replace(DEFMASK4_0_REGEXP, '$comp_month$', v_compmonth_regexp) OR
        v_datetimestring ~* pg_catalog.replace(DEFMASK5_0_REGEXP, '$comp_month$', v_compmonth_regexp) OR
        v_datetimestring ~* pg_catalog.replace(DEFMASK6_0_REGEXP, '$comp_month$', v_compmonth_regexp) OR
        v_datetimestring ~* pg_catalog.replace(DEFMASK7_0_REGEXP, '$comp_month$', v_compmonth_regexp) OR
        v_datetimestring ~* pg_catalog.replace(DEFMASK8_0_REGEXP, '$comp_month$', v_compmonth_regexp) OR
        v_datetimestring ~* pg_catalog.replace(DEFMASK9_0_REGEXP, '$comp_month$', v_compmonth_regexp) OR
        v_datetimestring ~* pg_catalog.replace(DEFMASK10_0_REGEXP, '$comp_month$', v_compmonth_regexp))
    THEN
        IF (v_datestring ~* pg_catalog.replace(DEFMASK1_1_REGEXP, '$comp_month$', v_compmonth_regexp))
        THEN
            v_regmatch_groups := regexp_matches(v_datestring, pg_catalog.replace(DEFMASK1_1_REGEXP, '$comp_month$', v_compmonth_regexp), 'gi');
            v_day := v_regmatch_groups[2];
            v_month := sys.shark_get_monthnum_by_name(v_regmatch_groups[1], v_lang_metadata_json);
            v_year := sys.shark_get_full_year(v_regmatch_groups[3]);

        ELSIF (v_datestring ~* pg_catalog.replace(DEFMASK2_1_REGEXP, '$comp_month$', v_compmonth_regexp))
        THEN
            v_regmatch_groups := regexp_matches(v_datestring, pg_catalog.replace(DEFMASK2_1_REGEXP, '$comp_month$', v_compmonth_regexp), 'gi');
            v_day := v_regmatch_groups[1];
            v_month := sys.shark_get_monthnum_by_name(v_regmatch_groups[2], v_lang_metadata_json);
            v_year := sys.shark_get_full_year(v_regmatch_groups[3]);

        ELSIF (v_datestring ~* pg_catalog.replace(DEFMASK3_1_REGEXP, '$comp_month$', v_compmonth_regexp))
        THEN
            v_regmatch_groups := regexp_matches(v_datestring, pg_catalog.replace(DEFMASK3_1_REGEXP, '$comp_month$', v_compmonth_regexp), 'gi');
            v_day := v_regmatch_groups[3];
            v_month := sys.shark_get_monthnum_by_name(v_regmatch_groups[2], v_lang_metadata_json);
            v_year := v_regmatch_groups[1];

        ELSIF (v_datestring ~* pg_catalog.replace(DEFMASK4_1_REGEXP, '$comp_month$', v_compmonth_regexp))
        THEN
            v_regmatch_groups := regexp_matches(v_datestring, pg_catalog.replace(DEFMASK4_1_REGEXP, '$comp_month$', v_compmonth_regexp), 'gi');
            v_day := v_regmatch_groups[2];
            v_month := sys.shark_get_monthnum_by_name(v_regmatch_groups[3], v_lang_metadata_json);
            v_year := v_regmatch_groups[1];

        ELSIF (v_datestring ~* pg_catalog.replace(DEFMASK5_1_REGEXP, '$comp_month$', v_compmonth_regexp))
        THEN
            v_regmatch_groups := regexp_matches(v_datestring, pg_catalog.replace(DEFMASK5_1_REGEXP, '$comp_month$', v_compmonth_regexp), 'gi');
            v_day := v_regmatch_groups[1];
            v_month := sys.shark_get_monthnum_by_name(v_regmatch_groups[3], v_lang_metadata_json);
            v_year := sys.shark_get_full_year(v_regmatch_groups[2]);

        ELSIF (v_datestring ~* pg_catalog.replace(DEFMASK6_1_REGEXP, '$comp_month$', v_compmonth_regexp))
        THEN
            v_regmatch_groups := regexp_matches(v_datestring, pg_catalog.replace(DEFMASK6_1_REGEXP, '$comp_month$', v_compmonth_regexp), 'gi');
            v_day := v_regmatch_groups[3];
            v_month := sys.shark_get_monthnum_by_name(v_regmatch_groups[1], v_lang_metadata_json);
            v_year := v_regmatch_groups[2];

        ELSIF (v_datestring ~* pg_catalog.replace(DEFMASK7_1_REGEXP, '$comp_month$', v_compmonth_regexp))
        THEN
            v_regmatch_groups := regexp_matches(v_datestring, pg_catalog.replace(DEFMASK7_1_REGEXP, '$comp_month$', v_compmonth_regexp), 'gi');
            v_day := v_regmatch_groups[2];
            v_month := sys.shark_get_monthnum_by_name(v_regmatch_groups[1], v_lang_metadata_json);
            v_year := sys.shark_get_full_year(v_regmatch_groups[3]);

        ELSIF (v_datestring ~* pg_catalog.replace(DEFMASK8_1_REGEXP, '$comp_month$', v_compmonth_regexp))
        THEN
            IF (v_datetimestring ~* pg_catalog.replace(DEFMASK8_2_REGEXP, '$comp_month$', v_compmonth_regexp))
            THEN
                RAISE invalid_datetime_format;
            END IF;

            v_regmatch_groups := regexp_matches(v_datestring, pg_catalog.replace(DEFMASK8_1_REGEXP, '$comp_month$', v_compmonth_regexp), 'gi');
            v_day := '01';
            v_month := sys.shark_get_monthnum_by_name(v_regmatch_groups[2], v_lang_metadata_json);
            v_year := sys.shark_get_full_year(v_regmatch_groups[1]);

        ELSIF (v_datestring ~* pg_catalog.replace(DEFMASK9_1_REGEXP, '$comp_month$', v_compmonth_regexp))
        THEN
            IF (v_datetimestring ~* pg_catalog.replace(DEFMASK9_2_REGEXP, '$comp_month$', v_compmonth_regexp) OR
                    (v_datestring !~* pg_catalog.replace(DEFMASK9_2_REGEXP, '$comp_month$', v_compmonth_regexp) AND
                     v_datestring !~* pg_catalog.replace(DEFMASK9_3_REGEXP, '$comp_month$', v_compmonth_regexp)))
            THEN
                RAISE invalid_datetime_format;
            END IF;

            v_regmatch_groups := regexp_matches(v_datestring, pg_catalog.replace(DEFMASK9_1_REGEXP, '$comp_month$', v_compmonth_regexp), 'gi');
            v_day := '01';
            v_month := sys.shark_get_monthnum_by_name(v_regmatch_groups[1], v_lang_metadata_json);
            v_year := sys.shark_get_full_year(v_regmatch_groups[2]);
        ELSE
            IF ((v_datestring !~* pg_catalog.replace(DEFMASK10_2_REGEXP, '$comp_month$', v_compmonth_regexp)) AND
                (v_datestring !~* pg_catalog.replace(DEFMASK10_3_REGEXP, '$comp_month$', v_compmonth_regexp)) AND
                (v_datestring !~* pg_catalog.replace(DEFMASK10_4_REGEXP, '$comp_month$', v_compmonth_regexp)))
            THEN
                RAISE invalid_datetime_format;
            END IF;

            v_regmatch_groups := regexp_matches(v_datestring, pg_catalog.replace(DEFMASK10_1_REGEXP, '$comp_month$', v_compmonth_regexp), 'gi');
            v_day := v_regmatch_groups[1];
            v_month := sys.shark_get_monthnum_by_name(v_regmatch_groups[2], v_lang_metadata_json);
            v_year := sys.shark_get_full_year(v_regmatch_groups[3]);
        END IF;
    ELSIF (v_datetimestring ~* DOT_SLASH_DASH_COMPYEAR1_0_REGEXP)
    THEN
        IF ((v_datestring !~* DASH_COMPYEAR1_1_REGEXP) AND
            (v_datestring !~* SLASH_COMPYEAR1_1_REGEXP) AND
            (v_datestring !~* DOT_COMPYEAR1_1_REGEXP))
        THEN
            RAISE invalid_datetime_format;
        END IF;

        IF (v_style IN (6, 7, 8, 9, 12, 13, 14, 24, 100, 106, 107, 108, 109, 112, 113, 114, 130))
        THEN
            RAISE invalid_regular_expression;
        END IF;

        v_regmatch_groups := regexp_matches(v_datestring, DOT_SLASH_DASH_COMPYEAR1_1_REGEXP, 'gi');
        v_leftpart := v_regmatch_groups[1];
        v_middlepart := v_regmatch_groups[2];
        v_rightpart := v_regmatch_groups[3];

        IF (v_datestring ~* DOT_SLASH_DASH_SHORTYEAR_REGEXP)
        THEN
            IF ((v_style IN (1, 10, 22)) OR
                ((v_style IS NULL OR v_style = 0) AND v_date_format = 'MDY'))
            THEN
                v_day := v_middlepart;
                v_month := v_leftpart;
                v_year := sys.shark_get_full_year(v_rightpart);

            ELSIF ((v_style IN (2, 11)) OR
                   ((v_style IS NULL OR v_style = 0) AND v_date_format = 'YMD'))
            THEN
                v_day := v_rightpart;
                v_month := v_middlepart;
                v_year := sys.shark_get_full_year(v_leftpart);

            ELSIF ((v_style IN (3, 4, 5)) OR
                   ((v_style IS NULL OR v_style = 0) AND v_date_format = 'DMY'))
            THEN
                v_day := v_leftpart;
                v_month := v_middlepart;
                v_year := sys.shark_get_full_year(v_rightpart);

            ELSIF ((v_style IS NULL OR v_style = 0) AND v_date_format = 'DYM')
            THEN
                v_day = v_leftpart;
                v_month = v_rightpart;
                v_year = sys.shark_get_full_year(v_middlepart);

            ELSIF ((v_style IS NULL OR v_style = 0) AND v_date_format = 'MYD')
            THEN
                v_day := v_rightpart;
                v_month := v_leftpart;
                v_year = sys.shark_get_full_year(v_middlepart);

            ELSIF ((v_style IS NULL OR v_style = 0) AND v_date_format = 'YDM')
            THEN
                    RAISE character_not_in_repertoire;
            ELSE
                RAISE invalid_datetime_format;
            END IF;
        ELSIF (v_datestring ~* DOT_SLASH_DASH_FULLYEAR1_1_REGEXP)
        THEN
            v_year := v_rightpart;
            IF ((v_style IN (103, 104, 105, 131)) OR
                ((v_style IS NULL OR v_style = 0) AND v_date_format = 'DMY'))
            THEN
                v_day := v_leftpart;
                v_month := v_middlepart;

            ELSIF ((v_style IN (101, 110)) OR
                    ((v_style IS NULL OR v_style = 0) AND v_date_format = 'MDY'))
            THEN
                v_day := v_middlepart;
                v_month := v_leftpart;
            ELSE
                RAISE invalid_datetime_format;
            END IF;
        END IF;
    ELSIF (v_datetimestring ~* FULLYEAR_DOT_SLASH_DASH1_0_REGEXP)
    THEN
        IF ((v_datestring !~* FULLYEAR_DASH1_1_REGEXP) AND
            (v_datestring !~* FULLYEAR_SLASH1_1_REGEXP) AND
            (v_datestring !~* FULLYEAR_DOT1_1_REGEXP))
        THEN
            RAISE invalid_datetime_format;
        END IF;

        IF (v_style IN (6, 7, 8, 9, 12, 13, 14, 24, 100, 106, 107, 108, 109, 112, 113, 114, 130))
        THEN
            RAISE invalid_regular_expression;
        ELSIF (v_style IN (1, 2, 3, 4, 5, 10, 11, 22, 101, 103, 104, 105, 110, 131)) THEN
            RAISE invalid_datetime_format;
        END IF;

        v_regmatch_groups := regexp_matches(v_datestring, FULLYEAR_DOT_SLASH_DASH1_1_REGEXP, 'gi');
        -- DATEFORMAT 'YDM' is not supported hence only applicable dateformat can be used here is YMD
        v_year := v_regmatch_groups[1];
        v_day := v_regmatch_groups[3];
        v_month := v_regmatch_groups[2];
    ELSIF (v_datetimestring ~* DOT_SLASH_DASH_FULLYEAR_DOT_SLASH_DASH1_0_REGEXP)
    THEN
        IF ((v_datestring !~* DASH_FULLYEAR_DASH1_1_REGEXP) AND
            (v_datestring !~* SLASH_FULLYEAR_SLASH1_1_REGEXP) AND
            (v_datestring !~* DOT_FULLYEAR_DOT1_1_REGEXP))
        THEN
            RAISE invalid_datetime_format;
        END IF;

        IF (v_style IN (6, 7, 8, 9, 12, 13, 14, 24, 100, 106, 107, 108, 109, 112, 113, 114, 130))
        THEN
            RAISE invalid_regular_expression;
        ELSIF (v_style IN (1, 2, 3, 4, 5, 10, 11, 20, 21, 22, 23, 25, 101, 102, 103, 104, 105, 110, 111, 120, 121, 126, 127, 131)) THEN
            RAISE invalid_datetime_format;
        END IF;

        v_regmatch_groups := regexp_matches(v_datestring, DOT_SLASH_DASH_FULLYEAR_DOT_SLASH_DASH1_1_REGEXP, 'gi');
        v_leftpart := v_regmatch_groups[1];
        v_year := v_regmatch_groups[2];
        v_rightpart := v_regmatch_groups[3];

        IF ((v_style IS NULL OR v_style = 0) AND v_date_format = 'MYD')
        THEN
            v_day := v_rightpart;
            v_month := v_leftpart;
        ELSIF ((v_style IS NULL OR v_style = 0) AND v_date_format = 'DYM')
        THEN
            v_day := v_leftpart;
            v_month := v_rightpart;
        ELSE
            RAISE invalid_datetime_format;
        END IF;
    ELSIF ((v_datetimestring ~* FULLYEAR_DIGITMASK1_0_REGEXP OR
           v_datetimestring ~* SHORT_DIGITMASK1_0_REGEXP OR
           v_datetimestring ~* FULL_DIGITMASK1_0_REGEXP))
    THEN
        IF (v_datestring ~* '^\d{4}$')
        THEN
            v_day := '01';
            v_month := '01';
            v_year := substr(v_datestring, 1, 4);

        ELSIF (v_datestring ~* '^\d{6}$')
        THEN
            v_day := substr(v_datestring, 5, 2);
            v_month := substr(v_datestring, 3, 2);
            v_year := sys.shark_get_full_year(substr(v_datestring, 1, 2));

        ELSIF (v_datestring ~* '^\d{8}$')
        THEN
            v_day := substr(v_datestring, 7, 2);
            v_month := substr(v_datestring, 5, 2);
            v_year := substr(v_datestring, 1, 4);
        END IF;
    ELSIF (v_datetimestring ~* HHMMSSFSOFF_REGEXP OR length(v_datetimestring) = 0)
    THEN
        v_day := '01';
        v_month := '01';
        v_year := '1900';
    ELSIF (v_datetimestring ~* W3C_XML_REGEXP)
    THEN
        v_regmatch_groups := regexp_matches(v_datetimestring, W3C_XML_REGEXP, 'gi');
        v_day := v_regmatch_groups[3];
        v_month := v_regmatch_groups[2];
        v_year := v_regmatch_groups[1];

        IF (v_datetimestring !~* W3C_XML_Z_REGEXP)
        THEN
            v_sign := v_regmatch_groups[5];
            v_offhours := v_regmatch_groups[6];
            v_offminutes := v_regmatch_groups[7];
            IF ((v_offhours::SMALLINT NOT BETWEEN 0 AND 14) OR
                (v_offminutes::SMALLINT NOT BETWEEN 0 AND 59) OR
                (v_offhours::SMALLINT = 14 AND v_offminutes::SMALLINT != 0))
            THEN
                RAISE invalid_datetime_format;
            END IF;
        ELSE
            v_sign := '+';
            v_offhours := '0';
            v_offminutes := '0';
        END IF;
    ELSIF (v_datetimestring ~* ISO_8601_DATETIMEOFFSET_REGEXP)
    THEN
        v_regmatch_groups := regexp_matches(v_datetimestring, ISO_8601_DATETIMEOFFSET_REGEXP, 'gi');

        v_day := v_regmatch_groups[3];
        v_month := v_regmatch_groups[2];
        v_year := v_regmatch_groups[1];
    ELSE
        RAISE invalid_datetime_format;
    END IF;

    IF (v_style IN (130, 131))
    THEN
        -- validate date according to hijri date format
        IF ((v_month::SMALLINT NOT BETWEEN 1 AND 12) OR
            (v_day::SMALLINT NOT BETWEEN 1 AND 30) OR
            ((MOD(v_month::SMALLINT, 2) = 0 AND v_month::SMALLINT != 12) AND v_day::SMALLINT = 30))
        THEN
            RAISE invalid_character_value_for_cast;
        END IF;

        -- for hijri leap year
        IF (v_month::SMALLINT = 12)
        THEN
            -- check for a leap year
            IF (MOD(v_year::SMALLINT, 30) IN (2, 5, 7, 10, 13, 16, 18, 21, 24, 26, 29))
            THEN
                IF (v_day::SMALLINT NOT BETWEEN 1 AND 30)
                THEN
                    RAISE invalid_character_value_for_cast;
                END IF;
            ELSE
                IF (v_day::SMALLINT NOT BETWEEN 1 AND 29)
                THEN
                    RAISE invalid_character_value_for_cast;
                END IF;
            END IF;
        END IF;

        v_hijridate := sys.shark_conv_hijri_to_greg(v_day, v_month, v_year) - 1;
        v_day = to_char(v_hijridate, 'DD');
        v_month = to_char(v_hijridate, 'MM');
        v_year = to_char(v_hijridate, 'YYYY');
    END IF;

    BEGIN
        v_hours := coalesce(sys.shark_get_timeunit_from_string(v_timepart, 'HOURS'), '0');
        v_minutes := coalesce(sys.shark_get_timeunit_from_string(v_timepart, 'MINUTES'), '0');
        v_seconds := coalesce(sys.shark_get_timeunit_from_string(v_timepart, 'SECONDS'), '0');
        v_fseconds := coalesce(sys.shark_get_timeunit_from_string(v_timepart, 'FRACTSECONDS'), '0');

        v_sign := coalesce(v_sign, sys.shark_get_timeunit_from_string(v_timepart, 'OFFSIGN'), '+');
        v_offhours := coalesce(v_offhours, sys.shark_get_timeunit_from_string(v_timepart, 'OFFHOURS'), '0');
        v_offminutes := coalesce(v_offminutes, sys.shark_get_timeunit_from_string(v_timepart, 'OFFMINUTES'), '0');
    EXCEPTION
        WHEN OTHERS THEN
            RAISE invalid_character_value_for_cast;
    END;

    -- validate time and offset
    IF ((v_hours::SMALLINT NOT BETWEEN 0 AND 23) OR
        (v_minutes::SMALLINT NOT BETWEEN 0 AND 59) OR
        (v_seconds::SMALLINT NOT BETWEEN 0 AND 59) OR
        (v_offhours::SMALLINT NOT BETWEEN 0 AND 14) OR
        (v_offminutes::SMALLINT NOT BETWEEN 0 AND 59) OR
        (v_offhours::SMALLINT = 14 AND v_offminutes::SMALLINT != 0))
    THEN
        RAISE invalid_character_value_for_cast;
    END IF;

    -- validate date according to gregorian date format
    IF ((v_year::SMALLINT NOT BETWEEN 1 AND 9999) OR
        (v_month::SMALLINT NOT BETWEEN 1 AND 12) OR
        ((v_month::SMALLINT IN (1,3,5,7,8,10,12)) AND (v_day::SMALLINT NOT BETWEEN 1 AND 31)) OR
        ((v_month::SMALLINT IN (4,6,9,11)) AND (v_day::SMALLINT NOT BETWEEN 1 AND 30)))
    THEN
        RAISE invalid_character_value_for_cast;
    ELSIF (v_month::SMALLINT = 2)
    THEN
        -- check for a leap year
        IF ((v_year::SMALLINT % 4 = 0) AND ((v_year::SMALLINT % 100 <> 0) or (v_year::SMALLINT % 400 = 0)))
        THEN
            IF (v_day::SMALLINT NOT BETWEEN 1 AND 29)
            THEN
                RAISE invalid_character_value_for_cast;
            END IF;
        ELSE
            IF (v_day::SMALLINT NOT BETWEEN 1 AND 28)
            THEN
                RAISE invalid_character_value_for_cast;
            END IF;
        END IF;
    END IF;

    IF (v_timepart !~* PG_CATALOG.concat('^(', HHMMSSFSOFF_DOT_PART_REGEXP, ')$') AND char_length(v_fseconds) > 3)
    THEN
        RAISE invalid_datetime_format;
    END IF;

    IF (v_timepart !~* PG_CATALOG.concat('^(', HHMMSSFSOFF_DOT_PART_REGEXP, ')$')) THEN
        -- if before fractional seconds there is a ':'
        v_fseconds := lpad(v_fseconds, 3, '0');
    END IF;

    IF (v_scale = 0) THEN
        v_seconds := pg_catalog.concat_ws('.', v_seconds, v_fseconds);
        v_seconds := round(v_seconds::NUMERIC, 0)::TEXT;
    ELSE
        v_fseconds := sys.shark_get_microsecs_from_fractsecs_v2(v_fseconds, v_scale);

        -- Following condition will handle overflow of fractsecs
        IF (v_fseconds::INTEGER < 0) THEN
            v_fseconds := PG_CATALOG.repeat('0', LEAST(v_scale, 6));
            v_seconds := (v_seconds::INTEGER + 1)::TEXT;
        END IF;

        v_seconds := pg_catalog.concat_ws('.', v_seconds, v_fseconds);
    END IF;

    IF (v_res_datatype = 'DATE')
    THEN
        v_resdatetime := make_timestamp(v_year::SMALLINT, v_month::SMALLINT, v_day::SMALLINT,0,0,0);
    ELSIF (v_res_datatype = 'TIME')
    THEN
        v_resdatetime := make_timestamp(9999, 12, 31, v_hours::SMALLINT, v_minutes::SMALLINT, v_seconds::NUMERIC);
    ELSE
        v_resdatetime := make_timestamp(v_year::SMALLINT, v_month::SMALLINT, v_day::SMALLINT,
                                            v_hours::SMALLINT, v_minutes::SMALLINT, v_seconds::NUMERIC);
    END IF;

    IF (v_resdatetime > make_timestamp(9999, 12, 31, 23, 59, 59.999999)) THEN
        -- if rounding of fractional seconds caused the date and time to go out of range
        -- then max date and time that can be stored for p_datatype will be used
        v_resdatetime := make_timestamp(9999, 12, 31, 23, 59, pg_catalog.concat_ws('.', '59', PG_CATALOG.repeat('9', LEAST(v_scale, 6)))::NUMERIC);
    END IF;

    RETURN v_resdatetime;
EXCEPTION
    WHEN most_specific_type_mismatch THEN
        RAISE USING MESSAGE := 'Argument data type numeric is invalid for argument 3 of conv_string_to_datetime2 function.',
                    DETAIL := 'Use of incorrect "style" parameter value during conversion process.',
                    HINT := 'Change "style" parameter to the proper value and try again.';

    WHEN invalid_parameter_value THEN
        RAISE USING MESSAGE := pg_catalog.format('The style %s is not supported for conversions from varchar to %s.', v_style, PG_CATALOG.lower(v_res_datatype)),
                    DETAIL := 'Use of incorrect "style" parameter value during conversion process.',
                    HINT := 'Change "style" parameter to the proper value and try again.';

    WHEN invalid_regular_expression THEN
        RAISE USING MESSAGE := pg_catalog.format('The input character string does not follow style %s, either change the input character string or use a different style.', v_style),
                    DETAIL := 'Selected "style" param value isn''t valid for conversion of passed character string.',
                    HINT := 'Either change the input character string or use a different style.';

    WHEN datatype_mismatch THEN
        RAISE USING MESSAGE := 'Data type should be one of these values: ''DATE'', ''TIME'', ''TIMESTAMP WITHOUT TIME ZONE'', ''TIMESTAMP WIT TIME ZONE''.',
                    DETAIL := 'Use of incorrect "datatype" parameter value during conversion process.',
                    HINT := 'Change "datatype" parameter to the proper value and try again.';

    WHEN invalid_indicator_parameter_value THEN
        RAISE USING MESSAGE := pg_catalog.format('CAST or CONVERT: invalid attributes specified for type ''%s''', v_res_datatype),
                    DETAIL := 'Use of incorrect scale value, which is not corresponding to specified data type.',
                    HINT := 'Change data type scale component or select different data type and try again.';

    WHEN interval_field_overflow THEN
        RAISE USING MESSAGE := pg_catalog.format('Specified scale %s is invalid.', v_scale),
                    DETAIL := 'Use of incorrect data type scale value during conversion process.',
                    HINT := 'Change scale component of data type parameter to be in range [0..7] and try again.';

    WHEN invalid_datetime_format THEN
        RAISE USING MESSAGE := 'Conversion failed when converting date and/or time from character string.',
                    DETAIL := 'Incorrect using of pair of input parameters values during conversion process.',
                    HINT := 'Check the input parameters values, correct them if needed, and try again.';

    WHEN invalid_character_value_for_cast THEN
        RAISE USING MESSAGE :=  pg_catalog.format('The conversion of a varchar data type to a %s data type resulted in an out-of-range value.', PG_CATALOG.lower(v_res_datatype)),
                    DETAIL := 'Use of incorrect pair of input parameter values during conversion process.',
                    HINT := 'Check input parameter values, correct them if needed, and try again.';

    WHEN character_not_in_repertoire THEN
        RAISE USING MESSAGE := 'This session''s YDM date format is not supported when converting from this character string format to date, time, timestamp without time zone or timestamp with time zone. Change the session''s date format or provide a style to the explicit conversion.',
                    DETAIL := 'Use of incorrect DATE_FORMAT constant value regarding string format parameter during conversion process.',
                    HINT := 'Change DATE_FORMAT constant to one of these values: MDY|DMY|DYM, recompile function and try again.';

    WHEN invalid_escape_sequence THEN
        RAISE USING MESSAGE := pg_catalog.format('Invalid CONVERSION_LANG constant value - ''%s''. Allowed values are: ''English'', ''Deutsch'', etc.',
                                      CONVERSION_LANG),
                    DETAIL := 'Compiled incorrect CONVERSION_LANG constant value in function''s body.',
                    HINT := 'Correct CONVERSION_LANG constant value in function''s body, recompile it and try again.';
END;
$BODY$
LANGUAGE plpgsql
IMMUTABLE
RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION sys.shark_conv_helper_to_varchar(IN typename TEXT,
                                                        IN arg ANYELEMENT,
                                                        IN p_try BOOL,
                                                        IN p_style NUMERIC DEFAULT -1)
RETURNS VARCHAR
AS
$BODY$
BEGIN
	IF p_try THEN
	    RETURN sys.shark_try_conv_to_varchar(typename, arg, p_style);
    ELSE
	    RETURN sys.shark_conv_to_varchar(typename, arg, p_style);
    END IF;
END;
$BODY$
LANGUAGE plpgsql
STABLE;

CREATE OR REPLACE FUNCTION sys.shark_conv_to_varchar(IN typename TEXT,
														IN arg anyelement,
														IN p_style NUMERIC DEFAULT -1)
RETURNS VARCHAR
AS
$BODY$
DECLARE
	v_style SMALLINT;
BEGIN
	v_style := floor(p_style)::SMALLINT;
    CASE pg_typeof(arg)
	WHEN 'date'::regtype THEN
		IF v_style = -1 THEN
			RETURN sys.shark_try_conv_date_to_string(typename, arg);
		ELSE
			RETURN sys.shark_try_conv_date_to_string(typename, arg, p_style);
		END IF;
	WHEN 'time'::regtype THEN
		IF v_style = -1 THEN
			RETURN sys.shark_try_conv_time_to_string(typename, 'TIME', arg);
		ELSE
			RETURN sys.shark_try_conv_time_to_string(typename, 'TIME', arg, p_style);
		END IF;
	WHEN 'timestamp'::regtype THEN
        IF v_style = -1 THEN
			RETURN sys.shark_try_conv_datetime_to_string(typename, 'TIMESTAMP WITHOUT TIME ZONE', arg::timestamp);
		ELSE
			RETURN sys.shark_try_conv_datetime_to_string(typename, 'TIMESTAMP WITHOUT TIME ZONE', arg::timestamp, p_style);
		END IF;
	WHEN 'float'::regtype THEN
		IF v_style = -1 THEN
			RETURN sys.shark_try_conv_float_to_string(typename, arg);
		ELSE
			RETURN sys.shark_try_conv_float_to_string(typename, arg, p_style);
		END IF;
	WHEN 'money'::regtype THEN
		IF v_style = -1 THEN
			RETURN sys.shark_try_conv_money_to_string(typename, arg::numeric(19,4));
		ELSE
			RETURN sys.shark_try_conv_money_to_string(typename, arg::numeric(19,4), p_style);
		END IF;
	ELSE
		RETURN CAST(arg AS VARCHAR);
	END CASE;
END;
$BODY$
LANGUAGE plpgsql
STABLE;

CREATE OR REPLACE FUNCTION sys.shark_try_conv_money_to_string(IN p_datatype TEXT,
														IN p_moneyval NUMERIC,
														IN p_style NUMERIC DEFAULT 0)
RETURNS TEXT
AS
$BODY$
DECLARE
	v_style SMALLINT;
	v_format VARCHAR COLLATE "C";
	v_moneyval NUMERIC(19,4) := p_moneyval::NUMERIC(19,4);
	v_moneysign NUMERIC(19,4) := sign(v_moneyval);
	v_moneyabs NUMERIC(19,4) := abs(v_moneyval);
	v_digits SMALLINT;
	v_integral_digits SMALLINT;
	v_decimal_digits SMALLINT;
	v_result TEXT;
BEGIN
	v_style := floor(p_style)::SMALLINT;
	v_digits := length(v_moneyabs::TEXT);
	v_decimal_digits := scale(v_moneyabs);
	IF (v_decimal_digits > 0) THEN
		v_integral_digits := v_digits - v_decimal_digits - 1;
	ELSE
		v_integral_digits := v_digits;
	END IF;
	IF (v_style = 0) THEN
		v_format := (pow(10, v_integral_digits)-10)::TEXT || 'D99';
		v_result := pg_catalog.btrim(to_char(v_moneyval, v_format));
	ELSIF (v_style = 1) THEN
		IF (v_moneysign::SMALLINT = -1) THEN
			v_result := substring(p_moneyval::PG_CATALOG.MONEY::TEXT, 1, 1) || substring(p_moneyval::PG_CATALOG.MONEY::TEXT, 3);
		ELSE
			v_result := substring(p_moneyval::PG_CATALOG.MONEY::TEXT, 2);
		END IF;
	ELSIF (v_style = 2 OR v_style = 126) THEN
		v_format := (pow(10, v_integral_digits)-10)::TEXT || 'D9999';
		v_result := pg_catalog.btrim(to_char(v_moneyval, v_format));
	ELSE
		RAISE invalid_parameter_value;
	END IF;

	RETURN v_result;
EXCEPTION
	WHEN invalid_parameter_value THEN
		RAISE USING MESSAGE := pg_catalog.format('%s is not a valid style number when converting from MONEY to a character string.', v_style),
					DETAIL := 'Use of incorrect "style" parameter value during conversion process.',
					HINT := 'Change "style" parameter to the proper value and try again.';
END;
$BODY$
LANGUAGE plpgsql
STABLE
RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION sys.shark_try_conv_float_to_string(IN p_datatype TEXT,
														  IN p_floatval FLOAT,
														  IN p_style NUMERIC DEFAULT 0)
RETURNS TEXT
AS
$BODY$
DECLARE
	v_style SMALLINT;
	v_format VARCHAR COLLATE "C";
	v_floatval NUMERIC := abs(p_floatval);
	v_digits SMALLINT;
	v_integral_digits SMALLINT;
	v_decimal_digits SMALLINT;
	v_sign SMALLINT := sign(p_floatval);
	v_result TEXT;
	v_res_length SMALLINT;
	MASK_REGEXP CONSTANT VARCHAR COLLATE "C" := '^\s*(?:character varying)\s*\(\s*(\d+|MAX)\s*\)\s*$';
BEGIN
	v_style := floor(p_style)::SMALLINT;
	IF (v_style = 0) THEN
		v_digits := length(v_floatval::NUMERIC::TEXT);
		v_decimal_digits := scale(v_floatval);
		IF (v_decimal_digits > 0) THEN
			v_integral_digits := v_digits - v_decimal_digits - 1;
		ELSE
			v_integral_digits := v_digits;
		END IF;
		IF (v_floatval >= 999999.5) THEN
			v_format := '9D99999EEEE';
			v_result := to_char(v_sign::NUMERIC * ceiling(v_floatval), v_format);
			v_result := to_char(substring(v_result, 1, 8)::NUMERIC, 'FM9D99999')::NUMERIC::TEXT || substring(v_result, 9);
		ELSIF (v_floatval < 0.0001 AND v_floatval != 0) THEN
			v_format := '9D99999EEEE';
			v_result := to_char(v_sign::NUMERIC * v_floatval, v_format);
			v_result := to_char(substring(v_result, 1, 8)::NUMERIC, 'FM9D99999')::NUMERIC::TEXT || substring(v_result, 9);
		ELSE
			IF (6 - v_integral_digits < v_decimal_digits) AND (trunc(abs(v_floatval)) != 0) THEN
				v_decimal_digits := 6 - v_integral_digits;
			ELSIF (6 - v_integral_digits < v_decimal_digits) THEN
				v_decimal_digits := 6;
			END IF;
			v_format := (pow(10, v_integral_digits)-10)::TEXT || 'D';
			IF (v_decimal_digits > 0) THEN
				v_format := v_format || (pow(10, v_decimal_digits)-1)::TEXT;
			END IF;
			v_result := to_char(p_floatval, v_format);
		END IF;
	ELSIF (v_style = 1) THEN
		v_format := '9D9999999EEEE';
		v_result := to_char(p_floatval, v_format);
	ELSIF (v_style = 2) THEN
		v_format := '9D999999999999999EEEE';
		v_result := to_char(p_floatval, v_format);
	ELSIF (v_style = 3) THEN
		v_format := '9D9999999999999999EEEE';
		v_result := to_char(p_floatval, v_format);
	ELSE
		RAISE invalid_parameter_value;
	END IF;

	v_res_length := substring(p_datatype COLLATE "C", MASK_REGEXP)::SMALLINT;
	IF v_res_length IS NULL THEN
		RETURN ltrim(v_result);
	ELSE
		RETURN rpad(ltrim(v_result),  v_res_length, ' ');
	END IF;
EXCEPTION
	WHEN invalid_parameter_value THEN
		RAISE USING MESSAGE := pg_catalog.format('%s is not a valid style number when converting from FLOAT to a character string.', v_style),
					DETAIL := 'Use of incorrect "style" parameter value during conversion process.',
					HINT := 'Change "style" parameter to the proper value and try again.';
END;
$BODY$
LANGUAGE plpgsql
STABLE
RETURNS NULL ON NULL INPUT;



CREATE OR REPLACE FUNCTION sys.shark_try_conv_datetime_to_string(IN p_datatype TEXT,
                                                                         IN p_src_datatype TEXT,
                                                                         IN p_datetimeval TIMESTAMP WITHOUT TIME ZONE,
                                                                         IN p_style NUMERIC DEFAULT -1)
RETURNS TEXT
AS
$BODY$
BEGIN
    RETURN sys.shark_conv_datetime_to_string(p_datatype,
                                                     p_src_datatype,
                                                     p_datetimeval,
                                                     p_style);
EXCEPTION
    WHEN OTHERS THEN
        RETURN NULL;
END;
$BODY$
LANGUAGE plpgsql
STABLE
RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION sys.shark_try_conv_to_varchar(IN typename TEXT,
														IN arg TEXT,
														IN p_style NUMERIC DEFAULT -1)
RETURNS VARCHAR
AS
$BODY$
BEGIN
    RETURN sys.shark_conv_to_varchar(typename, arg, p_style);
    EXCEPTION
        WHEN OTHERS THEN
            RETURN NULL;
END;
$BODY$
LANGUAGE plpgsql
STABLE;

CREATE OR REPLACE FUNCTION sys.shark_try_conv_to_varchar(IN typename TEXT,
														IN arg anyelement,
														IN p_style NUMERIC DEFAULT -1)
RETURNS VARCHAR
AS
$BODY$
BEGIN
    RETURN sys.shark_conv_to_varchar(typename, arg, p_style);
    EXCEPTION
        WHEN OTHERS THEN
            RETURN NULL;
END;
$BODY$
LANGUAGE plpgsql
STABLE;

CREATE OR REPLACE FUNCTION sys.shark_try_conv_date_to_string(IN p_datatype TEXT,
                                                                     IN p_dateval DATE,
                                                                     IN p_style NUMERIC DEFAULT 20)
RETURNS TEXT
AS
$BODY$
BEGIN
    RETURN sys.shark_conv_date_to_string(p_datatype,
                                                 p_dateval,
                                                 p_style);
EXCEPTION
    WHEN OTHERS THEN
        RETURN NULL;
END;
$BODY$
LANGUAGE plpgsql
STABLE
RETURNS NULL ON NULL INPUT;


CREATE OR REPLACE FUNCTION sys.shark_try_conv_time_to_string(IN p_datatype TEXT,
                                                                     IN p_src_datatype TEXT,
                                                                     IN p_timeval TIME WITHOUT TIME ZONE,
                                                                     IN p_style NUMERIC DEFAULT 25)
RETURNS TEXT
AS
$BODY$
BEGIN
    RETURN sys.shark_conv_time_to_string(p_datatype,
                                                 p_src_datatype,
                                                 p_timeval,
                                                 p_style);
EXCEPTION
    WHEN OTHERS THEN
        RETURN NULL;
END;
$BODY$
LANGUAGE plpgsql
STABLE
RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION sys.shark_try_conv_string_to_datetime2(IN p_datatype TEXT,
                                                                    IN p_datetimestring TEXT,
                                                                    IN p_style NUMERIC DEFAULT 0)
RETURNS TIMESTAMP WITHOUT TIME ZONE
AS
$BODY$
BEGIN
    RETURN sys.shark_conv_string_to_datetime2(p_datatype,
                                                    p_datetimestring,
                                                    p_style);
EXCEPTION
    WHEN OTHERS THEN
        RETURN NULL;
END;
$BODY$
LANGUAGE plpgsql
IMMUTABLE
RETURNS NULL ON NULL INPUT;

-- conversion to date
CREATE OR REPLACE FUNCTION sys.shark_conv_helper_to_date(IN arg TEXT,
                                                        IN p_try BOOL,
                                                        IN p_style NUMERIC DEFAULT 0)
RETURNS DATE
AS
$BODY$
BEGIN
    IF p_try THEN
        RETURN sys.shark_try_conv_string_to_datetime2('DATE', arg, p_style);
    ELSE
	    RETURN sys.shark_conv_string_to_datetime2('DATE', arg, p_style);
    END IF;
END;
$BODY$
LANGUAGE plpgsql
IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.shark_conv_helper_to_date(IN arg VARCHAR,
                                                        IN p_try BOOL,
                                                        IN p_style NUMERIC DEFAULT 0)
RETURNS DATE
AS
$BODY$
BEGIN
    RETURN sys.shark_conv_helper_to_date(arg::TEXT, p_try, p_style);
END;
$BODY$
LANGUAGE plpgsql
IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.shark_conv_helper_to_date(IN arg NVARCHAR2,
                                                        IN p_try BOOL,
                                                        IN p_style NUMERIC DEFAULT 0)
RETURNS DATE
AS
$BODY$
BEGIN
    RETURN sys.shark_conv_helper_to_date(arg::TEXT, p_try, p_style);
END;
$BODY$
LANGUAGE plpgsql
IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.shark_conv_helper_to_date(IN arg BPCHAR,
                                                        IN p_try BOOL,
                                                        IN p_style NUMERIC DEFAULT 0)
RETURNS DATE
AS
$BODY$
BEGIN
    RETURN sys.shark_conv_helper_to_date(arg::TEXT, p_try, p_style);
END;
$BODY$
LANGUAGE plpgsql
IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.shark_conv_helper_to_date(IN arg anyelement,
                                                        IN p_try BOOL,
												        IN p_style NUMERIC DEFAULT 0)
RETURNS DATE
AS
$BODY$
DECLARE
    resdate DATE;
BEGIN
    IF p_try THEN
        resdate := sys.shark_try_conv_to_date(arg);
    ELSE
        BEGIN
            resdate := CAST(arg AS DATE);
        EXCEPTION
            WHEN cannot_coerce THEN
                RAISE USING MESSAGE := pg_catalog.format('Explicit conversion from data type %s to date is not allowed.', format_type(pg_typeof(arg)::oid, NULL));
            WHEN datetime_field_overflow THEN
                RAISE USING MESSAGE := 'Arithmetic overflow error converting expression to data type date.';
        END;
    END IF;

    RETURN resdate;
END;
$BODY$
LANGUAGE plpgsql
IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.shark_conv_helper_to_time(IN typmod INTEGER,
                                                        IN arg TEXT,
                                                        IN p_try BOOL,
												        IN p_style NUMERIC DEFAULT 0)
RETURNS TIME
AS
$BODY$
DECLARE
    v_res_datatype TEXT COLLATE "C";
BEGIN
    IF (typmod = -1) THEN
        v_res_datatype := 'TIME';
    ELSE
        v_res_datatype := PG_CATALOG.format('TIME(%s)', typmod);
    END IF;

    IF p_try THEN
	    RETURN sys.shark_try_conv_string_to_datetime2(v_res_datatype, arg, p_style);
    ELSE
	    RETURN sys.shark_conv_string_to_datetime2(v_res_datatype, arg, p_style);
    END IF;
END;
$BODY$
LANGUAGE plpgsql
IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.shark_conv_helper_to_time(IN typmod INTEGER,
                                                        IN arg VARCHAR,
                                                        IN p_try BOOL,
                                                        IN p_style NUMERIC DEFAULT 0)
RETURNS TIME
AS
$BODY$
BEGIN
    RETURN sys.shark_conv_helper_to_time(typmod, arg::TEXT, p_try, p_style);
END;
$BODY$
LANGUAGE plpgsql
IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.shark_conv_helper_to_time(IN typmod INTEGER,
                                                        IN arg BPCHAR,
                                                        IN p_try BOOL,
                                                        IN p_style NUMERIC DEFAULT 0)
RETURNS TIME
AS
$BODY$
BEGIN
    RETURN sys.shark_conv_helper_to_time(typmod, arg::TEXT, p_try, p_style);
END;
$BODY$
LANGUAGE plpgsql
IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.shark_conv_helper_to_time(IN typmod INTEGER,
                                                        IN arg NVARCHAR2,
                                                        IN p_try BOOL,
                                                        IN p_style NUMERIC DEFAULT 0)
RETURNS TIME
AS
$BODY$
BEGIN
    RETURN sys.shark_conv_helper_to_time(typmod, arg::TEXT, p_try, p_style);
END;
$BODY$
LANGUAGE plpgsql
IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.shark_conv_helper_to_datetime2(IN typmod INTEGER,
                                                            IN arg TEXT,
                                                            IN p_try BOOL,
													        IN p_style NUMERIC DEFAULT 0)
RETURNS TIMESTAMP WITHOUT TIME ZONE
AS
$BODY$
DECLARE
    v_res_datatype TEXT COLLATE "C";
BEGIN
    IF (typmod = -1) THEN
        v_res_datatype := 'TIMESTAMP WITHOUT TIME ZONE';
    ELSE
        v_res_datatype := PG_CATALOG.format('TIMESTAMP WITHOUT TIME ZONE(%s)', typmod);
    END IF;

    IF p_try THEN
	    RETURN sys.shark_try_conv_string_to_datetime2(v_res_datatype, arg, p_style);
    ELSE
        RETURN sys.shark_conv_string_to_datetime2(v_res_datatype, arg, p_style);
    END IF;
END;
$BODY$
LANGUAGE plpgsql
IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.shark_conv_helper_to_datetime2(IN typmod INTEGER,
                                                            IN arg VARCHAR,
                                                            IN p_try BOOL,
													        IN p_style NUMERIC DEFAULT 0)
RETURNS TIMESTAMP WITHOUT TIME ZONE
AS
$BODY$
BEGIN
    RETURN sys.shark_conv_helper_to_datetime2(typmod, arg::TEXT, p_try, p_style);
END;
$BODY$
LANGUAGE plpgsql
IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.shark_conv_helper_to_datetime2(IN typmod INTEGER,
                                                            IN arg NVARCHAR2,
                                                            IN p_try BOOL,
													        IN p_style NUMERIC DEFAULT 0)
RETURNS TIMESTAMP WITHOUT TIME ZONE
AS
$BODY$
BEGIN
    RETURN sys.shark_conv_helper_to_datetime2(typmod, arg::TEXT, p_try, p_style);
END;
$BODY$
LANGUAGE plpgsql
IMMUTABLE;

CREATE OR REPLACE FUNCTION sys.shark_conv_helper_to_datetime2(IN typmod INTEGER,
                                                            IN arg BPCHAR,
                                                            IN p_try BOOL,
													        IN p_style NUMERIC DEFAULT 0)
RETURNS TIMESTAMP WITHOUT TIME ZONE
AS
$BODY$
BEGIN
    RETURN sys.shark_conv_helper_to_datetime2(typmod, arg::TEXT, p_try, p_style);
END;
$BODY$
LANGUAGE plpgsql
IMMUTABLE;


-- Helper function to convert to binary or varbinary
CREATE OR REPLACE FUNCTION sys.shark_try_conv_string_to_varbinary(IN arg VARCHAR,
                                                                      IN p_style NUMERIC DEFAULT 0)
RETURNS sys.varbinary
AS
$BODY$
BEGIN
    RETURN sys.shark_conv_string_to_varbinary(arg, p_style);
    EXCEPTION
        WHEN OTHERS THEN
            RETURN NULL;
END;
$BODY$
LANGUAGE plpgsql
IMMUTABLE;

-- date funcs
CREATE OR REPLACE FUNCTION sys.dateadd(cstring,integer,date)
RETURNS timestamp without time zone
language c
immutable strict NOT FENCED NOT SHIPPABLE
AS '$libdir/shark', $function$dateadddate$function$;

CREATE OR REPLACE FUNCTION sys.dateadd(cstring,integer,timestamp without time zone)
RETURNS timestamp without time zone
language c
immutable strict NOT FENCED NOT SHIPPABLE
AS '$libdir/shark', $function$dateaddtimestamp$function$;

CREATE OR REPLACE FUNCTION sys.dateadd(cstring,integer,timestamp with time zone)
RETURNS timestamp with time zone
language c
immutable strict NOT FENCED NOT SHIPPABLE
AS '$libdir/shark', $function$dateaddtimestamptz$function$;

CREATE OR REPLACE FUNCTION sys.dateadd(cstring,integer,time without time zone)
RETURNS timestamp without time zone
language c
immutable strict NOT FENCED NOT SHIPPABLE
AS '$libdir/shark', $function$dateaddtime$function$;

CREATE OR REPLACE FUNCTION sys.dateadd(cstring,integer,time with time zone)
RETURNS timestamp with time zone
language c
immutable strict NOT FENCED NOT SHIPPABLE
AS '$libdir/shark', $function$dateaddtimetz$function$;

CREATE OR REPLACE FUNCTION sys.datepart(cstring,date)
RETURNS integer
language c
immutable strict NOT FENCED NOT SHIPPABLE
AS '$libdir/shark', $function$datepartdate$function$;

CREATE OR REPLACE FUNCTION sys.datepart(cstring,timestamp without time zone)
RETURNS integer
language c
immutable strict NOT FENCED NOT SHIPPABLE
AS '$libdir/shark', $function$dateparttimestamp$function$;

CREATE OR REPLACE FUNCTION sys.datepart(cstring,timestamp with time zone)
RETURNS integer
language c
immutable strict NOT FENCED NOT SHIPPABLE
AS '$libdir/shark', $function$dateparttimestamptz$function$;

CREATE OR REPLACE FUNCTION sys.datepart(cstring,time without time zone)
RETURNS integer
language c
immutable strict NOT FENCED NOT SHIPPABLE
AS '$libdir/shark', $function$dateparttime$function$;

CREATE OR REPLACE FUNCTION sys.datepart(cstring,time with time zone)
RETURNS integer
language c
immutable strict NOT FENCED NOT SHIPPABLE
AS '$libdir/shark', $function$dateparttimetz$function$;

CREATE OR REPLACE FUNCTION sys.getdate() RETURNS timestamp
AS '$libdir/shark', 'getdate_internal'
LANGUAGE C STABLE NOT FENCED NOT SHIPPABLE;

CREATE OR REPLACE FUNCTION sys.datename(cstring,date)
RETURNS varchar
language c
immutable strict NOT FENCED NOT SHIPPABLE
AS '$libdir/shark', $function$datenamedate$function$;

CREATE OR REPLACE FUNCTION sys.datename(cstring,timestamp without time zone)
RETURNS varchar
language c
immutable strict NOT FENCED NOT SHIPPABLE
AS '$libdir/shark', $function$datenametimestamp$function$;

CREATE OR REPLACE FUNCTION sys.datename(cstring,timestamp with time zone)
RETURNS varchar
language c
immutable strict NOT FENCED NOT SHIPPABLE
AS '$libdir/shark', $function$datenametimestamptz$function$;

CREATE OR REPLACE FUNCTION sys.datename(cstring,time without time zone)
RETURNS varchar
language c
immutable strict NOT FENCED NOT SHIPPABLE
AS '$libdir/shark', $function$datenametime$function$;

CREATE OR REPLACE FUNCTION sys.datename(cstring,time with time zone)
RETURNS varchar
language c
immutable strict NOT FENCED NOT SHIPPABLE
AS '$libdir/shark', $function$datenametimetz$function$;

CREATE OR REPLACE FUNCTION sys.len(expr TEXT)
RETURNS INTEGER as $$
SELECT length(trim(trailing from expr));
$$
language sql immutable strict NOT FENCED NOT SHIPPABLE;

CREATE OR REPLACE FUNCTION sys.len(expr sys.VARBINARY)
RETURNS INTEGER
language c
immutable strict NOT FENCED NOT SHIPPABLE
AS '$libdir/shark', $function$varbinary_length$function$;
