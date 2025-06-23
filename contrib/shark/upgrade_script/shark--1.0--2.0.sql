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