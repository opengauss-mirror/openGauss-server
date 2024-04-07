CREATE DATABASE test_collate_A4 with dbcompatibility 'b' ENCODING='SQL_ASCII' LC_COLLATE='C' LC_CTYPE='C';
\c test_collate_A4

set b_format_behavior_compat_options = 'default_collation,enable_multi_charset';

create or replace function pg_catalog.get_index_columns(OUT namespace name, OUT indexrelid oid, OUT indrelid oid, OUT indisunique bool, OUT indisusable bool, OUT seq_in_index int2, OUT attrnum int2, OUT collation int2) returns setof record
as $$
declare
query_str text;
item int2;
row_data record;
begin
query_str := 'select n.nspname, i.indexrelid, i.indrelid, i.indisunique, i.indisusable, i.indkey, i.indoption, i.indnkeyatts
              from pg_catalog.pg_index i
                left join pg_class c on c.oid = i.indexrelid
                left join pg_catalog.pg_namespace n on n.oid = c.relnamespace
              where n.nspname <> ''pg_catalog''
                and n.nspname <> ''db4ai''
                and n.nspname <> ''information_schema''
                and n.nspname !~ ''^pg_toast''';
for row_data in EXECUTE(query_str) LOOP
    for item in 0..row_data.indnkeyatts - 1 loop
        namespace := row_data.nspname;
        indexrelid := row_data.indexrelid;
        indrelid := row_data.indrelid;
        indisunique := row_data.indisunique;
        indisusable := row_data.indisusable;
        seq_in_index := item + 1;
        attrnum := row_data.indkey[item];
        collation := row_data.indoption[item];
        return next;
    end loop;
end loop;
end; $$
LANGUAGE 'plpgsql';

create view public.index_statistic as
select
  i.namespace as "namespace",
  (select relname from pg_class tc where tc.oid = i.indrelid) as "table",
  not i.indisunique as "non_unique",
  c.relname as "key_name",
  i.seq_in_index as "seq_in_index",
  a.attname as "column_name",
  (case when m.amcanorder
    then (
      case when i.collation & 1 then 'D' else 'A' END
    ) else null end
  ) as "collation",
  (select
      (case when ts.stadistinct = 0
        then NULL else (
          case when ts.stadistinct > 0 then ts.stadistinct else ts.stadistinct * tc.reltuples * -1 end
        ) end
      )
    from pg_class tc
      left join pg_statistic ts on tc.oid = ts.starelid
    where
      tc.oid = i.indrelid
      and ts.staattnum = i.attrnum
  ) as "cardinality",
  null as "sub_part",
  null as "packed",
  (case when a.attnotnull then '' else 'YES' end) as "null",
  m.amname as "index_type",
  (case when i.indisusable then '' else 'disabled' end) as "comment",
  (select description from pg_description where objoid = i.indexrelid) as "index_comment"
from
  (select * from get_index_columns()) i
  left join pg_class c on c.oid = i.indexrelid
  left join pg_attribute a on a.attrelid = i.indrelid
  and a.attnum = i.attrnum
  left join pg_am m on m.oid = c.relam
order by
  c.relname;


CREATE OR REPLACE FUNCTION pg_catalog.pg_get_nonstrict_basic_value(typename text)
RETURNS text
AS
$$
BEGIN
    IF typename = 'timestamp' then
        return 'now';
    elsif typename = 'time' or typename = 'timetz' or typename = 'interval' or typename = 'reltime' then
        return '00:00:00';
    elsif typename = 'date' then
        return '1970-01-01';
    elsif typename = 'smalldatetime' then
        return '1970-01-01 08:00:00';
    elsif typename = 'abstime' then
        return '1970-01-01 08:00:00+08';
    elsif typename = 'uuid' then
        return '00000000-0000-0000-0000-000000000000';
    elsif typename = 'bool' then
        return 'false';
    elsif typename = 'point' or typename = 'polygon' then
        return '(0,0)';
    elsif typename = 'path' then
        return '((0,0))';
    elsif typename = 'circle' then
        return '(0,0),0';
    elsif typename = 'lseg' or typename = 'box' then
        return '(0,0),(0,0)';
    elsif typename = 'tinterval' then
        return '["1970-01-01 00:00:00+08" "1970-01-01 00:00:00+08"]';
    else
        return '0 or empty';
    end if;
end;
$$
LANGUAGE plpgsql;

CREATE VIEW public.pg_type_nonstrict_basic_value AS
    SELECT
            t.typname As typename,
            pg_get_nonstrict_basic_value(t.typname) As basic_value

    FROM pg_type t;


\c postgres
DROP DATABASE IF EXISTS test_collate_A4;