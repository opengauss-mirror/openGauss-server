-- Test keywords refers to `kwlist.h`
CREATE SCHEMA keywords;
SET CURRENT_SCHEMA TO keywords;
declare
keyword_name varchar;
catcode_name char;
create_sql varchar;
drop_sql varchar;
begin
    for keyword_name,catcode_name in (select word,catcode from pg_get_keywords()) loop
        if catcode_name = 'U' or catcode_name = 'C' then
            create_sql = 'create table keywords.' || keyword_name || '(' || keyword_name|| ' int)';
            drop_sql = 'drop table keywords.' || keyword_name;
        elsif catcode_name = 'T' then
            create_sql = 'create function keywords.' || keyword_name || '() returns int language sql as ''select 1''';
            drop_sql = 'drop function keywords.' || keyword_name;
        else
            continue;
        end if;
        begin
            EXECUTE IMMEDIATE create_sql;
            EXECUTE IMMEDIATE drop_sql;
            EXCEPTION when OTHERS then
            raise notice 'test failed. test sql: %; %;', create_sql, drop_sql;
        end;
    end loop;
end;
/
