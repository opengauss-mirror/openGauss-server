declare
    var_name text;
    query_str text;
begin
    select rolname into var_name from pg_authid where oid=10;

    query_str := 'create schema "' || var_name || '"';
    EXECUTE IMMEDIATE query_str;
    
    query_str := 'drop schema "' || var_name ||'" CASCADE';
    EXECUTE IMMEDIATE query_str;
end;
/

declare
    var_name text;
    query_str text;
begin
    select rolname into var_name from pg_authid where oid=10;

    query_str := 'create schema authorization "' || var_name || '"';
    EXECUTE IMMEDIATE query_str;
    
    query_str := 'drop schema "' || var_name ||'" CASCADE';
    EXECUTE IMMEDIATE query_str;
end;
/

declare
    var_name text;
    query_str text;
begin
    select rolname into var_name from pg_authid where oid=10;
    
    query_str := 'create schema "' || var_name ||'_123"';
    EXECUTE IMMEDIATE query_str;

    query_str := 'drop schema "' || var_name || '_123" CASCADE';
    EXECUTE IMMEDIATE query_str;
end;
/

create schema gs_role;
create schema gs_role_;
create schema gs_role_abc;

drop schema gs_role;
drop schema gs_role_;
drop schema gs_role_abc;