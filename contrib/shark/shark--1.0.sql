create schema sys;
grant usage on schema sys to public;
set search_path = 'sys';

create function pltsql_call_handler()
    returns language_handler as 'MODULE_PATHNAME' language C;

create function pltsql_validator(oid)
    returns void as 'MODULE_PATHNAME' language C;

create function pltsql_inline_handler(internal)
    returns void as 'MODULE_PATHNAME' language C;

create trusted language pltsql
    handler pltsql_call_handler
    inline pltsql_inline_handler
    validator pltsql_validator;

grant usage on language pltsql to public;
    
create function fetch_status()
    returns int as 'MODULE_PATHNAME' language C;

create function rowcount()
    returns int as 'MODULE_PATHNAME' language C;

create function rowcount_big()
    returns bigint as 'MODULE_PATHNAME' language C;

create function spid()
    returns bigint language sql as $$ select pg_current_sessid() $$;

reset search_path;