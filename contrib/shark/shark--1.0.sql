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

reset search_path;