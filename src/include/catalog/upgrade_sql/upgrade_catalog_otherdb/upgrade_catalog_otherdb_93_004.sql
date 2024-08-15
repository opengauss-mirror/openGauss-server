CREATE DOMAIN pg_catalog.xmltype AS xml;
DROP SCHEMA IF EXISTS xmltype cascade;
CREATE SCHEMA xmltype;
GRANT USAGE ON SCHEMA xmltype TO PUBLIC;
CREATE OR REPLACE FUNCTION pg_catalog.xmltype(xmlvalue text)
RETURNS xml as $$
declare
    dbcom text;
begin
    show sql_compatibility into dbcom;
    if dbcom != 'A' THEN
        raise exception 'Functions for type xmltype is only support in database which dbcompatibility = ''A''.';
    end if;
    return xml(xmlvalue);
end;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION pg_catalog.xmltype() IS 'pg_catalog function XMLTYPE';

/*
 * createxml function
 */
CREATE OR REPLACE FUNCTION xmltype.createxml(xmldata varchar2)
RETURNS xml 
AS $$ select xmltype($1); $$
LANGUAGE SQL IMMUTABLE STRICT;
