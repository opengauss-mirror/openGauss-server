/* contrib/gms_xmlgen/gms_xmlgen--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION gms_xmlgen" to load this file. \quit

CREATE SCHEMA gms_xmlgen;
GRANT USAGE ON SCHEMA gms_xmlgen TO PUBLIC;

-- create fake type gms_xmlgen.ctxhandle
create domain gms_xmlgen.ctxhandle as number;

-- gms_xmlgen.closecontext
CREATE OR REPLACE FUNCTION gms_xmlgen.close_context(ctx IN gms_xmlgen.ctxhandle)
RETURNS void
AS 'MODULE_PATHNAME', 'close_context'
LANGUAGE C STRICT IMMUTABLE;

CREATE OR REPLACE PROCEDURE gms_xmlgen.closecontext(ctx IN gms_xmlgen.ctxhandle)
AS
BEGIN
    gms_xmlgen.close_context(ctx);
END;

-- gms_xmlgen.convert
CREATE OR REPLACE FUNCTION gms_xmlgen.convert(xmlData IN VARCHAR2, flag IN NUMBER := 0)
RETURNS VARCHAR2 PACKAGE
AS 'MODULE_PATHNAME', 'convert_xml'
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION gms_xmlgen.convert(xmlData IN CLOB, flag IN NUMBER := 0)
RETURNS CLOB PACKAGE
AS 'MODULE_PATHNAME', 'convert_clob'
LANGUAGE C IMMUTABLE;

-- gms_xmlgen.getnumrowsprocessed
CREATE OR REPLACE FUNCTION gms_xmlgen.getnumrowsprocessed(ctx IN gms_xmlgen.ctxhandle)
RETURNS NUMBER
AS 'MODULE_PATHNAME', 'get_num_rows_processed'
LANGUAGE C STRICT VOLATILE;

-- gms_xmlgen.getxml
CREATE OR REPLACE FUNCTION gms_xmlgen.getxml(sqlQuery IN VARCHAR2, dtdOrSchema IN NUMBER := 0)
RETURNS CLOB PACKAGE
AS 'MODULE_PATHNAME', 'get_xml_by_query'
LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION gms_xmlgen.getxml(ctx IN gms_xmlgen.ctxhandle, dtdOrSchema IN NUMBER := 0)
RETURNS CLOB PACKAGE
AS 'MODULE_PATHNAME', 'get_xml_by_ctx_id'
LANGUAGE C VOLATILE;

CREATE OR REPLACE PROCEDURE gms_xmlgen.getxml(ctx IN gms_xmlgen.ctxhandle, tmpclob INOUT CLOB, dtdOrSchema IN NUMBER := 0)
PACKAGE
AS
BEGIN
    tmpclob := gms_xmlgen.getxml(ctx, dtdOrSchema);
END;

-- gms_xmlgen.getxmltype
CREATE OR REPLACE FUNCTION gms_xmlgen.getxmltype(ctx IN gms_xmlgen.ctxhandle, dtdOrSchema IN NUMBER := 0)
    RETURNS xmltype PACKAGE
AS $$
select xmltype(gms_xmlgen.getxml(ctx, dtdOrSchema))::xmltype;
$$
LANGUAGE SQL VOLATILE;

CREATE OR REPLACE FUNCTION gms_xmlgen.getxmltype(sqlQuery IN VARCHAR2, dtdOrSchema IN NUMBER := 0)
    RETURNS xmltype PACKAGE
AS $$
select xmltype(gms_xmlgen.getxml(sqlQuery, dtdOrSchema))::xmltype;
$$
LANGUAGE SQL VOLATILE;

-- gms_xmlgen.newcontext
CREATE OR REPLACE FUNCTION gms_xmlgen.newcontext(queryString IN VARCHAR2)
RETURNS gms_xmlgen.ctxhandle PACKAGE
AS 'MODULE_PATHNAME', 'new_context_by_query'
LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION gms_xmlgen.newcontext(queryString IN SYS_REFCURSOR)
RETURNS gms_xmlgen.ctxhandle PACKAGE
AS 'MODULE_PATHNAME', 'new_context_by_cursor'
LANGUAGE C VOLATILE;

-- gms_xmlgen.newcontextfromhierarchy
CREATE OR REPLACE FUNCTION gms_xmlgen.newcontextfromhierarchy(queryString IN VARCHAR2)
RETURNS gms_xmlgen.ctxhandle
AS 'MODULE_PATHNAME', 'new_context_from_hierarchy'
LANGUAGE C VOLATILE;

-- gms_xmlgen.restartquery
CREATE OR REPLACE FUNCTION gms_xmlgen.restart_query(ctx IN gms_xmlgen.ctxhandle)
RETURNS gms_xmlgen.ctxhandle
AS 'MODULE_PATHNAME', 'restart_query'
LANGUAGE C STRICT VOLATILE;

CREATE OR REPLACE PROCEDURE gms_xmlgen.restartquery(ctx IN gms_xmlgen.ctxhandle)
AS
BEGIN
    gms_xmlgen.restart_query(ctx);
END;

-- gms_xmlgen.setconvertspecialchars
CREATE OR REPLACE FUNCTION gms_xmlgen.set_convert_special_chars(ctx IN gms_xmlgen.ctxhandle, is_convert IN boolean)
RETURNS void
AS 'MODULE_PATHNAME', 'set_convert_special_chars'
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE PROCEDURE gms_xmlgen.setconvertspecialchars(ctx IN gms_xmlgen.ctxhandle, is_convert IN boolean)
AS
BEGIN
    gms_xmlgen.set_convert_special_chars(ctx, is_convert);
END;

-- gms_xmlgen.setmaxrows
CREATE OR REPLACE FUNCTION gms_xmlgen.set_max_rows(ctx IN gms_xmlgen.ctxhandle, maxrows IN NUMBER)
RETURNS void
AS 'MODULE_PATHNAME', 'set_max_rows'
LANGUAGE C STRICT IMMUTABLE;

CREATE OR REPLACE PROCEDURE gms_xmlgen.setmaxrows(ctx IN gms_xmlgen.ctxhandle, maxrows IN NUMBER)
AS
BEGIN
    gms_xmlgen.set_max_rows(ctx, maxrows);
END;

-- gms_xmlgen.setnullhandling
CREATE OR REPLACE FUNCTION gms_xmlgen.set_null_handling(ctx IN gms_xmlgen.ctxhandle, flag IN NUMBER)
RETURNS void
AS 'MODULE_PATHNAME', 'set_null_handling'
LANGUAGE C STRICT IMMUTABLE;

CREATE OR REPLACE PROCEDURE gms_xmlgen.setnullhandling(ctx IN gms_xmlgen.ctxhandle, flag IN NUMBER)
AS
BEGIN
    gms_xmlgen.set_null_handling(ctx, flag);
END;

-- gms_xmlgen.setrowsettag
CREATE OR REPLACE FUNCTION gms_xmlgen.set_row_set_tag(ctx IN gms_xmlgen.ctxhandle, rowSetTagName IN VARCHAR2)
RETURNS void
AS 'MODULE_PATHNAME', 'set_row_set_tag'
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE PROCEDURE gms_xmlgen.setrowsettag(ctx IN gms_xmlgen.ctxhandle, rowSetTagName IN VARCHAR2)
AS
BEGIN
    gms_xmlgen.set_row_set_tag(ctx, rowSetTagName);
END;

-- gms_xmlgen.setrowtag
CREATE OR REPLACE FUNCTION gms_xmlgen.set_row_tag(ctx IN gms_xmlgen.ctxhandle, rowTagName IN VARCHAR2)
RETURNS void
AS 'MODULE_PATHNAME', 'set_row_tag'
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE PROCEDURE gms_xmlgen.setrowtag(ctx IN gms_xmlgen.ctxhandle, rowTagName IN VARCHAR2)
AS
BEGIN
    gms_xmlgen.set_row_tag(ctx, rowTagName);
END;

-- gms_xmlgen.setskiprows
CREATE OR REPLACE FUNCTION gms_xmlgen.set_skip_rows(ctx IN gms_xmlgen.ctxhandle, skipRows IN NUMBER)
RETURNS void
AS 'MODULE_PATHNAME', 'set_skip_rows'
LANGUAGE C STRICT IMMUTABLE;

CREATE OR REPLACE PROCEDURE gms_xmlgen.setskiprows(ctx IN gms_xmlgen.ctxhandle, skipRows IN NUMBER)
AS
BEGIN
    gms_xmlgen.set_skip_rows(ctx, skipRows);
END;

-- gms_xmlgen.useitemtagsforcoll
CREATE OR REPLACE FUNCTION gms_xmlgen.use_item_tags_for_coll(ctx IN gms_xmlgen.ctxhandle)
RETURNS void
AS 'MODULE_PATHNAME', 'use_item_tags_for_coll'
LANGUAGE C STRICT IMMUTABLE;

CREATE OR REPLACE PROCEDURE gms_xmlgen.useitemtagsforcoll(ctx IN gms_xmlgen.ctxhandle)
AS
BEGIN
    gms_xmlgen.use_item_tags_for_coll(ctx);
END;

-- gms_xmlgen.usenullattributeindicator
CREATE OR REPLACE PROCEDURE gms_xmlgen.usenullattributeindicator(ctx IN gms_xmlgen.ctxhandle, attrind IN BOOLEAN := TRUE)
AS
BEGIN
    gms_xmlgen.set_null_handling(ctx, 1);
END;
