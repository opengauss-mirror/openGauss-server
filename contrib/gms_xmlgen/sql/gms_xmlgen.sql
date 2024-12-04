create extension gms_xmlgen;
create extension gms_output;
select gms_output.enable(100000);
create schema gms_xmlgen_test;
set search_path = gms_xmlgen_test;
set behavior_compat_options = 'bind_procedure_searchpath';
-- prepare data
create table t_types (
    "integer" integer,
    "float" float,
    "numeric" numeric(20, 6),
    "boolean" boolean,
    "char" char(20),
    "varchar" varchar(20),
    "text" text,
    "blob" blob,
    "raw" raw,
    "date" date,
    "time" time,
    "timestamp" timestamp,
    "json" json,
    "varchar_array" varchar(20)[]
);
insert into t_types
values(
        1,
        1.23456,
        1.234567,
        true,
        '"''<>&char test',
        'varchar"''<>&test',
        'text test"''<>&',
        'ff',
        hextoraw('ABCD'),
        '2024-01-02',
        '18:01:02',
        '2024-02-03 19:03:04',
        '{"a" : 1, "b" : 2}',
        array['abc', '"''<>&', '你好']
    ),
    (
        2,
        2.23456,
        2.234567,
        false,
        '2"''<>&char test',
        '2varchar"''<>&test',
        '2text test"''<>&',
        'eeee',
        hextoraw('ffff'),
        '2026-03-04',
        '20:12:13',
        '2026-05-06 21:13:00',
        '[9,8,7,6]',
        array['&^%@', '"''<>&', '<&y''">']
    ),
    (
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );

-- GMS_XMLGEN.NEWCONTEXT && GMS_XMLGEN.CLOSECONTEXT
select gms_xmlgen.newcontext('select * from t_types');
select gms_xmlgen.getxml(1);
select gms_xmlgen.closecontext(1);
-- procedure case
DECLARE
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- newcontext by cursor
DECLARE
CURSOR xc is select * from t_types;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('xc'::refcursor);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- newcontext by cursor expression
DECLARE
CURSOR xc is select "integer", CURSOR(select * from t_types) from t_types;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
open xc;
xml_cxt := gms_xmlgen.newcontext(xc);
gms_xmlgen.closecontext(xml_cxt);
close xc;
END;
/
-- invalid null parameter
select gms_xmlgen.newcontext(NULL);
-- ok for invalid query sql
select gms_xmlgen.newcontext('aabbccdd');
-- ok for closecontext NULL
select gms_xmlgen.closecontext(NULL);
-- ok for closecontext not exist id
select gms_xmlgen.closecontext(99);
-- error for closecontext invalid range
select gms_xmlgen.closecontext(-1);
select gms_xmlgen.closecontext(4294967296);

-- GMS_XMLGEN.GETXML
-- getxml by query
DECLARE
xml_output clob;
BEGIN
xml_output := gms_xmlgen.getxml('select * from t_types');
gms_output.put_line(xml_output);
END;
/
-- getxml by cursor
DECLARE
CURSOR xc is select * from t_types;
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
open xc;
xml_cxt := gms_xmlgen.newcontext(xc);
xml_output := gms_xmlgen.getxml(xml_cxt);
gms_output.put_line(xml_output);
close xc;
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- getxml by cursor expression
DECLARE
CURSOR xc is select "integer", CURSOR(select * from t_types) from t_types;
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
open xc;
xml_cxt := gms_xmlgen.newcontext(xc);
xml_output := gms_xmlgen.getxml(xml_cxt);
gms_output.put_line(xml_output);
close xc;
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- getxml by context id
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
xml_output := gms_xmlgen.getxml(xml_cxt);
gms_output.put_line(xml_output);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- getxml by context id with out parameter
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.getxml(xml_cxt, xml_output);
gms_output.put_line(xml_output);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- no result when getxml twice without restartquery
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.getxml(xml_cxt);
xml_output := gms_xmlgen.getxml(xml_cxt);
gms_output.put_line(xml_output);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- invalid null parameter
select gms_xmlgen.getxml(NULL);
-- invalid query sql
select gms_xmlgen.getxml('aabbccdd');
-- invalid xmlgen context id 
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');	
gms_xmlgen.closecontext(xml_cxt);
xml_output := gms_xmlgen.getxml(xml_cxt);
END;
/
-- invalid xmlgen context id with out parameter 
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');	
gms_xmlgen.closecontext(xml_cxt);
gms_xmlgen.getxml(xml_cxt, xml_output);
gms_output.put_line(xml_output);
END;
/

-- GMS_XMLGEN.GETXMLTYPE
-- getxmltype by query
DECLARE
xml_cxt gms_xmlgen.ctxhandle;
xml_type xmltype;
BEGIN
xml_type := gms_xmlgen.getxmltype('select * from t_types');
gms_output.put_line(xml_type::text);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- getxmltype by query with parameter 2
DECLARE
xml_cxt gms_xmlgen.ctxhandle;
xml_type xmltype;
BEGIN
xml_type := gms_xmlgen.getxmltype('select * from t_types', 1);
gms_output.put_line(xml_type::text);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- getxmltype by context id
DECLARE
xml_cxt gms_xmlgen.ctxhandle;
xml_type xmltype;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
xml_type := gms_xmlgen.getxmltype(xml_cxt);
gms_output.put_line(xml_type::text);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- getxmltype by context id with parameter 2
DECLARE
xml_cxt gms_xmlgen.ctxhandle;
xml_type xmltype;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
xml_type := gms_xmlgen.getxmltype(xml_cxt, 1);
gms_output.put_line(xml_type::text);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- getxmltype by cursor
DECLARE
CURSOR xc is select * from t_types;
xml_cxt gms_xmlgen.ctxhandle;
xml_output xmltype;
BEGIN
open xc;
xml_cxt := gms_xmlgen.newcontext(xc);
xml_output := gms_xmlgen.getxmltype(xml_cxt);
gms_output.put_line(xml_output::text);
close xc;
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- getxmltype by cursor expression
DECLARE
CURSOR xc is select "integer", CURSOR(select * from t_types) from t_types;
xml_cxt gms_xmlgen.ctxhandle;
xml_output xmltype;
BEGIN
open xc;
xml_cxt := gms_xmlgen.newcontext(xc);
xml_output := gms_xmlgen.getxmltype(xml_cxt);
gms_output.put_line(xml_output::text);
close xc;
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- invalid null parameter
select gms_xmlgen.getxmltype(NULL);
-- invalid query sql
select gms_xmlgen.getxmltype('aabbccdd');
-- invalid context id
DECLARE
xml_cxt gms_xmlgen.ctxhandle;
xml_type xmltype;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.closecontext(xml_cxt);
xml_type := gms_xmlgen.getxmltype(xml_cxt);
END;
/
-- invalid parameter 2 range
select gms_xmlgen.getxmltype('select * from t_types', -1);
select gms_xmlgen.getxmltype('select * from t_types', 4294967296);

-- GMS_XMLGEN.NEWCONTEXTFROMHIERARCHY
DECLARE
xml_output clob;
xml_cxt_from_hierarchy gms_xmlgen.ctxhandle;
BEGIN
xml_cxt_from_hierarchy := gms_xmlgen.newcontextfromhierarchy('
SELECT "integer", xmltype(gms_xmlgen.getxml(''select * from t_types''))
FROM t_types
START WITH "integer" = 1 OR "integer" = 2
CONNECT BY nocycle "integer" = PRIOR "integer"');
xml_output := gms_xmlgen.getxml(xml_cxt_from_hierarchy);
gms_output.put_line(xml_output);
gms_xmlgen.closecontext(xml_cxt_from_hierarchy);
END;
/
-- with set row set tag
DECLARE
xml_output clob;
xml_cxt_from_hierarchy gms_xmlgen.ctxhandle;
BEGIN
xml_cxt_from_hierarchy := gms_xmlgen.newcontextfromhierarchy('
SELECT "integer", xmltype(gms_xmlgen.getxml(''select * from t_types''))
FROM t_types
START WITH "integer" = 1 OR "integer" = 2
CONNECT BY nocycle "integer" = PRIOR "integer"');
gms_xmlgen.setrowsettag(xml_cxt_from_hierarchy, 'TopTag');
xml_output := gms_xmlgen.getxml(xml_cxt_from_hierarchy);
gms_output.put_line(xml_output);
gms_xmlgen.closecontext(xml_cxt_from_hierarchy);
END;
/
-- error with set row tag
DECLARE
xml_output clob;
xml_cxt_from_hierarchy gms_xmlgen.ctxhandle;
BEGIN
xml_cxt_from_hierarchy := gms_xmlgen.newcontextfromhierarchy('
SELECT "integer", xmltype(gms_xmlgen.getxml(''select * from t_types''))
FROM t_types
START WITH "integer" = 1 OR "integer" = 2
CONNECT BY nocycle "integer" = PRIOR "integer"');
gms_xmlgen.setrowtag(xml_cxt_from_hierarchy, 'TopTag');
xml_output := gms_xmlgen.getxml(xml_cxt_from_hierarchy);
gms_output.put_line(xml_output);
gms_xmlgen.closecontext(xml_cxt_from_hierarchy);
END;
/
-- error with setmaxrows
DECLARE
xml_output clob;
xml_cxt_from_hierarchy gms_xmlgen.ctxhandle;
BEGIN
xml_cxt_from_hierarchy := gms_xmlgen.newcontextfromhierarchy('
SELECT "integer", xmltype(gms_xmlgen.getxml(''select * from t_types''))
FROM t_types
START WITH "integer" = 1 OR "integer" = 2
CONNECT BY nocycle "integer" = PRIOR "integer"');
gms_xmlgen.setmaxrows(xml_cxt_from_hierarchy, 1);
xml_output := gms_xmlgen.getxml(xml_cxt_from_hierarchy);
gms_output.put_line(xml_output);
gms_xmlgen.closecontext(xml_cxt_from_hierarchy);
END;
/
-- error with setskiprows
DECLARE
xml_output clob;
xml_cxt_from_hierarchy gms_xmlgen.ctxhandle;
BEGIN
xml_cxt_from_hierarchy := gms_xmlgen.newcontextfromhierarchy('
SELECT "integer", xmltype(gms_xmlgen.getxml(''select * from t_types''))
FROM t_types
START WITH "integer" = 1 OR "integer" = 2
CONNECT BY nocycle "integer" = PRIOR "integer"');
gms_xmlgen.setskiprows(xml_cxt_from_hierarchy, 1);
xml_output := gms_xmlgen.getxml(xml_cxt_from_hierarchy);
gms_output.put_line(xml_output);
gms_xmlgen.closecontext(xml_cxt_from_hierarchy);
END;
/
-- invalid null parameter
select gms_xmlgen.newcontextfromhierarchy(NULL);
-- ok for invalid query sql
select gms_xmlgen.newcontextfromhierarchy('aabbccdd');
-- get xml error with invalid query sql
DECLARE
xml_output clob;
xml_cxt_from_hierarchy gms_xmlgen.ctxhandle;
BEGIN
xml_cxt_from_hierarchy := gms_xmlgen.newcontextfromhierarchy('aabbccdd');
xml_output := gms_xmlgen.getxml(xml_cxt_from_hierarchy);
gms_output.put_line(xml_output);
gms_xmlgen.closecontext(xml_cxt_from_hierarchy);
END;
/
-- get xml error with not hierarchy query sql
DECLARE
xml_output clob;
xml_cxt_from_hierarchy gms_xmlgen.ctxhandle;
BEGIN
xml_cxt_from_hierarchy := gms_xmlgen.newcontextfromhierarchy('select * from t_types');
xml_output := gms_xmlgen.getxml(xml_cxt_from_hierarchy);
gms_output.put_line(xml_output);
gms_xmlgen.closecontext(xml_cxt_from_hierarchy);
END;
/

-- GMS_XMLGEN.RESTARTQUERY
-- get xml twice
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
xml_output := gms_xmlgen.getxml(xml_cxt);
gms_output.put_line(xml_output);
gms_xmlgen.restartquery(xml_cxt);
xml_output := gms_xmlgen.getxml(xml_cxt);
gms_output.put_line(xml_output);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- ok for restartquery closed context id
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.closecontext(xml_cxt);
gms_xmlgen.restartquery(xml_cxt);
END;
/

-- GMS_XMLGEN.SETCONVERTSPECIALCHARS
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.setconvertspecialchars(xml_cxt, false);
xml_output := gms_xmlgen.getxml(xml_cxt);
gms_output.put_line(xml_output);
gms_xmlgen.setconvertspecialchars(xml_cxt, true);
gms_xmlgen.restartquery(xml_cxt);
xml_output := gms_xmlgen.getxml(xml_cxt);
gms_output.put_line(xml_output);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- parameter2 null is the same as false
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.setconvertspecialchars(xml_cxt, null);
xml_output := gms_xmlgen.getxml(xml_cxt);
gms_output.put_line(xml_output);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- error for missing parameter 2
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.setconvertspecialchars(xml_cxt);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- ok for closed context
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.closecontext(xml_cxt);
gms_xmlgen.setconvertspecialchars(xml_cxt, true);
END;
/

-- GMS_XMLGEN.SETMAXROWS
-- set max rows 0
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.setmaxrows(xml_cxt, 0);
xml_output := gms_xmlgen.getxml(xml_cxt);
gms_output.put_line(xml_output);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- set max rows 1
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.setmaxrows(xml_cxt, 1);
xml_output := gms_xmlgen.getxml(xml_cxt);
gms_output.put_line(xml_output);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- set max rows 1.4, the same as 1
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.setmaxrows(xml_cxt, 1.4);
xml_output := gms_xmlgen.getxml(xml_cxt);
gms_output.put_line(xml_output);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- set max rows 1.9, the same as 1
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.setmaxrows(xml_cxt, 1.9);
xml_output := gms_xmlgen.getxml(xml_cxt);
gms_output.put_line(xml_output);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- parameter nums error
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.setmaxrows(xml_cxt);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- parameter range error
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.setmaxrows(xml_cxt, -1);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- parameter range error
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.setmaxrows(xml_cxt, 4294967296);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- ok for closed context
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.closecontext(xml_cxt);
gms_xmlgen.setmaxrows(xml_cxt, 1);
END;
/

-- GMS_XMLGEN.SETNULLHANDLING
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.setnullhandling(xml_cxt, 0);
xml_output := gms_xmlgen.getxml(xml_cxt);
gms_output.put_line(xml_output);
gms_xmlgen.setnullhandling(xml_cxt, 1);
gms_xmlgen.restartquery(xml_cxt);
xml_output := gms_xmlgen.getxml(xml_cxt);
gms_output.put_line(xml_output);
gms_xmlgen.setnullhandling(xml_cxt, 2);
gms_xmlgen.restartquery(xml_cxt);
xml_output := gms_xmlgen.getxml(xml_cxt);
gms_output.put_line(xml_output);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- number 1.4, the same as 1
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.setnullhandling(xml_cxt, 1.4);
xml_output := gms_xmlgen.getxml(xml_cxt);
gms_output.put_line(xml_output);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- number 1.9, the same as 1
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.setnullhandling(xml_cxt, 1.9);
xml_output := gms_xmlgen.getxml(xml_cxt);
gms_output.put_line(xml_output);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- other numbers > 2, the same as 0
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.setnullhandling(xml_cxt, 3);
xml_output := gms_xmlgen.getxml(xml_cxt);
gms_output.put_line(xml_output);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- ok for NULL
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
gms_xmlgen.setnullhandling(NULL, 1);
END;
/
-- parameter nums error
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.setnullhandling(xml_cxt);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- parameter range error
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.setnullhandling(xml_cxt, -1);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- parameter range error
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.setnullhandling(xml_cxt, 4294967296);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- ok for closed context
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.closecontext(xml_cxt);
gms_xmlgen.setnullhandling(xml_cxt, 1);
END;
/

-- GMS_XMLGEN.SETROWSETTAG
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.setrowsettag(xml_cxt, 'test');
xml_output := gms_xmlgen.getxml(xml_cxt);
gms_output.put_line(xml_output);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- error for setrowsettag NULL
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.setrowsettag(xml_cxt, NULL);
xml_output := gms_xmlgen.getxml(xml_cxt);
gms_output.put_line(xml_output);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- ok for setrowsettag NULL with one row
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types where rownum = 1');
gms_xmlgen.setrowsettag(xml_cxt, NULL);
xml_output := gms_xmlgen.getxml(xml_cxt);
gms_output.put_line(xml_output);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- ok for setrowsettag context id null
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
gms_xmlgen.setrowsettag(NULL, 'test');
END;
/
-- parameter nums error
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.setrowsettag(xml_cxt);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- parameter type error
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.setrowsettag(xml_cxt, true);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- ok for closed context
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.closecontext(xml_cxt);
gms_xmlgen.setrowsettag(xml_cxt, 'test');
END;
/

-- GMS_XMLGEN.SETROWTAG
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.setrowtag(xml_cxt, 'test');
xml_output := gms_xmlgen.getxml(xml_cxt);
gms_output.put_line(xml_output);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- ok for setrowtag NULL
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.setrowtag(xml_cxt, NULL);
xml_output := gms_xmlgen.getxml(xml_cxt);
gms_output.put_line(xml_output);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- error for setrowsettag NULL && setrowtag NULL
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.setrowsettag(xml_cxt, NULL);
gms_xmlgen.setrowtag(xml_cxt, NULL);
xml_output := gms_xmlgen.getxml(xml_cxt);
gms_output.put_line(xml_output);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- ok for setrowtag context id null
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
gms_xmlgen.setrowtag(NULL, 'test');
END;
/
-- parameter nums error
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.setrowtag(xml_cxt);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- parameter type error
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.setrowtag(xml_cxt, true);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- ok for closed context
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.closecontext(xml_cxt);
gms_xmlgen.setrowtag(xml_cxt, 'test');
END;
/

-- GMS_XMLGEN.SETSKIPROWS
-- set skip row 0
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.setskiprows(xml_cxt, 0);
xml_output := gms_xmlgen.getxml(xml_cxt);
gms_output.put_line(xml_output);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- set skip row 1
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.setskiprows(xml_cxt, 1);
xml_output := gms_xmlgen.getxml(xml_cxt);
gms_output.put_line(xml_output);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- set skip row 1.4, the same as 1
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.setskiprows(xml_cxt, 1.4);
xml_output := gms_xmlgen.getxml(xml_cxt);
gms_output.put_line(xml_output);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- set skip row 1.9, the same as 1
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.setskiprows(xml_cxt, 1.9);
xml_output := gms_xmlgen.getxml(xml_cxt);
gms_output.put_line(xml_output);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- set skip row 2
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.setskiprows(xml_cxt, 2);
xml_output := gms_xmlgen.getxml(xml_cxt);
gms_output.put_line(xml_output);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- ok for setskiprows context id null
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.setskiprows(NULL, 1);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- parameter nums error
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.setskiprows(xml_cxt);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- parameter range error
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.setskiprows(xml_cxt, -1);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- parameter range error
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.setskiprows(xml_cxt, 4294967296);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- ok for closed context
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.closecontext(xml_cxt);
gms_xmlgen.setskiprows(xml_cxt, 1);
END;
/

-- GMS_XMLGEN.USEITEMTAGSFORCOLL
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.useitemtagsforcoll(xml_cxt);
xml_output := gms_xmlgen.getxml(xml_cxt);
gms_output.put_line(xml_output);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- ok for useitemtagsforcoll context id NULL
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.useitemtagsforcoll(NULL);
xml_output := gms_xmlgen.getxml(xml_cxt);
gms_output.put_line(xml_output);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- ok for closed context
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.closecontext(xml_cxt);
gms_xmlgen.useitemtagsforcoll(xml_cxt);
END;
/

-- GMS_XMLGEN.USENULLATTRIBUTEINDICATOR
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.usenullattributeindicator(xml_cxt);
xml_output := gms_xmlgen.getxml(xml_cxt);
gms_output.put_line(xml_output);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- ok for parameter 2 true
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.usenullattributeindicator(xml_cxt, true);
xml_output := gms_xmlgen.getxml(xml_cxt);
gms_output.put_line(xml_output);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- ok for parameter 2 false, the result is the same as the true's
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.usenullattributeindicator(xml_cxt, false);
xml_output := gms_xmlgen.getxml(xml_cxt);
gms_output.put_line(xml_output);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- ok for usenullattributeindicator context id NULL
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.usenullattributeindicator(NULL);
xml_output := gms_xmlgen.getxml(xml_cxt);
gms_output.put_line(xml_output);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- ok for closed context
DECLARE
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.closecontext(xml_cxt);
gms_xmlgen.usenullattributeindicator(xml_cxt);
END;
/

-- GMS_XMLGEN.GETNUMROWSPROCESSED
DECLARE
processed_row number;
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
processed_row := gms_xmlgen.getnumrowsprocessed(xml_cxt);
gms_output.put_line(processed_row);
xml_output := gms_xmlgen.getxml(xml_cxt);
gms_output.put_line(xml_output);
processed_row := gms_xmlgen.getnumrowsprocessed(xml_cxt);
gms_output.put_line(processed_row);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- getnumrowsprocessed with skip rows
DECLARE
processed_row number;
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.setskiprows(xml_cxt, 1);
processed_row := gms_xmlgen.getnumrowsprocessed(xml_cxt);
gms_output.put_line(processed_row);
xml_output := gms_xmlgen.getxml(xml_cxt);
gms_output.put_line(xml_output);
processed_row := gms_xmlgen.getnumrowsprocessed(xml_cxt);
gms_output.put_line(processed_row);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- getnumrowsprocessed with max rows
DECLARE
processed_row number;
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.setmaxrows(xml_cxt, 2);
processed_row := gms_xmlgen.getnumrowsprocessed(xml_cxt);
gms_output.put_line(processed_row);
xml_output := gms_xmlgen.getxml(xml_cxt);
gms_output.put_line(xml_output);
processed_row := gms_xmlgen.getnumrowsprocessed(xml_cxt);
gms_output.put_line(processed_row);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- getnumrowsprocessed with skip rows && max rows
DECLARE
processed_row number;
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.setskiprows(xml_cxt, 1);
gms_xmlgen.setmaxrows(xml_cxt, 1);
processed_row := gms_xmlgen.getnumrowsprocessed(xml_cxt);
gms_output.put_line(processed_row);
xml_output := gms_xmlgen.getxml(xml_cxt);
gms_output.put_line(xml_output);
processed_row := gms_xmlgen.getnumrowsprocessed(xml_cxt);
gms_output.put_line(processed_row);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- getnumrowsprocessed with skip rows && max rows
-- the second getxml should continue from the last
DECLARE
processed_row number;
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
gms_xmlgen.setskiprows(xml_cxt, 0);
gms_xmlgen.setmaxrows(xml_cxt, 1);
processed_row := gms_xmlgen.getnumrowsprocessed(xml_cxt);
gms_output.put_line(processed_row);
xml_output := gms_xmlgen.getxml(xml_cxt);
gms_output.put_line(xml_output);
processed_row := gms_xmlgen.getnumrowsprocessed(xml_cxt);
gms_output.put_line(processed_row);
xml_output := gms_xmlgen.getxml(xml_cxt);
gms_output.put_line(xml_output);
processed_row := gms_xmlgen.getnumrowsprocessed(xml_cxt);
gms_output.put_line(processed_row);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- ok for getnumrowsprocessed context id NULL
DECLARE
processed_row number;
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
xml_output := gms_xmlgen.getxml(xml_cxt);
processed_row := gms_xmlgen.getnumrowsprocessed(NULL);
gms_output.put_line(processed_row);
gms_xmlgen.closecontext(xml_cxt);
END;
/
-- ok for closed context
DECLARE
processed_row number;
xml_output clob;
xml_cxt gms_xmlgen.ctxhandle;
BEGIN
xml_cxt := gms_xmlgen.newcontext('select * from t_types');
xml_output := gms_xmlgen.getxml(xml_cxt);
gms_xmlgen.closecontext(xml_cxt);
processed_row := gms_xmlgen.getnumrowsprocessed(xml_cxt);
gms_output.put_line(processed_row);
END;
/

-- GMS_XMLGEN.CONVERT
select GMS_XMLGEN.CONVERT('"''<>&'::varchar2);
select GMS_XMLGEN.CONVERT('"''<>&'::varchar2, NULL);
select GMS_XMLGEN.CONVERT('"''<>&'::varchar2, 0);
select GMS_XMLGEN.CONVERT('"''<>&'::varchar2, 1);
select GMS_XMLGEN.CONVERT('"''<>&'::varchar2, 2);
select GMS_XMLGEN.CONVERT('"''<>&'::varchar2, -1);
select GMS_XMLGEN.CONVERT('"''<>&'::varchar2, 4294967295);
select GMS_XMLGEN.CONVERT('"''<>&'::varchar2, 4294967296);
select GMS_XMLGEN.CONVERT('&quot;&apos;&lt;&gt;&amp;'::varchar2);
select GMS_XMLGEN.CONVERT('&quot;&apos;&lt;&gt;&amp;'::varchar2, NULL);
select GMS_XMLGEN.CONVERT('&quot;&apos;&lt;&gt;&amp;'::varchar2, 0);
select GMS_XMLGEN.CONVERT('&quot;&apos;&lt;&gt;&amp;'::varchar2, 1);
select GMS_XMLGEN.CONVERT('&quot;&apos;&lt;&gt;&amp;'::varchar2, 2);
-- would not convert
select GMS_XMLGEN.CONVERT('&QUOT;&ApOS;&LT;&gT;&Amp;'::varchar2, 1);
select pg_typeof(GMS_XMLGEN.CONVERT('"''<>&'::varchar2, 1));
select GMS_XMLGEN.CONVERT('"''<>&'::clob);
select GMS_XMLGEN.CONVERT('"''<>&'::clob, NULL);
select GMS_XMLGEN.CONVERT('"''<>&'::clob, 0);
select GMS_XMLGEN.CONVERT('"''<>&'::clob, 1);
select GMS_XMLGEN.CONVERT('"''<>&'::clob, 2);
select GMS_XMLGEN.CONVERT('"''<>&'::clob, -1);
select GMS_XMLGEN.CONVERT('"''<>&'::clob, 4294967295);
select GMS_XMLGEN.CONVERT('"''<>&'::clob, 4294967296);
select GMS_XMLGEN.CONVERT('&quot;&apos;&lt;&gt;&amp;'::clob);
select GMS_XMLGEN.CONVERT('&quot;&apos;&lt;&gt;&amp;'::clob, NULL);
select GMS_XMLGEN.CONVERT('&quot;&apos;&lt;&gt;&amp;'::clob, 0);
select GMS_XMLGEN.CONVERT('&quot;&apos;&lt;&gt;&amp;'::clob, 1);
select GMS_XMLGEN.CONVERT('&quot;&apos;&lt;&gt;&amp;'::clob, 2);
-- would not convert
select GMS_XMLGEN.CONVERT('&QUOT;&ApOS;&LT;&gT;&Amp;'::clob, 1);
select pg_typeof(GMS_XMLGEN.CONVERT('"''<>&'::clob, 1));
-- error for NULL
select GMS_XMLGEN.CONVERT(NULL, 0);
select GMS_XMLGEN.CONVERT(NULL);

-- compatibility tool usecases
DECLARE ctx GMS_xmlgen.ctxHandle;
BEGIN ctx := GMS_xmlgen.newContext('select * FROM t_types');
GMS_xmlgen.closeContext(ctx);
GMS_output.put_line(ctx::text);
END;
/
DECLARE res XMLType;
BEGIN res := GMS_XMLGEN.GETXMLTYPE('123');
EXCEPTION
WHEN OTHERS THEN NULL;
END;
/
DECLARE res GMS_XMLGEN.ctxHandle;
BEGIN res := GMS_XMLGEN.NEWCONTEXTFROMHIERARCHY('123');
EXCEPTION
WHEN OTHERS THEN NULL;
END;
/
DECLARE d varchar2(100);
a varchar2(100);
BEGIN d := GMS_XMLGEN.CONVERT(a);
EXCEPTION
WHEN OTHERS THEN NULL;
END;
/
DECLARE d number;
a GMS_XMLGEN.ctxHandle;
BEGIN d := GMS_XMLGEN.GETNUMROWSPROCESSED(a);
EXCEPTION
WHEN OTHERS THEN NULL;
END;
/
DECLARE a GMS_XMLGEN.ctxHandle;
b clob;
BEGIN b := GMS_XMLGEN.GETXML(a);
EXCEPTION
WHEN OTHERS THEN NULL;
END;
/
DECLARE CTX GMS_XMLGEN.CTXHANDLE;
BEGIN CTX := GMS_XMLGEN.NEWCONTEXT('select * FROM t_types');
EXCEPTION
WHEN OTHERS THEN NULL;
END;
/
DECLARE CTX GMS_XMLGEN.CTXHANDLE;
BEGIN GMS_XMLGEN.RESTARTQUERY(CTX);
EXCEPTION
WHEN OTHERS THEN NULL;
END;
/
DECLARE CTX GMS_XMLGEN.CTXHANDLE;
BEGIN CTX := GMS_XMLGEN.NEWCONTEXT('select * FROM t_types');
GMS_XMLGEN.SETCONVERTSPECIALCHARS(CTX, false);
END;
/
DECLARE CTX GMS_XMLGEN.CTXHANDLE;
BEGIN GMS_XMLGEN.SETMAXROWS(CTX, 2);
EXCEPTION
WHEN OTHERS THEN NULL;
END;
/
DECLARE CTX GMS_XMLGEN.CTXHANDLE;
BEGIN CTX := GMS_XMLGEN.NEWCONTEXT('select * FROM t_types');
GMS_XMLGEN.SETNULLHANDLING(CTX, 1);
END;
/
DECLARE CTX GMS_XMLGEN.CTXHANDLE;
BEGIN CTX := GMS_XMLGEN.NEWCONTEXT('select * FROM t_types');
GMS_XMLGEN.SETROWSETTAG(CTX, 'srst');
END;
/
DECLARE CTX GMS_XMLGEN.CTXHANDLE;
BEGIN CTX := GMS_XMLGEN.NEWCONTEXT('select * FROM t_types');
GMS_XMLGEN.SETROWTAG(CTX, 'srst');
END;
/
DECLARE CTX GMS_XMLGEN.CTXHANDLE;
BEGIN GMS_XMLGEN.SETSKIPROWS(CTX, 5);
EXCEPTION
WHEN OTHERS THEN NULL;
END;
/
DECLARE CTX GMS_XMLGEN.CTXHANDLE;
BEGIN GMS_XMLGEN.USEITEMTAGSFORCOLL(CTX);
EXCEPTION
WHEN OTHERS THEN NULL;
END;
/
DECLARE CTX GMS_XMLGEN.CTXHANDLE;
BEGIN CTX := GMS_XMLGEN.NEWCONTEXT('select * FROM t_types');
GMS_XMLGEN.USENULLATTRIBUTEINDICATOR(CTX, false);
END;
/

reset search_path;
drop schema gms_xmlgen_test cascade;

create database test_b dbcompatibility='B';
\c test_b
create extension GMS_XMLGEN;

\c contrib_regression
drop database test_b;
