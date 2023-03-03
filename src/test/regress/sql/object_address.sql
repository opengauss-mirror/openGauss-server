--
-- Test for pg_get_object_address,also test effect caused by object_address
--
create database test_obj_address dbcompatibility 'PG';
\c test_obj_address;
-- test commemt on type and domain using schema.typename or schema.domainname
create schema commenton_schema;
-- type and domain
create type typ1 as (f1 int, f2 text);
create domain dom1 as text check(value > 'comment');
comment on type typ1 is 'type typ1';
comment on domain dom1 is 'domain dom1';

create type commenton_schema.typ2 as (f1 int, f2 text);
create domain commenton_schema.dom2 as text check(value > 'comment');
comment on type commenton_schema.typ2 is 'commenton_schema.typ2 ';
comment on domain commenton_schema.dom2 is 'commenton_schema.dom2';

drop type typ1 CASCADE;
drop domain dom1 CASCADE;
drop type commenton_schema.typ1 CASCADE;
drop domain commenton_schema.dom1 CASCADE;
drop schema commenton_schema CASCADE;

-- Clean up in case a prior regression run failed
SET client_min_messages TO 'warning';
DROP ROLE IF EXISTS regress_addr_user;
RESET client_min_messages;

CREATE USER regress_addr_user with sysadmin password 'Gaussdba@Mpp';

-- Test generic object addressing/identification functions
CREATE SCHEMA addr_nsp;
SET search_path TO 'addr_nsp';
CREATE FOREIGN DATA WRAPPER addr_fdw;
CREATE SERVER addr_fserv FOREIGN DATA WRAPPER addr_fdw;
CREATE TEXT SEARCH DICTIONARY addr_ts_dict (template=simple);
CREATE TEXT SEARCH CONFIGURATION addr_ts_conf (copy=english);
CREATE TABLE addr_nsp.gentable (
	a serial primary key CONSTRAINT a_chk CHECK (a > 0),
	b text DEFAULT 'hello');
CREATE VIEW addr_nsp.genview AS SELECT * from addr_nsp.gentable;
CREATE MATERIALIZED VIEW addr_nsp.genmatview AS SELECT * FROM addr_nsp.gentable;
CREATE TYPE addr_nsp.gencomptype AS (a int);
CREATE TYPE addr_nsp.genenum AS ENUM ('one', 'two');
CREATE FOREIGN TABLE addr_nsp.genftable (a int) SERVER addr_fserv;
CREATE AGGREGATE addr_nsp.genaggr(int4) (sfunc = int4pl, stype = int4);
CREATE DOMAIN addr_nsp.gendomain AS int4 CONSTRAINT domconstr CHECK (value > 0);
CREATE FUNCTION addr_nsp.trig() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN END; $$;
CREATE TRIGGER t BEFORE INSERT ON addr_nsp.gentable FOR EACH ROW EXECUTE PROCEDURE addr_nsp.trig();
CREATE SERVER "integer" FOREIGN DATA WRAPPER addr_fdw;
CREATE USER MAPPING FOR regress_addr_user SERVER "integer";
ALTER DEFAULT PRIVILEGES FOR ROLE regress_addr_user IN SCHEMA public GRANT ALL ON TABLES TO regress_addr_user;
ALTER DEFAULT PRIVILEGES FOR ROLE regress_addr_user REVOKE DELETE ON TABLES FROM regress_addr_user;
CREATE PUBLICATION addr_pub FOR TABLE addr_nsp.gentable;
CREATE SUBSCRIPTION addr_sub CONNECTION 'host=abc port=12345' publication pub WITH (CONNECT=false);
create function test_event_trigger() returns event_trigger as $$
BEGIN
    RAISE NOTICE 'test_event_trigger: % %', tg_event, tg_tag;
END
$$ language plpgsql;
create event trigger regress_event_trigger on ddl_command_start
   execute procedure test_event_trigger();
-- test some error cases
SELECT * from pg_get_object_address('stone', '{}', '{}');
SELECT * from pg_get_object_address('table', '{}', '{}');
SELECT * from pg_get_object_address('table', '{NULL}', '{}');

-- unrecognized object types
DO $$
DECLARE
	objtype text;
BEGIN
	FOR objtype IN VALUES ('toast table'), ('index column'), ('sequence column'),
		('toast table column'), ('view column'), ('materialized view column')
	LOOP
		BEGIN
			PERFORM pg_get_object_address(objtype, '{one}', '{}');
		EXCEPTION WHEN invalid_parameter_value THEN
			RAISE WARNING 'error for %: %', objtype, sqlerrm;
		END;
	END LOOP;
END;
$$;

-- miscellaneous other errors
select * from pg_get_object_address('operator of access method', '{btree,integer_ops,1}', '{int4,bool}');
select * from pg_get_object_address('operator of access method', '{btree,integer_ops,99}', '{int4,int4}');
select * from pg_get_object_address('function of access method', '{btree,integer_ops,1}', '{int4,bool}');
select * from pg_get_object_address('function of access method', '{btree,integer_ops,99}', '{int4,int4}');

DO $$
DECLARE
	objtype text;
	names	text[];
	args	text[];
BEGIN
	FOR objtype IN VALUES
		('table'), ('index'), ('sequence'), ('view'),
		('materialized view'), ('foreign table'),
		('table column'), ('foreign table column'),
		('aggregate'), ('function'), ('type'), ('cast'),
		('table constraint'), ('domain constraint'), ('conversion'), ('default value'),
		('operator'), ('operator class'), ('operator family'), ('rule'), ('trigger'),
		('text search parser'), ('text search dictionary'),
		('text search template'), ('text search configuration'),
		('user mapping'),
		('operator of access method'), ('function of access method'),
		('publication relation')
	LOOP
		FOR names IN VALUES ('{eins}'), ('{addr_nsp, zwei}'), ('{eins, zwei, drei}')
		LOOP
			FOR args IN VALUES ('{}'), ('{integer}')
			LOOP
				BEGIN
					PERFORM pg_get_object_address(objtype, names, args);
				EXCEPTION WHEN OTHERS THEN
						RAISE WARNING 'error for %,%,%: %', objtype, names, args, sqlerrm;
				END;
			END LOOP;
		END LOOP;
	END LOOP;
END;
$$;

-- these object types cannot be qualified names
SELECT * from pg_get_object_address('language', '{one}', '{}');
SELECT * from pg_get_object_address('language', '{one,two}', '{}');
SELECT * from pg_get_object_address('large object', '{123}', '{}');
SELECT * from pg_get_object_address('large object', '{123,456}', '{}');
SELECT * from pg_get_object_address('large object', '{blargh}', '{}');
SELECT * from pg_get_object_address('schema', '{one}', '{}');
SELECT * from pg_get_object_address('schema', '{one,two}', '{}');
SELECT * from pg_get_object_address('role', '{one}', '{}');
SELECT * from pg_get_object_address('role', '{one,two}', '{}');
SELECT * from pg_get_object_address('database', '{one}', '{}');
SELECT * from pg_get_object_address('database', '{one,two}', '{}');
SELECT * from pg_get_object_address('tablespace', '{one}', '{}');
SELECT * from pg_get_object_address('tablespace', '{one,two}', '{}');
SELECT * from pg_get_object_address('foreign-data wrapper', '{one}', '{}');
SELECT * from pg_get_object_address('foreign-data wrapper', '{one,two}', '{}');
SELECT * from pg_get_object_address('server', '{one}', '{}');
SELECT * from pg_get_object_address('server', '{one,two}', '{}');
SELECT * from pg_get_object_address('extension', '{one}', '{}');
SELECT * from pg_get_object_address('extension', '{one,two}', '{}');
SELECT * from pg_get_object_address('event trigger', '{one}', '{}');
SELECT * from pg_get_object_address('event trigger', '{one,two}', '{}');
SELECT * from pg_get_object_address('publication', '{one}', '{}');
SELECT * from pg_get_object_address('publication', '{one,two}', '{}');
SELECT * from pg_get_object_address('subscription', '{one}', '{}');
SELECT * from pg_get_object_address('subscription', '{one,two}', '{}');

-- test uncovered object class
SELECT (pg_identify_object(oid, 0, 0)).* FROM (select oid from pg_class where relname='pg_rlspolicy');
SELECT (pg_identify_object(oid, 0, 0)).* FROM (select oid from pg_class where relname='pg_constraint');
SELECT (pg_identify_object(oid, 0, 0)).* FROM (select oid from pg_class where relname='pg_largeobject');
SELECT (pg_identify_object(oid, 0, 0)).* FROM (select oid from pg_class where relname='pg_opclass');
SELECT (pg_identify_object(oid, 0, 0)).* FROM (select oid from pg_class where relname='pg_ts_parser');
SELECT (pg_identify_object(oid, 0, 0)).* FROM (select oid from pg_class where relname='pg_ts_template');
SELECT (pg_identify_object(oid, 0, 0)).* FROM (select oid from pg_class where relname='pg_ts_template');
SELECT (pg_identify_object(oid, 0, 0)).* FROM (select oid from pg_class where relname='pg_default_acl');
/*unsupported object class*/
SELECT (pg_identify_object(oid, 0, 0)).* FROM (select oid from pg_class where relname='pg_job');
-- test wrong oid cases
SELECT (pg_identify_object(classid, 0, subobjid)).* FROM pg_get_object_address('table', '{addr_nsp, gentable}'::text[], '{}'::text[]);
SELECT (pg_identify_object(classid, 0, subobjid)).* FROM pg_get_object_address('index', '{addr_nsp, gentable_pkey}', '{}');
SELECT (pg_identify_object(classid, 0, subobjid)).* FROM pg_get_object_address('sequence', '{addr_nsp, gentable_a_seq}', '{}');
SELECT (pg_identify_object(classid, 0, subobjid)).* FROM pg_get_object_address('view', '{addr_nsp, genview}', '{}');
SELECT (pg_identify_object(classid, 0, subobjid)).* FROM pg_get_object_address('materialized view', '{addr_nsp, genmatview}', '{}');
SELECT (pg_identify_object(classid, 0, subobjid)).* FROM pg_get_object_address('foreign table', '{addr_nsp, genftable}', '{}');
SELECT (pg_identify_object(classid, 0, subobjid)).* FROM pg_get_object_address('table column', '{addr_nsp, gentable, b}', '{}');
SELECT (pg_identify_object(classid, 0, subobjid)).* FROM pg_get_object_address('aggregate', '{addr_nsp, genaggr}', '{int4}');
SELECT (pg_identify_object(classid, 0, subobjid)).* FROM pg_get_object_address('foreign table column', '{addr_nsp, genftable, a}', '{}');
SELECT (pg_identify_object(classid, 0, subobjid)).* FROM pg_get_object_address('function', '{pg_catalog, pg_identify_object}', '{pg_catalog.oid, pg_catalog.oid, int4}');
SELECT (pg_identify_object(classid, 0, subobjid)).* FROM pg_get_object_address('type', '{addr_nsp.gendomain}', '{}');
SELECT (pg_identify_object(classid, 0, subobjid)).* FROM pg_get_object_address('cast', '{int8}', '{int4}');
SELECT (pg_identify_object(classid, 0, subobjid)).* FROM pg_get_object_address('collation', '{default}', '{}');
SELECT (pg_identify_object(classid, 0, subobjid)).* FROM pg_get_object_address('table constraint', '{addr_nsp, gentable, a_chk}', '{}');
SELECT (pg_identify_object(classid, 0, subobjid)).* FROM pg_get_object_address('domain constraint', '{addr_nsp.gendomain}', '{domconstr}');
SELECT (pg_identify_object(classid, 0, subobjid)).* FROM pg_get_object_address('conversion', '{pg_catalog, ascii_to_mic}', '{}');
SELECT (pg_identify_object(classid, 0, subobjid)).* FROM pg_get_object_address('default value', '{addr_nsp, gentable, b}', '{}');
SELECT (pg_identify_object(classid, 0, subobjid)).* FROM pg_get_object_address('language', '{plpgsql}', '{}');
SELECT (pg_identify_object(classid, 0, subobjid)).* FROM pg_get_object_address('operator class', '{btree, int4_ops}', '{}');
SELECT (pg_identify_object(classid, 0, subobjid)).* FROM pg_get_object_address('operator family', '{btree, integer_ops}', '{}');
SELECT (pg_identify_object(classid, 0, subobjid)).* FROM pg_get_object_address('operator of access method', '{btree,integer_ops,1}', '{integer,integer}');
SELECT (pg_identify_object(classid, 0, subobjid)).* FROM pg_get_object_address('function of access method', '{btree,integer_ops,2}', '{integer,integer}');
SELECT (pg_identify_object(classid, 0, subobjid)).* FROM pg_get_object_address('rule', '{addr_nsp, genview, _RETURN}', '{}');
SELECT (pg_identify_object(classid, 0, subobjid)).* FROM pg_get_object_address('trigger', '{addr_nsp, gentable, t}', '{}');
SELECT (pg_identify_object(classid, 0, subobjid)).* FROM pg_get_object_address('schema', '{addr_nsp}', '{}');
SELECT (pg_identify_object(classid, 0, subobjid)).* FROM pg_get_object_address('text search dictionary', '{addr_ts_dict}', '{}');
SELECT (pg_identify_object(classid, 0, subobjid)).* FROM pg_get_object_address('text search configuration', '{addr_ts_conf}', '{}');
SELECT (pg_identify_object(classid, 0, subobjid)).* FROM pg_get_object_address('role', '{regress_addr_user}', '{}');
SELECT (pg_identify_object(classid, 0, subobjid)).* FROM pg_get_object_address('database', '{test_obj_address}', '{}');
SELECT (pg_identify_object(classid, 0, subobjid)).* FROM pg_get_object_address('tablespace', '{pg_default}', '{}');
SELECT (pg_identify_object(classid, 0, subobjid)).* FROM pg_get_object_address('foreign-data wrapper', '{addr_fdw}', '{}');
SELECT (pg_identify_object(classid, 0, subobjid)).* FROM pg_get_object_address('server', '{addr_fserv}', '{}');
SELECT (pg_identify_object(classid, 0, subobjid)).* FROM pg_get_object_address('user mapping', '{regress_addr_user}', '{integer}');
SELECT (pg_identify_object(classid, 0, subobjid)).* FROM pg_get_object_address('extension', '{hstore}', '{}');
SELECT (pg_identify_object(classid, 0, subobjid)).* FROM pg_get_object_address('event trigger', '{regress_event_trigger}', '{}');
SELECT (pg_identify_object(classid, 0, subobjid)).* FROM pg_get_object_address('publication', '{addr_pub}', '{}');
SELECT (pg_identify_object(classid, 0, subobjid)).* FROM pg_get_object_address('publication relation', '{addr_nsp, gentable}', '{addr_pub}');
SELECT (pg_identify_object(classid, 0, subobjid)).* FROM pg_get_object_address('subscription', '{addr_sub}', '{}');

-- test successful cases
WITH objects (type, name, args) AS (VALUES
				('table', '{addr_nsp, gentable}'::text[], '{}'::text[]),
				('index', '{addr_nsp, gentable_pkey}', '{}'),
				('sequence', '{addr_nsp, gentable_a_seq}', '{}'),
				-- toast table
				('view', '{addr_nsp, genview}', '{}'),
				('materialized view', '{addr_nsp, genmatview}', '{}'),
				('foreign table', '{addr_nsp, genftable}', '{}'),
				('table column', '{addr_nsp, gentable, b}', '{}'),
				('foreign table column', '{addr_nsp, genftable, a}', '{}'),
				('aggregate', '{addr_nsp, genaggr}', '{int4}'),
				('function', '{pg_catalog, pg_identify_object}', '{pg_catalog.oid, pg_catalog.oid, int4}'),
				('type', '{pg_catalog._int4}', '{}'),
				('type', '{addr_nsp.gendomain}', '{}'),
				('type', '{addr_nsp.gencomptype}', '{}'),
				('type', '{addr_nsp.genenum}', '{}'),
				('cast', '{int8}', '{int4}'),
				('collation', '{default}', '{}'),
				('table constraint', '{addr_nsp, gentable, a_chk}', '{}'),
				('domain constraint', '{addr_nsp.gendomain}', '{domconstr}'),
				('conversion', '{pg_catalog, ascii_to_mic}', '{}'),
				('default value', '{addr_nsp, gentable, b}', '{}'),
				('language', '{plpgsql}', '{}'),
				-- large object
				('operator', '{+}', '{int4, int4}'),
				('operator class', '{btree, int4_ops}', '{}'),
				('operator family', '{btree, integer_ops}', '{}'),
				('operator of access method', '{btree,integer_ops,1}', '{integer,integer}'),
				('function of access method', '{btree,integer_ops,2}', '{integer,integer}'),
				('rule', '{addr_nsp, genview, _RETURN}', '{}'),
				('trigger', '{addr_nsp, gentable, t}', '{}'),
				('schema', '{addr_nsp}', '{}'),
				('text search dictionary', '{addr_ts_dict}', '{}'),
				('text search configuration', '{addr_ts_conf}', '{}'),
				('role', '{regress_addr_user}', '{}'),
				('database', '{test_obj_address}', '{}'),
				('tablespace', '{pg_default}', '{}'),
				('foreign-data wrapper', '{addr_fdw}', '{}'),
				('server', '{addr_fserv}', '{}'),
				('user mapping', '{regress_addr_user}', '{integer}'),
				('extension', '{hstore}', '{}'),
				('event trigger', '{regress_event_trigger}', '{}'),
				('publication', '{addr_pub}', '{}'),
				('publication relation', '{addr_nsp, gentable}', '{addr_pub}'),
				('subscription', '{addr_sub}', '{}')
        )
SELECT (pg_identify_object(addr1.classid, addr1.objid, addr1.subobjid)).*
	  FROM (SELECT (pg_get_object_address(type, name, args)).* FROM objects) AS addr1
	ORDER BY addr1.classid, addr1.objid, addr1.subobjid;

	
---
--- Cleanup resources
---
\set VERBOSITY terse \\ -- suppress cascade details
DROP EVENT TRIGGER regress_event_trigger;
DROP FOREIGN DATA WRAPPER addr_fdw CASCADE;
DROP PUBLICATION addr_pub;
DROP SUBSCRIPTION addr_sub;

DROP SCHEMA addr_nsp CASCADE;

DROP OWNED BY regress_addr_user;
DROP USER regress_addr_user;
\c regression
drop database test_obj_address;