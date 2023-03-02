-- Clean up in case a prior regression run failed
create database test_obj_rename dbcompatibility 'PG';
\c test_obj_rename;
SET client_min_messages TO 'warning';
DROP ROLE IF EXISTS regress_addr_user;
RESET client_min_messages;
CREATE USER regress_addr_user password 'Gaussdba@Mpp';
reset role;
CREATE OPERATOR CLASS gin_trgm_ops
FOR TYPE text USING gin
AS
        STORAGE         int4;
-- Test generic object addressing/identification functions
CREATE FOREIGN DATA WRAPPER addr_fdw;
CREATE SERVER addr_fserv FOREIGN DATA WRAPPER addr_fdw;
CREATE TEXT SEARCH DICTIONARY addr_ts_dict (template=simple);
CREATE TEXT SEARCH CONFIGURATION addr_ts_conf (copy=english);
CREATE AGGREGATE genaggr(int4) (sfunc = int4pl, stype = int4);
CREATE function test_event_trigger() returns event_trigger as $$
BEGIN
    RAISE NOTICE 'test_event_trigger: % %', tg_event, tg_tag;
END
$$ language plpgsql;
create event trigger regress_event_trigger on ddl_command_start
   execute procedure test_event_trigger();
create conversion ascii_to_utf8 for 'sql_ascii' to 'utf8' from ascii_to_utf8;

SET ROLE regress_addr_user PASSWORD 'Gaussdba@Mpp';
-- test alter table rename while permission checks are needed. All of these should failed.
ALTER AGGREGATE genaggr(int4) RENAME TO genaggr_rename;
ALTER COLLATION POSIX RENAME TO POSIX_rename;
ALTER CONVERSION ascii_to_utf8 RENAME TO ascii_to_utf8_rename;
ALTER EVENT TRIGGER regress_event_trigger RENAME TO regress_event_trigger_rename;
ALTER FOREIGN DATA WRAPPER addr_fdw RENAME TO addr_fdw_rename;
ALTER SERVER addr_fserv RENAME TO addr_fserv_rename;
ALTER OPERATOR CLASS int4_ops USING btree RENAME TO int4_ops_rename;
ALTER OPERATOR FAMILY integer_ops USING btree RENAME TO integer_ops_rename;
ALTER TEXT SEARCH DICTIONARY addr_ts_dict RENAME TO addr_ts_dict_rename;
ALTER TEXT SEARCH CONFIGURATION addr_ts_conf RENAME TO addr_ts_conf_rename;
reset role;

-- create name_rename object
CREATE FOREIGN DATA WRAPPER addr_fdw_rename;
CREATE SERVER addr_fserv_rename FOREIGN DATA WRAPPER addr_fdw;
CREATE TEXT SEARCH DICTIONARY addr_ts_dict_rename (template=simple);
CREATE TEXT SEARCH CONFIGURATION addr_ts_conf_rename (copy=english);
CREATE AGGREGATE genaggr_rename(int4) (sfunc = int4pl, stype = int4);
create event trigger regress_event_trigger_rename on ddl_command_start
   execute procedure test_event_trigger();
create conversion ascii_to_utf8_rename for 'sql_ascii' to 'utf8' from ascii_to_utf8;

-- test duplicate name , should failed.
ALTER AGGREGATE genaggr(int4) RENAME TO genaggr_rename;
ALTER COLLATION POSIX RENAME TO POSIX_rename;
ALTER CONVERSION ascii_to_utf8 RENAME TO ascii_to_utf8_rename;
ALTER EVENT TRIGGER regress_event_trigger RENAME TO regress_event_trigger_rename;
ALTER FOREIGN DATA WRAPPER addr_fdw RENAME TO addr_fdw_rename;
ALTER SERVER addr_fserv RENAME TO addr_fserv_rename;
ALTER OPERATOR CLASS int4_ops USING btree RENAME TO int4_ops_rename;
ALTER OPERATOR CLASS int4_ops_rename USING btree RENAME TO int4_ops;
ALTER OPERATOR FAMILY integer_ops USING btree RENAME TO integer_ops_rename;
ALTER OPERATOR FAMILY integer_ops_rename USING btree RENAME TO integer_ops;
ALTER TEXT SEARCH DICTIONARY addr_ts_dict RENAME TO addr_ts_dict_rename;
ALTER TEXT SEARCH CONFIGURATION addr_ts_conf RENAME TO addr_ts_conf_rename;

-- test alter object namespace
reset role;
create schema test_sch;
GRANT ALL ON SCHEMA public TO regress_addr_user;
GRANT ALL ON SCHEMA test_sch TO regress_addr_user;
SET ROLE regress_addr_user PASSWORD 'Gaussdba@Mpp';
ALTER AGGREGATE genaggr(int4) SET schema public;
-- should failed, not owner.
ALTER AGGREGATE genaggr(int4) SET schema test_sch;

reset role;
ALTER AGGREGATE genaggr(int4) owner to regress_addr_user;
SET ROLE regress_addr_user PASSWORD 'Gaussdba@Mpp';
ALTER AGGREGATE genaggr(int4) SET schema test_sch;
ALTER AGGREGATE test_sch.genaggr(int4) SET schema public;
reset role;
ALTER TEXT SEARCH DICTIONARY addr_ts_dict_rename SET schema test_sch;
ALTER TEXT SEARCH DICTIONARY test_sch.addr_ts_dict_rename SET schema public;
ALTER AGGREGATE genaggr(int4) owner to regress_addr_user;
ALTER OPERATOR CLASS gin_trgm_ops USING gin owner to regress_addr_user;
--test alter database
CREATE DATABASE test_db1;
ALTER DATABASE test_db1 RENAME TO test_db2;
ALTER DATABASE test_db2 RENAME TO test_db1;
ALTER DATABASE test_db1 owner TO regress_addr_user;
DROP DATABASE test_db1;

\set VERBOSITY terse \\ -- suppress cascade details
DROP EVENT TRIGGER regress_event_trigger;
DROP FOREIGN DATA WRAPPER addr_fdw CASCADE;
DROP SCHEMA test_sch cascade;
DROP OWNED BY regress_addr_user;
DROP USER regress_addr_user CASCADE;
\c regression;
drop database test_obj_rename;