DROP SCHEMA IF EXISTS dbe_pldebugger cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_NAMESPACE, 4992;
CREATE SCHEMA dbe_pldebugger;
COMMENT ON schema dbe_pldebugger IS 'dbe_pldebugger schema';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1503;
CREATE OR REPLACE FUNCTION dbe_pldebugger.abort()
RETURNS boolean
 LANGUAGE internal
 STABLE STRICT LEAKPROOF NOT FENCED NOT SHIPPABLE
 as 'debug_client_abort';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1504;
CREATE OR REPLACE FUNCTION dbe_pldebugger.attach(text, integer)
 RETURNS boolean
 LANGUAGE internal
 STABLE STRICT NOT FENCED NOT SHIPPABLE
AS 'debug_client_attatch';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1502;
CREATE OR REPLACE FUNCTION dbe_pldebugger.next(OUT lineno integer, OUT query text)
 RETURNS SETOF record
 LANGUAGE internal
 STABLE STRICT NOT FENCED NOT SHIPPABLE ROWS 1
AS 'debug_client_next';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1505;
CREATE OR REPLACE FUNCTION dbe_pldebugger.info_locals(OUT varname text, OUT vartype text, OUT value text, OUT package_name text)
 RETURNS SETOF record
 LANGUAGE internal
 STABLE STRICT NOT FENCED NOT SHIPPABLE ROWS 100
AS 'debug_client_local_variables';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1506;
CREATE OR REPLACE FUNCTION dbe_pldebugger.continue()
 RETURNS boolean
 LANGUAGE internal
 STABLE STRICT LEAKPROOF NOT FENCED NOT SHIPPABLE
AS 'debug_client_continue';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1501;
CREATE OR REPLACE FUNCTION dbe_pldebugger.turn_off(oid)
 RETURNS boolean
 LANGUAGE internal
 STABLE STRICT NOT FENCED NOT SHIPPABLE
AS 'debug_server_turn_off';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1500;
CREATE OR REPLACE FUNCTION dbe_pldebugger.turn_on(func_oid oid, OUT nodename text, OUT port integer)
 RETURNS SETOF record
 LANGUAGE internal
 STABLE STRICT NOT FENCED NOT SHIPPABLE ROWS 1
AS 'debug_server_turn_on';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1508;
CREATE OR REPLACE FUNCTION dbe_pldebugger.delete_breakpoint(integer)
 RETURNS boolean
 LANGUAGE internal
 STABLE STRICT NOT FENCED NOT SHIPPABLE
AS 'debug_client_delete_breakpoint';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1507;
CREATE OR REPLACE FUNCTION dbe_pldebugger.add_breakpoint(lineno integer, OUT breakpointno integer)
 RETURNS integer
 LANGUAGE internal
 STABLE STRICT NOT FENCED NOT SHIPPABLE
AS 'debug_client_add_breakpoint';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1509;
CREATE OR REPLACE FUNCTION dbe_pldebugger.info_breakpoints(OUT breakpointno integer, OUT lineno integer, OUT query text)
 RETURNS SETOF record
 LANGUAGE internal
 STABLE STRICT NOT FENCED NOT SHIPPABLE ROWS 100
AS 'debug_client_info_breakpoints';


SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1511;
CREATE OR REPLACE FUNCTION dbe_pldebugger.info_code(OUT lineno integer, OUT query text)
 RETURNS SETOF record
 LANGUAGE internal
 STABLE STRICT NOT FENCED NOT SHIPPABLE ROWS 100
AS 'debug_client_info_code';



SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1510;
CREATE OR REPLACE FUNCTION dbe_pldebugger.backtrace(OUT frameno integer, OUT func_name text)
 RETURNS SETOF record
 LANGUAGE internal
 STABLE STRICT NOT FENCED NOT SHIPPABLE ROWS 100
AS 'debug_client_backtrace';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1512;
CREATE OR REPLACE FUNCTION dbe_pldebugger.step(OUT func_oid oid, OUT funcname text, OUT lineno integer, OUT query text)
 RETURNS SETOF record
 LANGUAGE internal
 STABLE STRICT NOT FENCED NOT SHIPPABLE ROWS 1
AS 'debug_client_info_step';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1513;
CREATE OR REPLACE FUNCTION dbe_pldebugger.local_debug_server_info(OUT nodename text, OUT port int8, OUT funcoid oid)
 RETURNS SETOF record
 LANGUAGE internal
 STABLE STRICT NOT FENCED NOT SHIPPABLE ROWS 10
AS 'local_debug_server_info';

