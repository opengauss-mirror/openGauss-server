DROP SCHEMA IF EXISTS sqladvisor cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_NAMESPACE, 7813;
CREATE SCHEMA sqladvisor;
COMMENT ON schema sqladvisor IS 'sqladvisor schema';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7800;
CREATE OR REPLACE FUNCTION sqladvisor.init("char", boolean, boolean DEFAULT false, boolean DEFAULT true, integer DEFAULT 10000, integer DEFAULT 1024)
 RETURNS boolean
 LANGUAGE internal
 NOT FENCED NOT SHIPPABLE
AS 'init';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7801;
CREATE OR REPLACE FUNCTION sqladvisor.analyze_query(text, integer DEFAULT 1)
 RETURNS boolean
 LANGUAGE internal
 STRICT NOT FENCED NOT SHIPPABLE
AS 'analyze_query';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7802;
CREATE OR REPLACE FUNCTION sqladvisor.get_analyzed_result(OUT schema_name text, OUT table_name text, OUT col_name text, OUT operator text, OUT count integer)
 RETURNS SETOF record
 LANGUAGE internal
 NOT FENCED NOT SHIPPABLE ROWS 100
AS 'get_analyzed_result';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7803;
CREATE OR REPLACE FUNCTION sqladvisor.run()
 RETURNS boolean
 LANGUAGE internal
 STRICT NOT FENCED NOT SHIPPABLE
AS 'run';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7804;
CREATE OR REPLACE FUNCTION sqladvisor.clean()
 RETURNS boolean
 LANGUAGE internal
 STRICT NOT FENCED NOT SHIPPABLE
AS 'clean';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7805;
CREATE OR REPLACE FUNCTION sqladvisor.assign_table_type(text)
 RETURNS boolean
 LANGUAGE internal
 STRICT NOT FENCED NOT SHIPPABLE
AS 'assign_table_type';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7806;
CREATE OR REPLACE FUNCTION sqladvisor.clean_workload()
 RETURNS boolean
 LANGUAGE internal
 STRICT NOT FENCED NOT SHIPPABLE
AS 'clean_workload';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7807;
CREATE OR REPLACE FUNCTION sqladvisor.get_distribution_key(OUT db_name text, OUT schema_name text, OUT table_name text, OUT distribution_type text, OUT distribution_key text, OUT start_time timestamp with time zone, OUT end_time timestamp with time zone, OUT cost_improve text, OUT comment text)
 RETURNS SETOF record
 LANGUAGE internal
 NOT FENCED NOT SHIPPABLE ROWS 100
AS 'get_distribution_key';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7808;
CREATE OR REPLACE FUNCTION sqladvisor.set_weight_params(real, real, real)
 RETURNS boolean
 LANGUAGE internal
 STRICT NOT FENCED NOT SHIPPABLE
AS 'set_weight_params';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7809;
CREATE OR REPLACE FUNCTION sqladvisor.set_cost_params(bigint, boolean, text)
 RETURNS boolean
 LANGUAGE internal
 STRICT NOT FENCED NOT SHIPPABLE
AS 'set_cost_params';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7810;
CREATE OR REPLACE FUNCTION sqladvisor.start_collect_workload(integer DEFAULT 10000, integer DEFAULT 1024)
 RETURNS boolean
 LANGUAGE internal
 STRICT NOT FENCED NOT SHIPPABLE
AS 'start_collect_workload';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7811;
CREATE OR REPLACE FUNCTION sqladvisor.end_collect_workload()
 RETURNS boolean
 LANGUAGE internal
 STRICT NOT FENCED NOT SHIPPABLE
AS 'end_collect_workload';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7812;
CREATE OR REPLACE FUNCTION sqladvisor.analyze_workload()
 RETURNS boolean
 LANGUAGE internal
 STRICT NOT FENCED NOT SHIPPABLE
AS 'analyze_workload';

GRANT USAGE ON SCHEMA sqladvisor TO PUBLIC;
