-- audit query
DROP TABLE IF EXISTS t_audit_type;
CREATE TABLE t_audit_type(id INTEGER, content text[]);
INSERT INTO t_audit_type VALUES (1, array['login_success', 'login_failed', 'user_logout', 'system_start',
    'system_stop', 'system_recover', 'system_switch', 'lock_user',
    'unlock_user', 'grant_role', 'revoke_role', 'user_violation',
    'ddl_database', 'ddl_directory', 'ddl_tablespace', 'ddl_schema',
    'ddl_user', 'ddl_table', 'ddl_index', 'ddl_view',
    'ddl_trigger', 'ddl_function', 'ddl_resourcepool', 'ddl_workload',
    'ddl_serverforhadoop', 'ddl_datasource', 'ddl_nodegroup', 'ddl_rowlevelsecurity',
    'ddl_synonym', 'ddl_type', 'ddl_textsearch', 'dml_action',
    'dml_action_select', 'internal_event', 'function_exec', 'copy_to',
    'copy_from', 'set_parameter', 'audit_policy', 'masking_policy',
    'security_policy', 'ddl_sequence', 'ddl_key', 'ddl_package',
    'ddl_model', 'ddl_globalconfig', 'ddl_publication_subscription', 'ddl_foreign_data_wrapper',
    'ddl_sql_patch']);
INSERT INTO t_audit_type VALUES (2, array['user_violation', 'login_failed']);
INSERT INTO t_audit_type VALUES (3, array['system_start', 'system_stop', 'system_switch', 'system_recover',
    'ddl_directory', 'ddl_globalconfig', 'ddl_foreign_data_wrapper', 'copy_to', 'copy_from',
    'ddl_publication_subscription', 'internal_event']);
 
DROP FUNCTION IF EXISTS func_count_audit;
CREATE OR REPLACE FUNCTION func_count_audit(anyarray text[], audit_user text, size int)
RETURNS table (type_audit text, is_audit BOOL)
AS $$
DECLARE 
x int;
BEGIN
DROP TABLE IF EXISTS t_result;
CREATE TABLE t_result (type_audit text, is_audit BOOL);
FORALL x in 1..size
INSERT INTO t_result SELECT type, (SELECT count(*) > 0) AS is_audit FROM pg_query_audit(current_date,current_date + interval '24 hours') WHERE username = audit_user and type = anyarray[x] GROUP BY type;
RETURN query select * from t_result order by type_audit;
end;
$$ language plpgsql;