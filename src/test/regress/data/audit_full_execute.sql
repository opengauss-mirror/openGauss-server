-- DDL
-- ddl_database
DROP DATABASE IF EXISTS db_audit;
CREATE DATABASE db_audit OWNER user1;
-- ddl_tablespace
CREATE TABLESPACE ds_location1 RELATIVE LOCATION 'test_tablespace/test_tablespace_1';
-- ddl_schema
DROP SCHEMA IF EXISTS audit;
CREATE SCHEMA audit;
DROP TABLE IF EXISTS audit.t_audit;
CREATE TABLE audit.t_audit (id INTEGER, col1 VARCHAR(20));
--ddl_user
DROP USER IF EXISTS user_audit_test CASECADE;
CREATE USER user_audit_test identified by 'test@2023';
-- ddl_table
DROP TABLE IF EXISTS t_audit;
CREATE TABLE t_audit (id INTEGER, col1 VARCHAR(20), col2 INTEGER, col3 INTEGER);
DO $$DECLARE i record; 
BEGIN 
    FOR i IN 1..100 
    LOOP 
    execute 'INSERT INTO t_audit VALUES (' || i || ', ''audit'', ' || i+1 || ', ' || i+2 || ');';
    END LOOP;
END$$;
-- ddl_index
DROP INDEX IF EXISTS index1;
CREATE UNIQUE INDEX index1 ON audit.t_audit(id);
-- ddl_view
DROP VIEW IF EXISTS view1;
CREATE VIEW view1 AS SELECT * FROM t_audit;
-- ddl_trigger
CREATE OR REPLACE FUNCTION audit_trigger_func() RETURNS TRIGGER AS
    $$
    DECLARE
    BEGIN
        INSERT INTO audit.t_audit VALUES(NEW.id, NEW.col1);
        RETURN NEW;
    END
    $$ LANGUAGE PLPGSQL;
 
CREATE TRIGGER audit_trigger
    BEFORE INSERT ON t_audit
    FOR EACH ROW
    EXECUTE PROCEDURE audit_trigger_func();
-- ddl_function
DROP FUNCTION IF EXISTS func_sql;
CREATE FUNCTION func_sql(integer, integer) RETURNS integer
AS 'select $1 + $2;'
LANGUAGE SQL
IMMUTABLE
RETURNS NULL ON NULL INPUT;
-- ddl_resourcepool distributed
-- ddl_workload distributed
-- ddl_foreign_data_wrapper opengauss
-- ddl_serverforhadoop
create server server_audit foreign data wrapper log_fdw;
-- ddl_datasource
DROP DATA SOURCE IF EXISTS ds_audit;
CREATE DATA SOURCE ds_audit;
-- ddl_nodegroup distributed
-- ddl_rowlevelsecurity
DROP ROW LEVEL SECURITY POLICY IF EXISTS rls_audit ON t_audit;
CREATE ROW LEVEL SECURITY POLICY rls_audit ON t_audit USING(id = 0);
-- ddl_synonym
DROP SYNONYM IF EXISTS s_audit;
CREATE OR REPLACE SYNONYM s_audit FOR t_audit;
-- ddl_type
DROP TYPE IF EXISTS tp_audit;
CREATE TYPE tp_audit AS (col1 int, col2 text);
-- ddl_textsearch
DROP TEXT SEARCH CONFIGURATION IF EXISTS ts_audit;
CREATE TEXT SEARCH CONFIGURATION ts_audit (parser=ngram) WITH (gram_size = 2, grapsymbol_ignore = false);
-- ddl_sequence
DROP SEQUENCE IF EXISTS sq_audit CASCADE;
CREATE SEQUENCE sq_audit
START 101
CACHE 20
OWNED BY t_audit.id;
-- ddl_key
\! gs_ktool -d all
DROP COLUMN ENCRYPTION KEY IF EXISTS cek1;
DROP CLIENT MASTER KEY IF EXISTS cmk1;
\! gs_ktool -g
CREATE CLIENT MASTER KEY cmk1 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_GCM);
CREATE COLUMN ENCRYPTION KEY cek1 WITH VALUES (CLIENT_MASTER_KEY = cmk1, ALGORITHM = AES_256_GCM);
-- ddl_package
CREATE OR REPLACE PROCEDURE pro_test()
as
begin
    select count(*) from t_audit;
end;
/
CREATE OR REPLACE PACKAGE pkg_audit IS
var1 int:=1;
var2 int:=2;
PROCEDURE pro_test();
END pkg_audit;
/
-- ddl_model
CREATE MODEL m_audit USING linear_regression
FEATURES id, col2
TARGET col3
FROM t_audit
WITH learning_rate=0.88, max_iterations=default;
-- ddl_sql_patch
select * from dbe_sql_util.drop_sql_patch('patch_audit');
select * from dbe_sql_util.create_hint_sql_patch('patch_audit', 2578396627, 'indexscan(t_audit)');
-- audit_policy unified audit
DROP AUDIT POLICY IF EXISTS pol_audit;
CREATE AUDIT POLICY pol_audit PRIVILEGES CREATE;
-- masking_policy unified audit
DROP RESOURCE LABEL IF EXISTS mask_lb1;
CREATE RESOURCE LABEL mask_lb1 ADD COLUMN(t_audit.col1);
DROP MASKING POLICY IF EXISTS msk_audit;
CREATE MASKING POLICY msk_audit maskall ON LABEL(mask_lb1);
-- security_policy unified audit
DROP ROW LEVEL SECURITY POLICY IF EXISTS sec_audit ON t_audit;
CREATE ROW LEVEL SECURITY POLICY sec_audit ON t_audit USING(id = 1);
 
-- DML
-- dml_action
DO $$DECLARE i record; 
BEGIN 
    FOR i IN 1..100 
    LOOP 
    execute 'INSERT INTO t_audit VALUES (' || i || ', ''audit'', ' || i+1 || ', ' || i+2 || ');';
    END LOOP;
END$$;
-- dml_action_select
select count(*) from t_audit;
 
-- 用户锁定和解锁审计 audit_user_locked
-- lock_user
ALTER USER user_audit_test ACCOUNT LOCK;
-- unlock_user
ALTER USER user_audit_test ACCOUNT UNLOCK;
 
-- 授权和回收权限审计 audit_grant_revoke
-- grant_role
GRANT ALL PRIVILEGES ON TABLE t_audit TO user_audit_test;
-- revoke_role
REVOKE INSERT ON TABLE t_audit FROM user_audit_test;
 
-- 存储过程和自定义函数的执行审计 audit_function_exec
-- function_exec
CREATE OR REPLACE FUNCTION func_plpgsql(i integer) RETURNS integer AS $$
    BEGIN
        RETURN i + 1;
    END;
$$ LANGUAGE plpgsql;
select func_plpgsql(1);
 
-- SET审计 audit_set_parameter
-- set_parameter
SET datestyle TO postgres,dmy;
 
-- create audit file, always record
-- internal_event

--delete user
DROP DATABASE IF EXISTS db_audit;
DROP USER IF EXISTS user_audit_test CASCADE;
DROP COLUMN ENCRYPTION KEY IF EXISTS cek1;
DROP CLIENT MASTER KEY IF EXISTS cmk1;
\! gs_ktool -d all