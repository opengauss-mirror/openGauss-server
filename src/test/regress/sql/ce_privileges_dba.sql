\! gs_ktool -d all
\! gs_ktool -g

DROP CLIENT MASTER KEY IF EXISTS MyCMK CASCADE;

-- CHECK SCHEMA PRIVILEGES
SELECT has_schema_privilege(session_user, current_schema, 'CREATE');
SELECT has_schema_privilege(session_user, current_schema, 'USAGE');

-- CHECK CMK PRIVILEGES
CREATE CLIENT MASTER KEY MyCMK WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
SELECT count(session_user) from pg_roles join gs_client_global_keys on pg_roles.Oid = gs_client_global_keys.key_owner where gs_client_global_keys.global_key_name = 'mycmk';
SELECT has_cmk_privilege(session_user, 'MyCMK', 'USAGE');

-- CHECK CEK PRIVILEGES
CREATE COLUMN ENCRYPTION KEY MyCEK WITH VALUES (CLIENT_MASTER_KEY = MyCMK, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);
SELECT count(session_user) from pg_roles join gs_column_keys on pg_roles.Oid = gs_column_keys.key_owner where gs_column_keys.column_key_name = 'mycek';
SELECT has_cek_privilege(session_user, 'MyCEK', 'USAGE');

DROP TABLE IF EXISTS acltest1;
CREATE TABLE acltest1 (x int, x2 varchar(50) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC));
DROP TABLE acltest1;

CREATE USER user_check PASSWORD '1234567i*';
SELECT has_cmk_privilege('user_check','mycmk','USAGE');
SELECT has_cmk_privilege('mycmk','USAGE');
SELECT has_cmk_privilege('user_check',(select oid from gs_client_global_keys where global_key_name='mycmk'),'USAGE');
SELECT has_cmk_privilege((select oid from gs_client_global_keys where global_key_name='mycmk'),'USAGE');
SELECT has_cmk_privilege((select oid from pg_authid where rolname='user_check'),'mycmk','USAGE');
SELECT has_cmk_privilege((select oid from pg_authid where rolname='user_check'),(select oid from gs_client_global_keys where global_key_name='mycmk'),'USAGE');

SELECT has_cek_privilege('user_check','mycek','USAGE');
SELECT has_cek_privilege('mycek','USAGE');
SELECT has_cek_privilege('user_check',(select oid from gs_column_keys where column_key_name='mycek'),'USAGE');
SELECT has_cek_privilege((select oid from gs_column_keys where column_key_name='mycek'),'USAGE');
SELECT has_cek_privilege((select oid from pg_authid where rolname='user_check'),'mycek','USAGE');
SELECT has_cek_privilege((select oid from pg_authid where rolname='user_check'),(select oid from gs_column_keys where column_key_name='mycek'),'USAGE');

GRANT ALL ON CLIENT_MASTER_KEY mycmk to user_check;
GRANT ALL ON COLUMN_ENCRYPTION_KEY mycek to user_check;
DROP OWNED BY user_check CASCADE;
DROP USER user_check;

DROP CLIENT MASTER KEY IF EXISTS MyCMK CASCADE;

\! gs_ktool -d all