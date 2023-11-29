\! gs_ktool -d all
\! gs_ktool -g

DROP CLIENT MASTER KEY IF EXISTS rlspolicy_cmk CASCADE;
CREATE CLIENT MASTER KEY rlspolicy_cmk WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY rlspolicy_cek WITH VALUES (CLIENT_MASTER_KEY = rlspolicy_cmk, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);
CREATE ROLE alice PASSWORD 'Gauss@123';
CREATE ROLE bob PASSWORD 'Gauss@123';
CREATE TABLE all_data(id int, role varchar(100) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = rlspolicy_cek, ENCRYPTION_TYPE = DETERMINISTIC), data varchar(100));
INSERT INTO all_data VALUES(1, 'alice', 'alice data');
INSERT INTO all_data VALUES(2, 'bob', 'bob data');
INSERT INTO all_data VALUES(3, 'peter', 'peter data');
GRANT SELECT ON all_data TO alice, bob;
GRANT USAGE ON COLUMN_ENCRYPTION_KEY rlspolicy_cek to alice, bob;
GRANT USAGE ON CLIENT_MASTER_KEY rlspolicy_cmk to alice, bob;
ALTER TABLE all_data ENABLE ROW LEVEL SECURITY;
CREATE ROW LEVEL SECURITY POLICY all_data_rls ON all_data USING(role = 'alice');
SET ROLE alice PASSWORD 'Gauss@123';
SELECT * FROM all_data;
RESET ROLE;
ALTER ROW LEVEL SECURITY POLICY all_data_rls ON all_data USING(role = 'bob');
SET ROLE bob PASSWORD 'Gauss@123';
SELECT * FROM all_data;
RESET ROLE;
DROP ROW LEVEL SECURITY POLICY all_data_rls ON all_data;
DROP TABLE all_data;
DROP COLUMN ENCRYPTION KEY rlspolicy_cek;
DROP CLIENT MASTER KEY rlspolicy_cmk;
DROP ROLE alice;
DROP ROLE bob;

CREATE USER rlspolicy_user1 PASSWORD 'gauss@123';
CREATE USER rlspolicy_user2 PASSWORD 'gauss@123';
SET ROLE rlspolicy_user1 PASSWORD 'gauss@123';
CREATE CLIENT MASTER KEY rlspolicy_cmk1 WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY rlspolicy_cek1 WITH VALUES (CLIENT_MASTER_KEY = rlspolicy_cmk1, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);
SET ROLE rlspolicy_user2 PASSWORD 'gauss@123';
SELECT count(*) FROM gs_client_global_keys;
SELECT count(*) FROM gs_client_global_keys_args;
SELECT count(*) FROM gs_column_keys;
SELECT count(*) FROM gs_column_keys_args;
SET ROLE rlspolicy_user1 PASSWORD 'gauss@123';
GRANT USAGE ON COLUMN_ENCRYPTION_KEY rlspolicy_cek1 to rlspolicy_user2;
GRANT USAGE ON CLIENT_MASTER_KEY rlspolicy_cmk1 to rlspolicy_user2;
SET ROLE rlspolicy_user2 PASSWORD 'gauss@123';
SELECT count(*) FROM gs_client_global_keys;
SELECT count(*) FROM gs_client_global_keys_args;
SELECT count(*) FROM gs_column_keys;
SELECT count(*) FROM gs_column_keys_args;
RESET ROLE;
SELECT count(*) FROM gs_client_global_keys;
SELECT count(*) FROM gs_client_global_keys_args;
SELECT count(*) FROM gs_column_keys;
SELECT count(*) FROM gs_column_keys_args;
SET ROLE rlspolicy_user1 PASSWORD 'gauss@123';
DROP COLUMN ENCRYPTION KEY rlspolicy_cek1;
DROP CLIENT MASTER KEY rlspolicy_cmk1;
RESET ROLE;
DROP SCHEMA IF EXISTS rlspolicy_user1 CASCADE;
DROP SCHEMA IF EXISTS rlspolicy_user2 CASCADE;
DROP ROLE rlspolicy_user1;
DROP ROLE rlspolicy_user2;

\! gs_ktool -d all