\! gs_ktool -d all
\! gs_ktool -g
\! gs_ktool -g

CREATE USER test_security_admin CREATEROLE PASSWORD "Gauss@123";
CREATE USER test1 PASSWORD "Gauss@123";
CREATE USER test2 PASSWORD "Gauss@123";
SET ROLE test1 PASSWORD "Gauss@123";
CREATE CLIENT MASTER KEY test_cmk1 WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY test_drop_cek1 WITH VALUES (CLIENT_MASTER_KEY = test1.test_cmk1, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);
SET ROLE test2 PASSWORD "Gauss@123";
CREATE CLIENT MASTER KEY test_cmk2 WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/2" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY test_drop_cek2 WITH VALUES (CLIENT_MASTER_KEY = test2.test_cmk2, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);
SELECT  count(*), 'count' FROM gs_client_global_keys;
SELECT  count(*), 'count' FROM gs_column_keys;
SET ROLE test1 PASSWORD "Gauss@123";
DROP column encryption key test2.test_drop_cek2;
SELECT  count(*), 'count' FROM gs_column_keys;
SET ROLE test2 PASSWORD "Gauss@123";
DROP column encryption key test2.test_drop_cek2;
SELECT  count(*), 'count' FROM gs_column_keys;
SET ROLE test_security_admin PASSWORD "Gauss@123";
DROP COLUMN ENCRYPTION KEY test1.test_drop_cek1;
SELECT  count(*), 'count' FROM gs_column_keys;
RESET ROLE;
DROP COLUMN ENCRYPTION KEY test1.test_drop_cek1;
DROP CLIENT MASTER KEY IF EXISTS test1.test_cmk1 CASCADE;
DROP CLIENT MASTER KEY IF EXISTS test2.test_cmk2 CASCADE;
SELECT count(*), 'count' FROM gs_client_global_keys;
DROP USER test1,test2,test_security_admin;

\! gs_ktool -d all
