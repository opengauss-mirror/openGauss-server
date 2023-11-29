\! gs_ktool -d all
\! gs_ktool -g
\! gs_ktool -g

CREATE USER test_security_admin CREATEROLE PASSWORD "Gauss@123";
CREATE USER test1 PASSWORD "Gauss@123";
CREATE USER test2 PASSWORD "Gauss@123";
SELECT count(*), 'count' FROM gs_client_global_keys;
SET ROLE test1 PASSWORD "Gauss@123";
CREATE CLIENT MASTER KEY test_drop_cmk1 WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
SET ROLE test2 PASSWORD "Gauss@123";
CREATE CLIENT MASTER KEY test_drop_cmk2 WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/2" , ALGORITHM = AES_256_CBC);
SELECT count(*), 'count' FROM gs_client_global_keys;
SET ROLE test1 PASSWORD "Gauss@123";
DROP CLIENT MASTER KEY test_drop_cmk2;
SELECT count(*), 'count' FROM gs_client_global_keys;
DROP CLIENT MASTER KEY test2.test_drop_cmk2;
SELECT count(*), 'count' FROM gs_client_global_keys;
SET ROLE test2 PASSWORD "Gauss@123";
DROP CLIENT MASTER KEY test2.test_drop_cmk2;
SELECT count(*), 'count' FROM gs_client_global_keys;
SET ROLE test_security_admin PASSWORD "Gauss@123";
DROP CLIENT MASTER KEY test1.test_drop_cmk1;
SELECT count(*), 'count' FROM gs_client_global_keys;
RESET ROLE; 
DROP CLIENT MASTER KEY test1.test_drop_cmk1;
DROP CLIENT MASTER KEY IF EXISTS test1.test_drop_cmk1 CASCADE;
DROP CLIENT MASTER KEY IF EXISTS test2.test_drop_cmk2 CASCADE;
DROP USER test1,test2,test_security_admin;
SELECT count(*), 'count' FROM gs_client_global_keys;

\! gs_ktool -d all