\! gs_ktool -d all
\! gs_ktool -g
\! gs_ktool -g

CREATE SCHEMA drop_cmk_test;
CREATE CLIENT MASTER KEY test_drop_cmk1 WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY drop_cmk_test.test_drop_cmk2 WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/2" , ALGORITHM = AES_256_CBC);
DROP CLIENT MASTER KEY test_drop_cmk1;
SELECT COUNT(*) FROM gs_client_global_keys;
DROP CLIENT MASTER KEY test_drop_cmk2;
SELECT COUNT(*) FROM gs_client_global_keys;
DROP CLIENT MASTER KEY drop_cmk_test.test_drop_cmk2;
SELECT COUNT(*) FROM gs_client_global_keys;
DROP SCHEMA drop_cmk_test;
SELECT count(*), 'count' FROM gs_client_global_keys;

\! gs_ktool -d all