\! gs_ktool -d all
\! gs_ktool -g

DROP COLUMN ENCRYPTION KEY IF EXISTS exec_direct_cek;
DROP CLIENT MASTER KEY IF EXISTS exec_direct_cmk;
CREATE NODE GROUP ngroup1 WITH (datanode1);
CREATE CLIENT MASTER KEY exec_direct_cmk WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY exec_direct_cek WITH VALUES (CLIENT_MASTER_KEY = exec_direct_cmk, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);
CREATE TABLE IF NOT EXISTS exec_direct_t1 (c1 INT, c2 INT ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = exec_direct_cek , ENCRYPTION_TYPE = DETERMINISTIC)) TO GROUP ngroup1;
INSERT INTO exec_direct_t1 VALUES(1,1),(2,2),(3,3),(4,4),(5,5);
EXECUTE DIRECT ON(datanode1) 'SELECT * FROM exec_direct_t1 WHERE c2=1 or c2=2;';
EXECUTE DIRECT ON(datanode1) 'UPDATE exec_direct_t1 SET c1=2 WHERE c2=1;';
EXECUTE DIRECT ON(datanode1) 'CREATE TABLE t3(c1 INT, c2 INT);';
DROP TABLE exec_direct_t1;
DROP COLUMN ENCRYPTION KEY exec_direct_cek;
DROP CLIENT MASTER KEY exec_direct_cmk;
DROP NODE GROUP ngroup1;

\! gs_ktool -d all