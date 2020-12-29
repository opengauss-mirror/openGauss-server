\! gs_ktool -d all
\! gs_ktool -g

CREATE CLIENT MASTER KEY cmk1 WITH ( KEY_STORE = gs_ktool ,  KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY cek1 WITH VALUES (CLIENT_MASTER_KEY = cmk1, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);

---multi-nodegroup----
create node group ngroup1 with (datanode1, datanode3);
create node group ngroup2 with (datanode2, datanode4);
CREATE TABLE test2 (z int, a int ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = public.cek1, ENCRYPTION_TYPE = DETERMINISTIC), b int) TO GROUP ngroup1;
CREATE TABLE like_test2 (LIKE test2 including all) TO GROUP ngroup2;

INSERT INTO test2 VALUES(3, 4,5);
SELECT * from test2;

INSERT INTO like_test2 VALUES(3, 4,5);
SELECT * from like_test2;

DROP TABLE like_test2;
DROP TABLE test2;
DROP COLUMN ENCRYPTION KEY cek1 CASCADE;
DROP CLIENT MASTER KEY cmk1 CASCADE;
DROP NODE GROUP ngroup1;
DROP NODE GROUP ngroup2;

\! gs_ktool -d all