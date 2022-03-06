\! gs_ktool -d all
\! gs_ktool -g

DROP CLIENT MASTER KEY IF EXISTS cmk1 CASCADE;
CREATE CLIENT MASTER KEY cmk1 WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY cek1 WITH VALUES (CLIENT_MASTER_KEY = cmk1, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);

CREATE TABLE IF NOT EXISTS t_varchar
    (id INT, name varchar(50) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = cek1, ENCRYPTION_TYPE = DETERMINISTIC),
    address varchar(50) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = cek1, ENCRYPTION_TYPE = RANDOMIZED));
SELECT attname, atttypid::regtype FROM pg_attribute JOIN pg_class On attrelid = Oid WHERE relname = 't_varchar' AND attnum >0;
\d t_varchar;
\d+ t_varchar;
DROP TABLE t_varchar;
DROP COLUMN ENCRYPTION KEY cek1;
DROP CLIENT MASTER KEY cmk1;

\! gs_ktool -d all