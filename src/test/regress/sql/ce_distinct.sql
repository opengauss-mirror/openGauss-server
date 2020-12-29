\! gs_ktool -d all
\! gs_ktool -g

DROP TABLE IF EXISTS t_distinct;
CREATE CLIENT MASTER KEY MyCMK WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY MyCEK WITH VALUES (CLIENT_MASTER_KEY = MyCMK, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);
CREATE TABLE IF NOT EXISTS t_distinct(
    id INT,
    name VARCHAR(30) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK , ENCRYPTION_TYPE = DETERMINISTIC)
);
INSERT INTO t_distinct VALUES ( 1, 'John');
INSERT INTO t_distinct VALUES ( 2, 'Moses');
INSERT INTO t_distinct VALUES ( 3, 'Alex');
INSERT INTO t_distinct VALUES ( 4, 'Jorgen');
INSERT INTO t_distinct VALUES ( 5, 'John');
--SELECT distinct name FROM t_distinct;
SELECT distinct name, id FROM t_distinct order by id;
SELECT id, distinct name FROM t_distinct order by id;
DROP TABLE t_distinct;
DROP CLIENT MASTER KEY MyCMK CASCADE;

\! gs_ktool -d all