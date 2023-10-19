\! gs_ktool -d all
\! gs_ktool -g

CREATE CLIENT MASTER KEY MyCMK WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY MyCEK WITH VALUES (CLIENT_MASTER_KEY = MyCMK, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);
CREATE TABLE IF NOT EXISTS t_num(id INT, num int ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC));
INSERT INTO t_num (id, num) VALUES (1, 555);
INSERT INTO t_num (id, num) VALUES (2, 666666);
INSERT INTO t_num (id, num) VALUES (3, 777777);
INSERT INTO t_num (id, num) VALUES (4, 8888);
INSERT INTO t_num (id, num) VALUES (5, 999999);
INSERT INTO t_num (id, num) VALUES (6, 4);
INSERT INTO t_num (id, num) VALUES (7, 5653);
INSERT INTO t_num (id, num) VALUES (8, 6786578);
SELECT * from t_num order by id;
SELECT * from t_num order by id limit 3;
SELECT * from t_num order by id limit 5 offset 3;
DROP TABLE t_num;
DROP CLIENT MASTER KEY MyCMK CASCADE;

\! gs_ktool -d all