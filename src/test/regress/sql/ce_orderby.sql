\! gs_ktool -d all
\! gs_ktool -g
\! gs_ktool -g

DROP CLIENT MASTER KEY IF EXISTS UnsupportCMK CASCADE;
CREATE CLIENT MASTER KEY UnsupportCMK WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY UnsupportCEK WITH VALUES (CLIENT_MASTER_KEY = UnsupportCMK, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);
CREATE TABLE IF NOT EXISTS table_unsupport(id INT, num int ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = UnsupportCEK, ENCRYPTION_TYPE = DETERMINISTIC));

INSERT INTO table_unsupport (id, num) VALUES (1, 555555);
INSERT INTO table_unsupport (id, num) VALUES (2, 666666);
INSERT INTO table_unsupport (id, num) VALUES (3, 777777);
SELECT * from table_unsupport order by id limit 1;
SELECT * from table_unsupport order by num;
SELECT * from table_unsupport order by num,id;

CREATE TABLE IF NOT EXISTS table_unsupport_tmp(id INT, num int ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = UnsupportCEK, ENCRYPTION_TYPE = DETERMINISTIC));
INSERT INTO table_unsupport_tmp select * from table_unsupport order by num;

CREATE TABLE IF NOT EXISTS table_test(id INT, num int);
INSERT INTO table_test (id, num) VALUES (1, 555555);
INSERT INTO table_test (id, num) VALUES (2, 666666);
SELECT * from table_test order by num;

CREATE SCHEMA testns;

CREATE CLIENT MASTER KEY testns.UnsupportCMK WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/2" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY testns.UnsupportCEK WITH VALUES (CLIENT_MASTER_KEY = testns.UnsupportCMK, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);

SET search_path to testns;
CREATE TABLE IF NOT EXISTS table_test(id INT, num int ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = UnsupportCEK, ENCRYPTION_TYPE = DETERMINISTIC));
INSERT INTO table_test (id, num) VALUES (1, 555555);
INSERT INTO table_test (id, num) VALUES (2, 666666);

SELECT * from table_test order by id;
SELECT * from table_test order by num;
SELECT * from public.table_test order by num;

SET search_path to testns,public;
DROP TABLE IF EXISTS table_unsupport;
DROP TABLE IF EXISTS table_unsupport_tmp;
DROP TABLE IF EXISTS testns.table_test;
DROP TABLE IF EXISTS public.table_test;

DROP COLUMN ENCRYPTION KEY testns.UnsupportCEK;
DROP COLUMN ENCRYPTION KEY public.UnsupportCEK;

DROP CLIENT MASTER KEY testns.UnsupportCMK;
DROP CLIENT MASTER KEY public.UnsupportCMK;

\! gs_ktool -d all

