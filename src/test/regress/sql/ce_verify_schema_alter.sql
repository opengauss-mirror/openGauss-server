\! gs_ktool -d all
\! gs_ktool -g

DROP CLIENT MASTER KEY IF EXISTS MyCMK CASCADE;
CREATE SCHEMA test;
CREATE CLIENT MASTER KEY MyCMK WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY MyCEK WITH VALUES (CLIENT_MASTER_KEY = MyCMK, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);
CREATE TABLE IF NOT EXISTS test.t_varchar(id INT, name varchar(50) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC));
SELECT column_name from gs_encrypted_columns;
INSERT INTO test.t_varchar (id, name) VALUES (1, 'MyName');
SELECT * from test.t_varchar;
ALTER SCHEMA test RENAME TO test1;
SELECT c.relname, g.column_name from gs_encrypted_columns g join pg_class c on (g.rel_id=c.oid);
SELECT * FROM test1.t_varchar;
SELECT * FROM test1.t_varchar WHERE name = 'MyName';
DROP TABLE test1.t_varchar;
DROP SCHEMA test1;
DROP COLUMN ENCRYPTION KEY MyCEK;
DROP CLIENT MASTER KEY MyCMK;

\! gs_ktool -d all