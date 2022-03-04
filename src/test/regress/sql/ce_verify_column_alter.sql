\! gs_ktool -d all
\! gs_ktool -g

DROP CLIENT MASTER KEY IF EXISTS MyCMK CASCADE;
CREATE CLIENT MASTER KEY MyCMK WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY MyCEK WITH VALUES (CLIENT_MASTER_KEY = MyCMK, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);
CREATE TABLE IF NOT EXISTS t_varchar(id INT);

ALTER table t_varchar ADD COLUMN name varchar(50) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC);

SELECT c.relname, g.column_name from gs_encrypted_columns g join pg_class c on (g.rel_id=c.oid);
INSERT INTO t_varchar (id, name) VALUES (1, 'MyName');
SELECT * from t_varchar;
ALTER table t_varchar RENAME COLUMN name TO newname;
SELECT c.relname, g.column_name from gs_encrypted_columns g join pg_class c on (g.rel_id=c.oid);
SELECT * FROM t_varchar where newname = 'MyName';


--verify tablename alter 
INSERT INTO t_varchar VALUES (2, 'MyNumber');
SELECT * from t_varchar;
SELECT relname from pg_class join gs_encrypted_columns on pg_class.oid = gs_encrypted_columns.rel_id;
ALTER table t_varchar RENAME TO newtable;
SELECT * FROM newtable;
SELECT * FROM newtable where newname = 'MyName';
SELECT relname from pg_class join gs_encrypted_columns on pg_class.oid = gs_encrypted_columns.rel_id;
DROP TABLE newtable;

SELECT column_name from gs_encrypted_columns;
DROP COLUMN ENCRYPTION KEY MyCEK;
DROP CLIENT MASTER KEY MyCMK;

\! gs_ktool -d all
