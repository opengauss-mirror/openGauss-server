\! gs_ktool -d all
\! gs_ktool -g

DROP CLIENT MASTER KEY IF EXISTS MyCMK1 CASCADE;

-- create another user
DROP ROLE IF EXISTS newuser;
CREATE USER newuser PASSWORD 'gauss@123';

-- create schema
DROP SCHEMA IF EXISTS testns CASCADE;
CREATE SCHEMA testns;
SET search_path to testns;

-- grant privileges on schema (ALL = USAGE, CREATE)
GRANT ALL ON SCHEMA testns TO newuser;


-- CREATE CMK
CREATE CLIENT MASTER KEY MyCMK1 WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);

-- CREATE CEK
CREATE COLUMN ENCRYPTION KEY MyCEK1 WITH VALUES (CLIENT_MASTER_KEY = MyCMK1, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);

-------------------------
-- change to new user
-------------------------
SET SESSION AUTHORIZATION newuser PASSWORD 'gauss@123';
SET search_path to testns;


-- add permission to the keys to  newuser 
RESET SESSION AUTHORIZATION;
GRANT USAGE ON COLUMN_ENCRYPTION_KEY MyCEK1 to newuser;
GRANT USAGE ON CLIENT_MASTER_KEY MyCMK1 to newuser;
SET SESSION AUTHORIZATION newuser PASSWORD 'gauss@123';
SET search_path to testns;


-- create TABLE 
CREATE TABLE acltest1 (x int, x2 varchar(50) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK1, ENCRYPTION_TYPE = DETERMINISTIC));

SELECT has_cmk_privilege('newuser', 'testns.MyCMK1', 'USAGE');
SELECT has_cek_privilege('newuser', 'testns.MyCEK1', 'USAGE');
SELECT has_schema_privilege('newuser', 'testns', 'USAGE');
SELECT has_schema_privilege('newuser', 'testns', 'CREATE');
SELECT has_table_privilege('newuser', 'acltest1', 'INSERT, SELECT, UPDATE, DELETE, TRUNCATE, TRIGGER');


RESET SESSION AUTHORIZATION;
DROP TABLE acltest1;
REVOKE USAGE ON COLUMN_ENCRYPTION_KEY MyCEK1 FROM newuser;
REVOKE USAGE ON CLIENT_MASTER_KEY MyCMK1 FROM newuser;


SELECT has_cmk_privilege('newuser', 'testns.MyCMK1', 'USAGE');
SELECT has_cek_privilege('newuser', 'testns.MyCEK1', 'USAGE');
SELECT has_schema_privilege('newuser', 'testns', 'USAGE');
SELECT has_schema_privilege('newuser', 'testns', 'CREATE');
SELECT has_table_privilege('newuser', 'acltest1', 'INSERT, SELECT, UPDATE, DELETE, TRUNCATE, TRIGGER');


DROP COLUMN ENCRYPTION KEY MyCEK1;
DROP CLIENT MASTER KEY MyCMK1;
DROP SCHEMA IF EXISTS testns CASCADE;
DROP ROLE IF EXISTS newuser;

\! gs_ktool -d all

