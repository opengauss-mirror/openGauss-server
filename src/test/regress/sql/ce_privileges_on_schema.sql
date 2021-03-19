\! gs_ktool -d all
\! gs_ktool -g
\! gs_ktool -g
\! gs_ktool -g

DROP CLIENT MASTER KEY IF EXISTS MyCMK1 CASCADE;
DROP CLIENT MASTER KEY IF EXISTS MyCMK2 CASCADE;

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

-- SHOULD FAIL - check CANNOT drop existing objects
DROP COLUMN ENCRYPTION KEY MyCEK1;
DROP CLIENT MASTER KEY MyCMK1;

-- SHOULD FAIL - create TABLE using existing MyCEK1 (missing permissions to both MyCEK1 and MyCMK1)
CREATE TABLE acltest1 (x int, x2 varchar(50) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK1, ENCRYPTION_TYPE = DETERMINISTIC));

-- add speciifc permission to MyCEK1 and retry (should still fail - missing permission to MyCMK1)
RESET SESSION AUTHORIZATION;
GRANT USAGE ON COLUMN_ENCRYPTION_KEY MyCEK1 to newuser;
SET SESSION AUTHORIZATION newuser PASSWORD 'gauss@123';
SET search_path to testns;
CREATE TABLE acltest1 (x int, x2 varchar(50) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK1, ENCRYPTION_TYPE = DETERMINISTIC));

-- SUCCEED - - create TABLE using existing MyCEK1 (now has permission on SCHEMA, CEK and CMK)
RESET SESSION AUTHORIZATION;
GRANT USAGE ON CLIENT_MASTER_KEY MyCMK1 to newuser;
SET SESSION AUTHORIZATION newuser PASSWORD 'gauss@123';
SET search_path to testns;
CREATE TABLE acltest1 (x int, x2 varchar(50) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK1, ENCRYPTION_TYPE = DETERMINISTIC));

-- create TABLE using new CEK and existing CMK
CREATE COLUMN ENCRYPTION KEY MyCEK2 WITH VALUES (CLIENT_MASTER_KEY = MyCMK1, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);
CREATE TABLE acltest2 (x int, x2 varchar(50) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK2, ENCRYPTION_TYPE = DETERMINISTIC));

-- create table using new CEK and new CMK
CREATE CLIENT MASTER KEY MyCMK2 WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/3" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY MyCEK3 WITH VALUES (CLIENT_MASTER_KEY = MyCMK2, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);
CREATE TABLE acltest3 (x int, x2 varchar(50) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK3, ENCRYPTION_TYPE = DETERMINISTIC));

SELECT has_cmk_privilege('newuser', 'testns.MyCMK1', 'USAGE');
SELECT has_cmk_privilege('newuser', 'testns.MyCMK2', 'USAGE');
SELECT has_cek_privilege('newuser', 'testns.MyCEK1', 'USAGE');
SELECT has_cek_privilege('newuser', 'testns.MyCEK2', 'USAGE');
SELECT has_cek_privilege('newuser', 'testns.MyCEK3', 'USAGE');
SELECT has_schema_privilege('newuser', 'testns', 'USAGE');
SELECT has_schema_privilege('newuser', 'testns', 'CREATE');
SELECT has_table_privilege('newuser', 'acltest1', 'INSERT, SELECT, UPDATE, DELETE, TRUNCATE, TRIGGER');
SELECT has_table_privilege('newuser', 'acltest2', 'INSERT, SELECT, UPDATE, DELETE, TRUNCATE, TRIGGER');
SELECT has_table_privilege('newuser', 'acltest3', 'INSERT, SELECT, UPDATE, DELETE, TRUNCATE, TRIGGER');

DROP SCHEMA IF EXISTS testns CASCADE;
DROP ROLE IF EXISTS newuser;

RESET SESSION AUTHORIZATION;
DROP TABLE acltest3;
DROP TABLE acltest2;
DROP TABLE acltest1;
REVOKE USAGE ON COLUMN_ENCRYPTION_KEY MyCEK1 FROM newuser;
REVOKE USAGE ON CLIENT_MASTER_KEY MyCMK1 FROM newuser;
DROP COLUMN ENCRYPTION KEY MyCEK3;
DROP COLUMN ENCRYPTION KEY MyCEK2;
DROP COLUMN ENCRYPTION KEY MyCEK1;
DROP CLIENT MASTER KEY MyCMK2;
DROP CLIENT MASTER KEY MyCMK1;
DROP SCHEMA IF EXISTS testns CASCADE;
DROP SCHEMA IF EXISTS newuser CASCADE;
DROP ROLE IF EXISTS newuser;

\! gs_ktool -d all
