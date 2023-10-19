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

SET SESSION AUTHORIZATION newuser PASSWORD 'gauss@123';
SET search_path to testns;

-- SHOULD FAILL - create TABLE using existing MyCEK1 (missing permissions to both MyCEK1 and MyCMK1)
CREATE TABLE acltest1 (x int, x2 varchar(50) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK1, ENCRYPTION_TYPE = DETERMINISTIC));

RESET SESSION AUTHORIZATION;
-- add permission to the keys to newuser (ALL = USAGE, DROP)
GRANT USAGE ON COLUMN_ENCRYPTION_KEY MyCEK1 to newuser;
GRANT USAGE ON CLIENT_MASTER_KEY MyCMK1 to newuser;

-------------------------
-- change to new user
-------------------------
SET SESSION AUTHORIZATION newuser PASSWORD 'gauss@123';
SET search_path to testns;


-- create TABLE 
CREATE TABLE acltest1 (x int, x2 varchar(50) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK1, ENCRYPTION_TYPE = DETERMINISTIC));

SELECT has_cmk_privilege('newuser', 'testns.MyCMK1', 'USAGE');
SELECT has_cek_privilege('newuser', 'testns.MyCEK1', 'USAGE');
SELECT has_cmk_privilege('newuser', 'testns.MyCMK1', 'DROP');
SELECT has_cek_privilege('newuser', 'testns.MyCEK1', 'DROP');
SELECT has_schema_privilege('newuser', 'testns', 'USAGE');
SELECT has_schema_privilege('newuser', 'testns', 'CREATE');
SELECT has_table_privilege('newuser', 'acltest1', 'INSERT, SELECT, UPDATE, DELETE, TRUNCATE, TRIGGER');

--check key namespace
SELECT nspname FROM pg_namespace JOIN gs_client_global_keys on pg_namespace.Oid = key_namespace;
SELECT nspname FROM pg_namespace JOIN gs_column_keys on pg_namespace.Oid = key_namespace;

RESET SESSION AUTHORIZATION;

--check key owner
SELECT count(usename) FROM pg_user JOIN gs_client_global_keys on usesysid = key_owner;
SELECT count(usename) FROM pg_user JOIN gs_column_keys on usesysid = key_owner;

--check drop cek/cmk if encrypted table exist(false)
DROP COLUMN ENCRYPTION KEY MyCEK1;
DROP CLIENT MASTER KEY MyCMK1;

--check drop cek/cmk cascade if encrypted table exist(false)
DROP COLUMN ENCRYPTION KEY MyCEK1 CASCADE;
DROP CLIENT MASTER KEY MyCMK1 CASCADE;

--check drop table(success)
DROP TABLE acltest1;

SET SESSION AUTHORIZATION newuser PASSWORD 'gauss@123';
SET search_path to testns;

--check DROP KEY WITHOUT PREMISSION(false)
DROP COLUMN ENCRYPTION KEY MyCEK1;
DROP CLIENT MASTER KEY MyCMK1;

--check DELETE KEYS(false)
delete from gs_client_global_keys;
delete from gs_column_keys;


RESET SESSION AUTHORIZATION;
REVOKE USAGE ON COLUMN_ENCRYPTION_KEY MyCEK1 FROM newuser;
REVOKE USAGE ON CLIENT_MASTER_KEY MyCMK1 FROM newuser;
GRANT DROP ON COLUMN_ENCRYPTION_KEY testns.MyCEK1 to newuser;
GRANT DROP ON CLIENT_MASTER_KEY testns.MyCMK1 to newuser;

SELECT has_cmk_privilege('newuser', 'testns.MyCMK1', 'USAGE');
SELECT has_cek_privilege('newuser', 'testns.MyCEK1', 'USAGE');
SELECT has_cmk_privilege('newuser', 'testns.MyCMK1', 'DROP');
SELECT has_cek_privilege('newuser', 'testns.MyCEK1', 'DROP');

SET SESSION AUTHORIZATION newuser PASSWORD 'gauss@123';
SET search_path to testns;

--check DROP KEY WITH PREMISSION(success)
DROP COLUMN ENCRYPTION KEY MyCEK1;
DROP CLIENT MASTER KEY MyCMK1;

RESET SESSION AUTHORIZATION;

--check pg_depend(false)
SELECT exists (SELECT refobjid FROM pg_depend JOIN gs_client_global_keys on gs_client_global_keys.key_namespace = refobjid);

SET search_path to testns;
CREATE CLIENT MASTER KEY MyCMK1 WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY MyCEK1 WITH VALUES (CLIENT_MASTER_KEY = MyCMK1, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);

--check pg_depend(true)
SELECT exists (SELECT refobjid FROM pg_depend JOIN gs_client_global_keys on gs_client_global_keys.key_namespace = refobjid);

--check drop schema cascade if cek/cmk exist(success)
DROP SCHEMA IF EXISTS testns CASCADE;
DROP SCHEMA IF EXISTS newuser CASCADE;
DROP ROLE IF EXISTS newuser;

\! gs_ktool -d all
\! gs_ktool -g

CREATE USER d_user1 PASSWORD 'gauss@123';
GRANT ALL ON SCHEMA public TO d_user1;
SET SESSION AUTHORIZATION d_user1 PASSWORD 'gauss@123';
SET search_path to public;
CREATE CLIENT MASTER KEY MyCMK1 WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY MyCEK1 WITH VALUES (CLIENT_MASTER_KEY = MyCMK1, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);
CREATE TABLE acltest1 (x int, x2 varchar(50) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK1, ENCRYPTION_TYPE = DETERMINISTIC));
RESET SESSION AUTHORIZATION;
drop ROLE d_user1;
drop USER d_user1;
drop USER d_user1 cascade;
select count(*) from gs_client_global_keys;
select count(*) from gs_column_keys;
\! gs_ktool -d all

\! gs_ktool -g
CREATE USER sysadmin1 with sysadmin PASSWORD "Gauss_234";
CREATE USER rsr3 with PASSWORD "Gauss_234";
CREATE USER user1 with PASSWORD "Gauss_234";
RESET search_path;
set role user1 password "Gauss_234";
CREATE CLIENT MASTER KEY MyCMK1 WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY MyCEK1 WITH VALUES (CLIENT_MASTER_KEY = MyCMK1, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);
drop table if exists test_01;
CREATE TABLE test_01
(
 id_number INTEGER NOT NULL,
 name VARCHAR(20) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK1, ENCRYPTION_TYPE = DETERMINISTIC) NOT NULL 
) WITH (ORIENTATION = COLUMN, COMPRESSION=HIGH);
insert into test_01 values(1,123);

set role sysadmin1 password "Gauss_234";
insert into user1.test_01 values(1,123);

set role user1 password "Gauss_234";
insert into test_01 values(1,123);
select * from test_01;

set role sysadmin1 password "Gauss_234";
grant usage on schema user1 to rsr3;
grant insert on user1.test_01 to rsr3;

set role rsr3 password "Gauss_234";
select current_user;
insert into user1.test_01 values(1,123);

reset role;
drop table if exists user1.test_01 cascade;
drop COLUMN ENCRYPTION KEY user1.MyCEK1 cascade;
drop CLIENT MASTER KEY user1.MyCMK1 cascade;

drop USER rsr3 cascade;
drop USER user1 cascade;
drop USER sysadmin1 cascade;
\! gs_ktool -d all