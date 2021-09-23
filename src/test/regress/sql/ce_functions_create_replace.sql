\! gs_ktool -d all
\! gs_ktool -g

DROP CLIENT MASTER KEY IF EXISTS create_replace_cmk CASCADE;
CREATE CLIENT MASTER KEY create_replace_cmk WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY create_replace_cek WITH VALUES (CLIENT_MASTER_KEY = create_replace_cmk, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);

create table accounts (
    id serial,
    name varchar(100) not null ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = create_replace_cek, ENCRYPTION_TYPE = DETERMINISTIC),
    balance dec(15,2) not null ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = create_replace_cek, ENCRYPTION_TYPE = DETERMINISTIC),
    primary key(id)
);
INSERT INTO accounts VALUES (1, 'dani', 123.45);
CREATE OR REPLACE FUNCTION f_processed_in_plpgsql(a varchar(100), b dec(15,2)) 
RETURNS varchar(100) AS $$
declare
c varchar(100);
BEGIN
    SELECT into c name from accounts where name=$1 or balance=$2 LIMIT 1;
    RETURN c;
END; $$
LANGUAGE plpgsql;
SELECT COUNT(*) FROM gs_encrypted_proc where func_id NOT in (SELECT Oid FROM pg_proc);
CREATE OR REPLACE FUNCTION f_processed_in_plpgsql(a varchar(100), b dec(15,2))
RETURNS varchar(100) AS $$
declare
c varchar(100);
BEGIN
    SELECT into c name from accounts where name=$1 or balance=$2 LIMIT 1;
    RETURN c;
END; $$
LANGUAGE plpgsql;
\df
SELECT COUNT(*) FROM gs_encrypted_proc where func_id NOT in (SELECT Oid FROM pg_proc);
CREATE OR REPLACE FUNCTION f_processed_out_plpgsql(out1 OUT varchar(100), out2 OUT dec(15,2)) 
AS $$
BEGIN
SELECT INTO out1, out2 name, balance from accounts LIMIT 1;
END; $$
LANGUAGE plpgsql;
\df
-- FAILED
CREATE OR REPLACE FUNCTION f_processed_out_plpgsql(out1 OUT varchar(100), out2 OUT dec(15,2)) 
AS $$
BEGIN
SELECT INTO out1, out2 name, balance from accounts LIMIT 1;
END; $$
LANGUAGE plpgsql;
SELECT f_processed_out_plpgsql();
\df
DROP FUNCTION f_processed_in_plpgsql;
DROP FUNCTION f_processed_out_plpgsql;
DROP TABLE accounts;
DROP COLUMN ENCRYPTION KEY create_replace_cek;
DROP CLIENT MASTER KEY create_replace_cmk;
\! gs_ktool -d all