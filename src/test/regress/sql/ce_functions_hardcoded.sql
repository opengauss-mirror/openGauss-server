\! gs_ktool -d all
\! gs_ktool -g

DROP CLIENT MASTER KEY IF EXISTS hardcode_cmk CASCADE;
CREATE CLIENT MASTER KEY hardcode_cmk WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY hardcode_cek WITH VALUES (CLIENT_MASTER_KEY = hardcode_cmk, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);

CREATE TABLE IF NOT EXISTS t1(id int, i1 INT ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = hardcode_cek, ENCRYPTION_TYPE = DETERMINISTIC));

INSERT INTO t1 VALUES(1,1),(2,2),(3,3),(4,4),(5,5);

CREATE OR REPLACE FUNCTION select_func() RETURNS INT AS 'SELECT id FROM t1 WHERE i1=1;' LANGUAGE SQL;
SELECT select_func();
CREATE OR REPLACE FUNCTION select_func() RETURNS INT AS 'SELECT id FROM t1 WHERE i1=2' LANGUAGE SQL;
SELECT select_func();
CREATE OR REPLACE FUNCTION select_func() RETURNS INT AS $func_tag$SELECT id FROM t1 WHERE i1=3;$func_tag$ LANGUAGE SQL;
SELECT select_func();
CREATE OR REPLACE FUNCTION select_func() RETURNS INT AS $func_tag$SELECT id FROM t1 WHERE i1=4$func_tag$ LANGUAGE SQL;
SELECT select_func();

CREATE OR REPLACE FUNCTION insert_func() RETURNS VOID AS 'INSERT INTO t1 VALUES(9,9);' LANGUAGE SQL;
SELECT insert_func();
SELECT insert_func();
SELECT insert_func();
SELECT insert_func();

SELECT count(*) from t1 where id=9;

CREATE OR REPLACE FUNCTION insert_select_func() RETURNS SETOF INTEGER AS 'INSERT INTO t1 VALUES(8,8); SELECT id FROM t1 WHERE i1=9;' LANGUAGE SQL;
SELECT insert_select_func();
SELECT insert_select_func();
SELECT insert_select_func();
SELECT insert_select_func();

SELECT count(*) from t1 where id=8;

SELECT * from t1 order by id;

CREATE FUNCTION f_hardcoded_variable() RETURNS int AS $$
BEGIN
RETURN(SELECT id from t1 where i1 = 5 LIMIT 1);
END;
$$ LANGUAGE plpgsql;
SELECT f_hardcoded_variable();
DROP FUNCTION f_hardcoded_variable;
CREATE TABLE t_processed (name text, val INT ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = hardcode_cek, ENCRYPTION_TYPE = DETERMINISTIC), val2 INT);
CREATE FUNCTION f_hardcoded_variable() RETURNS void AS $$
BEGIN
INSERT INTO t_processed (val, val2) VALUES ( 5, 6);
END;
$$ LANGUAGE plpgsql;
CALL f_hardcoded_variable();
DROP FUNCTION f_hardcoded_variable;
SELECT * FROM t_processed ORDER BY name;
DROP TABLE t_processed;
DROP TABLE t1 CASCADE;

create table accounts (
    id serial,
    name varchar(100) not null ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = hardcode_cek, ENCRYPTION_TYPE = DETERMINISTIC),
    balance dec(15,2) not null ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = hardcode_cek, ENCRYPTION_TYPE = DETERMINISTIC),
    primary key(id)
);
CREATE OR REPLACE FUNCTION insert_func100() RETURNS VOID AS 'INSERT INTO accounts(name,balance) VALUES(''100'', 100);' LANGUAGE SQL;
select insert_func100();
CREATE OR REPLACE FUNCTION insert_func200() RETURNS VOID AS $$INSERT INTO accounts(name,balance) VALUES('200', 200);$$ LANGUAGE SQL;
select insert_func200();
CREATE OR REPLACE FUNCTION insert_func300() RETURNS VOID AS $abcd$INSERT INTO accounts(name,balance) VALUES('300', 300);$abcd$ LANGUAGE SQL;
select insert_func300();

select * from accounts ORDER BY id;

DROP TABLE accounts CASCADE;
DROP FUNCTION insert_func100;
DROP FUNCTION insert_func200;
DROP FUNCTION insert_func300;
DROP FUNCTION insert_func;
DROP FUNCTION select_func;
DROP FUNCTION insert_select_func;
DROP COLUMN ENCRYPTION KEY hardcode_cek;
DROP CLIENT MASTER KEY hardcode_cmk;
\! gs_ktool -d all