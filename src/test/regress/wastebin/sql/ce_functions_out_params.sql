\! gs_ktool -d all
\! gs_ktool -g

DROP CLIENT MASTER KEY IF EXISTS out_cmk CASCADE;
CREATE CLIENT MASTER KEY out_cmk WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY out_cek WITH VALUES (CLIENT_MASTER_KEY = out_cmk, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);

CREATE TABLE t_processed (name text, val INT ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = out_cek, ENCRYPTION_TYPE = DETERMINISTIC), val2 INT);
INSERT INTO t_processed VALUES('name', 1, 2);

CREATE TABLE t_processed_b (name text, val bytea ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = out_cek, ENCRYPTION_TYPE = DETERMINISTIC), val2 INT);
INSERT INTO t_processed_b VALUES('name', 'test', 2);
CREATE OR REPLACE FUNCTION f_processed_out_1param(out1 OUT int) AS 'SELECT val from t_processed LIMIT 1' LANGUAGE SQL; 

CREATE OR REPLACE FUNCTION f_processed_out(out1 OUT int, out2 OUT int) AS 'SELECT val, val2 from t_processed LIMIT 1' LANGUAGE SQL;
CREATE OR REPLACE FUNCTION f_processed_out_b(out1 OUT bytea, out2 OUT int) AS 'SELECT val, val2 from t_processed_b LIMIT 1' LANGUAGE SQL;

CREATE OR REPLACE FUNCTION f_processed_out_plpgsql(out out1 int, out out2 int) 
as $$ 
begin 
  select val, val2 INTO out1, out2 from t_processed; 
end;$$ 
LANGUAGE plpgsql; 
CREATE OR REPLACE FUNCTION f_processed_out_plpgsql2(out out1 t_processed.val%TYPE, out out2 t_processed.val%TYPE) 
as $$ 
begin 
  select val, val2 INTO out1, out2 from t_processed; 
end;$$ 
LANGUAGE plpgsql; 
CREATE OR REPLACE FUNCTION f_processed_aliases_plpgsql(out out1 int, out out2 int) as  
$BODY$ 
DECLARE  
 val1 ALIAS FOR out1; 
begin 
  select val, val2 INTO val1, out2 from t_processed; 
end; 
$BODY$
LANGUAGE plpgsql; 
select proname, prorettype, proallargtypes, prorettype_orig, proallargtypes_orig FROM pg_proc LEFT JOIN gs_encrypted_proc ON pg_proc.
Oid = gs_encrypted_proc.func_id WHERE proname IN ('f_processed_out', 'f_processed_out_plpgsql', 'f_processed_out_plpgsql2', 'f_processed_aliases_plpgsql', 'f_processed_out_1param') ORDER BY proname;
SELECT f_processed_out_1param();
SELECT f_processed_out();
SELECT f_processed_out_b();
SELECT f_processed_out_plpgsql();
SELECT f_processed_out_plpgsql2();
SELECT f_processed_aliases_plpgsql();
DROP FUNCTION f_processed_out_b;
DROP TABLE t_processed CASCADE;
DROP TABLE t_processed_b CASCADE;
DROP FUNCTION f_processed_out_1param;
DROP FUNCTION f_processed_out;
DROP FUNCTION f_processed_out_plpgsql;
DROP FUNCTION f_processed_out_plpgsql2;
DROP FUNCTION f_processed_aliases_plpgsql;
DROP COLUMN ENCRYPTION KEY out_cek;
DROP CLIENT MASTER KEY out_cmk;
\! gs_ktool -d all