\set verbosity verbose
\! gs_ktool -d all
\! gs_ktool -g

DROP CLIENT MASTER KEY IF EXISTS ret_cmk2 CASCADE;
CREATE CLIENT MASTER KEY ret_cmk2 WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY ret_cek2 WITH VALUES (CLIENT_MASTER_KEY = ret_cmk2, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);

CREATE TABLE IF NOT EXISTS t_num(id INT, num int ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = ret_cek2, ENCRYPTION_TYPE = DETERMINISTIC));
INSERT INTO t_num (id, num) VALUES (1, 5555);
INSERT INTO t_num (id, num) VALUES (2, 6666);
SELECT * from t_num;
CREATE FUNCTION select1 () RETURNS t_num LANGUAGE SQL
AS 'SELECT * from t_num;';
CREATE FUNCTION select2 () RETURNS t_num LANGUAGE SQL
AS 'SELECT id, num from t_num;';
CREATE FUNCTION select3 () RETURNS setof t_num LANGUAGE SQL
AS 'SELECT * from t_num;';
CREATE FUNCTION select4 () RETURNS setof t_num LANGUAGE SQL
AS 'SELECT id, num from t_num;';
CREATE FUNCTION select5 () RETURNS int LANGUAGE SQL
AS 'SELECT num from t_num;';
CREATE FUNCTION select6 () RETURNS setof int LANGUAGE SQL
AS 'SELECT  num from t_num;';
CREATE FUNCTION select7 () RETURNS TABLE(a INT, b INT) LANGUAGE SQL
AS 'SELECT id, num from t_num;';
CREATE FUNCTION reffunc(refcursor) RETURNS refcursor AS '
BEGIN
    OPEN $1 FOR SELECT * FROM t_num;
    RETURN $1;
END;
' LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION get_rows_setof() RETURNS SETOF t_num AS
$BODY$
DECLARE
    r t_num%rowtype;
BEGIN
    FOR r IN
        SELECT * FROM t_num
    LOOP
        -- can do some processing here
        RETURN NEXT r; -- return current row of SELECT
    END LOOP;
    RETURN;
END
$BODY$
LANGUAGE plpgsql;

CREATE FUNCTION f_processed_return_table() RETURNS TABLE(val_p int, val2_p int)
as
$BODY$
begin
    return query (SELECT id, num from t_num);
end;
$BODY$
language plpgsql ;

\df select1
\df select2
\df select3
\df select4
\df select5
\df select6
\df select7

call select1();
call select2();
call select3();
call select4();
call select5();
call select6();
call select7();

CALL f_processed_return_table();
BEGIN;
SELECT reffunc('funccursor');
FETCH ALL IN funccursor;
COMMIT;
SELECT * FROM get_rows_setof();

DROP TABLE t_num CASCADE;

DROP FUNCTION select6; 
DROP FUNCTION select5; 
DROP FUNCTION select7; 
DROP FUNCTION reffunc(refcursor);
DROP FUNCTION get_rows_setof();
DROP FUNCTION f_processed_return_table();

SELECT COUNT(*) FROM gs_encrypted_proc;
SELECT proname, prorettype, proallargtypes FROM gs_encrypted_proc JOIN pg_proc ON pg_proc.Oid = gs_encrypted_proc.func_id;
DROP COLUMN ENCRYPTION KEY ret_cek2;
DROP CLIENT MASTER KEY ret_cmk2;
\! gs_ktool -d all