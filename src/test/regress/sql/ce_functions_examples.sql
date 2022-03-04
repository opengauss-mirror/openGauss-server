\! gs_ktool -d all
\! gs_ktool -g

DROP CLIENT MASTER KEY IF EXISTS func_cmk CASCADE;
CREATE CLIENT MASTER KEY func_cmk WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY func_cek WITH VALUES (CLIENT_MASTER_KEY = func_cmk, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);

CREATE TABLE t_processed (name text, val INT ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = func_cek, ENCRYPTION_TYPE = DETERMINISTIC), val2 INT);
INSERT INTO t_processed VALUES('name', 1, 2);
CREATE FUNCTION f_processed_in(int, int) RETURNS int AS 'SELECT val2 from t_processed where val=$1 or val2=$2' LANGUAGE SQL;
CREATE FUNCTION f_processed_return() RETURNS int AS 'SELECT val from t_processed' LANGUAGE SQL;
CREATE TABLE t_plaintext (name text, val INT, val2 INT);
INSERT INTO t_plaintext VALUES('name', 1, 2);
CREATE FUNCTION f_plaintext_in(int, int) RETURNS int AS 'SELECT val2 from t_plaintext where val=$1 or val2=$2' LANGUAGE SQL;
CREATE FUNCTION f_plaintext_return() RETURNS int AS 'select val from t_plaintext' LANGUAGE SQL;
CREATE OR REPLACE FUNCTION f_plaintext_out(int, out1 OUT int, out2 OUT int) AS 'SELECT val, val2 from t_plaintext where val=$1' LANGUAGE SQL;
CREATE OR REPLACE FUNCTION f_processed_out(int, out1 OUT int, out2 OUT int) AS 'SELECT val, val2 from t_plaintext where val=$1' LANGUAGE SQL;
select (f_plaintext_out(1)).out1;
select (f_plaintext_out(1)).out2;
select (f_processed_out(1)).out1;
select (f_processed_out(1)).out2;

CREATE FUNCTION f_plaintext_in_plpgsql(int, int) 
RETURNS int AS $$
BEGIN
    RETURN(SELECT val2 from t_plaintext where val=$1 or val2=$2 LIMIT 1);
END; $$ 
LANGUAGE plpgsql;

CREATE FUNCTION f_processed_in_plpgsql() 
RETURNS int AS $$
BEGIN
    RETURN(SELECT val2 from t_processed LIMIT 1);
END; $$ 
LANGUAGE plpgsql;

CREATE FUNCTION f_plaintext_return_table(int, int) RETURNS TABLE(val_p int, val2_p int)
as
$BODY$
begin
    return query (SELECT val, val2 from t_plaintext where val=111 or val2=$2);
end;
$BODY$
language plpgsql ;

CREATE FUNCTION f_processed_return_table() RETURNS TABLE(val_p int, val2_p int)
as
$BODY$
begin
    return query (SELECT val, val2 from t_processed);
end;
$BODY$
language plpgsql ;

CREATE FUNCTION f_plaintext_return_table2(int, int) RETURNS SETOF t_plaintext AS $$
    SELECT * FROM t_plaintext WHERE val=$1 or val2=$2;
$$ LANGUAGE SQL;

CREATE FUNCTION f_plaintext_return_table3(int, int) RETURNS TABLE(name text, val_p int, val2_p int) AS $$
    SELECT * FROM t_plaintext WHERE val=$1 or val2=$2;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION get_all_plaintext_setof() RETURNS SETOF t_plaintext AS
$BODY$
DECLARE
    r t_plaintext%rowtype;
BEGIN
    FOR r IN
        SELECT * FROM t_plaintext WHERE val > 0
    LOOP
        -- can do some processing here
        RETURN NEXT r; -- return current row of SELECT
    END LOOP;
    RETURN;
END
$BODY$
LANGUAGE plpgsql;

SELECT * FROM get_all_plaintext_setof();


CREATE OR REPLACE FUNCTION get_all_plaintext() RETURNS int AS
$BODY$
DECLARE
var_val int;
BEGIN
        SELECT val INTO var_val FROM t_plaintext WHERE val > 0;
        RETURN var_val;
END
$BODY$
LANGUAGE plpgsql;

SELECT * FROM get_all_plaintext();


CREATE FUNCTION f_hardcoded(int) RETURNS int AS 
'SELECT val2 from t_plaintext where val = 5 or val2 = $1; 
SELECT val2 from t_plaintext where val2 = $1;' LANGUAGE SQL;

CREATE FUNCTION f_hardcoded(int) RETURNS int AS 
'SELECT val2 from t_plaintext where val = 5 or val2 = $1; 
SELECT val2 from t_plaintext where val2 = $1;' LANGUAGE SQL;

CREATE FUNCTION f_hardcoded(int) RETURNS int AS $$
BEGIN
    RETURN(SELECT val2 from t_plaintext where val = 5 or val2 = $1);
END;
$$  LANGUAGE plpgsql;


CREATE FUNCTION f_hardcoded_variable() RETURNS int AS $$
DECLARE
    quantity integer DEFAULT 1; 
BEGIN
    RETURN(SELECT val2 from t_plaintext where val = quantity);
END;
$$ LANGUAGE plpgsql;

select f_hardcoded_variable();

CREATE FUNCTION reffunc_plaintext(refcursor) RETURNS refcursor AS '
BEGIN
    OPEN $1 FOR SELECT * FROM t_plaintext;
    RETURN $1;
END;
' LANGUAGE plpgsql;

BEGIN;
SELECT reffunc_plaintext('funccursor_plaintext');
FETCH ALL IN funccursor_plaintext;
update t_plaintext  set val = 2 WHERE CURRENT OF funccursor_processed;
COMMIT;

CREATE OR REPLACE FUNCTION reffunc_processed(refcursor) RETURNS refcursor AS '
BEGIN
    OPEN $1 FOR SELECT * FROM t_processed;
    RETURN $1;
END;
' LANGUAGE plpgsql;

BEGIN;
SELECT reffunc_processed('funccursor_processed');
FETCH ALL IN funccursor_processed;
update t_processed  set val = 2 WHERE CURRENT OF funccursor_processed;
COMMIT;

CREATE OR REPLACE FUNCTION foo()
   RETURNS text[] AS
 $BODY$
 declare
     a text;
     b text;
     arr text[];
 begin
     a = 'a';
     b = 'b';
     arr[0] = a;
     arr[1] = b;
     return arr;
 end;
 $BODY$
   LANGUAGE 'plpgsql' VOLATILE;
select proname, prorettype::regtype from pg_proc where Oid in (select func_id  from gs_encrypted_proc) order by proname;
CALL f_processed_in_plpgsql();
CALL f_processed_return_table();
CALL f_processed_return_table();
DROP TABLE t_plaintext CASCADE;
DROP TABLE t_processed CASCADE;
DROP FUNCTION f_hardcoded;
DROP FUNCTION f_hardcoded_variable;
DROP FUNCTION f_plaintext_in;
DROP FUNCTION f_plaintext_in_plpgsql;
DROP FUNCTION f_plaintext_return;
DROP FUNCTION f_plaintext_return_table;
DROP FUNCTION f_plaintext_return_table3;
DROP FUNCTION f_processed_in;
DROP FUNCTION f_processed_in_plpgsql;
DROP FUNCTION f_processed_return;
DROP FUNCTION f_processed_return_table;
DROP FUNCTION foo;
DROP FUNCTION get_all_plaintext;
DROP FUNCTION reffunc_plaintext;
DROP FUNCTION reffunc_processed;
DROP FUNCTION f_plaintext_out;
DROP FUNCTION f_processed_out;
DROP COLUMN ENCRYPTION KEY func_cek;
DROP CLIENT MASTER KEY func_cmk CASCADE;
\! gs_ktool -d all