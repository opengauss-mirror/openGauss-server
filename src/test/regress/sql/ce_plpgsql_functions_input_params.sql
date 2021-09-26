\! gs_ktool -d all
\! gs_ktool -g

DROP CLIENT MASTER KEY IF EXISTS plpsql_cmk1 CASCADE;
CREATE CLIENT MASTER KEY plpsql_cmk1 WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY plpsql_cek1 WITH VALUES (CLIENT_MASTER_KEY = plpsql_cmk1, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);

CREATE TABLE t_processed (name text, val INT ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = plpsql_cek1, ENCRYPTION_TYPE = DETERMINISTIC), val2 INT);
insert into t_processed values('one',1,10),('two',2,20),('three',3,30),('four',4,40),('five',5,50),('six',6,60),('seven',7,70),('eight',8,80),('nine',9,90),('ten',10,100);

CREATE FUNCTION f_processed_in_plpgsql(int, int)  RETURNS int AS $$ BEGIN RETURN(SELECT val2 from t_processed where val=$1 or val2=$2 LIMIT 1); END; $$ LANGUAGE plpgsql;

\sf f_processed_in_plpgsql

select f_processed_in_plpgsql(1,2);

CREATE FUNCTION f_processed_in_columntype(val_1 t_processed.val%TYPE, val_2 t_processed.val2%TYPE) RETURNS int LANGUAGE plpgsql  AS $$ 
BEGIN 
    RETURN(SELECT val2 from t_processed where val=$1 or val2=$2 LIMIT 1); 
END; $$;

\sf f_processed_in_columntype



select f_processed_in_columntype (100,val_2 => 30 );

select * from t_processed where val2 = f_processed_in_columntype (val_1 := 7,val_2 => 300 );

delete t_processed where val2 = f_processed_in_columntype (val_1 := 6,val_2 => 600 );
CREATE FUNCTION f_processed_in_aliases(a int,b int)  RETURNS int LANGUAGE plpgsql AS $$ 
DECLARE 
 val_1 ALIAS FOR a;
 val_2 ALIAS for b;
BEGIN 
    RETURN(SELECT val2 from t_processed where val=val_1 or val2=val_2 LIMIT 1); 
END; $$;

select f_processed_in_aliases (5,b => 350 );

select * from t_processed where val2 = f_processed_in_aliases (a := 4,b => 300 );

delete t_processed where val2 = f_processed_in_aliases (a => 5,b := 500 );

CREATE OR REPLACE FUNCTION f_processed_dynamic(a float, OUT b text, c varchar , d integer)
 LANGUAGE plpgsql
AS $$
BEGIN
 EXECUTE IMMEDIATE 'select name from t_processed where val = $1 LIMIT 1' INTO b USING IN d;
END;  $$;

select f_processed_dynamic(1.1, 'gfds', 4 );

delete t_processed where name = (select f_processed_dynamic(1.1, 'gfds', 4 ));

select f_processed_dynamic(1.1, 'gfds', 4 );

create or replace function f_process_dynamic_no_validation(table_name text,column_name text) returns dec(15,2)as $$
declare
sql_temp text;
balance dec(15,2);
begin
sql_temp := 'select balance from ' || table_name || ' where ' || column_name || ' = ''Bob'' limit 1;' ;
execute immediate sql_temp using out balance;
end;
$$ language plpgsql;

\sf f_process_dynamic_no_validation

CREATE OR REPLACE FUNCTION f_processed_dynamic_ret(a float, OUT b int, c varchar , d integer)
 LANGUAGE plpgsql
AS $$
BEGIN
 EXECUTE IMMEDIATE 'select val  from t_processed where val = $1 LIMIT 1' INTO b USING IN d;
END;  $$;

CREATE OR REPLACE FUNCTION f_processed_dynamic_ret_2(a float, OUT b int, c varchar , d integer)
 LANGUAGE plpgsql
AS $$
BEGIN
 EXECUTE IMMEDIATE 'select val  from t_processed where name  = $1 LIMIT 1' INTO b USING IN c;
END;  $$;

select f_processed_dynamic_ret(1.1,'eight',3);
select f_processed_dynamic_ret_2(1.1,'eight',7);

drop FUNCTION f_processed_in_plpgsql;
drop FUNCTION f_processed_in_columntype;
drop FUNCTION f_processed_in_aliases;
drop FUNCTION f_processed_dynamic;
drop FUNCTION f_process_dynamic_no_validation;
drop FUNCTION f_processed_dynamic_ret;
drop FUNCTION f_processed_dynamic_ret_2;

drop table t_processed;
DROP COLUMN ENCRYPTION KEY plpsql_cek1;
DROP CLIENT MASTER KEY plpsql_cmk1;
\! gs_ktool -d all