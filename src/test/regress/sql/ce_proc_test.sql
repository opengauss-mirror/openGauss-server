\! gs_ktool -d all
\! gs_ktool -g

DROP CLIENT MASTER KEY IF EXISTS proc_cmk2 CASCADE;
CREATE CLIENT MASTER KEY proc_cmk2 WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY proc_cek2 WITH VALUES (CLIENT_MASTER_KEY = proc_cmk2, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);

-- function test1 ,we need to support the operator of insert, select, update,delete CLIENT_LOGIC data in function,and create CLIENT_LOGIC table, create cmk/cek(which will not frush the cache in function now) 
create or replace function fun_001() returns void as $$
declare
begin
    create table schema_tbl_001(a int, b int CLIENT_LOGIC WITH (COLUMN_SETTING = ImgCEK)) ;
    insert into schema_tbl_001 values(1,1);
end;
$$ LANGUAGE plpgsql;
call fun_001();
select * from schema_tbl_001;
\d schema_tbl_001


--function test2
CREATE TABLE sbtest1(
  a int,
  b INTEGER ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = proc_cek2, ENCRYPTION_TYPE = DETERMINISTIC) DEFAULT '0' NOT NULL,
  c CHAR(120) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = proc_cek2, ENCRYPTION_TYPE = DETERMINISTIC) DEFAULT '' NOT NULL,
  d CHAR(60) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = proc_cek2, ENCRYPTION_TYPE = DETERMINISTIC) DEFAULT '' NOT NULL);

create function select_data() returns table(a int, b INTEGER, c CHAR(120), d CHAR(60))
as
$BODY$
begin
return query(select * from sbtest1);
end;
$BODY$
LANGUAGE plpgsql;
call select_data();
--function test3
--normal table
CREATE TABLE basket_a (
    id INT PRIMARY KEY,
    fruit VARCHAR (100) NOT NULL,
    age INT NOT NULL 
);

CREATE TABLE basket_aa(
    id INT,
    fruit VARCHAR (100) NOT NULL ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = proc_cek2, ENCRYPTION_TYPE = DETERMINISTIC), 
    age INT NOT NULL ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = proc_cek2, ENCRYPTION_TYPE = DETERMINISTIC)
);

CREATE FUNCTION MyInsert1(_id integer, _fruit varchar, _age integer)
  RETURNS void AS
  $BODY$
      BEGIN
        INSERT INTO basket_a(id,fruit,age)
        VALUES(_id, _fruit, _age);
      END;
  $BODY$
  LANGUAGE 'plpgsql' VOLATILE
  COST 100;

CREATE FUNCTION MyInsert2(_id integer, _fruit varchar, _age integer)
  RETURNS void AS
  $BODY$
      BEGIN
        INSERT INTO basket_aa(id,fruit,age)
        VALUES(_id, _fruit, _age);
      END;
  $BODY$
  LANGUAGE 'plpgsql' VOLATILE
  COST 100;

select * from MyInsert1(1,'apple',1 );
select * from basket_a;
select * from MyInsert2(1,'apple',1 );
select * from basket_a;


-- procedure test1
CREATE TABLE sbtest2(
  id int,
  k INTEGER ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = proc_cek2, ENCRYPTION_TYPE = DETERMINISTIC) DEFAULT '0' NOT NULL,
  c CHAR(120) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = proc_cek2, ENCRYPTION_TYPE = DETERMINISTIC) DEFAULT '' NOT NULL,
  pad CHAR(60) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = proc_cek2, ENCRYPTION_TYPE = DETERMINISTIC) DEFAULT '' NOT NULL);

insert into sbtest2 values(1,1,1,1);

CREATE OR REPLACE PROCEDURE select2
(
    id IN int,
    k OUT int,
    c OUT int
)
AS
BEGIN
   EXECUTE IMMEDIATE 'select k, c from sbtest2 where id = 1'
       INTO k, c
       USING IN id;
END;
/
call select2(1,a,b);


-- procedure test2
create table staffs(staff_id int,
first_name varchar(20) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = proc_cek2, ENCRYPTION_TYPE = DETERMINISTIC),
salary numeric ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = proc_cek2, ENCRYPTION_TYPE = DETERMINISTIC));

insert into staffs values(1,'Alice',12.23);

CREATE OR REPLACE PROCEDURE dynamic_proc
AS
DECLARE
   staff_id     int;
   first_name   varchar(20);
   salary       numeric;
BEGIN
   EXECUTE IMMEDIATE 'begin select first_name, salary into :first_name, :salary from staffs where staff_id= 1; end;'
       USING OUT first_name, OUT salary, IN staff_id;
   dbe_output.print_line(first_name|| ' ' || salary);
-- I think dbe_output.print_line is not needed to support, because server user function is not need to support, but delete dbe_output.print_line doesn't affect other operator
END;
/
DROP TABLE schema_tbl_001  CASCADE;
DROP TABLE basket_a CASCADE;
DROP TABLE basket_aa CASCADE;
DROP TABLE sbtest1 CASCADE;
DROP TABLE sbtest2 CASCADE;
DROP TABLE staffs CASCADE;
DROP FUNCTION dynamic_proc;
DROP FUNCTION fun_001;
DROP FUNCTION myinsert1;
DROP FUNCTION myinsert2;
DROP FUNCTION select2;
DROP FUNCTION select_data;
DROP CLIENT MASTER KEY proc_cmk2 CASCADE;
\! gs_ktool -d all