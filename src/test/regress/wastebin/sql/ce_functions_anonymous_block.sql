\! gs_ktool -g

DROP CLIENT MASTER KEY IF EXISTS anonymous_block_cmk CASCADE;
CREATE CLIENT MASTER KEY anonymous_block_cmk WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM =  SM4);
CREATE COLUMN ENCRYPTION KEY anonymous_block_cek WITH VALUES (CLIENT_MASTER_KEY = anonymous_block_cmk, ALGORITHM = SM4_SM3);

BEGIN
CREATE TABLE creditcard_info (id_number    int, name         text encrypted with (column_encryption_key = anonymous_block_cek, encryption_type = DETERMINISTIC),
credit_card  varchar(19) encrypted with (column_encryption_key = anonymous_block_cek, encryption_type = DETERMINISTIC));
END;
/

do $$
<<first_block>>
begin
insert into creditcard_info values(0, 'King', '123456');
end first_block $$;
select * from creditcard_info;
delete from creditcard_info;

BEGIN
insert into creditcard_info values(1, 'Avi', '123456');
insert into creditcard_info values(2, 'Eli', '641245');
END;
/

select * from creditcard_info order by id_number;
delete from creditcard_info;

CREATE OR REPLACE PROCEDURE autonomous_1()  AS 
BEGIN
  insert into creditcard_info values(66, 66,66);
  commit;
  insert into creditcard_info values(77, 77,77);
  rollback;
END;
/
call autonomous_1();
select * from creditcard_info order by id_number;

--success without return
CREATE OR REPLACE PROCEDURE exec_insert1 () AS
BEGIN
    insert into creditcard_info values(3, 'Rafi', '3');
    update creditcard_info set name='Sun' where credit_card = 3;
END;
/
call exec_insert1 ();
--success  return void
CREATE or replace FUNCTION exec_insert2() RETURN void
AS
BEGIN 
   insert into creditcard_info values(4,'Gil',4);
   update creditcard_info set name='Joy' where credit_card = 4; 
END;
/
SELECT exec_insert2();
call exec_insert2();
--success RETURN integer
CREATE or replace FUNCTION exec_insert3() RETURN integer
AS
BEGIN 
   insert into creditcard_info values(5,'Peter',5);
   update creditcard_info set name= 'Xavier' where credit_card = 5; 
   return 1;
END;
/
SELECT exec_insert3();
call exec_insert3();

-- plpgsql IF 
CREATE or replace FUNCTION exec_insert4() RETURN void
AS
BEGIN
IF 2<5 THEN 
   insert into creditcard_info values(6,'Ziv',6);
   update creditcard_info set name='Peter' where credit_card = 6; 
END IF;
END;
/
SELECT exec_insert4();
call exec_insert4();
select * from creditcard_info order by id_number;

DROP TABLE creditcard_info;
DROP CLIENT MASTER KEY anonymous_block_cmk CASCADE;
\! gs_ktool -d all

