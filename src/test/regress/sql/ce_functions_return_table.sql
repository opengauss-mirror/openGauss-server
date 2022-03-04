\! gs_ktool -d all
\! gs_ktool -g

DROP CLIENT MASTER KEY IF EXISTS ret_cmk1 CASCADE;
CREATE CLIENT MASTER KEY ret_cmk1 WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY ret_cek1 WITH VALUES (CLIENT_MASTER_KEY = ret_cmk1, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);

create table accounts (
    id serial,
    name varchar(100) not null ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = ret_cek1, ENCRYPTION_TYPE = DETERMINISTIC),
    balance dec(15,2) not null ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = ret_cek1, ENCRYPTION_TYPE = DETERMINISTIC),
    primary key(id)
);
insert into accounts(name,balance) values('Bob',10000);
insert into accounts(name,balance) values('Alice',10000);

CREATE FUNCTION select2() RETURNS accounts LANGUAGE SQL AS 'SELECT * from accounts;'; 
CREATE FUNCTION select4() RETURNS SETOF accounts LANGUAGE SQL AS 'SELECT * from accounts;';
CALL select2();
CALL select4();

DROP TABLE IF EXISTS fuc_creditcard_info;
CREATE TABLE fuc_creditcard_info (id_number int, name text encrypted with (column_encryption_key = ret_cek1, encryption_type = DETERMINISTIC),
credit_card varchar(19) encrypted with (column_encryption_key = ret_cek1, encryption_type = DETERMINISTIC));
INSERT INTO fuc_creditcard_info VALUES (1,2,3);
--函数定义的返回表字段类型与加密表字段类型一致，可以正常加解密
DROP FUNCTION IF EXISTS select5();
CREATE or replace FUNCTION select5() RETURNS TABLE (
 name text ,
 credit_card varchar(19)
) LANGUAGE SQL
AS 'SELECT name, credit_card from fuc_creditcard_info;';
call select5();
--函数定义的返回表字段类型为VARCHAR与加密表name的text类型不一致，可以正常加解密
DROP FUNCTION IF EXISTS select6;
CREATE or replace FUNCTION select6() RETURNS TABLE (
name VARCHAR,
credit_card VARCHAR
) LANGUAGE SQL
AS 'SELECT name, credit_card from fuc_creditcard_info;';
call select6();
--函数定义的返回表字段类型为INT与加密表字段类型varchar(19)不一致，报错
DROP FUNCTION IF EXISTS select7;
CREATE or replace FUNCTION select7() RETURNS TABLE (
name text,
credit_card INT
) LANGUAGE SQL
AS 'SELECT name, credit_card from fuc_creditcard_info;';

DROP FUNCTION select2();
DROP FUNCTION select4();
DROP FUNCTION select5();
DROP FUNCTION select6();
DROP FUNCTION select7();
DROP TABLE accounts;
DROP TABLE fuc_creditcard_info;
DROP COLUMN ENCRYPTION KEY ret_cek1;
DROP CLIENT MASTER KEY ret_cmk1;
\! gs_ktool -d all