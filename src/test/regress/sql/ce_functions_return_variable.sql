\! gs_ktool -d all
\! gs_ktool -g

DROP CLIENT MASTER KEY IF EXISTS ret_cmk3 CASCADE;
CREATE CLIENT MASTER KEY ret_cmk3 WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY ret_cek3 WITH VALUES (CLIENT_MASTER_KEY = ret_cmk3, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);

create table accounts (
    id serial,
    name varchar(100) not null ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = ret_cek3, ENCRYPTION_TYPE = DETERMINISTIC),
    balance dec(15,2) not null ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = ret_cek3, ENCRYPTION_TYPE = DETERMINISTIC),
    primary key(id)
);
insert into accounts(name,balance) values('Bob',10000); 
insert into accounts(name,balance) values('Alice',10000);

CREATE OR REPLACE FUNCTION f_processed_in_plpgsql(a varchar(100), b dec(15,2)) 
RETURNS varchar(100) AS $$
declare
c varchar(100);
BEGIN
    SELECT into c name from accounts where name=$1 or balance=$2 LIMIT 1;
    RETURN c;
END; $$
LANGUAGE plpgsql;

SELECT f_processed_in_plpgsql('Bob', 10000);
CALL f_processed_in_plpgsql('Bob',10000);

DROP FUNCTION f_processed_in_plpgsql();
DROP TABLE accounts;

CREATE TABLE creditcard_info1 (id_number int,name text, credit_card varchar(19));
CREATE TABLE creditcard_info2 (id_number int,name text encrypted with (column_encryption_key = ret_cek3, encryption_type = DETERMINISTIC),credit_card varchar(19) encrypted with (column_encryption_key = ret_cek3, encryption_type = DETERMINISTIC));
CREATE or replace FUNCTION exec_insert1() RETURNS void AS $$
   insert into creditcard_info1 values(1,2,3);
   select credit_card from creditcard_info1;
    $$ LANGUAGE SQL;
CREATE or replace FUNCTION exec_insert2() RETURNS void AS $$
   insert into creditcard_info2 values(1,2,3);
   select credit_card from creditcard_info2;
    $$ LANGUAGE SQL;
CREATE or replace FUNCTION exec_insert1() RETURNS int AS $$
   insert into creditcard_info1 values(1,2,3);
   select credit_card from creditcard_info1;
    $$ LANGUAGE SQL;
CREATE or replace FUNCTION exec_insert2() RETURNS int AS $$
   insert into creditcard_info2 values(1,2,3);
   select credit_card from creditcard_info2;
    $$ LANGUAGE SQL;
DROP TABLE creditcard_info1;
DROP TABLE creditcard_info2;
DROP COLUMN ENCRYPTION KEY ret_cek3;
DROP CLIENT MASTER KEY ret_cmk3;
\! gs_ktool -d all
