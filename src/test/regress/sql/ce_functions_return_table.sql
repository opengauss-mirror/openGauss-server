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
SELECT select2();
SELECT select4();

DROP FUNCTION select2();
DROP FUNCTION select4();
DROP TABLE accounts;
DROP COLUMN ENCRYPTION KEY ret_cek1;
DROP CLIENT MASTER KEY ret_cmk1;
\! gs_ktool -d all