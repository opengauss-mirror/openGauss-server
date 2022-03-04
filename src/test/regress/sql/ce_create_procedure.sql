\! gs_ktool -d all
\! gs_ktool -g

DROP CLIENT MASTER KEY IF EXISTS proc_cmk1 CASCADE;
CREATE CLIENT MASTER KEY proc_cmk1 WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY proc_cek1 WITH VALUES (CLIENT_MASTER_KEY = proc_cmk1, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);

create table accounts (
    id serial,
    name varchar(100) not null ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = proc_cek1, ENCRYPTION_TYPE = DETERMINISTIC),
    balance dec(15,2) not null ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = proc_cek1, ENCRYPTION_TYPE = DETERMINISTIC),
    primary key(id)
);

insert into accounts(name,balance)
values('Bob',10000);

insert into accounts(name,balance)
values('Alice',10000);

select * from accounts;

create or replace procedure transfer(
   sender int,
   receiver int, 
   amount dec
)
as
begin
    -- subtracting the amount from the sender's account 
    update accounts 
    set balance = balance - amount 
    where id = sender;

    -- adding the amount to the receiver's account
    update accounts 
    set balance = balance + amount 
    where id = receiver;

    commit;
end;
/

call transfer(1,2,1000);
SELECT * FROM accounts;

drop table if exists accounts;

create table accounts (
    id serial,
    name varchar(100) not null ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = proc_cek1, ENCRYPTION_TYPE = DETERMINISTIC),
    balance dec(15,2) not null ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = proc_cek1, ENCRYPTION_TYPE = DETERMINISTIC),
    primary key(id)
) DISTRIBUTE BY REPLICATION;

insert into accounts(name,balance)
values('Bob',10000);

insert into accounts(name,balance)
values('Alice',10000);

select * from accounts;

create or replace procedure transfer(
   sender int,
   receiver int, 
   amount dec
)
as
begin
    -- subtracting the amount from the sender's account 
    update accounts 
    set balance = balance - amount 
    where id = sender;

    -- adding the amount to the receiver's account
    update accounts 
    set balance = balance + amount 
    where id = receiver;

    commit;
end;
/

call transfer(1,2,1000);
SELECT * FROM accounts;
DROP TABLE accounts CASCADE;
DROP FUNCTION transfer;
DROP COLUMN ENCRYPTION KEY proc_cmk1;
DROP CLIENT MASTER KEY proc_cek1;
\! gs_ktool -d all
