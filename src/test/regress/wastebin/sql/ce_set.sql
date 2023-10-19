\! gs_ktool -d all
\! gs_ktool -g

create table t3(a int, b int);
insert into t3 values(1, 2);
create table t2(c int, d int);
insert into t2 values(3, 4);
SELECT * FROM t3;
SELECT * FROM t2;
update t3 set b=(select c from t2 where d=4) where a=1;
SELECT * FROM t3;
SELECT * FROM t2;
drop table t3;
drop table t2;

DROP CLIENT MASTER KEY IF EXISTS MyCMK CASCADE;
CREATE CLIENT MASTER KEY MyCMK WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY MyCEK WITH VALUES (CLIENT_MASTER_KEY = MyCMK, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);
CREATE COLUMN ENCRYPTION KEY MyCEK2 WITH VALUES (CLIENT_MASTER_KEY = MyCMK, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);
create table t3(a int ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC), b int);
insert into t3 values(1, 2);
create table t2(c int, d int ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK2, ENCRYPTION_TYPE = DETERMINISTIC));
insert into t2 values(3, 4);
SELECT * FROM t3;
SELECT * FROM t2;
-- Distributed key column can't be updated in current version
update t3 set b=(select c from t2 where d=4) where a=1;
SELECT * FROM t3;
SELECT * FROM t2;
drop table t3;
drop table t2;

create table t3(id int, a int ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC), b int);
insert into t3 values(1, 1, 2);
create table t2(id int, c int, d int ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK2, ENCRYPTION_TYPE = DETERMINISTIC));
insert into t2 values(3, 3, 4);
SELECT * FROM t3;
SELECT * FROM t2;
update t3 set b=(select c from t2 where d=4) where a=1;
update t3 set a=(select d from t2 where c=3) where b=3;
--unsupport
update t3 set b=(select d from t2 where c=3) where a=4;
SELECT * FROM t3;
SELECT * FROM t2;
drop table t3;
drop table t2;
DROP COLUMN ENCRYPTION KEY MyCEK;
DROP COLUMN ENCRYPTION KEY MyCEK2;
DROP CLIENT MASTER KEY MyCMK;

\! gs_ktool -d all
