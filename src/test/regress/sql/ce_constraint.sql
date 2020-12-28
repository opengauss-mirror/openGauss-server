\! gs_ktool -d all
\! gs_ktool -g

CREATE CLIENT MASTER KEY colConstraintCMK WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY colConstraintCEK WITH VALUES (CLIENT_MASTER_KEY = colConstraintCMK, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);


CREATE TABLE IF NOT EXISTS columns_constraint (
id int,
col1 INTEGER NOT NULL ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = DETERMINISTIC),
col2 decimal(3,2) NULL ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = DETERMINISTIC) ,
col3 varchar(10) UNIQUE ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = DETERMINISTIC),
col4 numeric DEFAULT 9.99 ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = DETERMINISTIC)
) DISTRIBUTE BY hash (id);


INSERT INTO columns_constraint VALUES (1, 10, 1.34, 'John', DEFAULT);
INSERT INTO columns_constraint VALUES (2, NULL, 1.34, 'Moses', DEFAULT);
INSERT INTO columns_constraint VALUES (3, 10, NULL, 'Alex', DEFAULT);
INSERT INTO columns_constraint VALUES (4, 10, NULL, 'John', DEFAULT);
INSERT INTO columns_constraint VALUES (5, 10, NULL, 'Jorgen', 10.1);

select * from columns_constraint order by id;
ALTER TABLE columns_constraint ALTER COLUMN col4 SET DEFAULT 7.77;
INSERT INTO columns_constraint VALUES (6, 10, 1.34, 'UNIQUE5', DEFAULT);
SELECT * FROM columns_constraint order by id;

ALTER TABLE columns_constraint ALTER COLUMN col4 DROP DEFAULT;
INSERT INTO columns_constraint VALUES (7, 10, 1.34, 'UNIQUE6', DEFAULT);
SELECT * FROM columns_constraint order by id;


ALTER TABLE columns_constraint ADD CONSTRAINT test_check CHECK (col4 is NOT NULL);
INSERT INTO columns_constraint(id, col4) VALUES (7, 10, 1.34, 'UNIQUE6', NULL);

DROP TABLE columns_constraint;

-- no support
create table IF NOT EXISTS check_encrypt (id int, age int  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = imgCEK, ENCRYPTION_TYPE = DETERMINISTIC) check(age > 10));

--no support
CREATE TABLE IF NOT EXISTS tb_primary_key (
id int,
col1 INTEGER primary key ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCMK, ENCRYPTION_TYPE = DETERMINISTIC)
) DISTRIBUTE BY hash (id);

-- support
CREATE TABLE IF NOT EXISTS tb_primary_key2 (
id int,
col1 INTEGER ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCMK, ENCRYPTION_TYPE = DETERMINISTIC),
primary key (id, col1)
) DISTRIBUTE BY hash (id);

INSERT INTO table_constraint VALUES (1, 10);
INSERT INTO table_constraint VALUES (1, 10);
INSERT INTO table_constraint VALUES (2, 10);
INSERT INTO table_constraint VALUES (1, 11);
select * from tb_primary_key2;

drop table tb_primary_key;
drop table tb_primary_key2;

CREATE TABLE IF NOT EXISTS table_constraint (
id int,
col1 INTEGER ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCMK, ENCRYPTION_TYPE = DETERMINISTIC),
col2 decimal(3,2) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCMK, ENCRYPTION_TYPE = DETERMINISTIC),
UNIQUE (col1, col2)
);

INSERT INTO table_constraint VALUES (1, 10, 1.23);
INSERT INTO table_constraint VALUES (2, 10, 1.23);
INSERT INTO table_constraint VALUES (3, 10, 1.43);
INSERT INTO table_constraint VALUES (4, 12, 1.23);

select * from table_constraint;

drop table table_constraint;

select  count(*), 'count' from gs_client_global_keys;
select  count(*), 'count' from gs_column_keys;

\! gs_ktool -d all