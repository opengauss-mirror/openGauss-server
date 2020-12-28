\! gs_ktool -d all
\! gs_ktool -g

DROP CLIENT MASTER KEY IF EXISTS colConstraintCMK CASCADE;
CREATE CLIENT MASTER KEY colConstraintCMK WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY colConstraintCEK WITH VALUES (CLIENT_MASTER_KEY = colConstraintCMK, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);


CREATE TABLE IF NOT EXISTS columns_constraint (
id int,
col1 INTEGER NOT NULL ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = DETERMINISTIC),
col2 decimal(3,2) NULL ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = DETERMINISTIC),
col3 varchar(10) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = DETERMINISTIC),
col4 numeric DEFAULT 9.99 ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = DETERMINISTIC)
);

CREATE TABLE IF NOT EXISTS columns_constraint_unencrypt (
id int,
col1 INTEGER NOT NULL,
col2 decimal(3,2) NULL,
col3 varchar(10),
col4 numeric DEFAULT 9.99
);

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

ALTER TABLE IF EXISTS columns_constraint ADD  COLUMN col5 int ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = DETERMINISTIC);
ALTER TABLE IF EXISTS columns_constraint ADD  COLUMN col6 int unique ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = DETERMINISTIC);
ALTER TABLE IF EXISTS columns_constraint ADD  COLUMN col7 int ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = DETERMINISTIC) check (col7 > 9);
\d columns_constraint;

CREATE TABLE IF NOT EXISTS columns_constraint_alter_check (
id int,
col1 INTEGER NOT NULL ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = DETERMINISTIC),
col2 decimal(3,2) NULL ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = DETERMINISTIC)
);
ALTER TABLE IF EXISTS columns_constraint_alter_check ADD  COLUMN col3 int ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = DETERMINISTIC) check (col3 is not null);
\d columns_constraint_alter_check;



ALTER TABLE IF EXISTS columns_constraint_unencrypt ADD  COLUMN col5 int unique;
ALTER TABLE IF EXISTS columns_constraint_unencrypt ADD  COLUMN col6 int check (col6 is not null);
ALTER TABLE IF EXISTS columns_constraint_unencrypt ADD  COLUMN col7 int ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = DETERMINISTIC);
\d columns_constraint_unencrypt;

DROP TABLE IF EXISTS columns_constraint;
DROP TABLE IF EXISTS columns_constraint_unencrypt;
DROP TABLE IF EXISTS columns_constraint_alter_check;
-- support
CREATE TABLE IF NOT EXISTS columns_alter_constraint (
id int,
col1 INTEGER NOT NULL ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = DETERMINISTIC),
col2 decimal(3,2) NULL ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = DETERMINISTIC),
col3 varchar(10) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = RANDOMIZED));

ALTER TABLE columns_alter_constraint ADD CONSTRAINT test_check CHECK (col3 is NOT NULL);
\d columns_alter_constraint;
ALTER TABLE columns_alter_constraint DROP CONSTRAINT test_check;

ALTER TABLE IF EXISTS columns_alter_constraint ADD CONSTRAINT test_check_col1 CHECK (col1 > 4);

ALTER TABLE IF EXISTS columns_alter_constraint ADD CONSTRAINT test_check_col1 CHECK (col1 > 4 and col3 is not null);

ALTER TABLE IF EXISTS columns_alter_constraint ADD CONSTRAINT test_check_col2 CHECK (id > 5 and col3 is not null);

ALTER TABLE IF EXISTS columns_alter_constraint ADD CONSTRAINT test_check_col3 CHECK (id > 5 and col1 > 4);

ALTER TABLE IF EXISTS columns_alter_constraint ADD CONSTRAINT test_check_col3 CHECK (col2 is not null and col3 is not null);
\d columns_alter_constraint;

ALTER TABLE columns_alter_constraint DROP CONSTRAINT test_check_col2;
ALTER TABLE columns_alter_constraint DROP CONSTRAINT test_check_col3;

ALTER TABLE IF EXISTS columns_alter_constraint ADD CONSTRAINT columns_constraint_primary1 primary key (col1);
ALTER TABLE columns_alter_constraint DROP CONSTRAINT columns_constraint_primary1;
ALTER TABLE IF EXISTS columns_alter_constraint ADD CONSTRAINT columns_constraint_primary2 primary key (id, col1);
\d columns_alter_constraint;
ALTER TABLE columns_alter_constraint DROP CONSTRAINT columns_constraint_primary2;
ALTER TABLE IF EXISTS columns_alter_constraint ADD CONSTRAINT columns_constraint_primary3 primary key (id, col3);
\d columns_alter_constraint;
ALTER TABLE columns_alter_constraint DROP CONSTRAINT columns_constraint_primary3;

CREATE TABLE IF NOT EXISTS columns_alter_unique (
id int,
col1 INTEGER NOT NULL ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = DETERMINISTIC),
col2 decimal(3,2) NULL ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = DETERMINISTIC),
col3 varchar(10) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = RANDOMIZED)); 
ALTER TABLE IF EXISTS columns_alter_unique ADD CONSTRAINT columns_constraint_unique1 UNIQUE (id);
ALTER TABLE IF EXISTS columns_alter_unique ADD CONSTRAINT columns_constraint_unique2 UNIQUE (col1);
ALTER TABLE IF EXISTS columns_alter_unique ADD CONSTRAINT columns_constraint_unique3 UNIQUE (col3);
\d columns_alter_unique;
DROP TABLE IF EXISTS columns_alter_constraint;
DROP TABLE IF EXISTS columns_alter_unique;
-- test check
CREATE TABLE IF NOT EXISTS check_encrypt_age1 (id int, age int  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = DETERMINISTIC) check(age > 10));
CREATE TABLE IF NOT EXISTS check_encrypt_age2 (id int, age int  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = DETERMINISTIC), check(age > 10));

CREATE TABLE IF NOT EXISTS check_encrypt_age3 (id int, age int  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = DETERMINISTIC) check(id > 10));

CREATE TABLE IF NOT EXISTS check_encrypt_id1 (id int, age int  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = DETERMINISTIC), check(id > 10));
CREATE TABLE IF NOT EXISTS check_encrypt_id2 (id int check(id > 10), age int  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = DETERMINISTIC));

CREATE TABLE IF NOT EXISTS check_age_null_1 (id int, age int  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = DETERMINISTIC) check(age IS NOT NULL));
insert into check_age_null_1 values(1, 2);
insert into check_age_null_1 values(2, NULL);
select * from check_age_null_1 order by id;

CREATE TABLE IF NOT EXISTS check_age_null_2 (id int, age int  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = DETERMINISTIC), check(age IS NOT NULL));

insert into check_age_null_2 values(1, 2);
insert into check_age_null_2 values(2, NULL);
select * from check_age_null_2 order by id;


CREATE TABLE IF NOT EXISTS check_random_age1 (id int, age int  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = RANDOMIZED) check(age > 10));
CREATE TABLE IF NOT EXISTS check_random_age2 (id int, age int  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = RANDOMIZED), check(age > 10));
CREATE TABLE IF NOT EXISTS check_random_age3 (id int, age int  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = RANDOMIZED) check(id > 10));

CREATE TABLE IF NOT EXISTS check_random_id1 (id int, age int  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = RANDOMIZED), check(id > 10));
CREATE TABLE IF NOT EXISTS check_random_id2 (id int check(id > 10), age int  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = RANDOMIZED));

CREATE TABLE IF NOT EXISTS check_randomage_null_1 (id int, age int  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = RANDOMIZED) check(age IS NOT NULL));
insert into check_randomage_null_1 values(1, 2);
insert into check_randomage_null_1 values(2, NULL);
select * from check_randomage_null_1 order by id;

CREATE TABLE IF NOT EXISTS check_randomage_null_2 (id int, age int  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = RANDOMIZED), check(age IS NOT NULL));

insert into check_randomage_null_2 values(1, 2);
insert into check_randomage_null_2 values(2, NULL);
select * from check_randomage_null_2 order by id;

CREATE TABLE IF NOT EXISTS check_mult1 (id int, age int  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = DETERMINISTIC), check(id > 10 and age is not null));
CREATE TABLE IF NOT EXISTS check_mult2 (id int, age int  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = DETERMINISTIC) check(id > 10 and age is not null));
\d check_mult2;

CREATE TABLE IF NOT EXISTS check_mult3 (id int, age int  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = DETERMINISTIC) check(id > 10 and age < 100 ));
CREATE TABLE IF NOT EXISTS check_mult4 (id int, age int  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = DETERMINISTIC), check(id > 10 and age < 100 ));

CREATE TABLE IF NOT EXISTS check_mult5 (id int, age int  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = DETERMINISTIC) ,name varchar(10) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = DETERMINISTIC) check(id > 10 or age < 100 and name is not NULL ));
CREATE TABLE IF NOT EXISTS check_mult6 (id int, age int  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = DETERMINISTIC) ,name varchar(10) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = DETERMINISTIC) check(id > 10 or age is not NULL and name is not NULL));

DROP TABLE IF EXISTS check_encrypt_age1;
DROP TABLE IF EXISTS check_encrypt_age2;
DROP TABLE IF EXISTS check_encrypt_age3;
DROP TABLE IF EXISTS check_encrypt_id1;
DROP TABLE IF EXISTS check_encrypt_id2;
DROP TABLE IF EXISTS check_age_null_1;
DROP TABLE IF EXISTS check_age_null_2;
DROP TABLE IF EXISTS check_random_age1;
DROP TABLE IF EXISTS check_random_age2;
DROP TABLE IF EXISTS check_random_age3;

DROP TABLE IF EXISTS check_random_id1;
DROP TABLE IF EXISTS check_random_id2;

DROP TABLE IF EXISTS check_randomage_null_1;
DROP TABLE IF EXISTS check_randomage_null_2;

DROP TABLE IF EXISTS check_mult1;
DROP TABLE IF EXISTS check_mult2;
DROP TABLE IF EXISTS check_mult3;
DROP TABLE IF EXISTS check_mult4;
DROP TABLE IF EXISTS check_mult5;
DROP TABLE IF EXISTS check_mult6;
--support
CREATE TABLE IF NOT EXISTS test_primary_key (
id INT,
col1 INTEGER PRIMARY KEY ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = DETERMINISTIC)
);

\d test_primary_key;

ALTER TABLE test_primary_key  ADD CONSTRAINT check_pri1 PRIMARY KEY(id);
ALTER TABLE test_primary_key  ADD CONSTRAINT check_pri2 PRIMARY KEY(id, col1);

CREATE TABLE IF NOT EXISTS test_primary_key2 (
id INT,
col1 INTEGER ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = DETERMINISTIC), PRIMARY KEY(col1)
);


CREATE TABLE IF NOT EXISTS random_primary_key (
id INT,
col1 INTEGER PRIMARY KEY ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = RANDOMIZED)
);


CREATE TABLE IF NOT EXISTS random_primary_key2 (
id INT,
col1 INTEGER ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = RANDOMIZED), PRIMARY KEY(col1)
);

-- support
CREATE TABLE IF NOT EXISTS tb_primary_key2 (
id int,
col1 INTEGER ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = DETERMINISTIC),
primary key (id, col1)
);

INSERT INTO tb_primary_key2 VALUES (1, 1);


CREATE TABLE IF NOT EXISTS tb_primary_key3 (
id int,
col1 INTEGER ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = RANDOMIZED),
primary key (id, col1)
);


DROP TABLE IF EXISTS test_primary_key;
DROP TABLE IF EXISTS test_primary_key2;
DROP TABLE IF EXISTS random_primary_key;
DROP TABLE IF EXISTS random_primary_key2;
DROP TABLE IF EXISTS random_primary_key;
DROP TABLE IF EXISTS tb_primary_key2;
DROP TABLE IF EXISTS tb_primary_key3;
--  support
CREATE TABLE IF NOT EXISTS unique_constraint (
id int,
col varchar(10) UNIQUE ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = DETERMINISTIC)
);

insert into unique_constraint values (1,'unique1');
insert into unique_constraint values (2,'unique2');
insert into unique_constraint values (3,'unique2');
select * from unique_constraint order by id;

-- no support
CREATE TABLE IF NOT EXISTS unique_constraint (
id int,
col varchar(10) UNIQUE ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = RANDOMIZED)
);

CREATE TABLE IF NOT EXISTS unique_constraint_test (
id int,
col varchar(10) UNIQUE ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = DETERMINISTIC)
);
CREATE TABLE IF NOT EXISTS unique_tb(
    id int, name varchar(10) UNIQUE
);

CREATE TABLE IF NOT EXISTS unique_tb2(
    id int, name varchar(10) UNIQUE
);

DROP TABLE IF EXISTS unique_constraint;
DROP TABLE IF EXISTS unique_constraint_test;
DROP TABLE IF EXISTS unique_tb;
DROP TABLE IF EXISTS unique_tb2;
-- support
CREATE TABLE IF NOT EXISTS table_constraint (
id int,
col1 INTEGER ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = DETERMINISTIC),
col2 decimal(3,2) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = DETERMINISTIC),
UNIQUE (col1, col2)
);

CREATE TABLE IF NOT EXISTS table_constraint_random (
id int,
col1 INTEGER ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = DETERMINISTIC),
col2 decimal(3,2) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = RANDOMIZED),
UNIQUE (col1, col2)
);

CREATE TABLE IF NOT EXISTS tr2ex (i1 INT ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = DETERMINISTIC) , i2 INT, EXCLUDE USING GIST(i1 with =, i2 with <>));

--test empty data
CREATE TABLE IF NOT EXISTS columns_constraint_test (
id int,
col3 varchar(10) NOT NULL ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = colConstraintCEK, ENCRYPTION_TYPE = DETERMINISTIC)
);
insert into columns_constraint_test VALUES (1,'');
select * from columns_constraint_test;
DROP TABLE IF EXISTS columns_constraint_test;

DROP TABLE IF EXISTS table_constraint;
DROP TABLE IF EXISTS table_constraint_random;
DROP COLUMN ENCRYPTION KEY colConstraintCEK;
DROP CLIENT MASTER KEY colConstraintCMK;

\! gs_ktool -d all