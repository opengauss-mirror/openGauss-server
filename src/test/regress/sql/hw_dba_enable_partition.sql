--null
CREATE TABLE test_null(a INT NULL);
CREATE TABLE test_not_null_enable(a INT NULL ENABLE);--fail
--default
CREATE TABLE test_default(a INT DEFAULT 2);
CREATE TABLE test_default_enable(a INT DEFAULT 2 ENABLE);--fail
--not null
CREATE TABLE test_not_null(a INT NOT NULL);
CREATE TABLE test_not_null_enable(a INT NOT NULL ENABLE);
--unique
CREATE TABLE test_unique(a INT UNIQUE);
CREATE TABLE test_unique_enable(a INT UNIQUE ENABLE);
--primary key
CREATE TABLE test_primary_key(a INT PRIMARY KEY);
CREATE TABLE test_primary_key_enable(a INT PRIMARY KEY ENABLE);
--check
CREATE TABLE test_check(a INT  CHECK(a<10));
CREATE TABLE test_check_enable(a INT CHECK(a<10) ENABLE);
--references
CREATE TABLE test_references_a(a INT UNIQUE);
CREATE TABLE test_references_b(a INT REFERENCES test_references_a(a));
CREATE TABLE test_references_b_enbale(a INT REFERENCES test_references_a(a) ENABLE);

--constraint null
CREATE TABLE test_con_null(a INT CONSTRAINT con_null NULL);
CREATE TABLE test_con_null_enable(a INT CONSTRAINT con_null_enable NULL ENABLE); --fail
--constraint not null
CREATE TABLE test_con_not_null(a INT CONSTRAINT con_not_null NOT NULL);
CREATE TABLE test_con_not_null_enable(a INT CONSTRAINT con_not_null_enable NOT NULL ENABLE);
--constraint unique
CREATE TABLE test_con_unique(a INT CONSTRAINT un UNIQUE);
CREATE TABLE test_con_unique_enable(a INT CONSTRAINT un_enable UNIQUE ENABLE);
--constraint primary key
CREATE TABLE test_con_primary_key(a INT CONSTRAINT pk PRIMARY KEY);
CREATE TABLE test_con_primary_key_enable(a INT CONSTRAINT pk_enbale PRIMARY KEY ENABLE);
--constraint check
CREATE TABLE test_con_check(a INT CONSTRAINT ck CHECK(a<10));
CREATE TABLE test_con_check_enable(a INT CONSTRAINT ck_enable CHECK(a<10) ENABLE);
--constraint references
CREATE TABLE test_con_references_a(a INT CONSTRAINT unq UNIQUE);
CREATE TABLE test_con_references_b(a INT CONSTRAINT rf REFERENCES test_con_references_a(a));
CREATE TABLE test_con_references_b_enbale(a INT CONSTRAINT rf_enable REFERENCES test_con_references_a(a) ENABLE);

--create table named enable and a column named enable
CREATE TABLE enable(enable INT);

DROP TABLE test_null;
DROP TABLE test_default;
DROP TABLE test_not_null;
DROP TABLE test_not_null_enable;
DROP TABLE test_unique;
DROP TABLE test_unique_enable;
DROP TABLE test_primary_key;
DROP TABLE test_primary_key_enable;
DROP TABLE test_check;
DROP TABLE test_check_enable;
DROP TABLE test_references_b;
DROP TABLE test_references_b_enbale;
DROP TABLE test_references_a;
DROP TABLE test_con_null;
DROP TABLE test_con_not_null;
DROP TABLE test_con_not_null_enable;
DROP TABLE test_con_unique;
DROP TABLE test_con_unique_enable;
DROP TABLE test_con_primary_key;
DROP TABLE test_con_primary_key_enable;
DROP TABLE test_con_check;
DROP TABLE test_con_check_enable;
DROP TABLE test_con_references_b;
DROP TABLE test_con_references_b_enbale;
DROP TABLE test_con_references_a;
DROP TABLE enable;
