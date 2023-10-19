\! gs_ktool -d all
\! gs_ktool -g

CREATE CLIENT MASTER KEY intCMK WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY intCEK WITH VALUES (CLIENT_MASTER_KEY = intCMK, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);

CREATE TABLE IF NOT EXISTS int_type (id INT, 
int_col1 TINYINT ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = intCEK, ENCRYPTION_TYPE = DETERMINISTIC),
int_col2 smallint ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = intCEK, ENCRYPTION_TYPE = DETERMINISTIC),
int_col3 INTEGER ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = intCEK, ENCRYPTION_TYPE = DETERMINISTIC),
int_col4 BINARY_INTEGER ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = intCEK, ENCRYPTION_TYPE = DETERMINISTIC),
int_col5 BIGINT ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = intCEK, ENCRYPTION_TYPE = DETERMINISTIC)
);



ALTER TABLE int_type ADD COLUMN int_col6 int ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = intCEK, ENCRYPTION_TYPE = DETERMINISTIC);

\d int_type

select column_name, encryption_type, data_type_original_oid, data_type_original_mod from gs_encrypted_columns;

ALTER TABLE int_type DROP COLUMN int_col6;

\d int_type

select column_name, encryption_type, data_type_original_oid, data_type_original_mod from gs_encrypted_columns;

--TINYINT 0~255 smallint -32,768 ~ +32,767 integer -2,147,483,648 ~ +2,147,483,647 
--binary_integer -2,147,483,648 ~ +2,147,483,647 bigint -9,223,372,036,854,775,808 ~9,223,372,036,854,775,807
INSERT INTO int_type VALUES (1, 0, 0, 0, 0, 0);
INSERT INTO int_type VALUES (2, 255, 32767, 2147483647, 2147483647, 9223372036854775807);
INSERT INTO int_type VALUES (3, 0, -32768, -2147483648, -2147483648, -9223372036854775808);
INSERT INTO int_type VALUES (4, 20, 34, 565, 55, 67);
INSERT INTO int_type VALUES (5, -20, -34, -565, -55, -67);
INSERT INTO int_type (id, int_col1) VALUES (6, -1);
INSERT INTO int_type (id, int_col1) VALUES (7, 256);
INSERT INTO int_type (id, int_col2) VALUES (8, 32768);
INSERT INTO int_type (id, int_col2) VALUES (9, -32769);
INSERT INTO int_type (id, int_col3) VALUES (10, 2147483648);
INSERT INTO int_type (id, int_col3) VALUES (11, -2147483649);
INSERT INTO int_type (id, int_col4) VALUES (12, 2147483648);
INSERT INTO int_type (id, int_col4) VALUES (13, -2147483649);
INSERT INTO int_type (id, int_col5) VALUES (14, 9223372036854775808);
INSERT INTO int_type (id, int_col5) VALUES (15, -9223372036854775809);

SELECT * from int_type order by id;
SELECT * from int_type where int_col1 = 555;

DELETE FROM int_type where int_col1=255;
SELECT * from int_type order by id;

DELETE FROM int_type as alias_test where alias_test.int_col1 =20;
SELECT * from int_type order by id;

UPDATE int_type SET int_col1 = -200 where int_col1 = -20;

SELECT * from int_type order by id;

DROP TABLE int_type;

-- verify encrypted column deleted
SELECT column_name from gs_encrypted_columns;

create table IF NOT EXISTS serial_type_enc1(
c1 int, 
c2 SMALLSERIAL ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = intCEK, ENCRYPTION_TYPE = DETERMINISTIC), 
c3 SERIAL ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = intCEK, ENCRYPTION_TYPE = DETERMINISTIC), 
c4 BIGSERIAL ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = intCEK, ENCRYPTION_TYPE = DETERMINISTIC)
);

create table IF NOT EXISTS serial_type_enc2(
c1 int, 
c2 SMALLSERIAL, 
c3 SERIAL ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = intCEK, ENCRYPTION_TYPE = DETERMINISTIC), 
c4 BIGSERIAL ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = intCEK, ENCRYPTION_TYPE = DETERMINISTIC)
);

create table IF NOT EXISTS serial_type_enc3(
c1 int, 
c2 SMALLSERIAL, 
c3 SERIAL, 
c4 BIGSERIAL ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = intCEK, ENCRYPTION_TYPE = DETERMINISTIC)
);
--SMALLSERIAL  1 ~ 32,767
--SERIAL 1 ~ 2,147,483,647
--BIGSERIAL  1 ~ 9,223,372,036,854,775,807

--INSERT INTO serial_type VALUES(1, default, default, default);
--INSERT INTO serial_type VALUES(2, default, default, default);
--INSERT INTO serial_type VALUES(3, 1, 1, 1);
--INSERT INTO serial_type VALUES(4, 32767, 2147483647, 9223372036854775807);
--INSERT INTO serial_type(c1, c2) VALUES(5, 32768);
--INSERT INTO serial_type(c1, c2) VALUES(6, 0);
--INSERT INTO serial_type(c1, c3) VALUES(7, 2147483648);
--INSERT INTO serial_type(c1, c3) VALUES(8, 0);
--INSERT INTO serial_type(c1, c4) VALUES(9, 9223372036854775808);
--INSERT INTO serial_type(c1, c4) VALUES(10, 0);

--INSERT INTO serial_type(c1, c2) VALUES(11, -1);
--INSERT INTO serial_type(c1, c3) VALUES(12, -1);
--INSERT INTO serial_type(c1, c4) VALUES(13, -1);
--SELECT * FROM serial_type order by c1;

DROP TABLE IF EXISTS serial_type;
DROP TABLE IF EXISTS serial_type_enc1;
DROP TABLE IF EXISTS serial_type_enc2;
DROP TABLE IF EXISTS serial_type_enc3;
DROP COLUMN ENCRYPTION KEY intCEK;
DROP CLIENT MASTER KEY intCMK;
DROP CLIENT MASTER KEY IF EXISTS intCMK CASCADE;

\! gs_ktool -d all