\! gs_ktool -d all
\! gs_ktool -g

DROP CLIENT MASTER KEY IF EXISTS charCMK CASCADE;
CREATE CLIENT MASTER KEY charCMK WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY charCEK WITH VALUES (CLIENT_MASTER_KEY = charCMK, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);


create table IF NOT EXISTS char_type(
c1 int,
c2 char(2) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = charCEK, ENCRYPTION_TYPE = DETERMINISTIC),
c3 char ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = charCEK, ENCRYPTION_TYPE = DETERMINISTIC),
c4 VARCHAR(2) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = charCEK, ENCRYPTION_TYPE = DETERMINISTIC),
c5 VARCHAR ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = charCEK, ENCRYPTION_TYPE = DETERMINISTIC),
c6 NVARCHAR2(2) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = charCEK, ENCRYPTION_TYPE = DETERMINISTIC),
c7 NVARCHAR2 ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = charCEK, ENCRYPTION_TYPE = DETERMINISTIC),
c8 CLOB,
c9 name,
c10 "char" ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = charCEK, ENCRYPTION_TYPE = DETERMINISTIC),
c11 text ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = charCEK, ENCRYPTION_TYPE = DETERMINISTIC)
);


insert into char_type values(1, 'aa', 'a','aa','aaaaaaaaaaaaa', 'aa','aaaaaaaaaaaaaaaa', 'aaaa', 'aaaaa', 'aaaaaaaaaaaaaaaaaaaaaa','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');
insert into char_type(c1, c2) values(2, 'aaa');
insert into char_type(c1, c3) values(3, 'aa');
insert into char_type(c1, c4) values(4, 'aaa');
insert into char_type(c1, c6) values(5, 'aaa');
insert into char_type values(6, 'bb', 'b','bb','bbbbaaaaaaaaaaaaa', 'aa','aaaaaaaaaaaaaaaa', 'aaaa', 'aaaaa', 'aaaaaaaaaaaaaaaaaaaaaa','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');
insert into char_type values(7, 'cc', 'c','cc','ccccbbbbaaaaaaaaaaaaa', 'aa','aaaaaaaaaaaaaaaa', 'aaaa', 'aaaaa', 'aaaaaaaaaaaaaaaaaaaaaa','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');
insert into char_type values(8, 'cc', 'c','cc','ccc', 'aa','a', 'a', 'a', 'a',repeat('a', 5));
insert into char_type values(9, 'bb', 'c','cc','ccc', 'aa','a', 'a', 'a', 'a',repeat('a', 0));
insert into char_type values(10, 'bb', 'c','cc','ccc', 'aa','a', 'a', repeat('a', 0), 'a',repeat('a', 5));
insert into char_type values(11, 'bb', 'c','cc','ccc', 'aa','a', 'a', 'a', 'a','');

--when insert into encrypted char, it lost its range, "char" excepted.
SELECT * FROM char_type ORDER BY c1;
SELECT * FROM char_type where c11 ='' ORDER BY c1;
delete from char_type where c11 ='';
SELECT * FROM char_type where c11 ='' ORDER BY c1;

SELECT * from char_type where c2 = 'bb' order by c1;

DELETE FROM char_type where c2 = 'bb';
SELECT * from char_type order by c1;

DELETE FROM char_type as alias_test where alias_test.c3 = 'a';
SELECT * from char_type order by c1;

UPDATE char_type SET c4 = 'dd' where c4 = 'cc';

SELECT * from char_type order by c1;

-- test empty data
insert into char_type values(8, '', '','','', '','', '', '', '','');
insert into char_type values(9, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
SELECT * from char_type order by c1;

create table IF NOT EXISTS char_type_enc1(
c1 int,
c2 char(2) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = charCEK, ENCRYPTION_TYPE = DETERMINISTIC),
c3 char ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = charCEK, ENCRYPTION_TYPE = DETERMINISTIC),
c4 VARCHAR(2) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = charCEK, ENCRYPTION_TYPE = DETERMINISTIC),
c5 VARCHAR ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = charCEK, ENCRYPTION_TYPE = DETERMINISTIC),
c6 NVARCHAR2(2) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = charCEK, ENCRYPTION_TYPE = DETERMINISTIC),
c7 NVARCHAR2,
c8 CLOB,
c9 name,
c10 "char" ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = charCEK, ENCRYPTION_TYPE = DETERMINISTIC),
c11 text ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = charCEK, ENCRYPTION_TYPE = DETERMINISTIC)
);


create table IF NOT EXISTS char_type_enc3(
c1 int,
c2 char(2) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = charCEK, ENCRYPTION_TYPE = DETERMINISTIC),
c3 char ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = charCEK, ENCRYPTION_TYPE = DETERMINISTIC),
c4 VARCHAR(2) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = charCEK, ENCRYPTION_TYPE = DETERMINISTIC),
c5 VARCHAR ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = charCEK, ENCRYPTION_TYPE = DETERMINISTIC),
c6 NVARCHAR2(2) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = charCEK, ENCRYPTION_TYPE = DETERMINISTIC),
c7 NVARCHAR2 ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = charCEK, ENCRYPTION_TYPE = DETERMINISTIC),
c8 CLOB,
c9 name  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = charCEK, ENCRYPTION_TYPE = DETERMINISTIC),
c10 "char" ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = charCEK, ENCRYPTION_TYPE = DETERMINISTIC),
c11 text ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = charCEK, ENCRYPTION_TYPE = DETERMINISTIC)
);

DROP TABLE IF exists char_type;
DROP TABLE IF exists char_type_enc1;
DROP TABLE IF exists char_type_enc2;
DROP TABLE IF exists char_type_enc3;
DROP COLUMN ENCRYPTION KEY charCEK;
DROP CLIENT MASTER KEY charCMK;

\! gs_ktool -d all
