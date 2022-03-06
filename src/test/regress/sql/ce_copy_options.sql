\! gs_ktool -d all
\! gs_ktool -g

DROP CLIENT MASTER KEY IF EXISTS copyCMK CASCADE;
CREATE CLIENT MASTER KEY copyCMK WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
-- test AEAD_AES_128_CBC_HMAC_SHA256
CREATE COLUMN ENCRYPTION KEY copyCEK1 WITH VALUES (CLIENT_MASTER_KEY = copyCMK, ALGORITHM = AEAD_AES_128_CBC_HMAC_SHA256);
CREATE COLUMN ENCRYPTION KEY copyCEK2 WITH VALUES (CLIENT_MASTER_KEY = copyCMK, ALGORITHM = AEAD_AES_128_CBC_HMAC_SHA256);
CREATE COLUMN ENCRYPTION KEY copyCEK3 WITH VALUES (CLIENT_MASTER_KEY = copyCMK, ALGORITHM = AEAD_AES_128_CBC_HMAC_SHA256);


CREATE TABLE IF NOT EXISTS CopyTbl(
    i0 INT, 
    i1 INT ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = copyCEK1, ENCRYPTION_TYPE = DETERMINISTIC), 
    i2 TEXT ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = copyCEK2, ENCRYPTION_TYPE = DETERMINISTIC),
    i3 TEXT ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = copyCEK3, ENCRYPTION_TYPE = DETERMINISTIC)  default 'stuff'
    );




-- 1 check copy from
-- 1.a check copy the whole table
-- missing data: should fail
COPY CopyTbl FROM stdin;

\.

-- check \N
copy CopyTbl from stdin;
1	\N	\\N	\NN
\.

--1.b check copy part of the table
COPY CopyTbl (i0, i1,i2) FROM stdin;
10	10	7
11	20	8
\.

copy CopyTbl(i0,i2) from stdin;
1001	12
\.

-- should fail: non-existent column in column list
copy CopyTbl(col2) from stdin;

-- should fail: too many columns in column list
copy CopyTbl(i0,i1,i2,i3,i1,i3) from stdin;

SELECT * FROM CopyTbl ORDER BY i0;


--3 check options

--3.a format
COPY CopyTbl from stdin(FORMAT CSV);
3000,1,2,3
\.

COPY CopyTbl from stdin(FORMAT TEXT);
\.

--3.b oids DO NOT SUPPORT oids
-- should fail: table "CopyTbl" does not have OIDs
COPY CopyTbl from stdin WITH OIDS;

--3.c option:delimiter
copy CopyTbl from stdin with delimiter ',';
1002,1,2,3
\.

--should fail
copy CopyTbl from stdin with delimiter 'a';
--should fail
copy CopyTbl from stdin with delimiter E'\r';
--should fail:  delimiter must be no more than 10 bytes
copy CopyTbl from stdin with delimiter '|,%^&*@#$%%^||||';


--3.d option:null force not null
COPY CopyTbl from stdin WITH NULL AS '';
1006		2	3
\.

--should fail
COPY CopyTbl from stdin WITH NULL AS E'\r';
--should fail
COPY CopyTbl from stdin WITH delimiter ',' NULL ',';
--should fail
COPY CopyTbl from stdin WITH CSV quote ',' NULL ',';

-- force not null only available in csv mode and copy from
-- ? no use
COPY CopyTbl from stdin WITH CSV FORCE NOT NULL i2;
1,2,3,4
\.
COPY CopyTbl from stdin (FORMAT CSV, FORCE_NOT_NULL(i2));
1,2,3,4
\.


--3.e option:quote force_quote
COPY CopyTbl TO stdout WITH csv;
COPY CopyTbl TO stdout WITH csv quote '''' delimiter '|';

COPY CopyTbl TO stdout WITH CSV FORCE QUOTE i3;
COPY CopyTbl TO stdout WITH CSV FORCE QUOTE *;


--3.f escape
--fail to decrypt
-- COPY CopyTbl TO stdout (FORMAT CSV, ESCAPE E'\\');


--3.g option: eol
-- fail
-- COPY CopyTbl from stdin WITH EOL 'EOL_CRNL';
-- COPY CopyTbl from stdin WITH EOL 'EOL_CR';
-- COPY CopyTbl from stdin WITH EOL 'EOL_NL';


--3.h ignore extra data
copy CopyTbl from stdin with delimiter '|' ignore_extra_data;
1|2|3|4|5
\.


--3.h encoding 
COPY CopyTbl to stdout WITH DELIMITER AS ',' ENCODING 'utf8';
COPY CopyTbl to stdout WITH DELIMITER AS ',' ENCODING 'sql_ascii';


--4 check copy out
COPY CopyTbl TO stdout WITH CSV;
COPY CopyTbl TO stdout WITH CSV QUOTE '''' DELIMITER '|';
COPY CopyTbl TO stdout WITH CSV FORCE QUOTE *;
COPY CopyTbl TO stdout WITH CSV FORCE QUOTE i2 ENCODING 'sql_ascii';
-- Repeat above tests with new 9.0 option syntax
COPY CopyTbl TO stdout (FORMAT CSV);
COPY CopyTbl TO stdout (FORMAT TEXT);
COPY CopyTbl TO stdout (FORMAT CSV, QUOTE '''', DELIMITER '|');
COPY CopyTbl TO stdout (FORMAT CSV, FORCE_QUOTE *);
COPY CopyTbl TO stdout (FORMAT CSV, FORCE_QUOTE(i2),ENCODING 'sql_ascii');
-- Repeat above tests with \copy
\copy CopyTbl TO stdout (FORMAT CSV);
\copy CopyTbl TO stdout (FORMAT TEXT);
\copy CopyTbl TO stdout (FORMAT CSV, QUOTE '''', DELIMITER '|');
\copy CopyTbl TO stdout (FORMAT CSV, FORCE_QUOTE *);
\copy CopyTbl TO stdout (FORMAT CSV, FORCE_QUOTE(i2),ENCODING 'sql_ascii');


-- test end of copy marker
CREATE COLUMN ENCRYPTION KEY copyCEK4 WITH VALUES (CLIENT_MASTER_KEY = copyCMK, ALGORITHM = AEAD_AES_128_CBC_HMAC_SHA256);
create table test_eoc(
    a int,
    b text ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = copyCEK4, ENCRYPTION_TYPE = DETERMINISTIC)
);

copy test_eoc from stdin csv;
1,a\.
2,\.b
3,c\.d
4,"\."
\.

select * from test_eoc order by a;

--5 check copy select
CREATE COLUMN ENCRYPTION KEY copyCEK5 WITH VALUES (CLIENT_MASTER_KEY = copyCMK, ALGORITHM = AEAD_AES_128_CBC_HMAC_SHA256);
create table test_select(
    a int,
    b text ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = copyCEK5, ENCRYPTION_TYPE = DETERMINISTIC)
);

insert into test_select values (1, 'a');
insert into test_select values (2, 'b');
insert into test_select values (3, 'c');
insert into test_select values (4, 'd');
insert into test_select values (5, 'e');

CREATE COLUMN ENCRYPTION KEY copyCEK6 WITH VALUES (CLIENT_MASTER_KEY = copyCMK, ALGORITHM = AEAD_AES_128_CBC_HMAC_SHA256);
create table test_select_2(
    a int,
    b text ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = copyCEK6, ENCRYPTION_TYPE = DETERMINISTIC)
);

insert into test_select_2 values (1, 'A');
insert into test_select_2 values (2, 'B');
insert into test_select_2 values (3, 'C');
insert into test_select_2 values (4, 'D');
insert into test_select_2 values (5, 'E');


--6. test COPY select table TO
--a. test COPY (select) TO
copy (select * from test_select order by 1) to stdout;
copy (select * from test_select order by 1) to stdout;
copy (select b from test_select where a=1) to stdout;

--b. test COPY (select for update) TO
copy (select b from test_select where a=3 for update) to stdout;
-- should fail
copy (select * from test_select) from stdin;
-- should fail
copy (select * from test_select) (a,b) to stdout;

--c.test join
copy (select * from test_select join test_select_2 using (a) order by 1) to stdout;

--d. Test subselect
copy (select * from (select b from test_select where a = 1)) to stdout;

--e. test headers, CSV and quotes
copy (select b from test_select where a = 1) to stdout csv header force quote b;

--f. test psql builtins, plain table
\copy (select * from test_select order by 1) to stdout;

-- fail to decrypt
-- \copy (select "a",'a','a""'||b,(a + 1)*a,b,"test_select"."b" from test_select where a=3) to stdout;

DROP TABLE CopyTbl;
DROP TABLE test_eoc;
DROP TABLE test_select;
DROP TABLE test_select_2;
DROP CLIENT MASTER KEY copyCMK CASCADE;

\! gs_ktool -d all

