\! gs_ktool -d all
\! gs_ktool -g

create table IF NOT EXISTS unencrypted_boolean(c1 int, 
c2 BOOLEAN
);

insert into unencrypted_boolean values(1, 'true');
insert into unencrypted_boolean values(2, 't');
insert into unencrypted_boolean values(3, TRUE);
insert into unencrypted_boolean values(4, 'y');
insert into unencrypted_boolean values(5, 'yes');
insert into unencrypted_boolean values(6, '1');

insert into unencrypted_boolean values(7, 'false');
insert into unencrypted_boolean values(8, 'f');
insert into unencrypted_boolean values(9, FALSE);
insert into unencrypted_boolean values(10, 'n');
insert into unencrypted_boolean values(11, 'no');
insert into unencrypted_boolean values(12, '0');

insert into unencrypted_boolean values(13, 'ff');

select * from unencrypted_boolean ORDER BY c1;

SELECT * from unencrypted_boolean where c2 = 'n' order by c1;

DELETE FROM unencrypted_boolean where c2='1';
SELECT * from unencrypted_boolean order by c1;

DELETE FROM unencrypted_boolean as alias_test where alias_test.c2 ='t';
SELECT * from unencrypted_boolean order by c1;

UPDATE unencrypted_boolean SET c2 = 'n' where c2 = 'y';


DROP CLIENT MASTER KEY IF EXISTS boolCMK CASCADE;
CREATE CLIENT MASTER KEY boolCMK WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY boolCEK WITH VALUES (CLIENT_MASTER_KEY = boolCMK, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);

create table IF NOT EXISTS boolean_type(c1 int, 
c2 BOOLEAN ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = boolCEK, ENCRYPTION_TYPE = DETERMINISTIC)
);

-- insert into boolean_type values(1, 'true');
-- insert into boolean_type values(2, 't');
-- insert into boolean_type values(3, TRUE);
-- insert into boolean_type values(4, 'y');
-- insert into boolean_type values(5, 'yes');
-- insert into boolean_type values(6, '1');

-- insert into boolean_type values(7, 'false');
-- insert into boolean_type values(8, 'f');
-- insert into boolean_type values(9, FALSE);
-- insert into boolean_type values(10, 'n');
-- insert into boolean_type values(11, 'no');
-- insert into boolean_type values(12, '0');

-- insert into boolean_type values(13, 'ff');

-- select * from boolean_type ORDER BY c1;

-- SELECT * from boolean_type where c2 = 'n' order by c1;

-- DELETE FROM boolean_type where c2='1';
-- SELECT * from boolean_type order by c1;

-- DELETE FROM boolean_type as alias_test where alias_test.c2 ='t';
-- SELECT * from boolean_type order by c1;

-- UPDATE boolean_type SET c2 = 'n' where c2 = 'y';

DROP TABLE IF EXISTS unencrypted_boolean;
DROP TABLE IF EXISTS boolean_type;
DROP COLUMN ENCRYPTION KEY boolCEK;
DROP CLIENT MASTER KEY boolCMK;
select  count(*), 'count' from gs_client_global_keys;
select  count(*), 'count' from gs_column_keys;

\! gs_ktool -d all