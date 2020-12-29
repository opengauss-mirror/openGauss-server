\! gs_ktool -d all
\! gs_ktool -g

DROP CLIENT MASTER KEY IF EXISTS binaryCMK CASCADE;

CREATE CLIENT MASTER KEY binaryCMK WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY binaryCEK WITH VALUES (CLIENT_MASTER_KEY = binaryCMK, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);
create table IF NOT EXISTS binary_type(
c1 int, 
c2 BLOB ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = binaryCEK, ENCRYPTION_TYPE = DETERMINISTIC), 
c3 RAW, 
c4 BYTEA ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = binaryCEK, ENCRYPTION_TYPE = DETERMINISTIC)
);

insert into binary_type values(1, 'DEADBEEF', 'DEADBEEF', '\xDEADBEEF');
insert into binary_type values(2, 'BC9CA87B', 'BC9CA87B', '\xBC9CA87B');
insert into binary_type values(3, '6789ABCD', '6789ABCD', '\x6789ABCD');
insert into binary_type values(4, empty_blob(), 'DEADBEEF', '\xDEADBEEF');

select * from binary_type ORDER BY c1;

SELECT * from binary_type where c2 = 'DEADBEEF';

UPDATE binary_type SET c2 = 'DEADBEE5674' where c2 = 'DEADBEEF';

DELETE FROM binary_type where c2='DEADBEE5674';
SELECT * from binary_type order by c1;

DELETE FROM binary_type as alias_test where alias_test.c2 ='6789ABCD';
SELECT * from binary_type order by c1;

create table IF NOT EXISTS binary_type_enc(
c1 int ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = binaryCEK, ENCRYPTION_TYPE = DETERMINISTIC), 
c2 BLOB ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = binaryCEK, ENCRYPTION_TYPE = DETERMINISTIC), 
c3 RAW ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = binaryCEK, ENCRYPTION_TYPE = DETERMINISTIC), 
c4 BYTEA ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = binaryCEK, ENCRYPTION_TYPE = DETERMINISTIC)
);

DROP TABLE IF exists binary_type;
DROP TABLE IF exists binary_type_enc;
DROP COLUMN ENCRYPTION KEY binaryCEK;
DROP CLIENT MASTER KEY binaryCMK;
select  count(*), 'count' from gs_client_global_keys;
select  count(*), 'count' from gs_column_keys;

\! gs_ktool -d all