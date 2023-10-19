\! gs_ktool -d all
\! gs_ktool -g

DROP CLIENT MASTER KEY IF EXISTS MyCMK CASCADE;
CREATE CLIENT MASTER KEY MyCMK WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY MyCEK770 WITH VALUES (CLIENT_MASTER_KEY = MyCMK, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);

CREATE TABLE IF NOT EXISTS tr1(
i INT,
ii INT,
i1 INT1,
i2 INT2,
i4 INT4,
i8 INT8,
f4 FLOAT4,
f8 FLOAT8,
c  CHAR,
c8 CHAR(8),
v  VARCHAR,
v8 VARCHAR(8),
b  BYTEA,
n1 NUMERIC,
n2 NUMERIC (5),
n3 NUMERIC (5,2)
);

CREATE TABLE IF NOT EXISTS tr2(
i INT,
ii INT,
i1 INT1       ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK770, ENCRYPTION_TYPE = DETERMINISTIC),
i2 INT2       ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK770, ENCRYPTION_TYPE = DETERMINISTIC),
i4 INT4       ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK770, ENCRYPTION_TYPE = DETERMINISTIC),
i8 INT8       ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK770, ENCRYPTION_TYPE = DETERMINISTIC),
f4 FLOAT4     ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK770, ENCRYPTION_TYPE = DETERMINISTIC),
f8 FLOAT8     ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK770, ENCRYPTION_TYPE = DETERMINISTIC),
c  CHAR       ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK770, ENCRYPTION_TYPE = DETERMINISTIC),
c8 CHAR(8)    ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK770, ENCRYPTION_TYPE = DETERMINISTIC),
v  VARCHAR    ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK770, ENCRYPTION_TYPE = DETERMINISTIC),
v8 VARCHAR(8) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK770, ENCRYPTION_TYPE = DETERMINISTIC),
b  BYTEA      ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK770, ENCRYPTION_TYPE = DETERMINISTIC),
n1 NUMERIC ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK770, ENCRYPTION_TYPE = DETERMINISTIC),
n2 NUMERIC (5) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK770, ENCRYPTION_TYPE = DETERMINISTIC),
n3 NUMERIC (5,2) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK770, ENCRYPTION_TYPE = DETERMINISTIC)
);

prepare select_all_tr1      as select * from tr1 order by i;
prepare select_all_tr2      as select * from tr2 order by i;
prepare select_one_tr1      as select * from tr1 where i1=$1 AND ii=$2;
prepare select_one_tr2      as select * from tr2 where i1=$1 AND ii=$2;
prepare delete_all_tr1      as delete from tr1;
prepare delete_all_tr2      as delete from tr2;
prepare delete_one_tr1      as delete from tr1 where i1=$1 AND ii=$2;
prepare delete_one_tr2      as delete from tr2 where i1=$1 AND ii=$2;
prepare insert_tr1          as insert into tr1 values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13, $14, $15, $16);
prepare insert_tr2          as insert into tr2 values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13, $14, $15, $16);
prepare update_one_tr1      as update tr1 set ii=$1,i2=$2 where i1=$3 AND ii=$4;
prepare update_one_tr2      as update tr2 set ii=$1,i2=$2 where i1=$3 AND ii=$4;
prepare drop_column_key     as drop column encryption key MyCEK770;
prepare drop_master_key     as drop client master key MyCMK;
prepare insert_test_tr1          as insert into tr1 values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13, $14, $15, $16);

execute select_all_tr1;
execute select_all_tr2;
execute insert_tr1( 1, 1, 1, 1, 1, 1, 3.14, 3.14,'C','IDO'       ,'Ido''s'    , 'Shlomo'    , '\x1234',56032.50, 56032, 560.50 );
execute insert_tr2( 1, 1, 1, 1, 1, 1, 3.14, 3.14,'C','IDO'       ,'Ido''s'    , 'Shlomo'    , '\x1234',56032.50, 56032, 560.50);
execute select_all_tr1;
execute select_all_tr2;
execute insert_tr1(-1,-1, 0,-1,-1,-1,-3.14,-3.14,' ','AVI'       ,'Avi''s'    , 'Kessel'    , '\x5678', 561032063.5560,561032063.5560,561032063.5560);
execute insert_tr2(-1,-1, 0,-1,-1,-1,-3.14,-3.14,' ','AVI'       ,'Avi''s'    , 'Kessel'    , '\x5678', 561032063.5560,561032063.5560,561032063.5560);
execute select_all_tr1;
execute select_all_tr2;
execute insert_tr1( 0, 0, 0, 0, 0, 0, 0.14, 0.14,'z','ELI'       ,'Eli''s'    , 'Shemer'    , '\x09', 1563.0, 1563.0, 1563.0);
execute insert_tr2( 0, 0, 0, 0, 0, 0, 0.14, 0.14,'z','ELI'       ,'Eli''s'    , 'Shemer'    , '\x09', 1563.0, 1563.0, 1563.0);
execute select_all_tr1;
execute select_all_tr2;
execute insert_tr1( 4, 4, 3, 4, 0, 0,-0.14,-0.14,'z','A A       ','A a       ', 'A a       ', '\xababababababababababababababababababababab', 156.056, 15.056,156.05);
execute insert_tr2( 4, 4, 3, 4, 0, 0,-0.14,-0.14,'z','A A       ','A a       ', 'A a       ', '\xababababababababababababababababababababab', 156.056, 15.056,156.05);
execute select_all_tr1;
execute select_all_tr2;
execute select_one_tr1(1, 1);
execute select_one_tr2(1, 1);
execute update_one_tr1(5, 5, 1, 1);
execute update_one_tr2(5, 5, 1, 1);
execute select_one_tr1(1, 5);
execute select_one_tr2(1, 5);
execute delete_one_tr1(1, 5);
execute delete_one_tr2(1, 5);
execute select_one_tr1(1, 5);
execute select_one_tr2(1, 5);
execute select_all_tr1;
execute select_all_tr2;
execute delete_all_tr1;
execute delete_all_tr2;
execute select_all_tr1;
execute select_all_tr2;
execute insert_test_tr1(-1,-1, 0,-1,-1,-1,-3.14,-3.14,' ','AVI'       ,'Avi''s'    , 'Kessel'    , '\x5678');

drop table tr1;
drop table tr2;

execute drop_column_key;
execute drop_master_key;
DROP COLUMN ENCRYPTION KEY MyCEK770;
DROP CLIENT MASTER KEY MyCMK;

\! gs_ktool -d all


