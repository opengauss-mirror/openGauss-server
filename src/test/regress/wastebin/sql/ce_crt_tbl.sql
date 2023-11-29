
-------------------------------------------------------------------------------------------------------------------------
-- grop     : security
-- module   : client encrypt 
--
-- function : test {sql：CREATE/INSERT/UPDATE/DELETE/SELECT TABLE}
--      CREATE TABLE $tbl ($col $dat_type ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = $cek, ENCRYPTION_TYPE = $enc_type));
--
-- dependency : 
--      service : Huawei KMS (https://console.huaweicloud.com/dew/?region=cn-north-4#/kms/keyList/customKey)
--      cmk     : CREATE CLIENT MASTER KEY $cmk WITH (KEY_STORE = huawei_kms, ...)
--      cek     : CREATE COLUMN ENCRYPTION KEY $cek ...
-------------------------------------------------------------------------------------------------------------------------

-- prepare | succeed
CREATE CLIENT MASTER KEY cmk1 WITH (KEY_STORE = huawei_kms, KEY_PATH = "cec162c2-983d-4a66-8532-c67b915fb409" , ALGORITHM = AES_256);
CREATE CLIENT MASTER KEY cmk2 WITH (KEY_STORE = huawei_kms, KEY_PATH = "31938a5e-6460-49ce-a358-886f46c6f643" , ALGORITHM = AES_256);
CREATE COLUMN ENCRYPTION KEY cek1 WITH VALUES (CLIENT_MASTER_KEY = cmk1, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);
CREATE COLUMN ENCRYPTION KEY cek2 WITH VALUES (CLIENT_MASTER_KEY = cmk2, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);
CREATE COLUMN ENCRYPTION KEY cek3 WITH VALUES (CLIENT_MASTER_KEY = cmk2, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);

-- create table | succeed
CREATE TABLE IF NOT EXISTS tbl1 (
    col1 INT, 
    col2 INT ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = cek1, ENCRYPTION_TYPE = DETERMINISTIC),
    col3 TEXT ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = cek2, ENCRYPTION_TYPE = DETERMINISTIC),
    col4 VARCHAR(20) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = cek2, ENCRYPTION_TYPE = DETERMINISTIC));

-- insert | succeed
INSERT INTO tbl1 VALUES (1, 1, 'row1 col3', 'row1 col4');
INSERT INTO tbl1 VALUES (2, 11111, 'row2 col3', 'row2 col4');
INSERT INTO tbl1 VALUES (3, 11111111, 'row3 col3', 'row3 col4');

-- update | succeed
UPDATE tbl1 SET col2 = 22222 WHERE col1=1;
UPDATE tbl1 SET col3 = 'new row2 col3' WHERE col1=2;
UPDATE tbl1 SET col4 = 'new row3 col4' WHERE col1=3;

-- select | succeed
SELECT * FROM tbl1 ORDER BY col1;
SELECT * FROM tbl1 WHERE col2 = 1;
SELECT * FROM tbl1 WHERE col3 = 'new row2 col3';
SELECT * FROM tbl1 WHERE col4 = 'new row3 col4' AND col1 = 3;
SELECT * FROM tbl1 WHERE col3 = 'row1 col3' AND col4 = 'row1 col4';

-- delete | succeed
DELETE FROM tbl1 WHERE col2=22222;
DELETE FROM tbl1 WHERE col3='new row2 col3';
DELETE FROM tbl1 WHERE col4='row3 col4';

-- clear | succeed
SELECT * FROM tbl1;
DROP TABLE tbl1;
DROP CLIENT MASTER KEY cmk1 CASCADE;
DROP CLIENT MASTER KEY cmk2 CASCADE;
SELECT * FROM gs_column_keys;
SELECT * FROM gs_client_global_keys;

