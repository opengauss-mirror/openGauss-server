CREATE USER test_create_any_index_role PASSWORD 'Gauss@1234';
GRANT create any index to test_create_any_index_role;
CREATE USER test_create_any_index_role_test PASSWORD 'Gauss@1234';
GRANT create any index to test_create_any_index_role_test;
CREATE SCHEMA pri_index_schema;
set search_path=pri_index_schema;

CREATE TABLE pri_index_schema.pri_index
(
    SM_SHIP_MODE_SK           INTEGER               NOT NULL,
    SM_SHIP_MODE_ID           CHAR(16)              NOT NULL,
    SM_TYPE                   CHAR(30)                      ,
    SM_CODE                   CHAR(10)                      ,
    SM_CARRIER                CHAR(20)                      ,
    SM_CONTRACT               CHAR(20)
);

SET ROLE test_create_any_index_role PASSWORD 'Gauss@1234';

CREATE UNIQUE INDEX pri_index_schema.ds_ship_mode_t1_index1 ON pri_index_schema.pri_index(SM_SHIP_MODE_SK);
--failed
ALTER INDEX pri_index_schema.ds_ship_mode_t1_index1 UNUSABLE;
--在表上的SM_SHIP_MODE_SK字段上创建指定B-tree索引。
CREATE INDEX pri_index_schema.ds_ship_mode_t1_index4 ON pri_index_schema.pri_index USING btree(SM_SHIP_MODE_SK);

--在表上SM_CODE字段上创建表达式索引。
CREATE INDEX pri_index_schema.ds_ship_mode_t1_index2 ON pri_index_schema.pri_index(SUBSTR(SM_CODE,1 ,4));

--在表上的SM_SHIP_MODE_SK字段上创建SM_SHIP_MODE_SK大于10的部分索引。
CREATE UNIQUE INDEX pri_index_schema.ds_ship_mode_t1_index3 ON pri_index_schema.pri_index(SM_SHIP_MODE_SK) WHERE SM_SHIP_MODE_SK>10;

--不能删除成功
SET ROLE test_create_any_index_role_test PASSWORD 'Gauss@1234';
DROP INDEX pri_index_schema.ds_ship_mode_t1_index1;
DROP INDEX pri_index_schema.ds_ship_mode_t1_index2;
DROP INDEX pri_index_schema.ds_ship_mode_t1_index3;
DROP INDEX pri_index_schema.ds_ship_mode_t1_index4;

reset role;

CREATE TABLE pri_index_schema.tmp_tbl(id int, c1 tsvector);

SET ROLE test_create_any_index_role PASSWORD 'Gauss@1234';
CREATE INDEX pri_index_schema.tmp_tbl_id_index ON pri_index_schema.tmp_tbl USING gist (c1);

---failed
ALTER TABLE pri_index_schema.ds_ship_mode_t1_index1 ADD CONSTRAINT PK_TBL_DOMAIN PRIMARY KEY (SM_SHIP_MODE_SK) USING INDEX;
CREATE TABLE pri_index_schema.default_test (f1 int, f2 int);
CREATE SEQUENCE  pri_index_schema.sequence_test1 START WITH 32;
CREATE FUNCTION pri_index_schema.pri_func_add_sql(integer, integer) RETURNS integer
AS 'select $1 + $2;'
LANGUAGE SQL
IMMUTABLE
RETURNS NULL ON NULL INPUT;
CREATE TYPE pri_index_schema.compfoo AS (f1 int, f2 text);

DROP INDEX pri_index_schema.ds_ship_mode_t1_index1;
DROP INDEX pri_index_schema.ds_ship_mode_t1_index2;
DROP INDEX pri_index_schema.ds_ship_mode_t1_index3;
DROP INDEX pri_index_schema.ds_ship_mode_t1_index4;
DROP INDEX pri_index_schema.tmp_tbl_id_index;
reset role;
DROP TABLE pri_index;
DROP TABLE pri_index_schema.tmp_tbl;
DROP SCHEMA pri_index_schema cascade;
DROP USER test_create_any_index_role_test cascade;
DROP USER test_create_any_index_role cascade;