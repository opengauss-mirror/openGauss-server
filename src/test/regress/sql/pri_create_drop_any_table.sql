CREATE USER test_create_any_table_role PASSWORD 'Gauss@1234';
GRANT create any table to test_create_any_table_role;

CREATE SCHEMA pri_create_schema;
set search_path=pri_create_schema;

SET ROLE test_create_any_table_role PASSWORD 'Gauss@1234';

CREATE table pri_create_schema.tb_pri (id int, name VARCHAR(10));
  --create table
CREATE TABLE pri_create_schema.TBL_DOMAIN_PRI
(
  IDOMAINID   NUMBER(10) NOT NULL,
  SDOMAINNAME VARCHAR2(30) NOT NULL,
  b int
);

CREATE TABLE pri_create_schema.pri_test_hash (a int, b int);

reset role;
CREATE TYPE pri_create_schema.pri_person_type1 AS (id int, name text);
CREATE TYPE pri_create_schema.pri_person_type2 AS (id int, name text);
SET ROLE test_create_any_table_role PASSWORD 'Gauss@1234';
CREATE TABLE pri_create_schema.pri_persons OF pri_create_schema.pri_person_type1;
CREATE TABLE pri_create_schema.pri_stuff (id int);


--trigger
CREATE SEQUENCE pri_create_schema.serial1;--permission denied
create table pri_create_schema.pri_trigtest (i serial primary key);--failed

reset role;
GRANT create any sequence to test_create_any_table_role;
GRANT create any index to test_create_any_table_role;
SET ROLE test_create_any_table_role PASSWORD 'Gauss@1234';
CREATE SEQUENCE pri_create_schema.serial1;
create table pri_create_schema.pri_trigtest (i serial primary key);

reset role;
revoke create any sequence,create any index from test_create_any_table_role;
SET ROLE test_create_any_table_role PASSWORD 'Gauss@1234';
create function pri_create_schema.pri_trigtest() returns trigger as $$
begin
	raise notice '% % % %', TG_RELNAME, TG_OP, TG_WHEN, TG_LEVEL;
	return new;
end;$$ language plpgsql; --failed ok
reset role;
create function pri_create_schema.pri_trigtest() returns trigger as $$
begin
	raise notice '% % % %', TG_RELNAME, TG_OP, TG_WHEN, TG_LEVEL;
	return new;
end;$$ language plpgsql;

create table pri_create_schema.pri_trigtest_test (i serial primary key);
SET ROLE test_create_any_table_role PASSWORD 'Gauss@1234';
create trigger pri_trigtest_b_row_tg before insert or update or delete on pri_create_schema.pri_trigtest
for each row execute procedure pri_create_schema.pri_trigtest(); --success 在自己创建的表上创建触发器
create trigger pri_trigtest_b_row_tg_test before insert or update or delete on pri_create_schema.pri_trigtest_test
for each row execute procedure pri_create_schema.pri_trigtest(); --failed
create table pri_create_schema.pri_storage_para_t1 (a int4, b text)
WITH 
(
	fillfactor =85, 
	autovacuum_enabled = ON,
	toast.autovacuum_enabled = ON, 
	autovacuum_vacuum_threshold = 100,
	toast.autovacuum_vacuum_threshold = 100,
	autovacuum_vacuum_scale_factor = 10, 
	toast.autovacuum_vacuum_scale_factor = 10,
	autovacuum_analyze_threshold = 8,
	autovacuum_analyze_scale_factor = 9,
--  autovacuum_vacuum_cost_delay: Valid values are between "0" and "100".
	autovacuum_vacuum_cost_delay = 90, 
	toast.autovacuum_vacuum_cost_delay = 92,
--	autovacuum_vacuum_cost_limit: Valid values are between "1" and "10000".
	autovacuum_vacuum_cost_limit = 567, 
	toast.autovacuum_vacuum_cost_limit = 789,
	autovacuum_freeze_min_age = 5000, 
	toast.autovacuum_freeze_min_age = 6000,
--	autovacuum_freeze_max_age: Valid values are between "100000000" and "2000000000".
	autovacuum_freeze_max_age = 300000000, 
	toast.autovacuum_freeze_max_age = 250000000,
	autovacuum_freeze_table_age = 170000000, 
	toast.autovacuum_freeze_table_age = 180000000
)
partition by range (a)
(
	partition pri_storage_para_t1_p1 values less than (10),
	partition pri_storage_para_t1_p2 values less than (20),
	partition pri_storage_para_t1_p3 values less than (100)
);

CREATE TABLE pri_table(c_id int,c_first varchar(50) NOT NULL);

--temp table
CREATE TEMP TABLE pri_temp1 (a int primary key);
reset role;
CREATE TABLE pri_t1 (num int, name text);
CREATE TABLE pri_t2 (num2 int, value text);
SET ROLE test_create_any_table_role PASSWORD 'Gauss@1234';
CREATE TEMP TABLE pri_tt (num2 int, value text);

CREATE VIEW pri_create_schema.pri_nontemp1 AS SELECT * FROM pri_create_schema.pri_t1 CROSS JOIN pri_create_schema.pri_t2;
CREATE VIEW pri_temporal1 AS SELECT * FROM pri_create_schema.pri_t1 CROSS JOIN pri_tt;

create table pri_create_schema.replication_temp_test(id int);

--create materialized view
create table pri_create_schema.t1(c1 int,c2 int);
insert into pri_create_schema.t1 values(1,1),(2,2); --success
create incremental materialized view pri_create_schema.mv1 as select * from pri_create_schema.t1;

CREATE TABLE pri_create_schema.pri_store_returns
(
    W_WAREHOUSE_SK            INTEGER               NOT NULL,
    W_WAREHOUSE_ID            CHAR(16)              NOT NULL,
    sr_item_sk                VARCHAR(20)                   ,
    W_WAREHOUSE_SQ_FT         INTEGER                       
);
CREATE TABLE pri_create_schema.store_returns_t1 AS SELECT * FROM pri_create_schema.pri_store_returns WHERE sr_item_sk > '4795';


--failed
CREATE TYPE pri_create_schema.pri_type AS (id int, name text); --permission denied
\! gs_ktool -d all
\! gs_ktool -g
CREATE CLIENT MASTER KEY pri_create_schema.ImgCMK WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
\! gs_ktool -d all
CREATE SEQUENCE  pri_create_schema.sequence_test1 START WITH 32;
CREATE FUNCTION pri_create_schema.pri_func_add_sql(integer, integer) RETURNS integer
AS 'select $1 + $2;'
LANGUAGE SQL
IMMUTABLE
RETURNS NULL ON NULL INPUT;
reset role;
CREATE USER test_drop_any_table_role PASSWORD 'Gauss@1234';
GRANT drop any table to test_drop_any_table_role;

SET ROLE test_drop_any_table_role PASSWORD 'Gauss@1234';
set search_path = pri_create_schema;
drop table tbl_domain_pri;
drop table pri_test_hash;
drop table pri_persons;
drop table pri_stuff;
drop table pri_trigtest_test;
drop table pri_storage_para_t1;
drop table pri_table;
drop view pri_temporal1;
drop view pri_nontemp1;
drop table pri_t1 cascade;
drop table pri_t2 cascade;
drop table replication_temp_test;
drop materialized view mv1;
drop table t1 cascade;
--failed
drop sequence serial1;
drop function pri_trigtest();
drop type pri_create_schema.pri_person_type1;
drop type pri_create_schema.pri_person_type2;
drop SEQUENCE pri_create_schema.serial1;

reset role;
drop type pri_create_schema.pri_person_type1;
drop type pri_create_schema.pri_person_type2;
drop SEQUENCE pri_create_schema.serial1;
DROP USER test_drop_any_table_role cascade;
DROP USER test_create_any_table_role cascade;