drop schema if exists dependent_view cascade;
create schema dependent_view;
set current_schema = dependent_view;

-- test 1：视图
-- 创建基表
DROP TABLE IF EXISTS base_table;
CREATE TABLE base_table
(
  base_table_id INTEGER,
  base_table_field NUMERIC(10,4)
);
-- 插入数据
insert into base_table values (1,1);
insert into base_table values (2,2);

-- 创建第一层直接引用表字段的视图对象master_viewx
DROP VIEW IF EXISTS master_view1;
CREATE OR REPLACE VIEW master_view1 AS
  SELECT
    base_table_id AS id,
    base_table_field AS field
  FROM base_table;

DROP VIEW IF EXISTS master_view2;
CREATE OR REPLACE VIEW master_view2 AS
  SELECT
    base_table_field AS field
  FROM base_table;

DROP VIEW IF EXISTS master_view3;
CREATE OR REPLACE VIEW master_view3 AS
  SELECT
    base_table_id AS id
  FROM base_table;
-- 创建第二层间接引用表字段的视图对象dependent_viewx
DROP VIEW IF EXISTS dependent_view1;
CREATE OR REPLACE VIEW dependent_view1 AS
  SELECT
    id AS dependent_id,
    field AS dependent_field
  FROM master_view1;

DROP VIEW IF EXISTS dependent_view2;
CREATE OR REPLACE VIEW dependent_view2 AS
  SELECT
    id AS dependent_id
  FROM master_view1;

DROP VIEW IF EXISTS dependent_view3;
CREATE OR REPLACE VIEW dependent_view3 AS
  SELECT
    field AS dependent_field
  FROM master_view1;

DROP VIEW IF EXISTS dependent_view4;
CREATE OR REPLACE VIEW dependent_view4 AS
  SELECT
    field AS dependent_field1
  FROM master_view2;

DROP VIEW IF EXISTS dependent_view5;
CREATE OR REPLACE VIEW dependent_view5 AS
  SELECT
    id AS dependent_id
  FROM master_view3;
-- 创建第三层间接引用表字段的视图对象second_dependent_viewx
DROP VIEW IF EXISTS second_dependent_view1;
CREATE OR REPLACE VIEW second_dependent_view1 AS
  SELECT
    dependent_id AS sec_dependent_id,
    dependent_field AS sec_dependent_field
  FROM dependent_view1;

DROP VIEW IF EXISTS second_dependent_view2;
CREATE OR REPLACE VIEW second_dependent_view2 AS
  SELECT
    dependent_id AS sec_dependent_id
  FROM dependent_view2;

DROP VIEW IF EXISTS second_dependent_view3;
CREATE OR REPLACE VIEW second_dependent_view3 AS
  SELECT
    dependent_field AS sec_dependent_field
  FROM dependent_view3;

-- 初始valid字段为true
select relname, object_type, valid from pg_object join pg_class on object_oid=oid and relnamespace = (select Oid from pg_namespace where nspname='dependent_view') order by object_oid;

-- 修改表字段类型，预期视图失效，查询无效视图成功
ALTER TABLE base_table ALTER COLUMN base_table_field TYPE VARCHAR(32);
select relname, object_type, valid from pg_object join pg_class on object_oid=oid and relnamespace = (select Oid from pg_namespace where nspname='dependent_view') order by object_oid;
select * from second_dependent_view3;
select relname, object_type, valid from pg_object join pg_class on object_oid=oid and relnamespace = (select Oid from pg_namespace where nspname='dependent_view') order by object_oid;
\d base_table

-- 修改表字段长度，预期视图失效，查询无效视图成功
ALTER TABLE base_table ALTER COLUMN base_table_field TYPE VARCHAR(64);
select relname, object_type, valid from pg_object join pg_class on object_oid=oid and relnamespace = (select Oid from pg_namespace where nspname='dependent_view') order by object_oid;
select * from second_dependent_view3;
select relname, object_type, valid from pg_object join pg_class on object_oid=oid and relnamespace = (select Oid from pg_namespace where nspname='dependent_view') order by object_oid;
\d base_table

-- 删除表字段，预期视图失效，查询无效视图报错
ALTER TABLE base_table DROP COLUMN base_table_field;
select relname, object_type, valid from pg_object join pg_class on object_oid=oid and relnamespace = (select Oid from pg_namespace where nspname='dependent_view') order by object_oid;
select * from second_dependent_view3;
select relname, object_type, valid from pg_object join pg_class on object_oid=oid and relnamespace = (select Oid from pg_namespace where nspname='dependent_view') order by object_oid;
\d base_table

-- 重新插入表字段，预期失效的视图被查询时恢复有效，引用链上的父视图也恢复有效状态
ALTER TABLE base_table ADD base_table_field INTEGER;
select relname, object_type, valid from pg_object join pg_class on object_oid=oid and relnamespace = (select Oid from pg_namespace where nspname='dependent_view') order by object_oid;
select * from second_dependent_view3;
select * from dependent_view3;
select relname, object_type, valid from pg_object join pg_class on object_oid=oid and relnamespace = (select Oid from pg_namespace where nspname='dependent_view') order by object_oid;
\d base_table

-- 删除整个表，预期报错
DROP TABLE base_table;
-- 删除整个表，预期成功
DROP TABLE base_table CASCADE;

-- test2：物化视图
DROP TABLE IF EXISTS base_table;
CREATE TABLE base_table
(
  base_table_id INTEGER,
  base_table_field NUMERIC(10,4)
);

-- 插入数据
insert into base_table values (1,1);
insert into base_table values (2,2);

DROP MATERIALIZED VIEW IF EXISTS master_view1;
CREATE MATERIALIZED VIEW master_view1 AS
  SELECT
    base_table_id AS id,
    base_table_field AS field
  FROM base_table;
  
-- 插入新数据
insert into base_table values (3,3);
select relname, object_type, valid from pg_object join pg_class on object_oid=oid and relnamespace = (select Oid from pg_namespace where nspname='dependent_view') order by object_oid;

-- 重命名表字段，预期修改后查询到原数据
ALTER TABLE base_table rename COLUMN base_table_id to id;
select relname, object_type, valid from pg_object join pg_class on object_oid=oid and relnamespace = (select Oid from pg_namespace where nspname='dependent_view') order by object_oid;
select * from master_view1;
select relname, object_type, valid from pg_object join pg_class on object_oid=oid and relnamespace = (select Oid from pg_namespace where nspname='dependent_view') order by object_oid;
\d base_table

-- 修改表字段类型，预期修改后查询到原数据
ALTER TABLE base_table ALTER COLUMN id TYPE VARCHAR(32);
select relname, object_type, valid from pg_object join pg_class on object_oid=oid and relnamespace = (select Oid from pg_namespace where nspname='dependent_view') order by object_oid;
select * from master_view1;
select relname, object_type, valid from pg_object join pg_class on object_oid=oid and relnamespace = (select Oid from pg_namespace where nspname='dependent_view') order by object_oid;
\d base_table

-- 删除表字段，预期修改后查询到原数据
ALTER TABLE base_table DROP COLUMN base_table_field;
select relname, object_type, valid from pg_object join pg_class on object_oid=oid and relnamespace = (select Oid from pg_namespace where nspname='dependent_view') order by object_oid;
select * from master_view1;
select relname, object_type, valid from pg_object join pg_class on object_oid=oid and relnamespace = (select Oid from pg_namespace where nspname='dependent_view') order by object_oid;
\d base_table

-- 刷新物化视图，预期报错
REFRESH MATERIALIZED VIEW master_view1;

-- 重新添加表字段，刷新物化视图，预期查询到同步后数据
ALTER TABLE base_table ADD base_table_field INTEGER;
select relname, object_type, valid from pg_object join pg_class on object_oid=oid and relnamespace = (select Oid from pg_namespace where nspname='dependent_view') order by object_oid;
REFRESH MATERIALIZED VIEW master_view1;
select relname, object_type, valid from pg_object join pg_class on object_oid=oid and relnamespace = (select Oid from pg_namespace where nspname='dependent_view') order by object_oid;
select * from master_view1;
select relname, object_type, valid from pg_object join pg_class on object_oid=oid and relnamespace = (select Oid from pg_namespace where nspname='dependent_view') order by object_oid;
\d base_table

-- test 3: join方式创建视图
drop view if exists v_view_independent_1;
drop table if exists t_view_independent_1;
drop table if exists t_view_independent_2;
create table t_view_independent_1(c1 int primary key,c2 text);
insert into t_view_independent_1 select a,a || 'x' from generate_series(1,3) as a;
create table t_view_independent_2(c1 int primary key,c2 text);
insert into t_view_independent_2 select a,a || 'y' from generate_series(2,4) as a;
create view v_view_independent_1(vc1,vc2,vc3,vc4) as select * from t_view_independent_1 a inner join t_view_independent_2 b on a.c1 = b.c1 ;
select * from v_view_independent_1;
--- 删除字段
ALTER TABLE t_view_independent_1 DROP COLUMN c2;
--- 查看视图为失效状态
select relname, object_type, valid from pg_object join pg_class on object_oid=oid and relnamespace = (select Oid from pg_namespace where nspname='dependent_view') order by object_oid;
select * from v_view_independent_1;
ALTER TABLE t_view_independent_1 ADD COLUMN c2 text;
--- 查询视图
select * from v_view_independent_1;
-- 查看视图为有效状态
select relname, object_type, valid from pg_object join pg_class on object_oid=oid and relnamespace = (select Oid from pg_namespace where nspname='dependent_view') order by object_oid;
--- 修改字段类型
ALTER TABLE t_view_independent_1 ALTER COLUMN c1 TYPE text;
select relname, object_type, valid from pg_object join pg_class on object_oid=oid and relnamespace = (select Oid from pg_namespace where nspname='dependent_view') order by object_oid;
select * from v_view_independent_1;
select relname, object_type, valid from pg_object join pg_class on object_oid=oid and relnamespace = (select Oid from pg_namespace where nspname='dependent_view') order by object_oid;
--- 删除字段类型
ALTER TABLE t_view_independent_2 DROP COLUMN c1;
select relname, object_type, valid from pg_object join pg_class on object_oid=oid and relnamespace = (select Oid from pg_namespace where nspname='dependent_view') order by object_oid;
select * from v_view_independent_1;
ALTER TABLE t_view_independent_2 ADD COLUMN c1 text;
select * from v_view_independent_1;
select relname, object_type, valid from pg_object join pg_class on object_oid=oid and relnamespace = (select Oid from pg_namespace where nspname='dependent_view') order by object_oid;
--- 清理环境
drop view v_view_independent_1;
drop table t_view_independent_1;
drop table t_view_independent_2;

--- clean
drop schema dependent_view cascade;
