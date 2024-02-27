create schema tablesample_schema5;
set current_schema = tablesample_schema5;

-- 创建普通表
CREATE TABLE test (
    id int PRIMARY KEY,
    some_timestamp timestamptz,
    some_text text
);
-- 插入数据
INSERT INTO test (id, some_timestamp, some_text)
    SELECT
    	i,
        now() - random() * '1 year'::INTERVAL,
        'depesz #' || i
    FROM
        generate_series(1,100) i;
-- 分析表的块（页）数
analyze test;
-- 测试SAMPLE子句
SELECT count(*) FROM test tablesample bernoulli ( 99.9 ) REPEATABLE (1);
SELECT avg(id) FROM test tablesample bernoulli ( 99.9 ) REPEATABLE (1);
SELECT sum(id) FROM test tablesample bernoulli ( 99.9 ) REPEATABLE (1);
-- 测试SAMPLE BLOCK子句
SELECT count(*) FROM test tablesample system (99.9) REPEATABLE(1);
SELECT avg(id) FROM test tablesample system (99.9) REPEATABLE (1);
SELECT sum(id) FROM test tablesample system (99.9) REPEATABLE (1);
-- 添加其他过滤条件
SELECT count(*) FROM test tablesample bernoulli ( 99.9 ) REPEATABLE (105) limit 5;
SELECT count(*) FROM test tablesample system ( 99.9 ) REPEATABLE (105) where id < 5;
SELECT count(*) FROM test tablesample system ( 99.9 ) REPEATABLE (105) limit 5;

-- 分区表
--- 一级分区表
create table part_list_t1(
id number,
name varchar2(20),
age int)
partition by list(age)(
partition age_10 values(10) ,
partition age_20 values(20),
partition age_default values(default));

insert into part_list_t1 values (1,'aa',10);
insert into part_list_t1 values (2,'bb',10);
insert into part_list_t1 values (3,'cc',20);
insert into part_list_t1 values (4,'dd',20);
insert into part_list_t1 values (5,'ee',20);
insert into part_list_t1 values (6,'ff',30);
insert into part_list_t1 values (7,'gg',100);
insert into part_list_t1 values (8,'hh',110);

analyze part_list_t1;

select count(*) from part_list_t1 tablesample bernoulli ( 99.9 ) REPEATABLE (1);
select count(*) from part_list_t1 tablesample bernoulli ( 99.9 ) REPEATABLE (1) where id > 5;
select count(*) from part_list_t1 partition (age_10) tablesample bernoulli ( 99.9 ) REPEATABLE (1);
select count(*) from part_list_t1 partition for (20) tablesample system ( 99.9 ) REPEATABLE (1);

--- 二级分区表
create table pt_range_hash_test(
   pid         number(10),
   pname       varchar2(30),
   sex         varchar2(10),
   create_date date
) partition by range(create_date) 
  subpartition by hash(pid) subpartitions 4(
     partition p1 values less than(to_date('2020-01-01', 'YYYY-MM-DD')) ,
     partition p2 values less than(to_date('2021-01-01', 'YYYY-MM-DD')) ,
     partition p3 values less than(to_date('2022-01-01', 'YYYY-MM-DD')) ,
     partition p4 values less than(maxvalue)
  );
  
insert into pt_range_hash_test(pid, pname, sex, create_date) values(1, '瑶瑶', 'WOMAN', to_date('2018-01-01', 'YYYY-MM-DD'));
insert into pt_range_hash_test(pid, pname, sex, create_date) values(2, '壮壮', 'MAN', to_date('2019-01-01', 'YYYY-MM-DD'));
insert into pt_range_hash_test(pid, pname, sex, create_date) values(3, '晴晴', 'WOMAN', to_date('2020-01-01', 'YYYY-MM-DD'));
insert into pt_range_hash_test(pid, pname, sex, create_date) values(4, '琳琳', 'WOMAN', to_date('2020-01-01', 'YYYY-MM-DD'));
insert into pt_range_hash_test(pid, pname, sex, create_date) values(5, '强强', 'MAN', to_date('2021-01-01', 'YYYY-MM-DD'));
insert into pt_range_hash_test(pid, pname, sex, create_date) values(6, '团团', 'WOMAN', to_date('2022-01-01', 'YYYY-MM-DD'));

analyze pt_range_hash_test;

select count(*) from pt_range_hash_test tablesample bernoulli ( 99.9 ) REPEATABLE (1);
select count(*) from pt_range_hash_test tablesample bernoulli ( 99.9 ) REPEATABLE (1) where pid < 4;
select count(*) from pt_range_hash_test partition (P2) tablesample bernoulli ( 99.9 ) REPEATABLE (1);
select count(*) from pt_range_hash_test subpartition for(to_date('2020-01-01', 'YYYY-MM-DD'),3)  tablesample system ( 99.9 ) REPEATABLE (1);

-- 物化视图
CREATE TABLE base_table
(
  base_table_id INTEGER,
  base_table_field NUMERIC(10,4)
);

insert into base_table values(1,2);
insert into base_table values(3,4);
insert into base_table values(5,6);

CREATE MATERIALIZED VIEW master_view1 AS
  SELECT
    base_table_id AS id,
    base_table_field AS field
  FROM base_table;

analyze base_table;
analyze master_view1;

 
SELECT count(*) FROM master_view1 tablesample bernoulli ( 99.9 ) REPEATABLE (1);
SELECT avg(id) FROM master_view1 tablesample bernoulli ( 99.9 ) REPEATABLE (1);
SELECT sum(id) FROM master_view1 tablesample bernoulli ( 99.9 ) REPEATABLE (1);

SELECT count(*) FROM master_view1 tablesample system (99.9) REPEATABLE (1);
SELECT avg(id) FROM master_view1 tablesample system (99.9) REPEATABLE (1);
SELECT sum(id) FROM master_view1 tablesample system (99.9) REPEATABLE (1);

-- 普通视图
CREATE OR REPLACE VIEW master_view2 AS
  SELECT
    base_table_id AS id,
    base_table_field AS field
  FROM base_table;
 
SELECT count(*) FROM master_view2 tablesample bernoulli ( 99.9 ) REPEATABLE (1); 
SELECT count(*) FROM master_view2 tablesample system (99.9) REPEATABLE (1);
  
CREATE OR REPLACE VIEW dependent_view1 AS
  SELECT
    id AS dependent_id,
    field AS dependent_field
  FROM master_view2;

SELECT count(*) FROM dependent_view1 tablesample bernoulli ( 99.9 ) REPEATABLE (1); 
SELECT count(*) FROM dependent_view1 tablesample system (99.9)  REPEATABLE (1);

-- 连接视图
CREATE OR REPLACE VIEW join_view1 AS
  SELECT
    a.id AS join_id,
    base_table_field AS join_field
  FROM test a join base_table b on a.id = b.base_table_id;

explain (costs off) SELECT count(*) FROM join_view1 tablesample bernoulli ( 99.9 ) REPEATABLE (1); 
SELECT count(*) FROM join_view1 tablesample system (99.9);
  
CREATE OR REPLACE VIEW join_view1 AS
  SELECT
    a.id AS join_id,
    base_table_field AS join_field
  FROM base_table b join test a on b.base_table_id = a.id;
 
explain (costs off) SELECT count(*) FROM join_view1 tablesample bernoulli ( 99.9 ) REPEATABLE (1); 
SELECT count(*) FROM join_view1 tablesample system (99.9);

CREATE OR REPLACE VIEW join_view2 AS
  SELECT
    b.base_table_id AS nest_join_id,
    join_field AS nest_join_field
  FROM join_view1 a join base_table b on a.join_id = b.base_table_id;

explain (costs off) SELECT count(*) FROM join_view2 tablesample bernoulli ( 99.9 ) REPEATABLE (1); 
SELECT count(*) FROM join_view2 tablesample system (99.9);

CREATE OR REPLACE VIEW join_view2 AS
  SELECT
    b.id AS nest_join_id,
    join_field AS nest_join_field
  FROM join_view1 a join test b on a.join_id = b.id;

explain (costs off) SELECT count(*) FROM join_view2 tablesample bernoulli ( 99.9 ) REPEATABLE (1); 
SELECT count(*) FROM join_view2 tablesample system (99.9) REPEATABLE (1);

CREATE TABLE base_table2
(
  base_table_id INTEGER,
  base_table_field NUMERIC(10,4)
);

insert into base_table2 values(1,1);
insert into base_table2 values(2,2);
insert into base_table2 values(3,3);

analyze base_table2;

CREATE OR REPLACE VIEW join_view3 AS
  SELECT
    a.base_table_id AS id,
    b.some_text AS some_text,
    c.base_table_field AS field
  FROM base_table a join test b on a.base_table_id = b.id
                    join base_table2 c on a.base_table_id = c.base_table_id;

explain (costs off) SELECT count(*) FROM join_view3 tablesample bernoulli ( 99.9 ) REPEATABLE (1); 
SELECT count(*) FROM join_view3 tablesample system (99.9) REPEATABLE (1);

ALTER TABLE base_table ADD CONSTRAINT xx PRIMARY KEY (base_table_id);
explain (costs off) SELECT count(*) FROM join_view3 tablesample bernoulli ( 99.9 ) REPEATABLE (1); 
SELECT count(*) FROM join_view3 tablesample system (99.9) REPEATABLE (1);

ALTER TABLE base_table DROP CONSTRAINT xx;
ALTER TABLE base_table2 ADD CONSTRAINT yy PRIMARY KEY(base_table_id);
explain (costs off) SELECT count(*) FROM join_view3 tablesample bernoulli ( 99.9 ) REPEATABLE (1); 
SELECT count(*) FROM join_view3 tablesample system (99.9) REPEATABLE (1);

reset search_path;
drop schema  tablesample_schema5 cascade;

