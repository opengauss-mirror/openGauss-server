DROP SCHEMA IF EXISTS index_scan_node_reuse;

CREATE SCHEMA index_scan_node_reuse;
SET search_path = 'index_scan_node_reuse';

-- PREPARE BEGIN
begin;
drop table if EXISTS t1_2c;
drop table if EXISTS t2_2c;
create table t1_2c(a int primary key, b int);
create table t2_2c(a int primary key, b int);

create index t1_2c_idx ON t1_2c(b);
create index t2_2c_idx ON t2_2c(b);

insert into t1_2c values(generate_series(1, 10000), generate_series(10000, 1, -1));
insert into t2_2c values(generate_series(1, 10000), generate_series(10000, 1, -1));
drop table if EXISTS tes_tb1;
create table res_tb1(a int, b int);
end;

-- PREPARE END;

-- index scan is required

SET enable_indexscan = on;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
-- dont need bypass

SET enable_opfusion = off;

-- show execution plan, need indescan
explain insert into res_tb1 select * from t1_2c where b = 5 UNION select * from t2_2c where b = 50;

prepare p_insert1 as insert into res_tb1 select * from t1_2c where b = $1 UNION select * from t2_2c where b = $2;

execute p_insert1(5, 500);
execute p_insert1(15, 2500);
execute p_insert1(25, 3500);
execute p_insert1(35, 4500);
execute p_insert1(45, 5500);
execute p_insert1(55, 6500);
execute p_insert1(65, 7500);
execute p_insert1(75, 8500);
execute p_insert1(85, 9500);
execute p_insert1(95, 9500);

-- check result
select * from res_tb1;

DROP SCHEMA IF EXISTS index_scan_node_reuse CASCADE;
