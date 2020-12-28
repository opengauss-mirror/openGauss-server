set enable_compress_hll = on;

create schema hll_para;
set current_schema = hll_para;

--------------CONTENTS--------------------
-- test hll functions with parameters
------------------------------------------
--1. hll_empty
--2. hll_add_agg
--3. use cases with parameter
------------------------------------------

------------------------------------------
-- 1. hll_empty
------------------------------------------
select hll_print(hll_empty(10));

select hll_print(hll_empty(10,5));

select hll_print(hll_empty(10,5,-1));

select hll_print(hll_empty(10,5,-1,1));

-- Empty
select hll_empty(10,5,-1,1);

-- Explicit
select hll_add(hll_empty(10,5,-1,1),
           hll_hash_integer(1,0));

select hll_add(
       hll_add(hll_empty(10,5,-1,1),
           hll_hash_integer(2,0)),
           hll_hash_integer(1,0));

-- Sparse
select hll_add(
       hll_add(
       hll_add(hll_empty(10,5,-1,1),
           hll_hash_integer(3,0)),
           hll_hash_integer(2,0)),
           hll_hash_integer(1,0));

-- Sparse, has 15 filled
select hll_add(
       hll_add(
       hll_add(
       hll_add(
       hll_add(
       hll_add(
       hll_add(
       hll_add(
       hll_add(
       hll_add(
       hll_add(
       hll_add(
       hll_add(
       hll_add(
       hll_add(
       hll_add(hll_empty(5,5,-1,1),
           hll_hash_integer(17,0)),
           hll_hash_integer(15,0)),
           hll_hash_integer(14,0)),
           hll_hash_integer(13,0)),
           hll_hash_integer(12,0)),
           hll_hash_integer(11,0)),
           hll_hash_integer(10,0)),
           hll_hash_integer(9,0)),
           hll_hash_integer(8,0)),
           hll_hash_integer(7,0)),
           hll_hash_integer(6,0)),
           hll_hash_integer(5,0)),
           hll_hash_integer(4,0)),
           hll_hash_integer(3,0)),
           hll_hash_integer(2,0)),
           hll_hash_integer(1,0));

-- Compressed, has 16 filled
select hll_add(
       hll_add(
       hll_add(
       hll_add(
       hll_add(
       hll_add(
       hll_add(
       hll_add(
       hll_add(
       hll_add(
       hll_add(
       hll_add(
       hll_add(
       hll_add(
       hll_add(
       hll_add(
       hll_add(hll_empty(5,5,-1,1),
           hll_hash_integer(20,0)),
           hll_hash_integer(17,0)),
           hll_hash_integer(15,0)),
           hll_hash_integer(14,0)),
           hll_hash_integer(13,0)),
           hll_hash_integer(12,0)),
           hll_hash_integer(11,0)),
           hll_hash_integer(10,0)),
           hll_hash_integer(9,0)),
           hll_hash_integer(8,0)),
           hll_hash_integer(7,0)),
           hll_hash_integer(6,0)),
           hll_hash_integer(5,0)),
           hll_hash_integer(4,0)),
           hll_hash_integer(3,0)),
           hll_hash_integer(2,0)),
           hll_hash_integer(1,0));

------------------------------------------
-- 2.hll_add_agg
------------------------------------------
-- prepare data
create table test_khvengxf (
	val    integer
);

insert into test_khvengxf(val) values (1),(2),(3);

-- Check default and explicit signatures.
select hll_print(hll_add_agg(hll_hash_integer(val)))
       from test_khvengxf;

select hll_print(hll_add_agg(hll_hash_integer(val), 10))
       from test_khvengxf;

select hll_print(hll_add_agg(hll_hash_integer(val), NULL))
       from test_khvengxf;

select hll_print(hll_add_agg(hll_hash_integer(val), 10, 4))
       from test_khvengxf;

select hll_print(hll_add_agg(hll_hash_integer(val), 10, NULL))
       from test_khvengxf;

select hll_print(hll_add_agg(hll_hash_integer(val), 10, 4, 9))
       from test_khvengxf;

select hll_print(hll_add_agg(hll_hash_integer(val), 10, 4, -1))
       from test_khvengxf;

select hll_print(hll_add_agg(hll_hash_integer(val), 10, 4, NULL))
       from test_khvengxf;

select hll_print(hll_add_agg(hll_hash_integer(val), 10, 4, 9, 0))
       from test_khvengxf;

select hll_print(hll_add_agg(hll_hash_integer(val), 10, 4, 9, NULL))
       from test_khvengxf;

select hll_print(hll_add_agg(hll_hash_integer(val), NULL, NULL, NULL, NULL))
       from test_khvengxf;
-- Check range checking.
select hll_print(hll_add_agg(hll_hash_integer(val), -1))
       from test_khvengxf;

select hll_print(hll_add_agg(hll_hash_integer(val), 32))
       from test_khvengxf;

select hll_print(hll_add_agg(hll_hash_integer(val), 10, -1))
       from test_khvengxf;

select hll_print(hll_add_agg(hll_hash_integer(val), 10, 8))
       from test_khvengxf;

select hll_print(hll_add_agg(hll_hash_integer(val), 10, 4, -2))
       from test_khvengxf;

select hll_print(hll_add_agg(hll_hash_integer(val), 10, 4, 33))
       from test_khvengxf;

select hll_print(hll_add_agg(hll_hash_integer(val), 10, 4, 3, -1))
       from test_khvengxf;

select hll_print(hll_add_agg(hll_hash_integer(val), 10, 4, 3, 2))
       from test_khvengxf;

-- Check that we return hll_empty on null input.
select hll_print(hll_add_agg(NULL));

select hll_print(hll_add_agg(NULL, 10));

select hll_print(hll_add_agg(NULL, NULL));

select hll_print(hll_add_agg(NULL, 10, 4));

select hll_print(hll_add_agg(NULL, NULL, NULL));

select hll_print(hll_add_agg(NULL, 10, 4, 9));

select hll_print(hll_add_agg(NULL, 10, 4, -1));

select hll_print(hll_add_agg(NULL, NULL, NULL, NULL));

select hll_print(hll_add_agg(NULL, 10, 4, 9, 0));

select hll_print(hll_add_agg(NULL, NULL, NULL, NULL, NULL));

DROP TABLE test_khvengxf;

------------------------------------------
-- 3.use case with parameters
------------------------------------------
-- test parameter log2m
create table t_id(id int);
insert into t_id values(generate_series(1,500));
create table t_data(a int, c text);
insert into t_data select mod(id,2), id from t_id;

create table t_a_c_hll(a int, c hll);
create table t_a_c_hll1(a int, c hll);
create table t_a_c_hll2(a int, c hll);

insert into t_a_c_hll select a, hll_add_agg(hll_hash_text(c),10) from t_data group by a;
insert into t_a_c_hll1 select a, hll_add_agg(hll_hash_text(c),11) from t_data group by a;
insert into t_a_c_hll2 select a, hll_add_agg(hll_hash_text(c),13) from t_data group by a;

select a, #c as cardinality from t_a_c_hll order by a;
select a, #c as cardinality from t_a_c_hll1 order by a;
select a, #c as cardinality from t_a_c_hll2 order by a;

-- test parameter regwidth
create table t_id1(id int);
insert into t_id1 values(generate_series(1,10000));

select #hll_add_agg(hll_hash_integer(id), 10, 1) from t_id1;
select #hll_add_agg(hll_hash_integer(id), 10, 3) from t_id1;

-- test parameter expthresh
create table t_id2(id int);
insert into t_id2 values(generate_series(1,100));

select hll_print(hll_add_agg(hll_hash_integer(id), 11, 5, -1)) from t_id2;
select hll_print(hll_add_agg(hll_hash_integer(id), 11, 5, 0)) from t_id2;
select hll_print(hll_add_agg(hll_hash_integer(id), 11, 5, 6)) from t_id2;

-- test sparseon
select hll_add(
       hll_add(
       hll_add(hll_empty(5,5,-1,1),
           hll_hash_integer(3,0)),
           hll_hash_integer(2,0)),
           hll_hash_integer(1,0));

select hll_add(
       hll_add(
       hll_add(hll_empty(5,5,-1,0),
           hll_hash_integer(3,0)),
           hll_hash_integer(2,0)),
           hll_hash_integer(1,0));

-- final clear data
drop table t_id;
drop table t_id1;
drop table t_id2;
drop table t_data;
drop table t_a_c_hll;
drop table t_a_c_hll1;
drop table t_a_c_hll2;

drop schema hll_para cascade;
reset current_schema;
