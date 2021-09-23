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
-- default 14/10/12/0, max 16/12/14/1, min 10/0/0/0, but -1 means default
-- Empty
select hll_print(hll_empty(14));
select hll_print(hll_empty(14,10));
select hll_print(hll_empty(14,10,12));
select hll_print(hll_empty(14,10,12,0));

select hll_print(hll_empty(-1));
select hll_print(hll_empty(9));
select hll_print(hll_empty(17));

select hll_print(hll_empty(14,-1));
select hll_print(hll_empty(14,-2));
select hll_print(hll_empty(14,13));

select hll_print(hll_empty(14,10,-1));
select hll_print(hll_empty(14,10,-2));
select hll_print(hll_empty(14,10,15));

select hll_print(hll_empty(14,10,12,-1));
select hll_print(hll_empty(14,10,12,-2));
select hll_print(hll_empty(14,10,12,2));
select hll_print(hll_empty(14,10,12,false));

-- Explicit
select hll_print(
       hll_add(hll_empty(),
           hll_hash_integer(1,0)));
select hll_print(
       hll_add(
       hll_add(hll_empty(),
           hll_hash_integer(2,0)),
           hll_hash_integer(1,0)));
           
-- Sparse
select hll_print(
       hll_add(
       hll_add(
       hll_add(hll_empty(14,0),
           hll_hash_integer(3,0)),
           hll_hash_integer(2,0)),
           hll_hash_integer(1,0)));
           
-- Sparse, has 15 filled
select #hll_add(
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
       hll_add(hll_empty(14,0),
           hll_hash_integer(15,0)),
           hll_hash_integer(14,0)),
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

-- Full, has 17 filled
select #hll_add(
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
       hll_add(hll_empty(14,0,0,0),
           hll_hash_integer(17,0)),
           hll_hash_integer(16,0)),
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

-- Explicit->Sparse->Full, has 17 filled
select #hll_add(
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
       hll_add(hll_empty(14,5,6,1),
           hll_hash_integer(17,0)),
           hll_hash_integer(16,0)),
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

select hll_print(hll_add_agg(hll_hash_integer(val), -1))
       from test_khvengxf;

select hll_print(hll_add_agg(hll_hash_integer(val), 10, 4))
       from test_khvengxf;

select hll_print(hll_add_agg(hll_hash_integer(val), 10, NULL))
       from test_khvengxf;

select hll_print(hll_add_agg(hll_hash_integer(val), 10, -1))
       from test_khvengxf;

select hll_print(hll_add_agg(hll_hash_integer(val), 10, 4, 8))
       from test_khvengxf;

select hll_print(hll_add_agg(hll_hash_integer(val), 10, 4, NULL))
       from test_khvengxf;

select hll_print(hll_add_agg(hll_hash_integer(val), 10, 4, -1))
       from test_khvengxf;

select hll_print(hll_add_agg(hll_hash_integer(val), 10, 4, 8, 0))
       from test_khvengxf;

select hll_print(hll_add_agg(hll_hash_integer(val), 10, 4, 9, NULL))
       from test_khvengxf;

select hll_print(hll_add_agg(hll_hash_integer(val), 10, 4, 9, -1))
       from test_khvengxf;

select hll_print(hll_add_agg(hll_hash_integer(val), NULL, NULL, NULL, NULL))
       from test_khvengxf;

select hll_print(hll_add_agg(hll_hash_integer(val), -1,-1,-1,-1))
       from test_khvengxf;

-- Check range checking.
select hll_print(hll_add_agg(hll_hash_integer(val), 8))
       from test_khvengxf;

select hll_print(hll_add_agg(hll_hash_integer(val), 32))
       from test_khvengxf;

select hll_print(hll_add_agg(hll_hash_integer(val), 12, -2))
       from test_khvengxf;

select hll_print(hll_add_agg(hll_hash_integer(val), 12, 15))
       from test_khvengxf;

select hll_print(hll_add_agg(hll_hash_integer(val), 10, 4, -2))
       from test_khvengxf;

select hll_print(hll_add_agg(hll_hash_integer(val), 10, 4, 15))
       from test_khvengxf;

select hll_print(hll_add_agg(hll_hash_integer(val), 10, 4, 6, -2))
       from test_khvengxf;

select hll_print(hll_add_agg(hll_hash_integer(val), 10, 4, 6, 2))
       from test_khvengxf;

-- Check that we return hll_empty on null input.
select hll_print(hll_add_agg(NULL));

select hll_print(hll_add_agg(NULL, 10));

select hll_print(hll_add_agg(NULL, NULL));

select hll_print(hll_add_agg(NULL, -1));

select hll_print(hll_add_agg(NULL, 10, 4));

select hll_print(hll_add_agg(NULL, NULL, NULL));

select hll_print(hll_add_agg(NULL, -1, -1));

select hll_print(hll_add_agg(NULL, 10, 4, 8));

select hll_print(hll_add_agg(NULL, 10, 4, -1));

select hll_print(hll_add_agg(NULL, NULL, NULL, NULL));

select hll_print(hll_add_agg(NULL, 10, 4, 8, 0));

select hll_print(hll_add_agg(NULL, NULL, NULL, NULL, NULL));

DROP TABLE test_khvengxf;

------------------------------------------
-- 3.use case with parameters
------------------------------------------
-- test parameter registers
create table t_id(id int);
insert into t_id values(generate_series(1,5000));
create table t_data(a int, c text);
insert into t_data select mod(id,2), id from t_id;

create table t_a_c_hll(a int, c hll);
create table t_a_c_hll1(a int, c hll);
create table t_a_c_hll2(a int, c hll);

insert into t_a_c_hll select a, hll_add_agg(hll_hash_text(c),10) from t_data group by a;
insert into t_a_c_hll1 select a, hll_add_agg(hll_hash_text(c),12) from t_data group by a;
insert into t_a_c_hll2 select a, hll_add_agg(hll_hash_text(c),14) from t_data group by a;

select a, #c as cardinality from t_a_c_hll order by a;
select a, #c as cardinality from t_a_c_hll1 order by a;
select a, #c as cardinality from t_a_c_hll2 order by a;

-- test parameter explicitsize
create table t_id1(id int);
insert into t_id1 values(generate_series(1,20));

select #hll_add_agg(hll_hash_integer(id), 12, 5) from t_id1;
select #hll_add_agg(hll_hash_integer(id), 12, 10) from t_id1;

-- test parameter sparsesize
create table t_id2(id int);
insert into t_id2 values(generate_series(1,50));

select hll_print(hll_add_agg(hll_hash_integer(id), 14, 5, 8)) from t_id2;
select hll_print(hll_add_agg(hll_hash_integer(id), 14, 5, 10)) from t_id2;
select hll_print(hll_add_agg(hll_hash_integer(id), 14, 5, 12)) from t_id2;

-- test parameter duplicateCheck
select hll_print(
       hll_add(
       hll_add(
       hll_add(hll_empty(14,5,8,1),
           hll_hash_integer(3,0)),
           hll_hash_integer(2,0)),
           hll_hash_integer(1,0)));
		   
select hll_print(
       hll_add(
       hll_add(
       hll_add(hll_empty(14,5,8,0),
           hll_hash_integer(3,0)),
           hll_hash_integer(2,0)),
           hll_hash_integer(1,0)));

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
