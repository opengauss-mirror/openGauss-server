--id generator
set enable_compress_hll = on;

create schema hll_misc;
set current_schema = hll_misc;

create table t_id(id int);
insert into t_id values(generate_series(1,500));

--------------CONTENTS--------------------
--     hyperloglog test cases
------------------------------------------
--1. create table using hll type
--2. check if inplace upgrade sanity
------------------------------------------

------------------------------------------
-- 1. create table
------------------------------------------
-- column store
create table t_hll(a int, b hll) with (orientation = column);
insert into t_hll select mod(id, 2) a, hll_add_agg(hll_hash_integer(id)) from t_id group by a;
select a, #b from t_hll order by 1;
drop table t_hll;

-- row store
create table t_hll(a int, b hll);
insert into t_hll select mod(id, 2) a, hll_add_agg(hll_hash_integer(id)) from t_id group by a;
select a, #b from t_hll order by 1;
drop table t_hll;


------------------------------------------
-- 2. check inplace upgrade sanity
------------------------------------------

select oid, * from pg_proc where proname like 'hll%' order by oid;

select oid, * from pg_type where typname in 
	('hll', '_hll',
	'hll_hashval', '_hll_hashval',
	'hll_trans_type', '_hll_trans_type') order by oid;

with type_oids as
(
	select oid  from pg_type where typname in 
	('hll', '_hll',
	'hll_hashval', '_hll_hashval',
	'hll_trans_type', '_hll_trans_type')
)
select oid, * from pg_operator where oprleft in (select * from type_oids)
								or oprright in  (select * from type_oids)
								order by oid;

select * from pg_aggregate where aggtranstype = (select oid from pg_type where typname = 'hll_trans_type') order by 1;

with type_oids as
(
	select oid  from pg_type where typname in 
	('hll', '_hll',
	'hll_hashval', '_hll_hashval',
	'hll_trans_type', '_hll_trans_type')
)
select * from pg_cast where castsource in ( select * from type_oids )
						or casttarget in ( select * from type_oids ) order by 1;

--final cleaning
drop table t_id;
drop schema hll_misc cascade;
reset current_schema;
