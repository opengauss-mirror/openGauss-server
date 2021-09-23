create schema hll_func;
set current_schema = hll_func;

create table hll_func_test(id int);
create table hll_func_test1(id int);
insert into hll_func_test values (generate_series(1,5));
insert into hll_func_test1 values (generate_series(1,500));

--------------CONTENTS--------------------
-- hyperloglog compare function test cases
------------------------------------------
--1. hll_eq hll_ne
--2. smallint integer bigint
--3. bytea text
--4. hll_hash_any
------------------------------------------

------------------------------------------
-- 1. compare function
------------------------------------------
select hll_eq((hll_empty() || hll_hash_integer(1)), (hll_empty() || hll_hash_integer(1)));
select hll_eq((hll_empty() || hll_hash_integer(1)), (hll_empty() || hll_hash_integer(1) || hll_hash_integer(2)));
select hll_ne((hll_empty() || hll_hash_integer(1)), (hll_empty() || hll_hash_integer(1)));
select hll_ne((hll_empty() || hll_hash_integer(1)), (hll_empty() || hll_hash_integer(1) || hll_hash_integer(2)));

-- test hll_eq with four parameters, default paramters are (14,10,12,0)
select hll_eq((hll_empty() || hll_hash_integer(1)), (hll_empty(12,10,12,0) || hll_hash_integer(1)));
select hll_eq((hll_empty() || hll_hash_integer(1)), (hll_empty(14,8,12,0) || hll_hash_integer(1)));
select hll_eq((hll_empty() || hll_hash_integer(1)), (hll_empty(14,10,10,0) || hll_hash_integer(1)));
select hll_eq((hll_empty() || hll_hash_integer(1)), (hll_empty(14,10,12,1) || hll_hash_integer(1)));

-- test hll_ne with four parameter, default paramters are (14,10,12,0)
select hll_ne((hll_empty() || hll_hash_integer(1)), (hll_empty(12,10,12,0) || hll_hash_integer(1)));
select hll_ne((hll_empty() || hll_hash_integer(1)), (hll_empty(14,8,12,0) || hll_hash_integer(1)));
select hll_ne((hll_empty() || hll_hash_integer(1)), (hll_empty(14,10,10,0) || hll_hash_integer(1)));
select hll_ne((hll_empty() || hll_hash_integer(1)), (hll_empty(14,10,12,1) || hll_hash_integer(1)));

-- test hll_eq with parameter -1, -1 means default
select hll_eq((hll_empty() || hll_hash_integer(1)), (hll_empty(-1) || hll_hash_integer(1)));
select hll_eq((hll_empty() || hll_hash_integer(1)), (hll_empty(-1,-1) || hll_hash_integer(1)));
select hll_eq((hll_empty() || hll_hash_integer(1)), (hll_empty(-1,-1,-1) || hll_hash_integer(1)));
select hll_eq((hll_empty() || hll_hash_integer(1)), (hll_empty(-1,-1,-1,-1) || hll_hash_integer(1)));

select hll_hashval_eq(hll_hash_integer(1), hll_hash_integer(1));
select hll_hashval_ne(hll_hash_integer(1), hll_hash_integer(1));

-- test hll_hashval_eq and hll_hashval_ne with hash seed
select hll_hashval_eq(hll_hash_integer(1), hll_hash_integer(1,123));
select hll_hashval_ne(hll_hash_integer(1), hll_hash_integer(1,123));
select hll_hashval_ne(hll_hash_integer(1), hll_hash_integer(1,0));

------------------------------------------
-- 2. debug function
------------------------------------------
select hll_print(hll_empty());
select hll_print(hll_empty(12,0,10,0));
select hll_print(hll_empty() || E'\\x484c4c00000000002b05000000000000000000000000000000000000');
select hll_print(hll_union_agg(hll_add_value)) from (select hll_add_agg(hll_hash_integer(id)) hll_add_value from hll_func_test);
select hll_print(hll_union_agg(hll_add_value)) from (select hll_add_agg(hll_hash_integer(id)) hll_add_value from hll_func_test1);

select hll_type(hll_empty());
select hll_type(hll_empty() || E'\\x484c4c00000000002b05000000000000000000000000000000000000');
select hll_type(hll_empty() || hll_hash_integer(1));
select hll_type(hll_empty(-1,0) || hll_hash_integer(1));
select hll_type(hll_empty(-1,-1,0) || hll_hash_integer(1));

select hll_log2m(hll_empty());
select hll_log2m(hll_empty(-1));
select hll_log2m(hll_empty(10));

select hll_log2explicit(hll_empty());
select hll_log2explicit(hll_empty(-1,-1));
select hll_log2explicit(hll_empty(-1,6));

select hll_log2sparse(hll_empty());
select hll_log2sparse(hll_empty(-1,-1,-1));
select hll_log2sparse(hll_empty(-1,-1,6));

select hll_duplicatecheck(hll_empty());
select hll_duplicatecheck(hll_empty(-1,-1,-1,-1));
select hll_duplicatecheck(hll_empty(-1,-1,-1,1));

------------------------------------------
-- 3. operator function
------------------------------------------

select (hll_empty() || hll_hash_integer(1)) = (hll_empty() || hll_hash_integer(1));
select (hll_empty() || hll_hash_integer(1)) <> (hll_empty() || hll_hash_integer(2));

select (hll_hash_integer(1) || hll_empty()) = (hll_hash_integer(1) || hll_empty());
select hll_cardinality((hll_empty() || hll_hash_integer(1)) || (hll_empty() || hll_hash_integer(2)));
select #((hll_empty() || hll_hash_integer(1)) || (hll_empty() || hll_hash_integer(2)));

select hll_hash_integer(1) = hll_hash_integer(1);
select hll_hash_integer(1) <> hll_hash_integer(1);

-- test with hll parameters, default paramters are (11, 5, -1, 1)
select (hll_empty() || hll_hash_integer(1)) = (hll_empty(10,5,-1,1) || hll_hash_integer(1));
select (hll_empty() || hll_hash_integer(1)) <> (hll_empty(10,5,-1,1) || hll_hash_integer(1));

select hll_empty(11,5,3,1) = hll_empty(11,5,3,1);
select hll_empty(11,5,3,1) = hll_empty(11,5,3,0);
select hll_empty(11,5,3,1) = hll_empty(11,5,7,1);
select hll_empty(11,5,3,1) = hll_empty(11,4,3,1);
select hll_empty(11,5,3,1) = hll_empty(10,5,3,1);

select hll_cardinality((hll_empty() || hll_hash_integer(1)) || (hll_empty(10,0,0,0) || hll_hash_integer(2)));

select hll_hash_integer(1) = hll_hash_integer(1,123);
select hll_hash_integer(1) <> hll_hash_integer(1,123);

------------------------------------------
-- 4. cast function
------------------------------------------

select E'\\x484c4c00000000002b05000000000000000000000000000000000000'::hll;
select E'\\x484c4c00000000002b05000000000000000000000000000000000000'::hll(10);
select E'\\x484c4c00000000002b05000000000000000000000000000000000000'::hll(-1,6);
select E'\\x484c4c00000000002b05000000000000000000000000000000000000'::hll(-1,-1,8);
select E'\\x484c4c00000000002b05000000000000000000000000000000000000'::hll(-1,-1,-1,1);

select E'\\x484c4c00000000002b04000000000000000000000000000000000000'::hll(10);
select E'\\x484c4c00000000002b04000000000000000000000000000000000000'::hll(10,6);
select E'\\x484c4c00000000002b04000000000000000000000000000000000000'::hll(10,-1,8);
select E'\\x484c4c00000000002b04000000000000000000000000000000000000'::hll(10,-1,-1,1);

select E'\\x484c4c00000000001b05000000000000000000000000000000000000'::hll(10);
select E'\\x484c4c00000000001b05000000000000000000000000000000000000'::hll(-1,6);
select E'\\x484c4c00000000001b05000000000000000000000000000000000000'::hll(-1,-1,8);
select E'\\x484c4c00000000001b05000000000000000000000000000000000000'::hll(-1,-1,-1,1);

select E'\\x484c4c00000000002a05000000000000000000000000000000000000'::hll(10);
select E'\\x484c4c00000000002a05000000000000000000000000000000000000'::hll(-1,6);
select E'\\x484c4c00000000002a05000000000000000000000000000000000000'::hll(-1,-1,8);
select E'\\x484c4c00000000002a05000000000000000000000000000000000000'::hll(-1,-1,-1,1);

------------------------------------------
-- 5. other function
------------------------------------------

--hll_add, hll_add_rev, hll_add_agg, hll_cardinality, hll_hashval_int4
select hll_add(hll_empty(), hll_hash_any(1)) = hll_add_rev(hll_hash_any(1), hll_empty());
select (hll_empty() || hll_hash_any(1)) = (hll_hash_any(1) || hll_empty());

select hll_hashval_int4(1::integer);

select hll_cardinality(hll_empty());
select hll_cardinality(hll_empty() || hll_hash_any(1));

select hll_eq(hll_add_agg(hll_hash_any(id),14, 10, 12, 0), hll_add_agg(hll_hash_any(id),14)) from hll_func_test1;
select hll_eq(hll_add_agg(hll_hash_any(id),-1, -1, -1, -1), hll_add_agg(hll_hash_any(id),14)) from hll_func_test1;
select hll_eq(hll_add_agg(hll_hash_any(id), NULL, NULL, NULL, NULL), hll_add_agg(hll_hash_any(id),14)) from hll_func_test1;

-- only the first para impacts the result
select #hll_add_agg(hll_hash_any(id)) from hll_func_test1;
select #hll_add_agg(hll_hash_any(id), 10, 8) from hll_func_test1;
select #hll_add_agg(hll_hash_any(id), 10, 8, 12) from hll_func_test1;
select #hll_add_agg(hll_hash_any(id), 10, 8, 12, 1) from hll_func_test1;

-- cleaning up
drop table hll_func_test;
drop table hll_func_test1;

--final cleaning up
drop schema hll_func cascade;
reset current_schema;
