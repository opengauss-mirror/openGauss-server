create schema tableof_unsupported;
set search_path to tableof_unsupported;
------ Prepare ------
-- create type of TYPTYPE_TABLEOF
create type r0 is (c1 int1, c2 int2);

create type t0 is table of int4;
create type t1 is (c1 int1, c2 t0);
create type t2 is table of r0;

-- create table
create table tableof_unsupported(id t0);
create table tableof_unsupported(id t1);
create table tableof_unsupported(id t2);

-- composite type cannot be made a member of itself
create type test as (a int);
create type test_arr as(a test[]);
alter type test add attribute b test_arr;

-- pljson_list_data is an exception
create type pljson as (a int);
create type pljson_list_data as (pljson_list_data pljson[]);
alter type pljson add attribute b pljson_list_data;
reset search_path;
drop schema tableof_unsupported cascade;