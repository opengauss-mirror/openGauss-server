--
-- expression evaluation tests that don't fit into a more specific file
--

-- Tests for ScalarArrayOpExpr with a hashfn
--

-- create a stable function so that the tests below are not
-- evaluated using the planner's constant folding.
set enable_expr_fusion=1;
set enable_material = 0;
set enable_mergejoin = 0;
set enable_hashjoin = 0;
set enable_sortgroup_agg=1;
set enable_opfusion = 1;


begin;

create function return_int_input(int) returns int as $$
begin
 return $1;
end;
$$ language plpgsql stable;

create function return_text_input(text) returns text as $$
begin
 return $1;
end;
$$ language plpgsql stable;

select return_int_input(1) in (10, 9, 2, 8, 3, 7, 4, 6, 5, 1);
select return_int_input(1) in (10, 9, 2, 8, 3, 7, 4, 6, 5, null);
select return_int_input(1) in (null, null, null, null, null, null, null, null, null, null, null);
select return_int_input(1) in (10, 9, 2, 8, 3, 7, 4, 6, 5, 1, null);
select return_int_input(null::int) in (10, 9, 2, 8, 3, 7, 4, 6, 5, 1);
select return_int_input(null::int) in (10, 9, 2, 8, 3, 7, 4, 6, 5, null);
select return_text_input('a') in ('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j');
-- NOT IN
select return_int_input(1) not in (10, 9, 2, 8, 3, 7, 4, 6, 5, 1);
select return_int_input(1) not in (10, 9, 2, 8, 3, 7, 4, 6, 5, 0);
select return_int_input(1) not in (10, 9, 2, 8, 3, 7, 4, 6, 5, 2, null);
select return_int_input(1) not in (10, 9, 2, 8, 3, 7, 4, 6, 5, 1, null);
select return_int_input(1) not in (null, null, null, null, null, null, null, null, null, null, null);
select return_int_input(null::int) not in (10, 9, 2, 8, 3, 7, 4, 6, 5, 1);
select return_int_input(null::int) not in (10, 9, 2, 8, 3, 7, 4, 6, 5, null);
select return_text_input('a') not in ('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j');

rollback;

-- Test with non-strict equality function.
-- We need to create our own type for this.

begin;

create type myint;
create function myintin(cstring) returns myint strict immutable language
  internal as 'int4in';
create function myintout(myint) returns cstring strict immutable language
  internal as 'int4out';
create function myinthash(myint) returns integer strict immutable language
  internal as 'hashint4';

create type myint (input = myintin, output = myintout, like = int4);

create cast (int4 as myint) without function;
create cast (myint as int4) without function;

create function myinteq(myint, myint) returns bool as $$
begin
  if $1 is null and $2 is null then
    return true;
  else
    return $1::int = $2::int;
  end if;
end;
$$ language plpgsql immutable;

create function myintne(myint, myint) returns bool as $$
begin
  return not myinteq($1, $2);
end;
$$ language plpgsql immutable;

create operator = (
  leftarg    = myint,
  rightarg   = myint,
  commutator = =,
  negator    = <>,
  procedure  = myinteq,
  restrict   = eqsel,
  join       = eqjoinsel,
  merges
);

create operator <> (
  leftarg    = myint,
  rightarg   = myint,
  commutator = <>,
  negator    = =,
  procedure  = myintne,
  restrict   = eqsel,
  join       = eqjoinsel,
  merges
);

create operator class myint_ops
default for type myint using hash as
  operator    1   =  (myint, myint),
  function    1   myinthash(myint);

create table inttest (a myint);
insert into inttest values(1::myint),(null);

-- try an array with enough elements to cause hashing
select * from inttest where a in (1::myint,2::myint,3::myint,4::myint,5::myint,6::myint,7::myint,8::myint,9::myint, null);
select * from inttest where a not in (1::myint,2::myint,3::myint,4::myint,5::myint,6::myint,7::myint,8::myint,9::myint, null);
select * from inttest where a not in (0::myint,2::myint,3::myint,4::myint,5::myint,6::myint,7::myint,8::myint,9::myint, null);
-- ensure the result matched with the non-hashed version.  We simply remove
-- some array elements so that we don't reach the hashing threshold.
select * from inttest where a in (1::myint,2::myint,3::myint,4::myint,5::myint, null);
select * from inttest where a not in (1::myint,2::myint,3::myint,4::myint,5::myint, null);
select * from inttest where a not in (0::myint,2::myint,3::myint,4::myint,5::myint, null);

rollback;

-- test range partition key
create table test_scalar_array_op_t1 (
  column0 int,
  column1 text,
  column2 text,
  column3 text)
partition by range(column0)
(
partition p_1 values less than (1),
partition p_2 values less than (5),
partition p_3 values less than (10),
partition p_4 values less than (15),
partition p_5 values less than (20),
partition p_6 values less than (25),
partition p_7 values less than (30),
partition p_11 values less than (35),
partition p_12 values less than (40),
partition p_13 values less than (45),
partition p_14 values less than (50),
partition p_15 values less than (55),
partition p_16 values less than (60),
partition p_17 values less than (65),
partition p_18 values less than (MAXVALUE)
);
insert into test_scalar_array_op_t1 SELECT n as column0,'user'||n::TEXT column1,'col1'||n::TEXT column2,'col2'||n::TEXT column3 FROM generate_series(1,100) n;
select count(*) from test_scalar_array_op_t1 where column0 in (10, 9, 2, 8, 3, 18, 13, 19, 12, 7, 4, 6, 5, null);
drop table test_scalar_array_op_t1 ;

create table test_scalar_array_op_t2
(
  column0 int,
  column1 text,
  column2 text,
  column3 int)
PARTITION BY LIST (column3)
(
PARTITION p_1 VALUES (0),
PARTITION p_2 VALUES (1),
PARTITION p_3 VALUES (2),
PARTITION p_4 VALUES (3),
PARTITION p_5 VALUES (4),
PARTITION p_6 VALUES (DEFAULT)
);
insert into test_scalar_array_op_t2 SELECT n as column0,'user'||n::TEXT column1,'col1'||n::TEXT column2,n column3 FROM generate_series(1,100000) n;
select count(*) from test_scalar_array_op_t2 where column3 in (10, 9, 2, 8, 3, 18, 13, 19, 12, 7, 4, 6, 5, null);
drop table test_scalar_array_op_t2;

create table test_scalar_array_op_t3
(
  column0 int,
  column1 text,
  column2 text,
  column3 int);
insert into test_scalar_array_op_t3 SELECT n as column0,'user'||n::TEXT column1,'col1'||n::TEXT column2,n column3 FROM generate_series(1,100000) n;
set try_vector_engine_strategy='force';
EXPLAIN (COSTS OFF) select count(*) from test_scalar_array_op_t3 where column0 in (10, 9, 2, 8, 3, 18, 13, 19, 12, 7, 4, 6, 5, null);
drop table test_scalar_array_op_t3;
reset try_vector_engine_strategy;

create table test_scalar_array_op_t4
(
  column0 int,
  column1 text,
  column2 text,
  column3 int)
with (orientation=column);
insert into test_scalar_array_op_t4 SELECT n as column0,'user'||n::TEXT column1,'col1'||n::TEXT column2,n column3 FROM generate_series(1,100000) n;
EXPLAIN (COSTS OFF) select count(*) from test_scalar_array_op_t4 where column0 in (10, 9, 2, 8, 3, 18, 13, 19, 12, 7, 4, 6, 5, null);
drop table test_scalar_array_op_t4;

reset enable_expr_fusion;
reset enable_material;
reset enable_mergejoin;
reset enable_hashjoin;
reset enable_sortgroup_agg;
reset enable_opfusion;
