----
--- CREATE TABLE
----
create schema comm_explain_pretty;
set current_schema = comm_explain_pretty;
set codegen_cost_threshold=0;

create table comm_explain_pretty.explain_table_01
(
    a	varchar
);

copy explain_table_01 from stdin using delimiters '|';
1234
234
78906
12
325
\.

create table comm_explain_pretty.explain_table_02
(
    a	int
);

copy explain_table_02 from stdin using delimiters '|';
12
532
9087
67
\.

create table comm_explain_pretty.explain_table_03
(
    a	int
)with(orientation=column);

copy explain_table_03 from stdin using delimiters '|';
123
245
897
\.

analyze explain_table_01;
analyze explain_table_02;
analyze explain_table_03;

create or replace procedure test_explain_param (param_1 int)
as
begin 
   explain performance select count(*) from explain_table_03 where a = param_1;
end;
/
call test_explain_param(123);

----
--- test 1: Insert with nonzero rows
----
explain (costs off, analyze on) insert into explain_table_01 select * from explain_table_01;
explain (costs off, analyze on) insert into explain_table_01 select * from explain_table_02;
explain (costs off, analyze on) insert into explain_table_03 select * from explain_table_03;

----
--- Drop Tables
----
drop schema comm_explain_pretty cascade;
