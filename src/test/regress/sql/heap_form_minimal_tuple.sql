create schema sche1_heap_form_minimal_tuple;
set current_schema = sche1_heap_form_minimal_tuple;

create table function_table_01(f1 int, f2 float, f3 text);
insert into function_table_01 values(1,2.0,'abcde'),(2,4.0,'abcde'),(3,5.0,'affde');
insert into function_table_01 values(4,7.0,'aeede'),(5,1.0,'facde'),(6,3.0,'affde');
analyze function_table_01;

CREATE OR REPLACE FUNCTION test_function_immutable RETURNS BIGINT AS
$body$ 
BEGIN
RETURN 3;
END;
$body$
LANGUAGE 'plpgsql'
IMMUTABLE
CALLED ON NULL INPUT
SECURITY INVOKER
COST 100;

select f1,f3 from function_table_01 order by left(f3,test_function_immutable()::INT), f1 limit 3;


-- test the table with the same name with a pg_catalog table 
create schema sche2_heap_form_minimal_tuple;
create table sche2_heap_form_minimal_tuple.pg_class(id int);

set search_path=sche2_heap_form_minimal_tuple;
insert into pg_class values(1);
select * from sche2_heap_form_minimal_tuple.pg_class;


insert into sche2_heap_form_minimal_tuple.pg_class values(1);
select * from sche2_heap_form_minimal_tuple.pg_class;
delete from sche2_heap_form_minimal_tuple.pg_class;

drop schema sche1_heap_form_minimal_tuple cascade;
drop schema sche2_heap_form_minimal_tuple cascade;
SET search_path TO DEFAULT ;




create table heap_form_minimal_tuple(id int, name varchar2(20));
insert into heap_form_minimal_tuple values(1,'x');
insert into heap_form_minimal_tuple values(11,'xx');
insert into heap_form_minimal_tuple values(111,'xxx');
create table my_table(i int);
insert into heap_form_minimal_tuple values(1);
begin;
declare foo cursor with hold for select * from heap_form_minimal_tuple where id > 1;
declare foo1 cursor with hold for select * from heap_form_minimal_tuple, my_table where id != i;
end;
fetch from foo;
fetch from foo1;
close foo;
close foo1;

drop table heap_form_minimal_tuple cascade;
drop table my_table cascade;