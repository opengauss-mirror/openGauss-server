set behavior_compat_options ='accept_float_str_as_int';

create table test_accept_float_str_as_int(c2 int2, c3 int, c4 int8);

insert into test_accept_float_str_as_int values ('1.49', '1.49', '1.59');
insert into test_accept_float_str_as_int values ('1.50', '1.50', '1.50');
insert into test_accept_float_str_as_int values ('-1.59', '-1.49', '-1.49');
insert into test_accept_float_str_as_int values ('-1.50', '-1.50', '-1.50');

select * from test_accept_float_str_as_int order by 1;

select * from test_accept_float_str_as_int where c2 = '1.5';
select * from test_accept_float_str_as_int where c2 = '1.5'::varchar;
select * from test_accept_float_str_as_int where c2 = '1.5'::text;


select * from test_accept_float_str_as_int where c2 = '1.0';
select * from test_accept_float_str_as_int where c2 = '1.0'::varchar;
select * from test_accept_float_str_as_int where c2 = '1.0'::text;

select * from test_accept_float_str_as_int where c2 = '1';
select * from test_accept_float_str_as_int where c2 = '1'::varchar;
select * from test_accept_float_str_as_int where c2 = '1'::text;

create table test_accept_float_str_as_int2(c1 int1);
insert into test_accept_float_str_as_int2 values ('1.49');
insert into test_accept_float_str_as_int2 values ('1.50');
select * from test_accept_float_str_as_int2;

drop table test_accept_float_str_as_int;
drop table test_accept_float_str_as_int2;
reset behavior_compat_options;
