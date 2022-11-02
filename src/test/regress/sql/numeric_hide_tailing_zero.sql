set behavior_compat_options='';
select cast(123.123 as numeric(15,10));
set behavior_compat_options='hide_tailing_zero';
select cast(123.123 as numeric(15,10));
select cast(0 as numeric(15,10));
select cast(009.0000 as numeric(15,10));
set behavior_compat_options='';

set behavior_compat_options='truncate_numeric_tail_zero';
create table test_num_zero (a number,b int);
insert into test_num_zero values(0.1000, 1);
insert into test_num_zero values(0.2000,2);
insert into test_num_zero values(0.3000,3);
insert into test_num_zero values(0.1000, 1);
insert into test_num_zero values(0.2000,2);
explain performance insert into test_num_zero values(0.3000,3);

select * from test_num_zero order by a limit 2;
select to_char(1.2020000, '9D99999');
select to_char(1.2020000);
select to_text(1.2020000);
select to_nvarchar2(1.2020000);
select a::text,a::varchar,a::bpchar from test_num_zero where b = 1;
select pg_size_pretty(1.2000);
select cast(0.0::numeric as bool);
select 0::bool;

select listagg(a) within group (order by a) as ll , b from test_num_zero group by b;
select listagg(a,'@') within group (order by a) as ll , b from test_num_zero group by b;
select to_number(1234)/10;
select length(to_number(1234)/10);
select to_char(to_number(1234)/10);
select length(to_char(to_number(1234)/10));

create table numeric_hide_zero_t1(a numeric(10, 2));
insert into numeric_hide_zero_t1 values(0);
select * from numeric_hide_zero_t1;
select to_char(a, '999D99') from numeric_hide_zero_t1;

select to_char(a, '999D99') from numeric_hide_zero_t1;
set behavior_compat_options='hide_tailing_zero';

select to_char(a, '999D99') from numeric_hide_zero_t1;
select to_char(a, '999D99') from numeric_hide_zero_t1;
reset behavior_compat_options;
drop table test_num_zero;
drop table numeric_hide_zero_t1;

