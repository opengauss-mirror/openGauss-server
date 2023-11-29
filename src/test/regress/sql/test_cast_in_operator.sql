set transform_to_numeric_operators = on;
create table cast_operator(a varchar);
insert into cast_operator values('5.6');
insert into cast_operator values('5.6');
insert into cast_operator values('3.6');
insert into cast_operator values('2.6');
select * from cast_operator where a>3;
select count(*) from cast_operator group by a having a > 5;
drop table cast_operator;