-- pg compatibility case
drop database if exists pg_type_databse;
create database pg_type_databse dbcompatibility 'PG';

\c pg_type_databse
create table d_format_test(a varchar(10) not null);
insert into d_format_test values('');



-- concat test
select concat(null,'','','') is null;

select concat('','') is null;

select ''::int;

select concat_ws('','ABCDE', 2, null, 22);

select concat_ws(null,'ABCDE', 2, null, 22);

--char、varchar test
create table char_test(a char(10),b varchar(10));
insert into char_test values('零一二三四五六七八九','零一二三四五六七八九');
insert into char_test values('零1二3四5六7八9','零1二3四5六7八9');
insert into char_test values('零1二3四5六7八9','零1二3四5六7八90');
insert into char_test values('零1二3四5六7八90','零1二3四5六7八9');
insert into char_test values('零0','零1二3');
insert into char_test values('','');
insert into char_test values(null,null);
insert into char_test values('0','0');
select length(a),length(b) from char_test;
select lengthb(a),lengthb(b) from char_test;
select bit_length(a),bit_length(b) from char_test;

create index a on char_test(a);
create index b on char_test(b);
set enable_seqscan to off;
select * from char_test where a = '零0';
select * from char_test where b = '零1二3';

select int1mod(3, 0);
select int2mod(3, 0);
select int4mod(3, 0);
select int8mod(3, 0);
select numeric_mod(1234.5678,0.0);