-- test about issue
create database test_pg dbcompatibility 'PG';
\c test_pg
create schema accept_schema;
set current_schema to 'accept_schema';
create table tchar(c char(10));
set behavior_compat_options to 'accept_empty_str';
insert into tchar values(' ');
select * from tchar where c = '';
select * from tchar where c = ' ';
select * from tchar where c is null;
drop table tchar;

-- test about const str
select '' is null;
select ' ' is null;
select ' abc ' is null;
select length('');
select length(null);
select length(' ');
select length(' abc ');
select '123'::char(3) is null;
select ''::char(3) is null;
select '123'::varchar(3) is null;
select ''::varchar(3) is null;
select '123'::text is null;
select ''::text is null;
select '123'::clob is null;
select ''::clob is null;
select '123'::blob is null;
select ''::blob is null;
select '123'::bytea is null;
select ''::bytea is null;
select '123'::int1 is null;
select ''::int1 is null;
select '123'::int2 is null;
select ''::int2 is null;
select '123'::int is null;
select ''::int is null;
select '123'::int8 is null;
select ''::int8 is null;
select '123'::float4 is null;
select ''::float4 is null;
select '123'::float8 is null;
select ''::float8 is null;
select '123'::numeric is null;
select ''::numeric is null;
select ''::date is null;
select ''::time is null;
select ''::timestamp is null;

-- test about var str
create table result_tab ("statement" text, result text);
declare
str_empty text := '';
str_space text := ' ';
str_num text := '123';
str text := ' abc ';
begin
    insert into result_tab select 'select str_empty is null', str_empty is null;
    insert into result_tab select 'select str_space is null', str_space is null;
    insert into result_tab select 'select str is null', str is null;
    insert into result_tab select 'select length(str_empty)', length(str_empty);
    insert into result_tab select 'select length(null)', length(null);
    insert into result_tab select 'select length(str_space)', length(str_space);
    insert into result_tab select 'select length(str)', length(str);
    insert into result_tab select 'select str_num::text is null', str_num::text is null;
    insert into result_tab select 'select str_empty::text is null', str_empty::text is null;
    insert into result_tab select 'select str_num::bytea is null;', str_num::bytea is null;
    insert into result_tab select 'select str_empty::bytea is null', str_empty::bytea is null;
    insert into result_tab select 'select str_num::int is null', str_num::int is null;
    insert into result_tab select 'select str_num::float8 is null', str_num::float8 is null;
    insert into result_tab select 'select str_num::numeric is null', str_num::numeric is null;
end;
/

select * from result_tab;

-- test about function which return str
SELECT overlay('hello' placing 'world' from 2 for 3 ) is null;
SELECT overlay('hello' placing '' from 1 for 5 ) is null;
SELECT quote_ident('') is null;
SELECT quote_literal('') is null;
SELECT quote_nullable('') is null;
SELECT reverse('') is null;
SELECT ''||'' is null;
SELECT ''||41;
SELECT lower('') is null;
SELECT initcap('') is null;
SELECT ascii('');
SELECT lpad('yes', 5) is null;
SELECT lpad('yes', 1) is null;
SELECT lpad('yes', 0) is null;
SELECT lpad('yes', 5, 'z') is null;
SELECT lpad('yes', 1, 'z') is null;
SELECT lpad('yes', 0, 'z') is null;
SELECT rpad('yes', 5) is null;
SELECT rpad('yes', 1) is null;
SELECT rpad('yes', 0) is null;
SELECT rpad('yes', 5, 'z') is null;
SELECT rpad('yes', 1, 'z') is null;
SELECT rpad('yes', 0, 'z') is null;
SELECT btrim('yzy', 'y') is null;
SELECT btrim('zzz', 'z') is null;
SELECT ltrim('yzy', 'y') is null;
SELECT ltrim('zzz', 'z') is null;
SELECT rtrim('yzy', 'y') is null;
SELECT rtrim('zzz', 'z') is null;
SELECT btrim(' z ') is null;
SELECT btrim('   ') is null;
SELECT ltrim(' z ') is null;
SELECT ltrim('   ') is null;
SELECT rtrim(' z ') is null;
SELECT rtrim('   ') is null;
SELECT translate('xyx', 'x', 'z') is null;
SELECT translate('xzx', 'x', '') is null;
SELECT translate('xxx', 'x', '') is null;
SELECT translate('xxx', 'x', ' ') is null;
SELECT translate('xxx', '', 'z') is null;
SELECT translate('', 'x', 'z') is null;
SELECT repeat('a', 3) is null;
SELECT repeat('a', 0) is null;
SELECT repeat(' ', 3) is null;
SELECT repeat(' ', 0) is null;
SELECT repeat('', 3) is null;
SELECT repeat('', 0) is null;
SELECT pg_catalog.oidvectortypes('123 456') is null;
SELECT pg_catalog.oidvectortypes('') is null;
SELECT pg_catalog.oidvectortypes(' ') is null;
SELECT regexp_replace('Thomas', '.[mN]a.', 'M') is null;
SELECT regexp_replace('omas', '.[mN]a.', '');
SELECT regexp_replace('Thomas', '', 'M') is null;
SELECT regexp_replace('', '.[mN]a.', 'M') is null;
SELECT regexp_replace('omas', '.[mN]a.', '') is null;
SELECT regexp_replace('foobarbaz','b(..)', E'X\\1Y', 2, 2, 'n') is null;
SELECT regexp_replace('bar','b(..)', '', 1, 1, 'n') is null;
SELECT regexp_replace('foobarbaz','b(..)', E'X\\1Y', 'g') is null;
SELECT regexp_replace('abc','abc', '', 'g') is null;
SELECT regexp_substr('str','st') is null;
SELECT regexp_substr('str','[ac]') is null;
SELECT regexp_substr('str','') is null;
SELECT regexp_substr('','st') is null;
SELECT regexp_split_to_table('hello world', E'\\s+') is null;
SELECT regexp_split_to_table('', E'\\s+') is null;
SELECT regexp_split_to_table('hello world', '') is null;
SELECT regexp_split_to_table('hello world', null) is null;
SELECT substr('123', 3) is null;
SELECT substr('123', 4) is null;
SELECT substr('123', 1, 2) is null;
SELECT substr('123', 1, 0) is null;
SELECT substr('123', 1, -1) is null;
SELECT substr('123'::bytea, 3) is null;
SELECT substr('123'::bytea, 4) is null;
SELECT substr('123'::bytea, 1, 2) is null;
SELECT substr('123'::bytea, 1, 0) is null;
SELECT substr('123'::bytea, 1, -1) is null;
SELECT substrb('123', 3) is null;
SELECT substrb('123', 4) is null;
SELECT substrb('123', 1, 2) is null;
SELECT substrb('123', 1, 0) is null;
SELECT substrb('123', 1, -1) is null;
SELECT substring('123', 3) is null;
SELECT substring('123', 4) is null;
SELECT substring('123', 1, 2) is null;
SELECT substring('123', 1, 0) is null;
SELECT substring('123', 1, -1) is null;
SELECT substring('123'::bytea, 3) is null;
SELECT substring('123'::bytea, 4) is null;
SELECT substring('123'::bytea, 1, 2) is null;
SELECT substring('123'::bytea, 1, 0) is null;
SELECT substring('123'::bytea, 1, -1) is null;
SELECT replace('abc', 'ab', 'd') is null;
SELECT replace('abc', 'abc', '') is null;
SELECT replace('abc', 'abc', null) is null;
SELECT replace('abc', 'ab', '') is null;
SELECT replace('abc', 'ab', null) is null;
SELECT replace('abc', '', 'd') is null;
SELECT replace('abc', null, 'd') is null;
SELECT replace('', 'ab', 'd') is null;
SELECT replace(null, 'ab', 'd') is null;
SELECT replace('abc', 'ab') is null;
SELECT replace('abc', 'abc') is null;
SELECT replace('abc', '') is null;
SELECT replace('abc', null) is null;
SELECT replace('', 'ab') is null;
SELECT replace(null, 'ab') is null;
SELECT split_part('1~2~3', '~', 3) is null;
SELECT split_part('1~2~3~', '~', 4) is null;
SELECT split_part('1~2~3', '~', -1) is null;
SELECT split_part('1~2~3', '', 1) is null;
SELECT split_part('1~2~3', '', 2) is null;
SELECT split_part('1~2~3', null, 1) is null;
SELECT split_part('', '~', 1) is null;
SELECT split_part(null, '~', 1) is null;
SELECT split_part('1~2~3', '|', 1) is null;
SELECT split_part('1~2~3', '|', 2) is null;
SELECT concat('Hello', ' World!') is null;
SELECT concat('', ' World!') is null;
SELECT concat('Hello', '') is null;
SELECT concat('', '') is null;
SELECT concat('Hello', null) is null;
SELECT concat(null, ' World!') is null;
SELECT concat_ws(',', 'ABCDE', 2, NULL, 22);
SELECT concat_ws('', 'ABCDE', 2, NULL, 22);
SELECT concat_ws(',', '') is null;
SELECT concat_ws('', '') is null;
SELECT left('abcde', 2) is null;
SELECT left('abcde', 0) is null;
SELECT left('abcde', -1) is null;
SELECT left('', 1) is null;
SELECT left('', 0) is null;
SELECT left('', -1) is null;
SELECT right('abcde', 2) is null;
SELECT right('abcde', 0) is null;
SELECT right('abcde', -1) is null;
SELECT right('', 1) is null;
SELECT right('', 0) is null;
SELECT right('', -1) is null;
SELECT format('Hello %s', 'World');
SELECT format('Hello %s', '');
SELECT format('%s', 'World');
SELECT format('', 'World');
SELECT format('', '');
SELECT array_to_string(ARRAY[1, 2, 3, NULL, 5], ',') AS RESULT;
SELECT array_to_string(ARRAY[1, 2, 3, NULL, 5], '') AS RESULT;
SELECT array_to_string(ARRAY[1, 2, 3, NULL, 5], null) AS RESULT;
SELECT array_to_string(ARRAY[NULL], '') AS RESULT;
SELECT array_to_string(ARRAY[NULL], null) AS RESULT;
SELECT array_to_string(ARRAY[1, 2, 3, NULL, 5], ',', '*') AS RESULT;
SELECT array_to_string(ARRAY[1, 2, 3, NULL, 5], '', '') AS RESULT;
SELECT array_to_string(ARRAY[1, 2, 3, NULL, 5], null, null) AS RESULT;
SELECT array_to_string(ARRAY[NULL], '', '') AS RESULT;
SELECT array_to_string(ARRAY[NULL], null, null) AS RESULT;
SELECT nlssort('A', 'nls_sort=schinese_pinyin_m') is null;
SELECT nlssort('', 'nls_sort=schinese_pinyin_m') is null;
SELECT nlssort('A', '') is null;
SELECT convert('text_in_utf8'::bytea, 'UTF8', 'GBK') is null;
SELECT convert(''::bytea, 'UTF8', 'GBK') is null;
SELECT convert('text_in_utf8'::bytea, '', 'GBK') is null;
SELECT convert('text_in_utf8'::bytea, 'UTF8', '') is null;
SELECT convert_from('text_in_utf8'::bytea, 'UTF8') is null;
SELECT convert_from(''::bytea, 'UTF8') is null;
SELECT convert_from('text_in_utf8'::bytea, '') is null;
SELECT convert_to('text_in_utf8'::bytea, 'UTF8') is null;
SELECT convert_to(''::bytea, 'UTF8') is null;
SELECT convert_to('text_in_utf8'::bytea, '') is null;
SELECT md5('ABC') is null;
SELECT md5('') is null;
SELECT sha('ABC') is null;
SELECT sha('') is null;
SELECT sha1('ABC') is null;
SELECT sha1('') is null;
SELECT sha2('ABC') is null;
SELECT sha2('') is null;
SELECT decode('MTIzAAE=', 'base64') is null;
SELECT decode('', 'base64') is null;
SELECT decode('MTIzAAE=', '') is null;
select similar_escape('\s+ab','2') is null;
select similar_escape('\s+ab','') is null;
select similar_escape('','2') is null;
select svals('"aa"=>"bb"') is null;
select svals('') is null;
select tconvert('aa', 'bb') is null;
select tconvert('', 'bb') is null;
select tconvert('aa', '') is null;
select tconvert('', '') is null;
SELECT encode(E'123\\000\\001', 'base64') is null;
SELECT encode('', 'base64') is null;
SELECT encode(E'123\\000\\001', '') is null;

-- test about vec
CREATE TABLE vec_t1
(
    c varchar
) WITH (ORIENTATION = COLUMN);

CREATE TABLE vec_t2
(
    c varchar not null
) WITH (ORIENTATION = COLUMN);

insert into vec_t1 values('');
insert into vec_t1 values(' ');
insert into vec_t1 values('abc');

insert into vec_t2 values('');
insert into vec_t2 values(' ');
insert into vec_t2 values('abc');

select c as input, substr(c, 1, 2) as result, result is null as is_null from vec_t1;
select c as input, substr(c, 1, 1) as result, result is null as is_null from vec_t1;
select c as input, substr(c, 0, 2) as result, result is null as is_null from vec_t1;
select c as input, substr(c, 1, 0) as result, result is null as is_null from vec_t1;
select c as input, substr(c, 1, -1) as result, result is null as is_null from vec_t1;

delete from vec_t1;
insert into vec_t1 values('');
insert into vec_t1 values('yzy');
insert into vec_t1 values('zzz');
insert into vec_t1 values(' z ');
insert into vec_t1 values('   ');

SELECT c as input, btrim(c, 'y') as result, result is null as isnull from vec_t1;
SELECT c as input, btrim(c, 'z') as result, result is null as isnull from vec_t1;
SELECT c as input, btrim(c) as result, result is null as isnull from vec_t1;
SELECT c as input, rtrim(c, 'y') as result, result is null as isnull from vec_t1;
SELECT c as input, rtrim(c, 'z') as result, result is null as isnull from vec_t1;
SELECT c as input, rtrim(c) as result, result is null as isnull from vec_t1;

-- test row to vec
set try_vector_engine_strategy=force;
create table vec_t3(c text);
insert into vec_t3 values('');
insert into vec_t3 values(' ');
insert into vec_t3 values('abc');

explain analyze select c as input, substr(c, 1, 2) as result, result is null as is_null from vec_t3;
select c as input, substr(c, 1, 2) as result, result is null as is_null from vec_t3;
select c as input, substr(c, 1, 1) as result, result is null as is_null from vec_t3;
select c as input, substr(c, 0, 2) as result, result is null as is_null from vec_t3;
select c as input, substr(c, 1, 0) as result, result is null as is_null from vec_t3;

delete from vec_t3;
insert into vec_t3 values('');
insert into vec_t3 values('yzy');
insert into vec_t3 values('zzz');
insert into vec_t3 values(' z ');
insert into vec_t3 values('   ');

explain analyze SELECT c as input, rtrim(c, 'y') as result, result is null as isnull from vec_t3;
SELECT c as input, rtrim(c, 'y') as result, result is null as isnull from vec_t3;
SELECT c as input, rtrim(c, 'z') as result, result is null as isnull from vec_t3;
SELECT c as input, rtrim(c) as result, result is null as isnull from vec_t3;

create table pad2_tab(a text, b int);
insert into pad2_tab values('yes', 5), ('yes', 1), ('yes', 0);
create table pad3_tab(a text, b int, c text);
insert into pad3_tab values('yes', 5, 'z'), ('yes', 1, 'z'), ('yes', 0, 'z');
SELECT a as p1, b as p2, lpad(a, b) is null from pad2_tab;
SELECT a as p1, b as p2, lpad(a, b, c) is null from pad3_tab;
SELECT a as p1, b as p2, rpad(a, b) is null from pad2_tab;
SELECT a as p1, b as p2, rpad(a, b, c) is null from pad3_tab;

create table trim2(a text, b text);
insert into trim2 values('yzy', 'y'), ('zzz', 'z');
create table trim1(a text);
insert into trim1 values(' z '), ('   ');
SELECT a as p1, b as p2, btrim(a, b) is null from trim2;
SELECT a as p1, b as p2, ltrim(a, b) is null from trim2;
SELECT a as p1, b as p2, rtrim(a, b) is null from trim2;
SELECT a as p1, btrim(a) is null from trim1;
SELECT a as p1, ltrim(a) is null from trim1;
SELECT a as p1, rtrim(a) is null from trim1;

create table translate3(a text, b text, c text);
insert into translate3 values('xyx', 'x', 'z'), ('xzx', 'x', ''), ('xxx', 'x', ''), ('xxx', 'x', ' '), ('xxx', '', 'z'), ('', 'x', 'z');
SELECT a as p1, b as p2, c as p3, translate(a, b, c) is null from translate3;

create table repeat2 (a text, b int);
insert into repeat2 values('a', 3), ('a', 0), (' ', 3), ('', 3), ('', 0);
SELECT a as p1, b as p2, repeat(a, b) is null from repeat2;

create table oidvectortypes1 (a oidvector);
insert into oidvectortypes1 values ('123 456'), (''), (' ');
SELECT a as p1, oidvectortypes(a) is null from oidvectortypes1;

create table regexp_replace3 (a text, b text, c text);
insert into regexp_replace3 values('Thomas', '.[mN]a.', 'M'), ('omas', '.[mN]a.', ''), ('Thomas', '', 'M'), ('', '.[mN]a.', 'M'), ('omas', '.[mN]a.', '');
SELECT a as p1, b as p2, c as p3, regexp_replace(a, b, c) is null from regexp_replace3;

create table regexp_replace6 (a text, b text, c text, d int, e int, f text);
insert into regexp_replace6 values ('foobarbaz','b(..)', E'X\\1Y', 2, 2, 'n'), ('bar','b(..)', '', 1, 1, 'n');
SELECT a as p1, b as p2, c as p3, d as p4, e as p5, f as p6, regexp_replace(a, b, c, d, e, f) is null from regexp_replace6;

create table regexp_split_to_table2 (a text, b text);
insert into regexp_split_to_table2 values('hello world', E'\\s+'), ('', E'\\s+'), ('hello world', ''), ('hello world', null);
SELECT a as p1, b as p2, regexp_split_to_table(a, b) is null from regexp_split_to_table2;

create table substr2(a bytea, b int);
insert into substr2 values ('123'::bytea, 3), ('123'::bytea, 4);
select a as p1, b as p2, substr(a, b) is null from substr2;

create table substr3(a bytea, b int, c int);
insert into substr3 values('123'::bytea, 1, 2), ('123'::bytea, 1, 0);
select a as p1, b as p2, c as p3, substr(a, b, c) is null from substr3;

create table replace3 (a text, b text, c text);
insert into replace3 values ('abc', 'ab', 'd'), ('abc', 'abc', ''), ('abc', 'abc', ''), ('abc', 'ab', ''), ('abc', 'ab', null), ('abc', '', 'd'), ('abc', null, 'd'), ('', 'ab', 'd'), (null, 'ab', 'd');
SELECT a as p1, b as p2, c as p3, replace(a, b, c) is null from replace3;

create table replace2 (a text, b text);
insert into replace2 values('abc', 'ab'), ('abc', 'abc'), ('abc', ''), ('abc', null), ('', 'ab'), (null, 'ab');
SELECT a as p1, b as p2, replace(a, b) is null from replace2;

create table split_part3 (a text, b text, c int);
insert into split_part3 values('1~2~3', '~', 3), ('1~2~3~', '~', 4), ('1~2~3', '', 1), ('1~2~3', null, 1), ('', '~', 1), (null, '~', 1);
SELECT a as p1, b as p2, c as p3, split_part(a, b, c) is null from split_part3;

create table array_to_string2(a integer[], b text);
insert into array_to_string2 values(ARRAY[1, 2, 3, NULL, 5], ','), (ARRAY[1, 2, 3, NULL, 5], ''), (ARRAY[1, 2, 3, NULL, 5], null), (ARRAY[NULL], ''), (ARRAY[NULL], null);
SELECT a as p1, b as p2, array_to_string(a, b) AS RESULT from array_to_string2;

create table array_to_string3(a integer[], b text, c text);
insert into array_to_string3 values(ARRAY[1, 2, 3, NULL, 5], ',', '*'), (ARRAY[1, 2, 3, NULL, 5], '', ''), (ARRAY[1, 2, 3, NULL, 5], null, null), (ARRAY[NULL], '', ''), (ARRAY[NULL], null, null);
SELECT a as p1, b as p2, c as p3, array_to_string(a, b, c) AS RESULT from array_to_string3;

set try_vector_engine_strategy=off;