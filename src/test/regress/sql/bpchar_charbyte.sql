-- test char semantic limit size
SET client_min_messages = warning;
drop table if exists test_charbyte_char;
create table test_charbyte_char (id int, city char (4 char));
insert into test_charbyte_char values(1, 'abcd');
insert into test_charbyte_char values(2, '中国天津');
insert into test_charbyte_char values(3, 'abcde');
insert into test_charbyte_char values(5, '中国abc');
select table_name, column_name, data_type, character_maximum_length, character_octet_length  from information_schema.columns where table_name = 'test_charbyte_char';
drop table test_charbyte_char;

-- test byte semantic limit size
drop table if exists test_charbyte_byte;
create table test_charbyte_byte (id int, city char (4 byte));
insert into test_charbyte_byte values(1, 'abcd');
insert into test_charbyte_byte values(2, '中国天津');
insert into test_charbyte_byte values(3, 'abcde');
insert into test_charbyte_byte values(5, '中国abc');
select table_name, column_name, data_type, character_maximum_length, character_octet_length  from information_schema.columns where table_name = 'test_charbyte_byte';
drop table test_charbyte_byte;

-- test char semantic operator
drop table if exists test_char_operator;
create table test_char_operator (id int, city char (10 char));
insert into test_char_operator values(1, '北京');
insert into test_char_operator values(2, '上海');
insert into test_char_operator values(3, '天津');
insert into test_char_operator values(4, '广州');
select * from test_char_operator where city = '北京' order by id;
select * from test_char_operator where city <> '北京' order by id;
select * from test_char_operator where city > '上海' order by id;
select * from test_char_operator where city >= '上海' order by id;
select * from test_char_operator where city < '上海' order by id;
select * from test_char_operator where city <= '上海' order by id;
drop table test_char_operator;

-- test byte semanmtic operator
drop table if exists test_byte_operator;
create table test_byte_operator (id int, city char (10 char));
insert into test_byte_operator values(1, 'beijing');
insert into test_byte_operator values(2, 'shanghai');
insert into test_byte_operator values(3, 'tianjin');
insert into test_byte_operator values(4, 'guangzhou');
select * from test_byte_operator where city = 'beijing' order by id;
select * from test_byte_operator where city <> 'beijing' order by id;
select * from test_byte_operator where city > 'shanghai' order by id;
select * from test_byte_operator where city >= 'shanghai' order by id;
select * from test_byte_operator where city < 'shanghai' order by id;
select * from test_byte_operator where city <= 'shanghai' order by id;
