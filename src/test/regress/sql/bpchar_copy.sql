-- test char semantic copy
SET client_min_messages = warning;
drop table if exists test_copy_char;
create table test_copy_char (id int, city char (4 char));
insert into test_copy_char values(1, 'abcd');
insert into test_copy_char values(2, '中国天津');
insert into test_copy_char values(3, 'abcde');
insert into test_copy_char values(4, '中国abc');
COPY test_copy_char to STDOUT;
truncate table test_copy_char;
COPY test_copy_char from STDIN;
1	abcd
2	中国天津
3	中a国b
4	中国ab
\.
select * from test_copy_char;
drop table test_copy_char;

-- test byte semantic copy
drop table if exists test_copy_byte;
create table test_copy_byte (id int, city char (4 byte));
insert into test_copy_byte values(1, 'abcd');
insert into test_copy_byte values(2, '中国天津');
insert into test_copy_byte values(3, 'abcde');
insert into test_copy_byte values(4, '中国abc');
COPY test_copy_byte to STDOUT;
truncate table test_copy_byte;
COPY test_copy_byte from STDIN;
1	abcd
2	中国天津
3	中a国b
4	中国ab
\.
select * from test_copy_byte;
drop table test_copy_byte;