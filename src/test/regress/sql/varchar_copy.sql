-- test char semantic copy
SET client_min_messages = warning;
drop table if exists test_varchar_copy;
create table test_varchar_copy (id int, city varchar (4 char));
insert into test_varchar_copy values(1, 'abcd');
insert into test_varchar_copy values(2, '中国天津');
insert into test_varchar_copy values(3, 'abcde');
insert into test_varchar_copy values(4, '中国abc');
COPY test_varchar_copy to STDOUT;
truncate table test_varchar_copy;
COPY test_varchar_copy from STDIN;
1	abcd
2	中国天津
3	中a国b
4	中国ab
\.
select * from test_varchar_copy;
drop table test_varchar_copy;

-- test byte semantic copy
drop table if exists test_varchar_byte_copy;
create table test_varchar_byte_copy (id int, city varchar (4 byte));
insert into test_varchar_byte_copy values(1, 'abcd');
insert into test_varchar_byte_copy values(2, '中国天津');
insert into test_varchar_byte_copy values(3, 'abcde');
insert into test_varchar_byte_copy values(4, '中国abc');
COPY test_varchar_byte_copy to STDOUT;
truncate table test_varchar_byte_copy;
COPY test_varchar_byte_copy from STDIN;
1	abcd
2	中国天津
3	中a国b
4	中国ab
\.
select * from test_varchar_byte_copy;
drop table test_varchar_byte_copy;