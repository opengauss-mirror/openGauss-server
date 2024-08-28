-- test char semantic limit size
SET client_min_messages = warning;
show nls_length_semantics;
drop table if exists test_semantic_char;
set nls_length_semantics='char';
create table test_semantic_char (id int, city char (4));
set nls_length_semantics='byte';
\d+ test_semantic_char;
insert into test_semantic_char values(1, 'abcd');
insert into test_semantic_char values(2, '中国天津');
insert into test_semantic_char values(3, 'abcde');
insert into test_semantic_char values(5, '中国abc');
drop table test_semantic_char;

drop table if exists test_semantic_byte;
set nls_length_semantics='byte';
create table test_semantic_byte (id int, city char (4));
set nls_length_semantics='char';
\d+ test_semantic_byte;
insert into test_semantic_byte values(1, 'abcd');
insert into test_semantic_byte values(2, '中国天津');
insert into test_semantic_byte values(3, 'abcde');
insert into test_semantic_byte values(5, '中国abc');
drop table test_semantic_byte;

reset nls_length_semantics;
