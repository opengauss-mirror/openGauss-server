SET client_min_messages = warning;
-- test char semantic regular expression case
drop table if exists test_char_regex;
create table test_char_regex (id int, city char (10 char));
insert into test_char_regex values(1, '北京');
insert into test_char_regex values(2, '广州');
insert into test_char_regex values(3, '京广');
insert into test_char_regex values(4, '北上广');
-- test char semantic regular expression match
select * from test_char_regex where city ~ '北' order by id;
select * from test_char_regex where city !~ '北' order by id;
select * from test_char_regex where city ~ '广' order by id;
select * from test_char_regex where city !~ '广' order by id;
-- test char semantic like expression match
select * from test_char_regex where city like '北%' order by id;
select * from test_char_regex where city like '广%' order by id;
select * from test_char_regex where city like '%广' order by id;
select * from test_char_regex where city like '_广' order by id;
select * from test_char_regex where city like '广_' order by id;
-- test char semantic like operator match
select * from test_char_regex where city ~~ '北%' order by id;
select * from test_char_regex where city ~~ '广%' order by id;
select * from test_char_regex where city ~~ '%广' order by id;
select * from test_char_regex where city ~~ '_广' order by id;
select * from test_char_regex where city ~~ '广_' order by id;
-- test char semantic not like operator match
select * from test_char_regex where city !~~ '北%' order by id;
select * from test_char_regex where city !~~ '广%' order by id;
select * from test_char_regex where city !~~ '%广' order by id;
select * from test_char_regex where city !~~ '_广' order by id;
select * from test_char_regex where city !~~ '广_' order by id;
-- test char semantic ignore case regular expression match
truncate table test_char_regex;
insert into test_char_regex values (1, 'beijing');
insert into test_char_regex values (1, 'tianjin');
select * from test_char_regex where city ~* 'Bei' order by id;
select * from test_char_regex where city !~* 'Bei' order by id;
-- test char semantic ignore case like operator
select * from test_char_regex where city ~~* 'Bei%' order by id;
select * from test_char_regex where city !~~* 'Bei%' order by id;
-- test char semantic pattern compare
select * from test_char_regex where city ~<~ 'bejng';
select * from test_char_regex where city ~<=~ 'bejng';
select * from test_char_regex where city ~>~ 'bejng';
select * from test_char_regex where city ~>=~ 'bejng';
drop table test_char_regex;

-- test byte semantic regular expression case
drop table if exists test_byte_regex;
create table test_byte_regex (id int, city char (10 byte));
insert into test_byte_regex values(1, '北京');
insert into test_byte_regex values(2, '广州');
insert into test_byte_regex values(3, '京广');
insert into test_byte_regex values(4, '北上广');
-- test char semantic regular expression match
select * from test_byte_regex where city ~ '北' order by id;
select * from test_byte_regex where city !~ '北' order by id;
select * from test_byte_regex where city ~ '广' order by id;
select * from test_byte_regex where city !~ '广' order by id;
-- test char semantic like expression match
select * from test_byte_regex where city like '北%' order by id;
select * from test_byte_regex where city like '广%' order by id;
select * from test_byte_regex where city like '%广' order by id;
select * from test_byte_regex where city like '_广' order by id;
select * from test_byte_regex where city like '广_' order by id;
-- test char semantic like operator match
select * from test_byte_regex where city ~~ '北%' order by id;
select * from test_byte_regex where city ~~ '广%' order by id;
select * from test_byte_regex where city ~~ '%广' order by id;
select * from test_byte_regex where city ~~ '_广' order by id;
select * from test_byte_regex where city ~~ '广_' order by id;
-- test char semantic not like operator match
select * from test_byte_regex where city !~~ '北%' order by id;
select * from test_byte_regex where city !~~ '广%' order by id;
select * from test_byte_regex where city !~~ '%广' order by id;
select * from test_byte_regex where city !~~ '_广' order by id;
select * from test_byte_regex where city !~~ '广_' order by id;

-- test byte semantic ignore case regular expression match
truncate table test_byte_regex;
insert into test_byte_regex values (1, 'beijing');
insert into test_byte_regex values (1, 'tianjin');
select * from test_byte_regex where city ~* 'Bei' order by id;
select * from test_byte_regex where city !~* 'Bei' order by id;
-- test byte semantic ignore case like operator
select * from test_byte_regex where city ~~* 'Bei%' order by id;
select * from test_byte_regex where city !~~* 'Bei%' order by id;
-- test byte semantic pattern compare
select * from test_byte_regex where city ~<~ 'bejng';
select * from test_byte_regex where city ~<=~ 'bejng';
select * from test_byte_regex where city ~>~ 'bejng';
select * from test_byte_regex where city ~>=~ 'bejng';
drop table test_byte_regex;