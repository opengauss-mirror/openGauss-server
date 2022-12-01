-- B db compatibility case
drop database if exists my_test;
create database my_test dbcompatibility 'B';
\c my_test

--Test default delimiter
select 1; 

--Test delimiter aa
delimiter aa;
select 1aa
select 1aaselect 1;aa
select kaa
delimiter ;aa

--Test delimiter //
delimiter //;
select 1//
delimiter ;//

--Test delimiter length
delimiter aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa;

--Test delimiter %
delimiter %;
select 1%
delimiter ;%

--Test delimiter 'Mysql'
delimiter 'Mysql';
select 1Mysql
delimiter ;Mysql

--Test other
delimiter sds;
delimiter aasds
select 1aasds
delimiter ;aasds

--
delimiter asd ss;
select 1asd
delimiter ;asd

delimiter bb
delimiter aa
select 1aa
delimiter ;

delimiter de
delimiter abcde
select 1abcde
delimiter zz sdsd aasds
delimiter kkasda "sdsd" sdsda
select 1kkasda
delimiter
delimiter "sdsd sd"
select 1"sdsd sd"
delimiter ;

\c regression
drop database my_test;