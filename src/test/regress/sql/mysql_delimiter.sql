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
delimiter ""
delimiter ''
delimiter aaaaaaaaaaaaaaaa
delimiter "aaaaaaaaaaaaaaaa"
delimiter aaaaaaaaaaaaaaa
delimiter ;

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

-- test delimiter use in create procedure situation
-- report gram error in server ,not subprogram end error, success in plugin 
create table test_table (dot_no int);
insert into test_table values(1);
insert into test_table values(NULL);

delimiter //

create procedure test91()
begin
  declare rec_curs_value int;
  declare curs_dot cursor for select dot_no from test_table;
    open curs_dot;
    fetch curs_dot into rec_curs_value;
    while rec_curs_value is not null do
      fetch curs_dot into rec_curs_value;
    end while;
    close curs_dot;
end;
//
delimiter ;


delimiter //

create procedure test92()
begin
  declare rec_curs_value int;
  declare curs_dot cursor for select dot_no from test_table;
    open curs_dot;
    fetch curs_dot into rec_curs_value;
    while rec_curs_value is null do
      fetch curs_dot into rec_curs_value;
    end while;
    close curs_dot;
end;
//

delimiter ;


-- test deterministic error

create function fun2(age1 int)return int DETERMINISTIC 
NOT SHIPPABLE NOT FENCED EXTERNAL SECURITY INVOKER  
AS 
declare
a1 int;
begin
return a1;
end;
/

select fun2(1);

-- test create procedure select error 
create table t1 (a int);

insert into t1 values (1),(2);

-- server should has gram error,plugin pass
create procedure pro_test() select a from t1;

-- server should has gram error, plugin pass;
create procedure pro_test2() select a as b from t1;



\c regression
drop database my_test;
