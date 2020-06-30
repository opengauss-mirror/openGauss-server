set td_compatible_truncation=on;
set td_compatible_truncation=off;
drop database if exists td_db_char;
create database td_db_char DBCOMPATIBILITY 'C';

\c td_db_char

set client_encoding='GBK'; 
show td_compatible_truncation;
drop table if exists testtd;
--Create table.
create table testtd(
c_nchar nchar(10),
c_character1 character(10),
c_character2 character varying(10),
c_varchar2 varchar2(10)
);
drop table if exists testtdn;

create table testtdn(
c_nchar nchar(10),
c_character1 character(10),
c_character2 character varying(10),
c_varchar2 varchar2(10),
c_nvarchar2 nvarchar2(10)
);

--test not include nvarchar2 type.
insert into testtd values('aaaaaaaaaaaaaaa', 'aaaaaaaaaaaaaa','aaaaaaaaaaaaaaa', 'aaaaaaaaaaaaaa');
select * from testtd order by c_nchar;
insert into testtd values('aaa一二aaaaaa一二', 'aaaaaaaaa一二','aaaaaaaaa一二', 'aaaaaaaaa一二');
insert into testtd values('aaaaaaaaa一二三','aaaaaaaaa一二三','aaaaaaaaa一二三', 'aaaaaaaaa一二三');
insert into testtd values('aaaaaa一s二','aaaaaa一s二三','aaaaaa一s二三', 'aaaaaa一s二三');
select * from testtd order by c_nchar;
delete from testtd;
insert into testtd values('aaaaaa已经aaaaaaaaa', 'aaaaaaaaaaaaaa','aaaaaaaaaaaaaaa', 'aaaaaaaaaaaaaa'),
('bbbbbbbbbbbbbbbb', 'bbbbb同意bbbbbbbbbbb','bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
('ccccccccccccccccc','ccccccccccccc','ccccc收获cccccccc','ccccccccccccccc');
select * from testtd order by c_nchar;

--test include nvarchar2 type.

insert into testtdn values('aaaaaaaaaaaaaaa', 'aaaaaaaaaaaaaa','aaaaaaaaaaaaaaa', 'aaaaaaaaaaaaaa','aaaaaaaaaaaaaa');
select * from testtdn order by c_nchar;
insert into testtdn values('aaa一二aaaaaa一二', 'aaaaaaaaa一二','aaaaaaaaa一二', 'aaaaaaaaa一二','aaaaaaaaa一二');
insert into testtdn values('aaaaaaaaa一二三','aaaaaaaaa一二三','aaaaaaaaa一二三', 'aaaaaaaaa一二三','aaaaaaaaa一二三');
insert into testtdn values('aaaaaa一s二','aaaaaa一s二三','aaaaaa一s二三', 'aaaaaa一s二三','aaaaaa一s二三');
select * from testtdn order by c_nchar;
delete from testtdn;
insert into testtdn values('aaaaaa已经aaaaaaaaa', 'aaaaaaaaaaaaaa','aaaaaaaaaaaaaaa', 'aaaaaaaaaaaaaa','aaaaaaaaaaaaaa'),
('bbbbbbbbbbbbbbbb', 'bbbbb同意bbbbbbbbbbb','bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb','bbbbbbbbbbbbbbbb'),
('ccccccccccccccccc','ccccccccccccc','ccccc收获cccccccc','ccccccccccccccc', 'cccccccccccccc');
select * from testtdn order by c_nchar;


drop table if exists testtd1;
create table testtd1(
c_nchar nchar(8),
c_character1 character(5),
c_character2 character varying(5),
c_varchar2 varchar2(5)
);

drop table if exists testtd1n;
create table testtd1n(
c_nchar nchar(8),
c_character1 character(5),
c_character2 character varying(5),
c_varchar2 varchar2(5),
c_nvarchar2 nvarchar2(5)
);

insert into testtd1 select * from testtd;
select * from testtd1 order by c_nchar;

insert into testtd1n select * from testtdn;
select * from testtd1n order by c_nchar;


drop table if exists testtd2 ;
create table testtd2 (c_int int primary key,
c_nchar nchar(6),
c_character1 character(6),
c_character2 character varying(6),
c_varchar2 varchar2(6)
);

drop table if exists testtd2n ;
create table testtd2n (c_int int primary key,
c_nchar nchar(6),
c_character1 character(6),
c_character2 character varying(6),
c_varchar2 varchar2(6),
c_nvarchar2 nvarchar2(20)
);

insert into testtd2 values(1, 'tttttt', 'tttttt','tttttt','tttttt');
insert into testtd1 values((select c_varchar2 from testtd2 where c_int = 1), 'tttttt','tttttt','tttttt');
select * from testtd1 order by c_nchar;

insert into testtd2n values(1, 'tttttt', 'tttttt','tttttt','tttttt','tttttt');
insert into testtd1n values((select c_nvarchar2 from testtd2n where c_int = 1), 'tttttt','tttttt','tttttt','tttttt');
select * from testtd1n order by c_nchar;

drop table if exists testtd_def1;
drop table if exists testtd_def2;

create table testtd_def1(
c_nchar nchar(10) default 'aaaaaaaaaaaaaaaaaaaaaa',
c_character1 character(10) default 'aaaaaaaaaaaaaaaaaaaaaa',
c_character2 character varying(10) default 'aaaaaaaaaaaaaaaaaaaaaa',
c_varchar2 varchar2(10) default 'aaaaaaaaaaaaaaaaaaaaaa'
);

create table testtd_def2(
c_nvarchar2 nvarchar2(10) default 'aaaaaaaaaaaaaaaaaaaaaa'
);

drop table if exists testtd_def1;
drop table if exists testtd_def2;

set td_compatible_truncation=on;

insert into testtd values('aaaaaaaaaaaaaaa', 'aaaaaaaaaaaaaa','aaaaaaaaaaaaaaa', 'aaaaaaaaaaaaaa');
select * from testtd order by c_nchar;
insert into testtd values('aaa一二aaaaaa一二', 'aaaaaaaaa一二','aaaaaaaaa一二', 'aaaaaaaaa一二');
insert into testtd values('aaaaaaaaa一二三','aaaaaaaaa一二三','aaaaaaaaa一二三', 'aaaaaaaaa一二三');
insert into testtd values('aaaaaa一s二','aaaaaa一s二三','aaaaaa一s二三', 'aaaaaa一s二三');
select * from testtd order by c_nchar;
delete from testtd;
insert into testtd values('aaaaaa已经aaaaaaaaa', 'aaaaaaaaaaaaaa','aaaaaaaaaaaaaaa', 'aaaaaaaaaaaaaa'),
('bbbbbbbbbbbbbbbb', 'bbbbb同意bbbbbbbbbbb','bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
('ccccccccccccccccc','ccccccccccccc','ccccc收获cccccccc','ccccccccccccccc');
select * from testtd order by c_nchar;

insert into testtd1 select * from testtd;
select * from testtd1 order by c_nchar;

--insert into testtd2 values(1, 'tttttt', 'tttttt','tttttt','tttttt');
insert into testtd1 values((select c_nvarchar2 from testtd2n where c_int = 1), 'tttttt','tttttt','tttttt');

select * from testtd1 order by c_nchar;

--Include nvarchar2 

insert into testtdn values('aaaaaaaaaaaaaaa', 'aaaaaaaaaaaaaa','aaaaaaaaaaaaaaa', 'aaaaaaaaaaaaaa','aaaaaaaaaaaaaa');
select * from testtdn order by c_nchar;
insert into testtdn values('aaa一二aaaaaa一二', 'aaaaaaaaa一二','aaaaaaaaa一二', 'aaaaaaaaa一二','aaaaaaaaa一二');
insert into testtdn values('aaaaaaaaa一二三','aaaaaaaaa一二三','aaaaaaaaa一二三', 'aaaaaaaaa一二三','aaaaaaaaa一二三');
insert into testtdn values('aaaaaa一s二','aaaaaa一s二三','aaaaaa一s二三', 'aaaaaa一s二三','aaaaaa一s二三');
select * from testtdn order by c_nchar;
delete from testtdn;
insert into testtdn values('aaaaaa已经aaaaaaaaa', 'aaaaaaaaaaaaaa','aaaaaaaaaaaaaaa', 'aaaaaaaaaaaaaa','aaaaaaaaaaaaaa'),
('bbbbbbbbbbbbbbbb', 'bbbbb同意bbbbbbbbbbb','bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb','bbbbbbbbbbbbbbbb'),
('ccccccccccccccccc','ccccccccccccc','ccccc收获cccccccc','ccccccccccccccc', 'cccccccccccccc');
select * from testtdn order by c_nchar;

insert into testtd1n select * from testtdn;
select * from testtd1n order by c_nchar;

--insert into testtd2n values(1, 'tttttt', 'tttttt','tttttt','tttttt','tttt@%$6666');
insert into testtd1n values((select c_nvarchar2 from testtd2n where c_int = 1), 'tttttt','tttttt','tttttt','tttttt');

select * from testtd1n order by c_nchar;

drop table if exists testtd_def1;
drop table if exists testtd_def2;


create table testtd_def1(
c_nchar nchar(10) default 'aaaaaaaaaaaaaaaaaaaaaa',
c_character1 character(10) default 'aaaaaaaaaaaaaaaaaaaaaa',
c_character2 character varying(10) default 'aaaaaaaaaaaaaaaaaaaaaa',
c_varchar2 varchar2(10) default 'aaaaaaaaaaaaaaaaaaaaaa'
);

create table testtd_def2(
c_nvarchar2 nvarchar2(10) default 'aaaaaaaaaaaaaaaaaaaaaa',
c_nvarchar22 nvarchar2(10) default 'aaaaaaaaaaaaaaaaaaaaaa'
);

insert into testtd_def1 values(default,default,default,default);
insert into testtd_def1 values(default,default,default,default),(default,default,default,default),(default,default,default,default);
insert into testtd_def2 values(default,default),(default,default),(default,default);
insert into testtd_def2 values(default,default);

insert into testtd_def1 values('111b',default,default,default);
insert into testtd_def1 values(default,'adkjdkjgsd;uao;s','adkjdkjgsd;uao;s','adkjdkjgsd;uao;s');
insert into testtd_def2 values('11111fdk',default);
insert into testtd_def2 values(default,'nldjfdlkf');

drop table if exists testtd_def1;
drop table if exists testtd_def2;

--stored procedure
set td_compatible_truncation=off;
drop table if exists inst_trc_t1_01;
drop procedure if exists pro_insert_trunc;
create table inst_trc_t1_01(
c_char1 char(6),
c_char2 char(6),
c_varchar1 varchar(6),
c_varchar2 varchar2(6),
c_nvarchar nvarchar2(6));

CREATE OR REPLACE PROCEDURE pro_insert_trunc(
c_varchar2 varchar2(50),
c_date date,
c_timestamp_wztimezone timestamp with time zone,
c_number number
)
AS 
BEGIN 
insert into inst_trc_t1_01 values(c_varchar2,c_date,c_timestamp_wztimezone,c_number,substr(c_date,3,6)); 
insert into inst_trc_t1_01 values(cast(c_varchar2 as char(10)),cast(c_date as varchar),cast(c_timestamp_wztimezone as char(30)),cast(c_number as varchar),cast(c_date as char(6)));
END;
/

show td_compatible_truncation;
call pro_insert_trunc(43947123894723895743728943.5425235,'2016-06-30','2013-12-11 pst',321548764546.154564);

set td_compatible_truncation=on;
show td_compatible_truncation;

call pro_insert_trunc(43947123894723895743728943.5425235,'2016-06-30','2013-12-11 pst',321548764546.154564);

set td_compatible_truncation=off;
show td_compatible_truncation;
call pro_insert_trunc(43947123894723895743728943.5425235,'2016-06-30','2013-12-11 pst',321548764546.154564);

drop table if exists inst_trc_t1_01;
drop procedure if exists pro_insert_trunc;

--Prepare stmt test
drop table if exists pre_table;
create table pre_table(a char(6), b varchar(6));
show td_compatible_truncation;

prepare insert1(char(6), varchar(6)) as insert into pre_table values($1,$2);

execute insert1('aaaaaaaaaaaaaaaa', 'bbbbbbbbbbbbbbbb');

set td_compatible_truncation=on;
show td_compatible_truncation;

execute insert1('aaaaaaaaaaaaaaaa', 'bbbbbbbbbbbbbbbb');

set td_compatible_truncation=off;
show td_compatible_truncation;

execute insert1('aaaaaaaaaaaaaaaa', 'bbbbbbbbbbbbbbbb');

drop table if exists pre_table;

drop table testtd;
drop table testtdn;
drop table testtd1;
drop table testtd2;
drop table testtd1n;
drop table testtd2n;

commit;

set td_compatible_truncation = on;
create table error_callback_tbl(a int, b int);
insert into error_callback_tbl values(generate_series(1, 1000),generate_series(1, 1000));

CREATE or replace FUNCTION pro_refcursor_010(in i_num integer,in sum_num integer, OUT o_resultnum character varying, OUT o_result_cur refcursor) RETURNS record
LANGUAGE plpgsql NOT SHIPPABLE
AS $$
declare
    v_tmpsql    varchar(100);
    v_num       integer;
begin
    v_tmpsql := 'select b from error_callback_tbl where a =' || i_num || '';
    if i_num > 0 then
        execute immediate v_tmpsql into v_num;
        pro_refcursor_010(i_num-1.0,sum_num+i_num+0.0,o_resultnum,o_result_cur);
        o_resultnum := sum_num;
        o_result_cur := v_tmpsql;
    else
        o_resultnum := sum_num;
        o_result_cur := v_tmpsql;
    end if;
end$$;
call pro_refcursor_010(10.0,0.1,0,0);
drop function pro_refcursor_010;
drop table error_callback_tbl;
reset td_compatible_truncation;

\c regression
