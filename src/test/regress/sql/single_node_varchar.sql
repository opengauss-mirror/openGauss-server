--
-- VARCHAR
--

CREATE TABLE VARCHAR_TBL(f1 varchar(1));

INSERT INTO VARCHAR_TBL (f1) VALUES ('a');

INSERT INTO VARCHAR_TBL (f1) VALUES ('A');

-- any of the following three input formats are acceptable
INSERT INTO VARCHAR_TBL (f1) VALUES ('1');

INSERT INTO VARCHAR_TBL (f1) VALUES (2);

INSERT INTO VARCHAR_TBL (f1) VALUES ('3');

-- zero-length char
INSERT INTO VARCHAR_TBL (f1) VALUES ('');

-- try varchar's of greater than 1 length
INSERT INTO VARCHAR_TBL (f1) VALUES ('cd');
INSERT INTO VARCHAR_TBL (f1) VALUES ('c     ');


SELECT '' AS seven, * FROM VARCHAR_TBL;

SELECT '' AS six, c.*
   FROM VARCHAR_TBL c
   WHERE c.f1 <> 'a';

SELECT '' AS one, c.*
   FROM VARCHAR_TBL c
   WHERE c.f1 = 'a';

SELECT '' AS five, c.*
   FROM VARCHAR_TBL c
   WHERE c.f1 < 'a';

SELECT '' AS six, c.*
   FROM VARCHAR_TBL c
   WHERE c.f1 <= 'a';

SELECT '' AS one, c.*
   FROM VARCHAR_TBL c
   WHERE c.f1 > 'a';

SELECT '' AS two, c.*
   FROM VARCHAR_TBL c
   WHERE c.f1 >= 'a';

DROP TABLE VARCHAR_TBL;

--
-- Now test longer arrays of char
--

CREATE TABLE VARCHAR_TBL(f1 varchar(4));

INSERT INTO VARCHAR_TBL (f1) VALUES ('a');
INSERT INTO VARCHAR_TBL (f1) VALUES ('ab');
INSERT INTO VARCHAR_TBL (f1) VALUES ('abcd');
INSERT INTO VARCHAR_TBL (f1) VALUES ('abcde');
INSERT INTO VARCHAR_TBL (f1) VALUES ('abcd    ');

SELECT '' AS four, * FROM VARCHAR_TBL;

create table tab_1(col1 varchar(3)); 
create table tab_2(col2 char(3));
insert into tab_2 values('   ');
insert into tab_1 select col2 from tab_2;
select * from tab_1 where col1 is null;
select * from tab_1 where col1='   ';

delete from tab_1;
set behavior_compat_options = 'char_coerce_compat';
insert into tab_1 select col2 from tab_2;
select * from tab_1 where col1 is null;
select * from tab_1 where col1='   ';
set behavior_compat_options = ''; 
drop table tab_1;
drop table tab_2;

select length(rtrim('123   '));
set behavior_compat_options = 'char_coerce_compat';
select length(rtrim('123   '));
set behavior_compat_options = '';

set behavior_compat_options = 'char_coerce_compat';
create table tbl_111 (id int);
insert into tbl_111 values(1);

select count(*) from tbl_111 where cast(' ' as char(5)) = cast(' ' as varchar(5));
select count(*) from tbl_111 where cast('abc ' as char(10)) = cast('abc ' as varchar(10));

select count(*) from tbl_111 where cast(' ' as char(5)) != cast(' ' as varchar(5));
select count(*) from tbl_111 where cast('abc ' as char(10)) != cast('abc ' as varchar(10));

select count(*) from tbl_111 where cast(' ' as char(5)) > cast(' ' as varchar(5));
select count(*) from tbl_111 where cast('abc ' as char(10)) > cast('abc ' as varchar(10));

select count(*) from tbl_111 where cast(' ' as char(5)) >= cast(' ' as varchar(5));
select count(*) from tbl_111 where cast('abc ' as char(10)) >= cast('abc ' as varchar(10));

select count(*) from tbl_111 where cast(' ' as char(5)) < cast(' ' as varchar(5));
select count(*) from tbl_111 where cast('abc ' as char(10)) < cast('abc ' as varchar(10));

select count(*) from tbl_111 where cast(' ' as char(5)) <= cast(' ' as varchar(5));
select count(*) from tbl_111 where cast('abc ' as char(10)) <= cast('abc ' as varchar(10));

drop table tbl_111;

create table tbl_111 (a char(5), b varchar(5));
insert into tbl_111 values (' ', ' ');
insert into tbl_111 values ('abc ', 'abc ');
select count(*) from tbl_111 where a = b;
select count(*) from tbl_111 where a != b;
select count(*) from tbl_111 where a > b;
select count(*) from tbl_111 where a >= b;
select count(*) from tbl_111 where a < b;
select count(*) from tbl_111 where a <= b;

set try_vector_engine_strategy='force';
select count(*) from tbl_111 where a = b;
select count(*) from tbl_111 where a != b;
select count(*) from tbl_111 where a > b;
select count(*) from tbl_111 where a >= b;
select count(*) from tbl_111 where a < b;
select count(*) from tbl_111 where a <= b;
set try_vector_engine_strategy='off';

drop table tbl_111;

create table tbl_111 (a char(5), b varchar(5)) with (orientation = column);
insert into tbl_111 values (' ', ' ');
insert into tbl_111 values ('abc ', 'abc ');
select count(*) from tbl_111 where a = b;
select count(*) from tbl_111 where a != b;
select count(*) from tbl_111 where a > b;
select count(*) from tbl_111 where a >= b;
select count(*) from tbl_111 where a < b;
select count(*) from tbl_111 where a <= b;

set enable_codegen to true;
set codegen_cost_threshold to 0;
select count(*) from tbl_111 where a = b;
select count(*) from tbl_111 where a != b;
select count(*) from tbl_111 where a > b;
select count(*) from tbl_111 where a >= b;
select count(*) from tbl_111 where a < b;
select count(*) from tbl_111 where a <= b;

drop table tbl_111;
set behavior_compat_options = '';
