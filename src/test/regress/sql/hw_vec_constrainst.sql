SET ENABLE_VECTOR_ENGINE=TRUE;

create table source_table(c1 int,c2 int,c3 varchar(10));

CREATE TABLE xc_parent_rep (a int PRIMARY KEY) ; -- OK

--1. NOT NULL
--
create table not_null (c1 int not null,c2 int not null,c3 varchar(10) not null)with (orientation = column);

insert into source_table values
(1,0,'aaaaa'),
(2,0,'aaaaa');
insert into source_table values(3);

insert into not_null select * from source_table;
delete from source_table;

--2. check c1 > x
--
create table check_table1(c1 int,c2 int,c3 varchar(10),check (c1 > 0))with (orientation = column);

--3. c1 > c2
--
create table check_table2(c1 int,c2 int,c3 varchar(10),check (c1 > c2))with (orientation=column);

--4. foreign key
CREATE TABLE xc_child_hash_to_rep (b int, FOREIGN KEY (b) REFERENCES xc_parent_rep(a)) with (orientation = column) ;-- error

drop table source_table;
drop table not_null;
