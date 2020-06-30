--
-- checksum table column and the whole talbe
--
drop table if exists zktt1;
drop table if exists zktt2;
drop table if exists zktt3;
create table zktt1 (
	c1 			tinyint, 
	c2 			smallint, 
	c3 			integer, 
	c4 			bigint, 
	c5 			numeric, 
	c6 			text, 
	c7 			varchar(10), 
	c8 			char, 
	c9 			smalldatetime, 
	c10 		timestamp, 
	c11 		timestamptz, 
	c12 		date, 
	c13 		money, 
	c14 		bool
);

create table zktt2 (
	a 			int, 
	b 			text
);

create table zktt3 (
	a 			int, 
	b 			text
) 
with (orientation = column);

--
-- insert values
--

insert into zktt1 values (1, 2, 3, 4, 5.6, '789', '1112', '1', smalldatetime '2010-01-10 08:08:08', timestamp '2010-01-10 08:08:08',timestamptz '2010-01-10 08:08:08',date '2010-08-08', money '234234.90', true);
insert into zktt1 values (4, 3, 2, 1, 6.5, '432', '2222', '2', smalldatetime '2012-01-10 08:08:08', timestamp '2011-12-10 08:08:08',timestamptz '2011-12-10 08:08:08',date '1234-05-06', money '1234.56', false);

insert into zktt2 values (1, '');

insert into zktt2 values (2, null);

insert into zktt2 values (3, 3);

insert into zktt3 select * from zktt2;

--
-- checksum for given date types
--

select checksum(c1) from zktt1;

select checksum(c2) from zktt1; 

select checksum(c3) from zktt1; 

select checksum(c4) from zktt1; 

select checksum(c5) from zktt1; 

select checksum(c6) from zktt1; 

select checksum(c7) from zktt1; 

select checksum(c8) from zktt1; 

select checksum(c9) from zktt1; 

select checksum(c11) from zktt1; 

select checksum(c12) from zktt1; 

select checksum(c13) from zktt1; 

select checksum(c13::text) from zktt1;

select checksum(c14) from zktt1;       

select checksum(c14::text) from zktt1;

--
-- checksum for null values
--

select checksum(b) from zktt2 where a < 3;

select checksum(b) from zktt2;

select checksum(b), checksum(b) from zktt2;

--
-- checksum for group by order by
--

select checksum(a) from zktt2 group by a order by a;

--
-- checksum for join tables
--

select checksum(a) from zktt2 join zktt1 on zktt2.a=zktt1.c1;

--
-- checksum for sub query and complex query
--

select checksum(a) from zktt2 where a in (select c1 from zktt1);

select * from (select checksum(a) from zktt2 group by a order by a) order by checksum;

select checksum(tbl::text) from (select checksum(a) from zktt2 group by a) as tbl; 

--
-- checksum for update table data
--

begin;
select checksum(zktt2::text) from zktt2;
update zktt2 set b='4' where a=1;                
select checksum(zktt2::text) from zktt2;
rollback;

--
-- checksum for insert table data
--
begin;
select checksum(zktt2::text) from zktt2;
insert into zktt2 values (1,2);   
select checksum(zktt2::text) from zktt2;
rollback;

--
-- checksum for delete table data
--

begin;
select checksum(zktt2::text) from zktt2;
delete from zktt2 where a=1;   
select checksum(zktt2::text) from zktt2;
rollback;

--
-- checksum for column-stored table 
--

select checksum(zktt3::text) from zktt3;

DROP TABLE zktt1;
DROP TABLE zktt2;
DROP TABLE zktt3;

--
-- test pg_stat_get_activity_for_temptable
--

select * from pg_stat_get_activity_for_temptable() limit 1;
