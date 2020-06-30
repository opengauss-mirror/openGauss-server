set time zone 'PRC';
create table dcs_cpu(
	vcpu_num int,
	node text,
	scope_name text,
	server_ip text not null,
	iowait real,
	time_string timestamp
)with (orientation = column,compression = middle, COMPRESSLEVEL=1);
insert into dcs_cpu values(20, 'node','cloudimage', '00.00.000.0',4,'2019-12-09T02:20:35.000+08:00'),(20, 'node','cloudimage', '00.00.000.0',4,'2019-12-09T02:20:36.000+08:00'),(20, 'node','cloudimage', '00.00.000.0',4,'2019-12-09T02:20:35.000+08:00'),(20, 'node','cloudimage', '00.00.000.0',4,'2019-12-09T02:20:35.000+08:00'),(20, 'node','cloudimage', '00.00.000.0',4,'2019-12-09T02:20:35.000+08:00');
select * from dcs_cpu;


-- create table with 
create table test_timestamp(time timestamp)with(orientation=column);

insert into test_timestamp values('2019-09-11 14:05:50'),('3019-09-11 14:05:50'),('1019-09-11 14:05:50'),('1019-09-11 14:05:50'),('1019-09-11 14:05:50');
select * from test_timestamp order by time;
delete from test_timestamp;
insert into test_timestamp values('2019-09-11 14:05:50'),('3019-09-11 14:05:50'),('1019-09-11 14:05:50'),('1019-09-11 14:05:50'),('1019-09-11 14:05:50'),('1019-09-11 14:05:50'),('1019-09-11 14:05:50'),('1019-09-11 14:05:50'),('2019-09-11 14:05:50'),('3019-09-11 14:05:50'),('1019-09-11 14:05:50'),('1019-09-11 14:05:50');
select * from test_timestamp order by time;
delete from test_timestamp;
insert into test_timestamp values('9999-09-11 14:05:50'),('1000-09-11 14:05:50'),('1019-09-11 14:05:50'),('1019-09-11 15:05:50'),('1019-09-11 15:05:50'),('1019-09-11 15:35:50'),('1019-09-11 15:35:50'),('1019-09-11 15:35:00'),('2019-09-11 14:05:50'),('3019-09-11 14:05:50'),('1019-09-11 14:05:50'),('1019-09-11 14:05:50');
select * from test_timestamp order  by time;
delete from test_timestamp;
insert into test_timestamp values('9999-09-11 14:05:50'),('9999-09-11 14:06:50'),('9999-09-11 14:07:00'),('9999-09-11 14:07:50');

-- check with compression = middle, compresslevel = 0
delete from test_timestamp;
alter table test_timestamp set(compression=middle);
insert into test_timestamp values('2019-09-11 14:05:50'),('3019-09-11 14:05:50'),('1019-09-11 14:05:50'),('1019-09-11 14:05:50'),('1019-09-11 14:05:50');
select * from test_timestamp order by time;
delete from test_timestamp;
insert into test_timestamp values('2019-09-11 14:05:50'),('3019-09-11 14:05:50'),('1019-09-11 14:05:50'),('1019-09-11 14:05:50'),('1019-09-11 14:05:50'),('1019-09-11 14:05:50'),('1019-09-11 14:05:50'),('1019-09-11 14:05:50'),('2019-09-11 14:05:50'),('3019-09-11 14:05:50'),('1019-09-11 14:05:50'),('1019-09-11 14:05:50');
select * from test_timestamp order by time;
delete from test_timestamp;
insert into test_timestamp values('9999-09-11 14:05:50'),('1000-09-11 14:05:50'),('1019-09-11 14:05:50'),('1019-09-11 15:05:50'),('1019-09-11 15:05:50'),('1019-09-11 15:35:50'),('1019-09-11 15:35:50'),('1019-09-11 15:35:00'),('2019-09-11 14:05:50'),('3019-09-11 14:05:50'),('1019-09-11 14:05:50'),('1019-09-11 14:05:50');
select * from test_timestamp order by time;
delete from test_timestamp;
insert into test_timestamp values('9999-09-11 14:05:50'),('9999-09-11 14:06:50'),('9999-09-11 14:07:00'),('9999-09-11 14:07:50');
select * from test_timestamp order by time;

-- check with compression = middle, compresslevel = 1
delete from test_timestamp;
alter table test_timestamp set(compression=middle, COMPRESSLEVEL = 1);
insert into test_timestamp values('2019-09-11 14:05:50'),('3019-09-11 14:05:50'),('1019-09-11 14:05:50'),('1019-09-11 14:05:50'),('1019-09-11 14:05:50');
select * from test_timestamp order by time;
delete from test_timestamp;
insert into test_timestamp values('2019-09-11 14:05:50'),('3019-09-11 14:05:50'),('1019-09-11 14:05:50'),('1019-09-11 14:05:50'),('1019-09-11 14:05:50'),('1019-09-11 14:05:50'),('1019-09-11 14:05:50'),('1019-09-11 14:05:50'),('2019-09-11 14:05:50'),('3019-09-11 14:05:50'),('1019-09-11 14:05:50'),('1019-09-11 14:05:50');
select * from test_timestamp order by time;
delete from test_timestamp;
insert into test_timestamp values('9999-09-11 14:05:50'),('1000-09-11 14:05:50'),('1019-09-11 14:05:50'),('1019-09-11 15:05:50'),('1019-09-11 15:05:50'),('1019-09-11 15:35:50'),('1019-09-11 15:35:50'),('1019-09-11 15:35:00'),('2019-09-11 14:05:50'),('3019-09-11 14:05:50'),('1019-09-11 14:05:50'),('1019-09-11 14:05:50');
select * from test_timestamp order by time;
delete from test_timestamp;
insert into test_timestamp values('9999-09-11 14:05:50'),('9999-09-11 14:06:50'),('9999-09-11 14:07:00'),('9999-09-11 14:07:50');
select * from test_timestamp order by time;

-- check with compression = high, compresslevel = 3
delete from test_timestamp;
alter table test_timestamp set(compression=high, COMPRESSLEVEL = 3);
insert into test_timestamp values('2019-09-11 14:05:50'),('3019-09-11 14:05:50'),('1019-09-11 14:05:50'),('1019-09-11 14:05:50'),('1019-09-11 14:05:50');
select * from test_timestamp order by time;
delete from test_timestamp;
insert into test_timestamp values('2019-09-11 14:05:50'),('3019-09-11 14:05:50'),('1019-09-11 14:05:50'),('1019-09-11 14:05:50'),('1019-09-11 14:05:50'),('1019-09-11 14:05:50'),('1019-09-11 14:05:50'),('1019-09-11 14:05:50'),('2019-09-11 14:05:50'),('3019-09-11 14:05:50'),('1019-09-11 14:05:50'),('1019-09-11 14:05:50');
select * from test_timestamp order by time;
delete from test_timestamp;
insert into test_timestamp values('9999-09-11 14:05:50'),('1000-09-11 14:05:50'),('1019-09-11 14:05:50'),('1019-09-11 15:05:50'),('1019-09-11 15:05:50'),('1019-09-11 15:35:50'),('1019-09-11 15:35:50'),('1019-09-11 15:35:00'),('2019-09-11 14:05:50'),('3019-09-11 14:05:50'),('1019-09-11 14:05:50'),('1019-09-11 14:05:50');
select * from test_timestamp order by time;
delete from test_timestamp;
insert into test_timestamp values('9999-09-11 14:05:50'),('9999-09-11 14:06:50'),('9999-09-11 14:07:00'),('9999-09-11 14:07:50');
select * from test_timestamp order by time;

-- check with orderd data
delete from test_timestamp;
alter table test_timestamp set(compression=high, COMPRESSLEVEL = 3);
insert into test_timestamp values('2019-09-11 14:05:50'),('2019-09-11 14:06:50'),('2019-09-11 14:07:50'),('2019-09-11 14:08:50'),('2019-09-11 14:09:50');
select * from test_timestamp order by time;
drop table dcs_cpu;
drop table test_timestamp;

-- check with varchar 
CREATE TABLE  delta2_varchar (
                         w_id int ,
                         w_name varchar(10)
                         ) with (orientation=column);
insert into delta2_varchar values(1, 'afetetad'),(2, 'aeft'),(3, 'dfettt'),(4, 'dasttt'),(5, 'daste');
select * from delta2_varchar order by w_id desc;
drop table delta2_varchar;