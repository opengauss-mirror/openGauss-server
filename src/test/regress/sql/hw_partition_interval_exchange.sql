--
-- Test exchange operator for interval partitioned table
--

--
---- create interval partitioned table
--
CREATE TABLE interval_normal_exchange (logdate date not null) 
PARTITION BY RANGE (logdate)
INTERVAL ('1 month') 
(
	PARTITION interval_normal_exchange_p1 VALUES LESS THAN ('2020-03-01'),
	PARTITION interval_normal_exchange_p2 VALUES LESS THAN ('2020-04-01'),
	PARTITION interval_normal_exchange_p3 VALUES LESS THAN ('2020-05-01')
);

-- see about the info of the partitioned table in pg_partition
select relname, parttype, partstrategy, boundaries from pg_partition
	where parentid = (select oid from pg_class where relname = 'interval_normal_exchange')
	order by relname;

-- insert the record that is smaller than the lower boundary
insert into interval_normal_exchange values ('2020-02-21');
insert into interval_normal_exchange values ('2020-02-22');
insert into interval_normal_exchange values ('2020-02-23');
insert into interval_normal_exchange values ('2020-5-01');
insert into interval_normal_exchange values ('2020-5-02');
insert into interval_normal_exchange values ('2020-5-03');

-- see about the info of the partitioned table in pg_partition
select relname, parttype, partstrategy, boundaries from pg_partition
	where parentid = (select oid from pg_class where relname = 'interval_normal_exchange')
	order by relname;

--
---- create to be exchanged table and test range partition exchange
--
CREATE TABLE interval_exchange_test (logdate date not null);
insert into interval_exchange_test values ('2020-02-24');
insert into interval_exchange_test values ('2020-02-25');
insert into interval_exchange_test values ('2020-02-26');

-- do exchange partition interval_normal_exchange_p1 and interval_exchange_test
-- The data they have belongs to the same range.
ALTER TABLE interval_normal_exchange EXCHANGE PARTITION (interval_normal_exchange_p1)
        WITH TABLE interval_exchange_test;
select * from interval_normal_exchange partition (interval_normal_exchange_p1)order by logdate;
select * from interval_exchange_test order by logdate;
-- exchange back
ALTER TABLE interval_normal_exchange EXCHANGE PARTITION (interval_normal_exchange_p1)
        WITH TABLE interval_exchange_test;
select * from interval_normal_exchange partition (interval_normal_exchange_p1)order by logdate;
select * from interval_exchange_test order by logdate;

-- Insert a new record not belongs to interval_normal_exchange_p1
insert into interval_exchange_test values ('2020-3-05');
-- defaut is WITH VALIDATION, and the exchange will be failed
ALTER TABLE interval_normal_exchange EXCHANGE PARTITION (interval_normal_exchange_p1)
        WITH TABLE interval_exchange_test;
--  WITHOUT VALIDATION and the exchange will be success, but some date will in the wrong range;
ALTER TABLE interval_normal_exchange EXCHANGE PARTITION (interval_normal_exchange_p1)
        WITH TABLE interval_exchange_test WITHOUT VALIDATION;
select * from interval_normal_exchange partition (interval_normal_exchange_p1)order by logdate;
select * from interval_exchange_test order by logdate;
-- not include '2020-3-05'
select * from interval_normal_exchange where logdate > '2020-03-01' order by logdate;

--
---- clean the data and test interval partition exchange
--
truncate table interval_exchange_test;
insert into interval_exchange_test values ('2020-5-04');
insert into interval_exchange_test values ('2020-5-05');
insert into interval_exchange_test values ('2020-5-06');

-- exchange table
ALTER TABLE interval_normal_exchange EXCHANGE PARTITION (sys_p1)
        WITH TABLE interval_exchange_test;
select * from interval_normal_exchange partition (sys_p1)order by logdate;
select * from interval_exchange_test order by logdate;
-- exchange back
ALTER TABLE interval_normal_exchange EXCHANGE PARTITION (sys_p1)
        WITH TABLE interval_exchange_test;
select * from interval_normal_exchange partition (sys_p1)order by logdate;
select * from interval_exchange_test order by logdate;

insert into interval_exchange_test values ('2020-6-05');
-- defaut is WITH VALIDATION, and the exchange will be failed
ALTER TABLE interval_normal_exchange EXCHANGE PARTITION (interval_normal_exchange_p1)
        WITH TABLE interval_exchange_test;
--  WITHOUT VALIDATION and the exchange will be success, but some date will in the wrong range;
ALTER TABLE interval_normal_exchange EXCHANGE PARTITION (interval_normal_exchange_p1)
        WITH TABLE interval_exchange_test WITHOUT VALIDATION;
select * from interval_normal_exchange partition (interval_normal_exchange_p1)order by logdate;
select * from interval_exchange_test order by logdate;
-- not include '2020-6-05'
select * from interval_normal_exchange order by logdate;
select * from interval_normal_exchange where logdate > '2020-06-01' order by logdate;
drop table interval_normal_exchange;
