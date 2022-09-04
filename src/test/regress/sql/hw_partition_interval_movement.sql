--
---- test row movement
--
drop table if exists hw_partition_interval_movement;
create table hw_partition_interval_movement
(
	c1 int,
	c2 int,
	C3 date not null
)
partition by range (C3)
INTERVAL ('1 month') 
(
	PARTITION hw_partition_interval_movement_p0 VALUES LESS THAN ('2020-03-01'),
	PARTITION hw_partition_interval_movement_p1 VALUES LESS THAN ('2020-04-01'),
	PARTITION hw_partition_interval_movement_p2 VALUES LESS THAN ('2020-05-01')
) DISABLE ROW MOVEMENT;

create index hw_partition_interval_movement_ind1 on hw_partition_interval_movement(c1) local;
create index hw_partition_interval_movement_ind2 on hw_partition_interval_movement(c2) local;
create index hw_partition_interval_movement_ind3 on hw_partition_interval_movement(c3) local;

--insert into table 
insert into hw_partition_interval_movement values(7,2,'2020-02-01');
insert into hw_partition_interval_movement values(3,1,'2020-03-01');
insert into hw_partition_interval_movement values(5,3,'2020-04-01');
insert into hw_partition_interval_movement values(7,5,'2020-05-01');
insert into hw_partition_interval_movement values(1,4,'2020-06-01');

select relname, parttype, partstrategy, boundaries from pg_partition
	where parentid = (select oid from pg_class where relname = 'hw_partition_interval_movement')
	order by 1;

-- fail: update record belongs to a range partition which will be move to other range partition
update hw_partition_interval_movement set C3 = '2020-04-22' where C3 = '2020-03-01';
-- fail: update record belongs to a range partition which will be move to an existed interval partition
update hw_partition_interval_movement set C3 = '2020-05-22' where C3 = '2020-03-01';
-- fail: update record belongs to a range partition which will be move to a not existed interval partition
update hw_partition_interval_movement set C3 = '2020-07-22' where C3 = '2020-03-01';
-- fail: update record belongs to a interval partition which will be move to a range partition
update hw_partition_interval_movement set C3 = '2020-03-22' where C3 = '2020-05-01';
-- fail: update record belongs to a interval partition which will be move to a not existed interval partition
update hw_partition_interval_movement set C3 = '2020-07-22' where C3 = '2020-05-01';

-- enable row movement 
alter table hw_partition_interval_movement ENABLE ROW MOVEMENT;

-- succeed: update record belongs to a range partition which will be move to other range partition
update hw_partition_interval_movement set C3 = '2020-04-22' where C3 = '2020-03-01';
-- succeed: update record belongs to a range partition which will be move to an existed interval partition
update hw_partition_interval_movement set C3 = '2020-05-22' where C3 = '2020-04-22';
-- succeed: update record belongs to a range partition which will be move to a not existed interval partition
update hw_partition_interval_movement set C3 = '2020-07-22' where C3 = '2020-04-01';
-- succeed: update record belongs to a interval partition which will be move to a range partition
update hw_partition_interval_movement set C3 = '2020-03-22' where C3 = '2020-05-01';
-- succeed: update record belongs to a interval partition which will be move to a not existed interval partition
update hw_partition_interval_movement set C3 = '2020-08-22' where C3 = '2020-06-01';

select * from hw_partition_interval_movement;
-- add two new interval ranges 
select relname, parttype, partstrategy, boundaries from pg_partition
	where parentid = (select oid from pg_class where relname = 'hw_partition_interval_movement')
	order by 1;
drop table hw_partition_interval_movement;
