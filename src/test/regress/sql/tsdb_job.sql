set time zone 'PRC';
create table dcs_cpu(
	idle real check(idle > 0),
	vcpu_num int,
	node text,
	scope_name text,
	server_ip text not null,
	iowait real,
	time_string timestamp
)distribute by hash(server_ip)
PARTITION BY RANGE(time_string)
(
partition p2 values less than('2020-06-28 00:00:00'),
partition p3 values less than('2020-06-28 01:00:00'),
partition p4 values less than('2020-06-28 02:00:00')
);

create table dcs_cpu2(
	idle real check(idle > 0),
	vcpu_num int,
	node text,
	scope_name text,
	server_ip text not null,
	iowait real,
	time_string timestamptz
)distribute by hash(server_ip)
PARTITION BY RANGE(time_string)
(
partition p2 values less than('2020-06-28 00:00:00'),
partition p3 values less than('2020-06-28 01:00:00'),
partition p4 values less than('2020-06-28 02:00:00')
);

with s as 
(select add_create_partition_policy('dcs_cpu' , '1 hour', '1 hour'))
select count(*) from s;
DO $$ BEGIN PERFORM pg_sleep(3); END $$;
with s as 
(select add_create_partition_policy('dcs_cpu2' , '1 hour', '1 hour'))
select count(*) from s;
DO $$ BEGIN PERFORM pg_sleep(3); END $$;

select count(*) from user_jobs where what like '%proc_add_partition(''dcs_cpu''%';
select status from user_jobs where what like '%proc_add_partition(''dcs_cpu''%';

select count(*) from user_jobs where what like '%proc_add_partition(''dcs_cpu2''%';
select status from user_jobs where what like '%proc_add_partition(''dcs_cpu2''%';

select remove_create_partition_policy('dcs_cpu1');

with s as 
(select remove_create_partition_policy('dcs_cpu'))
select count(*) from s;

with s as 
(select remove_create_partition_policy('dcs_cpu2'))
select count(*) from s;

select count(*) from user_jobs where what like '%proc_add_partition(''dcs_cpu''%';
select count(*) from user_jobs where what like '%proc_add_partition(''dcs_cpu2''%';

with s as 
(select add_drop_partition_policy('dcs_cpu' , '1 day'))
select count(*) from s;
DO $$ BEGIN PERFORM pg_sleep(3); END $$;

with s as 
(select add_drop_partition_policy('dcs_cpu2' , '1 day'))
select count(*) from s;
DO $$ BEGIN PERFORM pg_sleep(3); END $$;

select count(*) from user_jobs where what like '%proc_drop_partition(''dcs_cpu''%';
select status from user_jobs where what like '%proc_drop_partition(''dcs_cpu''%';

select count(*) from user_jobs where what like '%proc_drop_partition(''dcs_cpu2''%';
select status from user_jobs where what like '%proc_drop_partition(''dcs_cpu2''%';

select remove_drop_partition_policy('dcs_cpu1');
with s as 
(select remove_drop_partition_policy('dcs_cpu'))
select count(*) from s;

with s as 
(select remove_drop_partition_policy('dcs_cpu2'))
select count(*) from s;

select count(*) from user_jobs where what like '%proc_drop_partition(''dcs_cpu''%';
select count(*) from user_jobs where what like '%proc_drop_partition(''dcs_cpu2''%';

drop table dcs_cpu;
drop table dcs_cpu2;
