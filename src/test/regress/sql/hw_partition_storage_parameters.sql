--(Y) CREATE TABLE WITH storage parameters
--(Y) ALTER TABLE SET storage parameters
--(Y) ALTER TABLE RESET storage parameters

-- step 1.1: create table: storage parameters
create table storage_para_t1 (a int4, b text)
WITH 
(
	fillfactor =85, 
	autovacuum_enabled = ON,
	toast.autovacuum_enabled = ON, 
	autovacuum_vacuum_threshold = 100,
	toast.autovacuum_vacuum_threshold = 100,
	autovacuum_vacuum_scale_factor = 10, 
	toast.autovacuum_vacuum_scale_factor = 10,
	autovacuum_analyze_threshold = 8,
	autovacuum_analyze_scale_factor = 9,
--  autovacuum_vacuum_cost_delay: Valid values are between "0" and "100".
	autovacuum_vacuum_cost_delay = 90, 
	toast.autovacuum_vacuum_cost_delay = 92,
--	autovacuum_vacuum_cost_limit: Valid values are between "1" and "10000".
	autovacuum_vacuum_cost_limit = 567, 
	toast.autovacuum_vacuum_cost_limit = 789,
	autovacuum_freeze_min_age = 5000, 
	toast.autovacuum_freeze_min_age = 6000,
--	autovacuum_freeze_max_age: Valid values are between "100000000" and "2000000000".
	autovacuum_freeze_max_age = 300000000, 
	toast.autovacuum_freeze_max_age = 250000000,
	autovacuum_freeze_table_age = 170000000, 
	toast.autovacuum_freeze_table_age = 180000000
)
partition by range (a)
(
	partition storage_para_t1_p1 values less than (10),
	partition storage_para_t1_p2 values less than (20),
	partition storage_para_t1_p3 values less than (100)
);
alter table storage_para_t1 add partition p4_rtest_t1 values less than (200);
-- select torage parameters of table and toast table
with storage_para_t1_oid as
(
	select oid 
	from pg_class 
	where relname = 'storage_para_t1'
)
select c.reloptions 
	from pg_class as c
	where c.relname = 'storage_para_t1'
	or c.oid in
	(
		select p.reltoastrelid
		from pg_partition as p 
		where p.parentid in 
			(select oid from storage_para_t1_oid)
	) order by c.reloptions;
-- step 1.2: reset, back to default
alter table storage_para_t1
RESET 
(
	fillfactor,
	autovacuum_enabled,
	autovacuum_vacuum_threshold,
	autovacuum_vacuum_scale_factor,
	autovacuum_analyze_threshold,
	autovacuum_analyze_scale_factor,
	autovacuum_vacuum_cost_delay,
	autovacuum_vacuum_cost_limit,
	autovacuum_freeze_min_age,
	autovacuum_freeze_max_age,
	autovacuum_freeze_table_age
);
-- select torage parameters of table and toast table
with storage_para_t1_oid as
(
	select oid 
	from pg_class 
	where relname = 'storage_para_t1'
)
select c.reloptions 
	from pg_class as c
	where c.relname = 'storage_para_t1'
	or c.oid in
	(
		select p.reltoastrelid
		from pg_partition as p 
		where p.parentid in 
			(select oid from storage_para_t1_oid)
	) order by c.reloptions ;
-- step 2.1: alter table: storage parameters
alter table storage_para_t1
SET 
(
	fillfactor =86, 
	autovacuum_enabled = OFF,
	toast.autovacuum_enabled = ON, 
	autovacuum_vacuum_threshold = 1000,
	toast.autovacuum_vacuum_threshold = 1000,
	--"0.000000" and "100.000000"
	autovacuum_vacuum_scale_factor = 15, 
	toast.autovacuum_vacuum_scale_factor = 89,
	autovacuum_analyze_threshold = 800,
	--"0.000000" and "100.000000"
	autovacuum_analyze_scale_factor = 55,
--  autovacuum_vacuum_cost_delay: Valid values are between "0" and "100".
	autovacuum_vacuum_cost_delay = 99, 
	toast.autovacuum_vacuum_cost_delay = 98,
--	autovacuum_vacuum_cost_limit: Valid values are between "1" and "10000".
	autovacuum_vacuum_cost_limit = 555, 
	toast.autovacuum_vacuum_cost_limit = 798,
	autovacuum_freeze_min_age = 6000, 
	toast.autovacuum_freeze_min_age = 4000,
--	autovacuum_freeze_max_age: Valid values are between "100000000" and "2000000000".
	autovacuum_freeze_max_age = 400000000, 
	toast.autovacuum_freeze_max_age = 280000000,
	autovacuum_freeze_table_age = 150000000, 
	toast.autovacuum_freeze_table_age = 160000000
);
-- select torage parameters of table and toast table
with storage_para_t1_oid as
(
	select oid 
	from pg_class 
	where relname = 'storage_para_t1'
)
select c.reloptions 
	from pg_class as c
	where c.relname = 'storage_para_t1'
	or c.oid in
	(
		select p.reltoastrelid
		from pg_partition as p 
		where p.parentid in 
			(select oid from storage_para_t1_oid)
	) order by c.reloptions ;
-- step 2.2: reset, back to default
alter table storage_para_t1
RESET 
(
	fillfactor,
	autovacuum_enabled,
	autovacuum_vacuum_threshold,
	autovacuum_vacuum_scale_factor,
	autovacuum_analyze_threshold,
	autovacuum_analyze_scale_factor,
	autovacuum_vacuum_cost_delay,
	autovacuum_vacuum_cost_limit,
	autovacuum_freeze_min_age,
	autovacuum_freeze_max_age,
	autovacuum_freeze_table_age,
	
	toast.autovacuum_enabled,
	toast.autovacuum_vacuum_threshold,
	toast.autovacuum_vacuum_scale_factor,
	toast.autovacuum_vacuum_cost_delay,
	toast.autovacuum_vacuum_cost_limit,
	toast.autovacuum_freeze_min_age,
	toast.autovacuum_freeze_max_age,
	toast.autovacuum_freeze_table_age
);
-- select torage parameters of table and toast table
with storage_para_t1_oid as
(
	select oid 
	from pg_class 
	where relname = 'storage_para_t1'
)
select c.reloptions 
	from pg_class as c
	where c.relname = 'storage_para_t1'
	or c.oid in
	(
		select p.reltoastrelid
		from pg_partition as p 
		where p.parentid in 
			(select oid from storage_para_t1_oid)
	) order by c.reloptions ;
drop table storage_para_t1;
