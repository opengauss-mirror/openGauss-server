--
-- test vacuum full partition
--

--i1. create table
create table hw_partition_vacuum_full_partition_table(id int,name text, city text)
partition by range(id) 
(
	partition hw_partition_vacuum_full_partition_table_p1 values less than(1000),
	partition hw_partition_vacuum_full_partition_table_p2 values less than(2000)
);

--i2. create btree index
create index inx_part0_id on hw_partition_vacuum_full_partition_table(id) global;

--i3. insert data
create or replace function insert_part0_data() returns void as $$
declare
	times integer :=1;
begin
	loop
		insert into hw_partition_vacuum_full_partition_table values(times, 'xian', 'beijing');
		times = times + 1;
		if times > 1998 then
			exit;
		end if;
  end loop;
end;
$$ language plpgsql;

select insert_part0_data();

--i4. delete data
delete from hw_partition_vacuum_full_partition_table where id%3=0;
select count(*) from hw_partition_vacuum_full_partition_table where id%3=2;

--i5. vacuum full hw_partition_vacuum_full_partition_table_p1
analyze hw_partition_vacuum_full_partition_table partition(hw_partition_vacuum_full_partition_table_p1);
select relpages > 0 as relpagesgtzero, reltuples > 0 as reltuplesgtzero from pg_partition where relname='hw_partition_vacuum_full_partition_table_p1';
vacuum full hw_partition_vacuum_full_partition_table partition(hw_partition_vacuum_full_partition_table_p1);
select relpages > 0 as relpagesgtzero, reltuples > 0 as reltuplesgtzero from pg_partition where relname='hw_partition_vacuum_full_partition_table_p1';
select relpages > 0 as relpagesgtzero, reltuples > 0 as reltuplesgtzero from pg_class where relname='hw_partition_vacuum_full_partition_table';

--i6. vacuum full hw_partition_vacuum_full_partition_table_p2
analyze hw_partition_vacuum_full_partition_table partition(hw_partition_vacuum_full_partition_table_p2);
select relpages > 0 as relpagesgtzero, reltuples > 0 as reltuplesgtzero from pg_partition where relname='hw_partition_vacuum_full_partition_table_p2';
vacuum full hw_partition_vacuum_full_partition_table partition(hw_partition_vacuum_full_partition_table_p2);
select count(*) from hw_partition_vacuum_full_partition_table where id%3=2;
select relpages > 0 as relpagesgtzero, reltuples > 0 as reltuplesgtzero from pg_partition where relname='hw_partition_vacuum_full_partition_table_p2';
select relpages > 0 as relpagesgtzero, reltuples > 0 as reltuplesgtzero from pg_class where relname='hw_partition_vacuum_full_partition_table';

--i7. delete all the data
delete from hw_partition_vacuum_full_partition_table where id%3=1;

--i8. vacuum full hw_partition_vacuum_full_partition_table
vacuum full hw_partition_vacuum_full_partition_table;
select count(*) from hw_partition_vacuum_full_partition_table where id%3=2;
select relpages > 0 as relpagesgtzero, reltuples > 0 as reltuplesgtzero from pg_class where relname='hw_partition_vacuum_full_partition_table';

--i9. drop table
drop table hw_partition_vacuum_full_partition_table;

