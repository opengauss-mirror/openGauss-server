--
-- test for vacuuming partition
--

--i1. create table
create table hw_partition_vacuum_partition_table(id int,name varchar(10))
partition by range(id)
(
	partition hw_partition_vacuum_partition_table_p1 values less than(1000),
	partition hw_partition_vacuum_partition_table_p2 values less than(2000),
	partition hw_partition_vacuum_partition_table_p3 values less than(3000),
	partition hw_partition_vacuum_partition_table_p4 values less than(4000)
);

--i2. create btree index
create index inx_btree_id on hw_partition_vacuum_partition_table(id) local;
create index inx_hash_id on hw_partition_vacuum_partition_table using hash (id) local;

--i3. insert data into table
create or replace function insert_part_data() returns void as $$
declare
	times integer :=1;
begin
	loop
		insert into hw_partition_vacuum_partition_table values(times, 'xian');
		times = times + 1;
		if times > 1999 then
			exit;
		end if;
  end loop;
end;
$$ language plpgsql;

select insert_part_data();

--i4. select the relpages and reltuples from pg_class
select relname, relpages >0 as relpagesgtzero, reltuples >0 as reltuplesgtzero from pg_class where relname='hw_partition_vacuum_partition_table' or relname='inx_btree_id' or relname='inx_hash_id' order by 1;

--i5. delete half of tuples
delete from hw_partition_vacuum_partition_table where id%2=1;

--i6. select again
select relname, relpages >0 as relpagesgtzero, reltuples >0 as reltuplesgtzero from pg_class where relname='hw_partition_vacuum_partition_table' or relname='inx_btree_id' or relname='inx_hash_id' order by 1;

--i7. vacuum hw_partition_vacuum_partition_table
vacuum hw_partition_vacuum_partition_table;

--i8. select again
select relname, relpages >0 as relpagesgtzero, reltuples >0 as reltuplesgtzero from pg_class where relname='hw_partition_vacuum_partition_table' or relname='inx_btree_id' or relname='inx_hash_id' order by 1;

--i9. vacuum index
vacuum inx_btree_id;

--i10. select again
select relname, relpages >0 as relpagesgtzero, reltuples >0 as reltuplesgtzero from pg_class where relname='hw_partition_vacuum_partition_table' or relname='inx_btree_id' or relname='inx_hash_id' order by 1;

--i11. delete all the tuples
delete from hw_partition_vacuum_partition_table;

--i12. vacuum hw_partition_vacuum_partition_table and index
vacuum hw_partition_vacuum_partition_table;
vacuum inx_btree_id;
vacuum hw_partition_vacuum_partition_table partition(hw_partition_vacuum_partition_table_p1);
vacuum inx_btree_id partition(p1_id_idx);
vacuum hw_partition_vacuum_partition_table partition(pp1);
analyze hw_partition_vacuum_partition_table;
analyze hw_partition_vacuum_partition_table partition(hw_partition_vacuum_partition_table_p1);
vacuum;

--i13. select again
select relname, relpages >0 as relpagesgtzero, reltuples >0 as reltuplesgtzero from pg_class where relname='hw_partition_vacuum_partition_table' or relname='inx_btree_id' or relname='inx_hash_id' order by 1;

--i14. selete table
select * from hw_partition_vacuum_partition_table;

--i15. don't drop table and insert data to test autovacuum
create or replace function insert_part_data() returns void as $$
declare
	times integer :=1;
begin
	loop
		insert into hw_partition_vacuum_partition_table values(times, 'xian');
		times = times + 1;
		if times > 3998 then
		exit;
		end if;
	end loop;
end;
$$ language plpgsql;

select insert_part_data();

delete from hw_partition_vacuum_partition_table where id%2=1;
drop table hw_partition_vacuum_partition_table;