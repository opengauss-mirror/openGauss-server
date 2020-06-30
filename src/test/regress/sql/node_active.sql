
--test column nodeis_active;
select nodeis_active from pgxc_node where node_name = 'coordinator1';
create database "testDB";
--exception case
alter coordinator coordinator2 set false with (coordinator3);
alter coordinator datanode1 set false with (coordinator1, coordinator3);
alter coordinator coordinator2 set false with (coordinator1, coordinator3,datanode2);
alter coordinator coordinator2 set mmo with (coordinator3, coordinator1);


alter coordinator coordinator2 set false with (coordinator3, coordinator1);
create role testdb  password 'huawei@124';
set role testdb  password 'huawei@124';
alter coordinator coordinator2 set false with (coordinator3, coordinator1);
reset role;

alter coordinator coordinator2 set true with (coordinator3, coordinator1);
select * from pgxc_pool_reload();

select nodeis_active from pgxc_node where node_name = 'coordinator2';
 alter coordinator coordinator2 set true with (coordinator3, coordinator1);
select nodeis_active from pgxc_node where node_name = 'coordinator2';
