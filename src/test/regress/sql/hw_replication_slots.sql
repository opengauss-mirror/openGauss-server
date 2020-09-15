--replication slots test 

select * from pg_create_physical_replication_slot('dummystandby_slot',true);
select * from pg_create_physical_replication_slot('standby_slot',false);
select * from pg_create_physical_replication_slot('standby_slot',false);
select * from pg_create_physical_replication_slot('my_physical_slot1my_physical_slot2my_physical_slot6my_physical_slot7', 'True');

select * from pg_replication_slots order by 1;

select * from pg_drop_replication_slot('dummystandby_slot');
select * from pg_drop_replication_slot('standby_slot');
select * from pg_drop_replication_slot('wrong_slot_name');
select * from pg_drop_replication_slot('my_physical_slot1my_physical_slot2my_physical_slot6my_physical_');
