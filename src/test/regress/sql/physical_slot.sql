select * from pg_create_physical_replication_slot('test_slot',true);
select * from pg_create_physical_replication_slot('test_slot',false);
select * from pg_get_replication_slots();
select * from pg_drop_replication_slot('test_slot');