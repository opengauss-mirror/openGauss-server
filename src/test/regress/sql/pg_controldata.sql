select count(*) > 0 as ok from pg_control_system();

select count(*) > 0 as ok from pg_control_checkpoint();
