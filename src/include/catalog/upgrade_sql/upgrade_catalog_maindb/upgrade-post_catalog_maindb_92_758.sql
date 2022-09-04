DO $$
DECLARE
cnt int;
BEGIN
    select count(*) into cnt from pg_type where typname = 'int16';
    if cnt = 1 then
        SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4078;
        CREATE FUNCTION pg_catalog.last_insert_id()
         RETURNS int16
         LANGUAGE internal
         STABLE NOT FENCED NOT SHIPPABLE AS 'last_insert_id_no_args';
         
        SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4079;
        CREATE FUNCTION pg_catalog.last_insert_id(int16)
         RETURNS int16
         LANGUAGE internal
         IMMUTABLE NOT FENCED NOT SHIPPABLE AS 'last_insert_id';
    end if;
END$$;
