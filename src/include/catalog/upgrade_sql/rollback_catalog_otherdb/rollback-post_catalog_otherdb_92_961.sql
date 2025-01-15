declare
    dependencies_obj_exist int:=0;
begin
    select count(*) into dependencies_obj_exist from pg_catalog.pg_class where oid = 7169;
    if dependencies_obj_exist != 0 then
        ALTER TABLE  pg_catalog.gs_dependencies_obj ALTER COLUMN type SET NOT NULL;
    end if;
end;
/