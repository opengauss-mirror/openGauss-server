--
--test the pmk schema
--
DECLARE
    pmk_oid oid;
    class_count int;
    proc_count int;
BEGIN
    --if pmk schema not exist, it will raise an error.
    SELECT oid FROM pg_namespace WHERE nspname='pmk' INTO pmk_oid;
    --select the count of class_count
    SELECT COUNT(*) FROM pg_class WHERE relnamespace=pmk_oid INTO class_count;
    --select the count of proc_count
    SELECT COUNT(*) FROM pg_proc WHERE pronamespace=pmk_oid INTO proc_count;
    RAISE INFO 'pmk schema exist. class count is %, proc count is %.', class_count , proc_count;
END;
/