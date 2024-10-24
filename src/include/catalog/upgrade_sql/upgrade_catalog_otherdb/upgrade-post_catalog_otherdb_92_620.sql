DECLARE
    SPACE_NAME VARCHAR(64);
    REL_NAME VARCHAR(64);
    SQL_COMMAND VARCHAR(200);
    CURSOR C1 IS 
        SELECT n.nspname, c.relname
            FROM pg_catalog.pg_namespace n INNER JOIN pg_catalog.pg_class c ON n.oid = c.relnamespace
            WHERE c.relkind = 'r' AND c.parttype IN ('p', 's');
BEGIN
    OPEN C1;
    LOOP
        FETCH C1 INTO SPACE_NAME, REL_NAME;
        EXIT WHEN C1%NOTFOUND;
        SQL_COMMAND := 'ALTER TABLE "' || SPACE_NAME || '"."' || REL_NAME || '" RESET PARTITION;';
        EXECUTE SQL_COMMAND;
    END LOOP;
    CLOSE C1;
END;
/
