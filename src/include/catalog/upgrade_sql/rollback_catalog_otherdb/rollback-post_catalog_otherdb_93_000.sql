do $$
DECLARE
ans boolean;
BEGIN
    for ans in select case when count(*)=1 then true else false end as ans from (select extname FROM pg_catalog.pg_extension where extname='dolphin')
    LOOP
        if ans = true then
            ALTER EXTENSION dolphin UPDATE TO '4.5';
        end if;
        exit;
    END LOOP;
END$$;

-- Restore the 6.0.7 Shark endpoint when rolling back across the 7.0 boundary.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_extension WHERE extname = 'shark') THEN
        ALTER EXTENSION shark UPDATE TO '2.1';
    END IF;
END$$;
