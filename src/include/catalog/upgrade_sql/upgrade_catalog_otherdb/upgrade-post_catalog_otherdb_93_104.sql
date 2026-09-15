DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_extension WHERE extname = 'shark') THEN
        ALTER EXTENSION shark UPDATE TO '4.0';
    END IF;
END$$;
