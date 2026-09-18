DO $$
DECLARE
    age_installed boolean;
BEGIN
    SELECT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_extension
        WHERE extname = 'age'
    ) INTO age_installed;

    IF age_installed THEN
        ALTER EXTENSION age UPDATE TO '1.0.1';
    END IF;
END$$;
