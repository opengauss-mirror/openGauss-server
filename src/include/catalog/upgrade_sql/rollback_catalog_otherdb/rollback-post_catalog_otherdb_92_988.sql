-- Deliberately retain the DB4AI snapshot security fixes when rolling back.
-- These SQL definitions use only types and functions already present in 92987;
-- they introduce no catalog layout, WAL or data-format changes. USAGE on the
-- sequence is supported by the old binaries and is sufficient for nextval.
-- Restoring the old definitions or PUBLIC UPDATE would reopen the fixed issues.
DO $$
BEGIN
    NULL;
END$$;
