DO $$
DECLARE
    compress_table_count INT;
BEGIN
  SELECT COUNT(*) INTO compress_table_count
  FROM pg_class
  WHERE reloptions IS NOT NULL
  AND EXISTS (
    SELECT 1
    FROM unnest(reloptions) AS opt
    WHERE opt LIKE 'compresstype=%'
    AND split_part(opt, '=', 2) <> '0'
  );
  IF compress_table_count > 0 THEN
      RAISE EXCEPTION 'Upgrade check failed: % compressed table(s) found. Please resolve these before proceeding with the upgrade.', compress_table_count;
  END IF;
END $$;