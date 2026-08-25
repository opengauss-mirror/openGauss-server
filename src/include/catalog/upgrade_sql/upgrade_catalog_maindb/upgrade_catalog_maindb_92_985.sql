DO $$
DECLARE
    compressed_relation_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO compressed_relation_count
    FROM (
        SELECT reloptions
        FROM pg_catalog.pg_class
        UNION ALL
        SELECT reloptions
        FROM pg_catalog.pg_partition
    ) AS relations
    WHERE reloptions IS NOT NULL
      AND EXISTS (
          SELECT 1
          FROM unnest(reloptions) AS opt
          WHERE opt LIKE 'compresstype=%'
            AND split_part(opt, '=', 2) <> '0'
      );

    IF compressed_relation_count > 0 THEN
        RAISE EXCEPTION
            'Upgrade check failed: compressed relation(s) use an incompatible CFS on-disk format. '
            'Remove or rebuild them as uncompressed relations before upgrading.';
    END IF;
END $$;
