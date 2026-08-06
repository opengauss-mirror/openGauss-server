DO $$
DECLARE
    compressed_relation_count INTEGER;
BEGIN
    -- Only block upgrades from versions before the CFS on-disk layout backport (6.0.6+).
    -- Versions >= 92990 include the aligned CFS format and can upgrade safely.
    IF working_version_num() < 92990 THEN
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
                'Upgrade check failed: compressed relation(s) found with incompatible CFS on-disk format. '
                'Upgrade to openGauss 6.0.6 or later first, or remove compressed relations before upgrading.';
        END IF;
    END IF;
END $$;
