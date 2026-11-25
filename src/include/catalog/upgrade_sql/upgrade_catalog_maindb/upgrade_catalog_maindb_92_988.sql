-- Older shared-storage installations added segment=on to views during initdb.
UPDATE pg_catalog.pg_class
SET reloptions = NULLIF(pg_catalog.array_remove(reloptions, 'segment=on'), ARRAY[]::text[])
WHERE relkind = 'v'
  AND 'segment=on' = ANY(reloptions);
