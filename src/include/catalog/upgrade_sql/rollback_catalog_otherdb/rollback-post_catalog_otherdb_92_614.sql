DELETE FROM pg_catalog.pg_attribute WHERE attrelid = 2604 AND attname = 'adbin_on_update';
DELETE FROM pg_catalog.pg_attribute WHERE attrelid = 2604 AND attname = 'adsrc_on_update';

UPDATE PG_CATALOG.pg_class set relnatts = 5 where relname = 'pg_attrdef' AND relnamespace = 11;
