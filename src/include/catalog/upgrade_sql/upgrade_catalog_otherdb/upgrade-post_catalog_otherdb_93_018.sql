DROP FUNCTION IF EXISTS pg_catalog.json_exists(text, text, int2) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8810;
CREATE OR REPLACE FUNCTION pg_catalog.json_exists(text, text, int2) RETURNS bool LANGUAGE INTERNAL IMMUTABLE as 'json_path_exists';

DROP FUNCTION IF EXISTS pg_catalog.json_textcontains(text, text, cstring) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8811;
CREATE OR REPLACE FUNCTION pg_catalog.json_textcontains(text, text, cstring) RETURNS bool LANGUAGE INTERNAL IMMUTABLE as 'json_textcontains';

DROP FUNCTION IF EXISTS pg_catalog.json_textcontains(text, text, text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8812;
CREATE OR REPLACE FUNCTION pg_catalog.json_textcontains(text, text, text) RETURNS bool LANGUAGE INTERNAL IMMUTABLE as 'json_textcontains';