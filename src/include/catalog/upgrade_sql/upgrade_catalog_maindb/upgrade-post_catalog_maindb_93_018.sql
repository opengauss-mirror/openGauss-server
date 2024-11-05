DROP FUNCTION IF EXISTS pg_catalog.json_exists(text, text, int2) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8810;
CREATE OR REPLACE FUNCTION pg_catalog.json_exists(text, text, int2) RETURNS bool LANGUAGE INTERNAL IMMUTABLE as 'json_path_exists';

DROP FUNCTION IF EXISTS pg_catalog.json_textcontains(text, text, character varying) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8811;
CREATE OR REPLACE FUNCTION pg_catalog.json_textcontains(text, text, character varying) RETURNS bool LANGUAGE INTERNAL IMMUTABLE as 'json_textcontains';