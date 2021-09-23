DROP FUNCTION IF EXISTS pg_catalog.gs_index_advise(cstring);                       
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC,4888;                 
CREATE OR REPLACE FUNCTION pg_catalog.gs_index_advise(cstring) returns table ("schema" text, "table" text, "column" text) language INTERNAL NOT FENCED as 'gs_index_advise';
