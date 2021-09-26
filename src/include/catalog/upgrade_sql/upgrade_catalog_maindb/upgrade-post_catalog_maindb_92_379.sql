DROP FUNCTION IF EXISTS pg_catalog.gs_hadr_do_switchover() cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9136;
CREATE OR REPLACE FUNCTION pg_catalog.gs_hadr_do_switchover
(  OUT service_truncation_result boolean)
RETURNS SETOF record LANGUAGE INTERNAL ROWS 1 STRICT as 'gs_hadr_do_switchover';

DROP FUNCTION IF EXISTS pg_catalog.gs_hadr_has_barrier_creator() cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9137;
CREATE OR REPLACE FUNCTION pg_catalog.gs_hadr_has_barrier_creator
(  OUT has_barrier_creator boolean)
RETURNS SETOF record LANGUAGE INTERNAL ROWS 1 STRICT as 'gs_hadr_has_barrier_creator';