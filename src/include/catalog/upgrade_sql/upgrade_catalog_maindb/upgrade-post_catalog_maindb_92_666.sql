SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4098;
CREATE OR REPLACE FUNCTION pg_catalog.group_concat_finalfn(internal)
 RETURNS text
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS 'group_concat_finalfn';

SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4099;
CREATE OR REPLACE FUNCTION pg_catalog.group_concat_transfn(internal, text, VARIADIC "any")
 RETURNS internal
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE AS 'group_concat_transfn';
UPDATE pg_catalog.pg_proc SET provariadic=0 WHERE oid=4099;

SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4097;
CREATE AGGREGATE pg_catalog.group_concat(text, "any") (SFUNC=group_concat_transfn, STYPE=internal, FINALFUNC=group_concat_finalfn);
UPDATE pg_catalog.pg_proc SET proallargtypes=ARRAY[25,2276], proargmodes=ARRAY['i','v'] WHERE oid=4097;
