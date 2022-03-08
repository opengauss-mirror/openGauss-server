--------------------------------------------------------------
-- add new type  jsonb
--------------------------------------------------------------
DROP TYPE IF EXISTS pg_catalog._jsonb;
DROP TYPE IF EXISTS pg_catalog.jsonb;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_TYPE, 3802, 3807, b;
CREATE TYPE pg_catalog.jsonb;

DROP FUNCTION IF EXISTS pg_catalog.jsonb_in(cstring) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3806;
CREATE FUNCTION pg_catalog.jsonb_in (
cstring
) RETURNS jsonb LANGUAGE INTERNAL IMMUTABLE STRICT as 'jsonb_in';

DROP FUNCTION IF EXISTS pg_catalog.jsonb_out(jsonb) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3436;
CREATE FUNCTION pg_catalog.jsonb_out (
jsonb
) RETURNS cstring LANGUAGE INTERNAL IMMUTABLE STRICT as 'jsonb_out';

DROP FUNCTION IF EXISTS pg_catalog.jsonb_send(jsonb) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3444;
CREATE FUNCTION pg_catalog.jsonb_send (
jsonb
) RETURNS bytea LANGUAGE INTERNAL IMMUTABLE STRICT as 'jsonb_send';

DROP FUNCTION IF EXISTS pg_catalog.jsonb_recv(internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3443;
CREATE FUNCTION pg_catalog.jsonb_recv (
internal
) RETURNS jsonb LANGUAGE INTERNAL IMMUTABLE STRICT as 'jsonb_recv';

CREATE TYPE pg_catalog.jsonb (input=jsonb_in, output=jsonb_out, RECEIVE = jsonb_recv, SEND = jsonb_send, STORAGE=EXTENDED, category='C');
COMMENT ON TYPE pg_catalog.jsonb IS 'json binary';
COMMENT ON TYPE pg_catalog._jsonb IS 'json binary';


----------------------------------------------------------------
-- add two agg funcs
----------------------------------------------------------------
DROP FUNCTION IF EXISTS pg_catalog.json_agg_finalfn(internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3125;
CREATE FUNCTION pg_catalog.json_agg_finalfn (
internal
) RETURNS json LANGUAGE INTERNAL IMMUTABLE  as 'json_agg_finalfn';

DROP FUNCTION IF EXISTS pg_catalog.json_agg_transfn(internal, anyelement) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3126;
CREATE FUNCTION pg_catalog.json_agg_transfn (
internal, anyelement
) RETURNS internal LANGUAGE INTERNAL IMMUTABLE  as 'json_agg_transfn';

drop aggregate if exists pg_catalog.json_agg(anyelement);
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 3124;
create aggregate pg_catalog.json_agg(anyelement) (SFUNC=json_agg_transfn, STYPE= internal, finalfunc = json_agg_finalfn);


DROP FUNCTION IF EXISTS pg_catalog.json_object_agg_finalfn(internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3404;
CREATE FUNCTION pg_catalog.json_object_agg_finalfn (
internal
) RETURNS json LANGUAGE INTERNAL IMMUTABLE  as 'json_object_agg_finalfn';

DROP FUNCTION IF EXISTS pg_catalog.json_object_agg_transfn(internal, "any", "any") CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3405;
CREATE FUNCTION pg_catalog.json_object_agg_transfn (
internal, "any", "any"
) RETURNS internal LANGUAGE INTERNAL IMMUTABLE  as 'json_object_agg_transfn';

drop aggregate if exists pg_catalog.json_object_agg("any", "any");
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 3403;
create aggregate pg_catalog.json_object_agg("any", "any") (SFUNC=json_object_agg_transfn, STYPE= internal, finalfunc = json_object_agg_finalfn);


----------------------------------------------------------------
-- modify normal funcs
----------------------------------------------------------------
ALTER FUNCTION json_in(cstring) IMMUTABLE;
ALTER FUNCTION json_send(json) IMMUTABLE;
ALTER FUNCTION json_recv(internal) IMMUTABLE;


----------------------------------------------------------------
-- add normal funcs
----------------------------------------------------------------
DROP FUNCTION IF EXISTS pg_catalog.gin_compare_jsonb(text, text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3498;
CREATE FUNCTION pg_catalog.gin_compare_jsonb (
text, text
) RETURNS integer LANGUAGE INTERNAL IMMUTABLE STRICT as 'gin_compare_jsonb';

DROP FUNCTION IF EXISTS pg_catalog.gin_consistent_jsonb(internal, smallint, anyarray, integer, internal, internal, internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3497;
CREATE FUNCTION pg_catalog.gin_consistent_jsonb (
internal, smallint, anyarray, integer, internal, internal, internal, internal
) RETURNS boolean LANGUAGE INTERNAL IMMUTABLE STRICT as 'gin_consistent_jsonb';

DROP FUNCTION IF EXISTS pg_catalog.gin_consistent_jsonb_hash(internal, smallint, anyarray, integer, internal, internal, internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3487;
CREATE FUNCTION pg_catalog.gin_consistent_jsonb_hash (
internal, smallint, anyarray, integer, internal, internal, internal, internal
) RETURNS boolean LANGUAGE INTERNAL IMMUTABLE STRICT as 'gin_consistent_jsonb_hash';

DROP FUNCTION IF EXISTS pg_catalog.gin_extract_jsonb(internal, internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3482;
CREATE FUNCTION pg_catalog.gin_extract_jsonb (
internal, internal, internal
) RETURNS internal LANGUAGE INTERNAL IMMUTABLE STRICT as 'gin_extract_jsonb';

DROP FUNCTION IF EXISTS pg_catalog.gin_extract_jsonb_hash(internal, internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3492;
CREATE FUNCTION pg_catalog.gin_extract_jsonb_hash (
internal, internal, internal
) RETURNS internal LANGUAGE INTERNAL IMMUTABLE STRICT as 'gin_extract_jsonb_hash';

DROP FUNCTION IF EXISTS pg_catalog.gin_extract_jsonb_query(anyarray, internal, smallint, internal, internal, internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3493;
CREATE FUNCTION pg_catalog.gin_extract_jsonb_query (
anyarray, internal, smallint, internal, internal, internal, internal
) RETURNS internal LANGUAGE INTERNAL IMMUTABLE STRICT as 'gin_extract_jsonb_query';

DROP FUNCTION IF EXISTS pg_catalog.gin_extract_jsonb_query_hash(anyarray, internal, smallint, internal, internal, internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3486;
CREATE FUNCTION pg_catalog.gin_extract_jsonb_query_hash (
anyarray, internal, smallint, internal, internal, internal, internal
) RETURNS internal LANGUAGE INTERNAL IMMUTABLE STRICT as 'gin_extract_jsonb_query_hash';

DROP FUNCTION IF EXISTS pg_catalog.gin_triconsistent_jsonb(internal, smallint, anyarray, integer, internal, internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3494;
CREATE FUNCTION pg_catalog.gin_triconsistent_jsonb (
internal, smallint, anyarray, integer, internal, internal, internal
) RETURNS boolean LANGUAGE INTERNAL IMMUTABLE STRICT as 'gin_triconsistent_jsonb';

DROP FUNCTION IF EXISTS pg_catalog.gin_triconsistent_jsonb_hash(internal, smallint, anyarray, integer, internal, internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3495;
CREATE FUNCTION pg_catalog.gin_triconsistent_jsonb_hash (
internal, smallint, anyarray, integer, internal, internal, internal
) RETURNS boolean LANGUAGE INTERNAL IMMUTABLE STRICT as 'gin_triconsistent_jsonb_hash';

DROP FUNCTION IF EXISTS pg_catalog.json_array_element(json, integer) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3949;
CREATE FUNCTION pg_catalog.json_array_element (
json, integer
) RETURNS json LANGUAGE INTERNAL IMMUTABLE STRICT as 'json_array_element';

DROP FUNCTION IF EXISTS pg_catalog.json_array_element_text(json, integer) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3079;
CREATE FUNCTION pg_catalog.json_array_element_text (
json, integer
) RETURNS text LANGUAGE INTERNAL IMMUTABLE STRICT as 'json_array_element_text';

DROP FUNCTION IF EXISTS pg_catalog.json_array_elements(from_json json, OUT value json) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3549;
CREATE FUNCTION pg_catalog.json_array_elements (
from_json json, OUT value json
) RETURNS SETOF json LANGUAGE INTERNAL IMMUTABLE ROWS 100 STRICT as 'json_array_elements';

DROP FUNCTION IF EXISTS pg_catalog.json_array_elements_text(from_json json, OUT value text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3127;
CREATE FUNCTION pg_catalog.json_array_elements_text (
from_json json, OUT value text
) RETURNS SETOF text LANGUAGE INTERNAL IMMUTABLE ROWS 100 STRICT as 'json_array_elements_text';

DROP FUNCTION IF EXISTS pg_catalog.json_array_length(json) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3128;
CREATE FUNCTION pg_catalog.json_array_length (
json
) RETURNS integer LANGUAGE INTERNAL IMMUTABLE STRICT as 'json_array_length';

DROP FUNCTION IF EXISTS pg_catalog.json_build_array() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3238;
CREATE FUNCTION pg_catalog.json_build_array (

) RETURNS json LANGUAGE INTERNAL IMMUTABLE  as 'json_build_array_noargs';

DROP FUNCTION IF EXISTS pg_catalog.json_build_array(VARIADIC "any") CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3239;
CREATE FUNCTION pg_catalog.json_build_array (
VARIADIC "any"
) RETURNS json LANGUAGE INTERNAL IMMUTABLE  as 'json_build_array';

DROP FUNCTION IF EXISTS pg_catalog.json_build_object() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3260;
CREATE FUNCTION pg_catalog.json_build_object (

) RETURNS json LANGUAGE INTERNAL IMMUTABLE  as 'json_build_object_noargs';

DROP FUNCTION IF EXISTS pg_catalog.json_build_object(VARIADIC "any") CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3261;
CREATE FUNCTION pg_catalog.json_build_object (
VARIADIC "any"
) RETURNS json LANGUAGE INTERNAL IMMUTABLE  as 'json_build_object';

DROP FUNCTION IF EXISTS pg_catalog.json_each(from_json json, OUT key text, OUT value json) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3258;
CREATE FUNCTION pg_catalog.json_each (
from_json json, OUT key text, OUT value json
) RETURNS SETOF record LANGUAGE INTERNAL IMMUTABLE ROWS 100 STRICT as 'json_each';

DROP FUNCTION IF EXISTS pg_catalog.json_each_text(from_json json, OUT key text, OUT value text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3259;
CREATE FUNCTION pg_catalog.json_each_text (
from_json json, OUT key text, OUT value text
) RETURNS SETOF record LANGUAGE INTERNAL IMMUTABLE ROWS 100 STRICT as 'json_each_text';

DROP FUNCTION IF EXISTS pg_catalog.json_extract_path(json, VARIADIC text[]) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3262;
CREATE FUNCTION pg_catalog.json_extract_path (
json, VARIADIC text[]
) RETURNS json LANGUAGE INTERNAL IMMUTABLE STRICT as 'json_extract_path';

DROP FUNCTION IF EXISTS pg_catalog.json_extract_path_op(json, text[]) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3263;
CREATE FUNCTION pg_catalog.json_extract_path_op (
json, text[]
) RETURNS json LANGUAGE INTERNAL IMMUTABLE STRICT as 'json_extract_path';

DROP FUNCTION IF EXISTS pg_catalog.json_extract_path_text(json, VARIADIC text[]) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3264;
CREATE FUNCTION pg_catalog.json_extract_path_text (
json, VARIADIC text[]
) RETURNS text LANGUAGE INTERNAL IMMUTABLE STRICT as 'json_extract_path_text';

DROP FUNCTION IF EXISTS pg_catalog.json_extract_path_text_op(json, text[]) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3318;
CREATE FUNCTION pg_catalog.json_extract_path_text_op (
json, text[]
) RETURNS text LANGUAGE INTERNAL IMMUTABLE STRICT as 'json_extract_path_text';

DROP FUNCTION IF EXISTS pg_catalog.json_object(text[], text[]) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3401;
CREATE FUNCTION pg_catalog.json_object (
text[], text[]
) RETURNS json LANGUAGE INTERNAL STABLE STRICT as 'json_object_two_arg';

DROP FUNCTION IF EXISTS pg_catalog.json_object(text[]) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3402;
CREATE FUNCTION pg_catalog.json_object (
text[]
) RETURNS json LANGUAGE INTERNAL STABLE STRICT as 'json_object';

DROP FUNCTION IF EXISTS pg_catalog.json_object_field(json, text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3406;
CREATE FUNCTION pg_catalog.json_object_field (
json, text
) RETURNS json LANGUAGE INTERNAL IMMUTABLE STRICT as 'json_object_field';

DROP FUNCTION IF EXISTS pg_catalog.json_object_field_text(json, text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3948;
CREATE FUNCTION pg_catalog.json_object_field_text (
json, text
) RETURNS text LANGUAGE INTERNAL IMMUTABLE STRICT as 'json_object_field_text';

DROP FUNCTION IF EXISTS pg_catalog.json_object_keys(json) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3407;
CREATE FUNCTION pg_catalog.json_object_keys (
json
) RETURNS SETOF text LANGUAGE INTERNAL IMMUTABLE ROWS 100 STRICT as 'json_object_keys';

DROP FUNCTION IF EXISTS pg_catalog.json_populate_record(anyelement, json, boolean) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3408;
CREATE FUNCTION pg_catalog.json_populate_record (
anyelement, json, boolean DEFAULT false
) RETURNS anyelement LANGUAGE INTERNAL STABLE as 'json_populate_record';

DROP FUNCTION IF EXISTS pg_catalog.json_populate_recordset(anyelement, json, boolean) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3409;
CREATE FUNCTION pg_catalog.json_populate_recordset (
anyelement, json, boolean DEFAULT false
) RETURNS SETOF anyelement LANGUAGE INTERNAL ROWS 100 STABLE as 'json_populate_recordset';

DROP FUNCTION IF EXISTS pg_catalog.json_to_record(json, boolean) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3410;
CREATE FUNCTION pg_catalog.json_to_record (
json, boolean
) RETURNS record LANGUAGE INTERNAL STABLE  as 'json_to_record';

DROP FUNCTION IF EXISTS pg_catalog.json_to_recordset(json, boolean) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3411;
CREATE FUNCTION pg_catalog.json_to_recordset (
json, boolean
) RETURNS SETOF record LANGUAGE INTERNAL STABLE ROWS 100 as 'json_to_recordset';

DROP FUNCTION IF EXISTS pg_catalog.json_typeof(json) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3270;
CREATE FUNCTION pg_catalog.json_typeof (
json
) RETURNS text LANGUAGE INTERNAL IMMUTABLE STRICT as 'json_typeof';

DROP FUNCTION IF EXISTS pg_catalog.jsonb_array_element(jsonb, integer) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3412;
CREATE FUNCTION pg_catalog.jsonb_array_element (
jsonb, integer
) RETURNS jsonb LANGUAGE INTERNAL IMMUTABLE STRICT as 'jsonb_array_element';

DROP FUNCTION IF EXISTS pg_catalog.jsonb_array_element_text(jsonb, integer) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3413;
CREATE FUNCTION pg_catalog.jsonb_array_element_text (
jsonb, integer
) RETURNS text LANGUAGE INTERNAL IMMUTABLE STRICT as 'jsonb_array_element_text';

DROP FUNCTION IF EXISTS pg_catalog.jsonb_array_elements(from_json jsonb, OUT value jsonb) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3414;
CREATE FUNCTION pg_catalog.jsonb_array_elements (
from_json jsonb, OUT value jsonb
) RETURNS SETOF jsonb LANGUAGE INTERNAL IMMUTABLE ROWS 100 STRICT as 'jsonb_array_elements';

DROP FUNCTION IF EXISTS pg_catalog.jsonb_array_elements_text(from_json jsonb, OUT value text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3415;
CREATE FUNCTION pg_catalog.jsonb_array_elements_text (
from_json jsonb, OUT value text
) RETURNS SETOF text LANGUAGE INTERNAL IMMUTABLE ROWS 100 STRICT as 'jsonb_array_elements_text';

DROP FUNCTION IF EXISTS pg_catalog.jsonb_array_length(jsonb) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3416;
CREATE FUNCTION pg_catalog.jsonb_array_length (
jsonb
) RETURNS integer LANGUAGE INTERNAL IMMUTABLE STRICT as 'jsonb_array_length';

DROP FUNCTION IF EXISTS pg_catalog.jsonb_cmp(jsonb, jsonb) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3417;
CREATE FUNCTION pg_catalog.jsonb_cmp (
jsonb, jsonb
) RETURNS integer LANGUAGE INTERNAL IMMUTABLE STRICT as 'jsonb_cmp';

DROP FUNCTION IF EXISTS pg_catalog.jsonb_contained(jsonb, jsonb) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4050;
CREATE FUNCTION pg_catalog.jsonb_contained (
jsonb, jsonb
) RETURNS boolean LANGUAGE INTERNAL IMMUTABLE STRICT as 'jsonb_contained';

DROP FUNCTION IF EXISTS pg_catalog.jsonb_contains(jsonb, jsonb) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3418;
CREATE FUNCTION pg_catalog.jsonb_contains (
jsonb, jsonb
) RETURNS boolean LANGUAGE INTERNAL IMMUTABLE STRICT as 'jsonb_contains';

DROP FUNCTION IF EXISTS pg_catalog.jsonb_each(from_json jsonb, OUT key text, OUT value jsonb) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3419;
CREATE FUNCTION pg_catalog.jsonb_each (
from_json jsonb, OUT key text, OUT value jsonb
) RETURNS SETOF record LANGUAGE INTERNAL IMMUTABLE ROWS 100 STRICT as 'jsonb_each';

DROP FUNCTION IF EXISTS pg_catalog.jsonb_each_text(from_json jsonb, OUT key text, OUT value text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3420;
CREATE FUNCTION pg_catalog.jsonb_each_text (
from_json jsonb, OUT key text, OUT value text
) RETURNS SETOF record LANGUAGE INTERNAL IMMUTABLE ROWS 100 STRICT as 'jsonb_each_text';

DROP FUNCTION IF EXISTS pg_catalog.jsonb_eq(jsonb, jsonb) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3421;
CREATE FUNCTION pg_catalog.jsonb_eq (
jsonb, jsonb
) RETURNS boolean LANGUAGE INTERNAL IMMUTABLE STRICT as 'jsonb_eq';

DROP FUNCTION IF EXISTS pg_catalog.jsonb_exists(jsonb, text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3422;
CREATE FUNCTION pg_catalog.jsonb_exists (
jsonb, text
) RETURNS boolean LANGUAGE INTERNAL IMMUTABLE STRICT as 'jsonb_exists';

DROP FUNCTION IF EXISTS pg_catalog.jsonb_exists_all(jsonb, text[]) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3423;
CREATE FUNCTION pg_catalog.jsonb_exists_all (
jsonb, text[]
) RETURNS boolean LANGUAGE INTERNAL IMMUTABLE STRICT as 'jsonb_exists_all';

DROP FUNCTION IF EXISTS pg_catalog.jsonb_exists_any(jsonb, text[]) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3424;
CREATE FUNCTION pg_catalog.jsonb_exists_any (
jsonb, text[]
) RETURNS boolean LANGUAGE INTERNAL IMMUTABLE STRICT as 'jsonb_exists_any';

DROP FUNCTION IF EXISTS pg_catalog.jsonb_extract_path(jsonb, VARIADIC text[]) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3425;
CREATE FUNCTION pg_catalog.jsonb_extract_path (
jsonb, VARIADIC text[]
) RETURNS jsonb LANGUAGE INTERNAL IMMUTABLE STRICT as 'jsonb_extract_path';

DROP FUNCTION IF EXISTS pg_catalog.jsonb_extract_path_op(jsonb, text[]) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3319;
CREATE FUNCTION pg_catalog.jsonb_extract_path_op (
jsonb, text[]
) RETURNS jsonb LANGUAGE INTERNAL IMMUTABLE STRICT as 'jsonb_extract_path';

DROP FUNCTION IF EXISTS pg_catalog.jsonb_extract_path_text(jsonb, VARIADIC text[]) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3426;
CREATE FUNCTION pg_catalog.jsonb_extract_path_text (
jsonb, VARIADIC text[]
) RETURNS text LANGUAGE INTERNAL IMMUTABLE STRICT as 'jsonb_extract_path_text';

DROP FUNCTION IF EXISTS pg_catalog.jsonb_extract_path_text_op(jsonb, text[]) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3427;
CREATE FUNCTION pg_catalog.jsonb_extract_path_text_op (
jsonb, text[]
) RETURNS text LANGUAGE INTERNAL IMMUTABLE STRICT as 'jsonb_extract_path_text';

DROP FUNCTION IF EXISTS pg_catalog.jsonb_ge(jsonb, jsonb) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3428;
CREATE FUNCTION pg_catalog.jsonb_ge (
jsonb, jsonb
) RETURNS boolean LANGUAGE INTERNAL IMMUTABLE STRICT as 'jsonb_ge';

DROP FUNCTION IF EXISTS pg_catalog.jsonb_gt(jsonb, jsonb) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3429;
CREATE FUNCTION pg_catalog.jsonb_gt (
jsonb, jsonb
) RETURNS boolean LANGUAGE INTERNAL IMMUTABLE STRICT as 'jsonb_gt';

DROP FUNCTION IF EXISTS pg_catalog.jsonb_hash(jsonb) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3430;
CREATE FUNCTION pg_catalog.jsonb_hash (
jsonb
) RETURNS integer LANGUAGE INTERNAL IMMUTABLE STRICT as 'jsonb_hash';

DROP FUNCTION IF EXISTS pg_catalog.jsonb_le(jsonb, jsonb) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3431;
CREATE FUNCTION pg_catalog.jsonb_le (
jsonb, jsonb
) RETURNS boolean LANGUAGE INTERNAL IMMUTABLE STRICT as 'jsonb_le';

DROP FUNCTION IF EXISTS pg_catalog.jsonb_lt(jsonb, jsonb) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4039;
CREATE FUNCTION pg_catalog.jsonb_lt (
jsonb, jsonb
) RETURNS boolean LANGUAGE INTERNAL IMMUTABLE STRICT as 'jsonb_lt';

DROP FUNCTION IF EXISTS pg_catalog.jsonb_ne(jsonb, jsonb) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3432;
CREATE FUNCTION pg_catalog.jsonb_ne (
jsonb, jsonb
) RETURNS boolean LANGUAGE INTERNAL IMMUTABLE STRICT as 'jsonb_ne';

DROP FUNCTION IF EXISTS pg_catalog.jsonb_object_field(jsonb, text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3433;
CREATE FUNCTION pg_catalog.jsonb_object_field (
jsonb, text
) RETURNS jsonb LANGUAGE INTERNAL IMMUTABLE STRICT as 'jsonb_object_field';

DROP FUNCTION IF EXISTS pg_catalog.jsonb_object_field_text(jsonb, text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3434;
CREATE FUNCTION pg_catalog.jsonb_object_field_text (
jsonb, text
) RETURNS text LANGUAGE INTERNAL IMMUTABLE STRICT as 'jsonb_object_field_text';

DROP FUNCTION IF EXISTS pg_catalog.jsonb_object_keys(jsonb) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3435;
CREATE FUNCTION pg_catalog.jsonb_object_keys (
jsonb
) RETURNS SETOF text LANGUAGE INTERNAL IMMUTABLE ROWS 100 STRICT as 'jsonb_object_keys';

DROP FUNCTION IF EXISTS pg_catalog.jsonb_populate_record(anyelement, jsonb, boolean) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3437;
CREATE FUNCTION pg_catalog.jsonb_populate_record (
anyelement, jsonb, boolean DEFAULT false
) RETURNS anyelement LANGUAGE INTERNAL STABLE  as 'jsonb_populate_record';

DROP FUNCTION IF EXISTS pg_catalog.jsonb_populate_recordset(anyelement, jsonb, boolean) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3438;
CREATE FUNCTION pg_catalog.jsonb_populate_recordset (
anyelement, jsonb, boolean DEFAULT false
) RETURNS SETOF anyelement LANGUAGE INTERNAL STABLE ROWS 100 as 'jsonb_populate_recordset';

DROP FUNCTION IF EXISTS pg_catalog.jsonb_typeof(jsonb) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3445;
CREATE FUNCTION pg_catalog.jsonb_typeof (
jsonb
) RETURNS text LANGUAGE INTERNAL IMMUTABLE STRICT as 'jsonb_typeof';

DROP FUNCTION IF EXISTS pg_catalog.to_json(anyelement) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3446;
CREATE FUNCTION pg_catalog.to_json (
anyelement
) RETURNS json LANGUAGE INTERNAL STABLE STRICT as 'to_json';


--------------------------------------------------------------
-- add new operator
--------------------------------------------------------------
DROP OPERATOR IF EXISTS pg_catalog.->(json, text) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 3962;
CREATE OPERATOR pg_catalog.->(
leftarg =  json, rightarg = text, procedure = json_object_field
);
COMMENT ON OPERATOR pg_catalog.->(json, text) IS 'json_object_field';

DROP OPERATOR IF EXISTS pg_catalog.->>(json, text) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 3963;
CREATE OPERATOR pg_catalog.->>(
leftarg =  json, rightarg = text, procedure = json_object_field_text
);
COMMENT ON OPERATOR pg_catalog.->>(json, text) IS 'json_object_field_text';

DROP OPERATOR IF EXISTS pg_catalog.->(json, integer) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 3964;
CREATE OPERATOR pg_catalog.->(
leftarg =  json, rightarg = integer, procedure = json_array_element
);
COMMENT ON OPERATOR pg_catalog.->(json, integer) IS 'json_array_element';

DROP OPERATOR IF EXISTS pg_catalog.->>(json, integer) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 3965;
CREATE OPERATOR pg_catalog.->>(
leftarg =  json, rightarg = integer, procedure = json_array_element_text
);
COMMENT ON OPERATOR pg_catalog.->>(json, integer) IS 'json_array_element_text';

DROP OPERATOR IF EXISTS pg_catalog.#>(json, _text) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 3966;
CREATE OPERATOR pg_catalog.#>(
leftarg =  json, rightarg = _text, procedure = json_extract_path_op
);
COMMENT ON OPERATOR pg_catalog.#>(json, _text) IS 'json_extract_path_op';

DROP OPERATOR IF EXISTS pg_catalog.#>>(json, _text) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 3967;
CREATE OPERATOR pg_catalog.#>>(
leftarg =  json, rightarg = _text, procedure = json_extract_path_text_op
);
COMMENT ON OPERATOR pg_catalog.#>>(json, _text) IS 'json_extract_path_text_op';

DROP OPERATOR IF EXISTS pg_catalog.->(jsonb, text) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 3211;
CREATE OPERATOR pg_catalog.->(
leftarg = jsonb, rightarg = text, procedure = jsonb_object_field
);
COMMENT ON OPERATOR pg_catalog.->(jsonb, text) IS 'jsonb_object_field';

DROP OPERATOR IF EXISTS pg_catalog.->>(jsonb, text) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 3477;
CREATE OPERATOR pg_catalog.->>(
leftarg = jsonb, rightarg = text, procedure = jsonb_object_field_text
);
COMMENT ON OPERATOR pg_catalog.->>(jsonb, text) IS 'jsonb_object_field_text';

DROP OPERATOR IF EXISTS pg_catalog.->(jsonb, integer) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 3212;
CREATE OPERATOR pg_catalog.->(
leftarg = jsonb, rightarg = integer, procedure = jsonb_array_element
);
COMMENT ON OPERATOR pg_catalog.->(jsonb, integer) IS 'jsonb_array_element';

DROP OPERATOR IF EXISTS pg_catalog.->>(jsonb, integer) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 3481;
CREATE OPERATOR pg_catalog.->>(
leftarg = jsonb, rightarg = integer, procedure = jsonb_array_element_text
);
COMMENT ON OPERATOR pg_catalog.->>(jsonb, integer) IS 'jsonb_array_element_text';

DROP OPERATOR IF EXISTS pg_catalog.#>(jsonb, _text) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 3213;
CREATE OPERATOR pg_catalog.#>(
leftarg = jsonb, rightarg = _text, procedure = jsonb_extract_path_op
);
COMMENT ON OPERATOR pg_catalog.#>(jsonb, _text) IS 'jsonb_extract_path_op';

DROP OPERATOR IF EXISTS pg_catalog.#>>(jsonb, _text) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 3206;
CREATE OPERATOR pg_catalog.#>>(
leftarg = jsonb, rightarg = _text, procedure = jsonb_extract_path_text_op
);
COMMENT ON OPERATOR pg_catalog.#>>(jsonb, _text) IS 'jsonb_extract_path_text_op';

DROP OPERATOR IF EXISTS pg_catalog.=(jsonb, jsonb) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 3240;
CREATE OPERATOR pg_catalog.=(
leftarg = jsonb, rightarg = jsonb, procedure = jsonb_eq,
commutator=operator(pg_catalog.=),
restrict = eqsel, join = eqjoinsel,
HASHES, MERGES
);
COMMENT ON OPERATOR pg_catalog.=(jsonb, jsonb) IS 'jsonb_eq';

DROP OPERATOR IF EXISTS pg_catalog.<>(jsonb, jsonb) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 3241;
CREATE OPERATOR pg_catalog.<>(
leftarg = jsonb, rightarg = jsonb, procedure = jsonb_ne,
commutator=operator(pg_catalog.<>), negator=operator(pg_catalog.=),
restrict = neqsel, join = neqjoinsel
);
COMMENT ON OPERATOR pg_catalog.<>(jsonb, jsonb) IS 'jsonb_ne';

DROP OPERATOR IF EXISTS pg_catalog.<(jsonb, jsonb) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 3242;
CREATE OPERATOR pg_catalog.<(
leftarg = jsonb, rightarg = jsonb, procedure = jsonb_lt,
restrict = scalarltsel, join = scalarltjoinsel
);
COMMENT ON OPERATOR pg_catalog.<(jsonb, jsonb) IS 'jsonb_lt';

DROP OPERATOR IF EXISTS pg_catalog.>(jsonb, jsonb) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 3243;
CREATE OPERATOR pg_catalog.>(
leftarg = jsonb, rightarg = jsonb, procedure = jsonb_gt,
commutator=operator(pg_catalog.<),
restrict = scalargtsel, join = scalargtjoinsel
);
COMMENT ON OPERATOR pg_catalog.>(jsonb, jsonb) IS 'jsonb_gt';

DROP OPERATOR IF EXISTS pg_catalog.<=(jsonb, jsonb) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 3244;
CREATE OPERATOR pg_catalog.<=(
leftarg = jsonb, rightarg = jsonb, procedure = jsonb_le,
negator=operator(pg_catalog.>),
restrict = scalarltsel, join = scalarltjoinsel
);
COMMENT ON OPERATOR pg_catalog.<=(jsonb, jsonb) IS 'jsonb_le';

DROP OPERATOR IF EXISTS pg_catalog.>=(jsonb, jsonb) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 3245;
CREATE OPERATOR pg_catalog.>=(
leftarg = jsonb, rightarg = jsonb, procedure = jsonb_ge,
commutator=operator(pg_catalog.<=), negator=operator(pg_catalog.<),
restrict = scalargtsel, join = scalargtjoinsel
);
COMMENT ON OPERATOR pg_catalog.>=(jsonb, jsonb) IS 'jsonb_ge';

DROP OPERATOR IF EXISTS pg_catalog.@>(jsonb, jsonb) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 3246;
CREATE OPERATOR pg_catalog.@>(
leftarg = jsonb, rightarg = jsonb, procedure = jsonb_contains,
restrict = contsel, join = contjoinsel
);
COMMENT ON OPERATOR pg_catalog.@>(jsonb, jsonb) IS 'jsonb_contains';

DROP OPERATOR IF EXISTS pg_catalog.?(jsonb, text) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 3247;
CREATE OPERATOR pg_catalog.?(
leftarg = jsonb, rightarg = text, procedure = jsonb_exists,
restrict = contsel, join = contjoinsel
);
COMMENT ON OPERATOR pg_catalog.?(jsonb, text) IS 'jsonb_exists';

DROP OPERATOR IF EXISTS pg_catalog.?|(jsonb, _text) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 3248;
CREATE OPERATOR pg_catalog.?|(
leftarg = jsonb, rightarg = _text, procedure = jsonb_exists_any,
restrict = contsel, join = contjoinsel
);
COMMENT ON OPERATOR pg_catalog.?|(jsonb, _text) IS 'jsonb_exists_any';

DROP OPERATOR IF EXISTS pg_catalog.?&(jsonb, _text) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 3249;
CREATE OPERATOR pg_catalog.?&(
leftarg = jsonb, rightarg = _text, procedure = jsonb_exists_all,
restrict = contsel, join = contjoinsel
);
COMMENT ON OPERATOR pg_catalog.?&(jsonb, _text) IS 'jsonb_exists_all';

DROP OPERATOR IF EXISTS pg_catalog.<@(jsonb, jsonb) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 3250;
CREATE OPERATOR pg_catalog.<@(
leftarg = jsonb, rightarg = jsonb, procedure = jsonb_contained,
negator=operator(pg_catalog.@>),
restrict = contsel, join = contjoinsel
);
COMMENT ON OPERATOR pg_catalog.<@(jsonb, jsonb) IS 'jsonb_contained';


--------------------------------------------------------------
-- add pg_opfamily
--------------------------------------------------------------
CREATE OR REPLACE FUNCTION Insert_pg_opfamily_temp(
IN imethod integer,
IN iname text,
IN inamespace integer,
IN iowner integer
)
RETURNS void
AS $$
DECLARE
  row_name record;
  query_str_nodes text;
BEGIN
  query_str_nodes := 'select * from dbe_perf.node_name';
  FOR row_name IN EXECUTE(query_str_nodes) LOOP
      insert into pg_catalog.pg_opfamily values (imethod, iname, inamespace, iowner);
  END LOOP;
  return;
END; $$
LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 4033;
select Insert_pg_opfamily_temp(403, 'jsonb_ops', 11, 10);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 4034;
select Insert_pg_opfamily_temp(405, 'jsonb_ops', 11, 10);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 4035;
select Insert_pg_opfamily_temp(783, 'jsonb_ops', 11, 10);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 4036;
select Insert_pg_opfamily_temp(2742, 'jsonb_ops', 11, 10);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 4037;
select Insert_pg_opfamily_temp(2742, 'jsonb_hash_ops', 11, 10);

DROP FUNCTION Insert_pg_opfamily_temp();


--------------------------------------------------------------
-- add pg_cast
--------------------------------------------------------------
DROP CAST IF EXISTS (json AS jsonb) CASCADE;
CREATE CAST(json AS jsonb) WITH INOUT;

DROP CAST IF EXISTS (jsonb AS json) CASCADE;
CREATE CAST(jsonb AS json) WITH INOUT;


--------------------------------------------------------------
-- add pg_opclass
--------------------------------------------------------------
CREATE OR REPLACE FUNCTION Insert_pg_opclass_temp(
IN icmethod integer,
IN icname text,
IN icnamespace integer,
IN icowner integer,
IN icfamily integer,
IN icintype integer,
IN icdefault boolean,
IN ickeytype integer
)
RETURNS void
AS $$
DECLARE
  row_name record;
  query_str_nodes text;
BEGIN
  query_str_nodes := 'select * from dbe_perf.node_name';
  FOR row_name IN EXECUTE(query_str_nodes) LOOP
      insert into pg_catalog.pg_opclass values (icmethod, icname, icnamespace, icowner, icfamily, icintype, icdefault, ickeytype);
  END LOOP;
  return;
END; $$
LANGUAGE 'plpgsql';

select Insert_pg_opclass_temp(403, 'jsonb_ops', 11, 10, 4033, 3802, true, 0);

select Insert_pg_opclass_temp(405, 'jsonb_ops', 11, 10, 4034, 3802, true, 0);

select Insert_pg_opclass_temp(2742, 'jsonb_ops', 11, 10, 4036, 3802, true, 25);

select Insert_pg_opclass_temp(2742, 'jsonb_hash_ops', 11, 10, 4037, 3802, false, 23);

DROP FUNCTION Insert_pg_opclass_temp();



--------------------------------------------------------------
-- add pg_amproc
--------------------------------------------------------------
CREATE OR REPLACE FUNCTION Insert_pg_amproc_temp(
IN iprocfamily    oid,
IN iproclefttype  oid,
IN iprocrighttype oid,
IN iprocnum       smallint,
IN iproc          regproc
)
RETURNS void
AS $$
DECLARE
  row_name record;
  query_str_nodes text;
BEGIN
  query_str_nodes := 'select * from dbe_perf.node_name';
  FOR row_name IN EXECUTE(query_str_nodes) LOOP
      insert into pg_catalog.pg_amproc values (iprocfamily, iproclefttype, iprocrighttype, iprocnum, iproc);
  END LOOP;
  return;
END; $$
LANGUAGE 'plpgsql';

SELECT Insert_pg_amproc_temp(4033, 3802, 3802, 1, 3417);

SELECT Insert_pg_amproc_temp(4034, 3802, 3802, 1, 3430);

SELECT Insert_pg_amproc_temp(4036, 3802, 3802, 1, 3498);

SELECT Insert_pg_amproc_temp(4036, 3802, 3802, 2, 3482);

SELECT Insert_pg_amproc_temp(4036, 3802, 3802, 3, 3493);

SELECT Insert_pg_amproc_temp(4036, 3802, 3802, 4, 3497);

SELECT Insert_pg_amproc_temp(4036, 3802, 3802, 6, 3494);

SELECT Insert_pg_amproc_temp(4037, 3802, 3802, 1,  351);

SELECT Insert_pg_amproc_temp(4037, 3802, 3802, 2, 3492);

SELECT Insert_pg_amproc_temp(4037, 3802, 3802, 3, 3486);

SELECT Insert_pg_amproc_temp(4037, 3802, 3802, 4, 3487);

SELECT Insert_pg_amproc_temp(4037, 3802, 3802, 6, 3495);

DROP FUNCTION Insert_pg_amproc_temp();


--------------------------------------------------------------
-- add pg_amop
--------------------------------------------------------------
CREATE OR REPLACE FUNCTION Insert_pg_amop_temp(
IN iopfamily     integer,
IN ioplefttype   integer,
IN ioprighttype  integer,
IN iopstrategy   integer,
IN ioppurpose    character,
IN iopopr        integer,
IN iopmethod     integer,
IN iopsortfamily integer
)
RETURNS void
AS $$
DECLARE
  row_name record;
  query_str_nodes text;
BEGIN
  query_str_nodes := 'select * from dbe_perf.node_name';
  FOR row_name IN EXECUTE(query_str_nodes) LOOP
      insert into pg_catalog.pg_amop values (iopfamily, ioplefttype, ioprighttype, iopstrategy, ioppurpose, iopopr, iopmethod, iopsortfamily);
  END LOOP;
  return;
END; $$
LANGUAGE 'plpgsql';

select Insert_pg_amop_temp(4033, 3802, 3802,  1, 's', 3242,  403, 0);

select Insert_pg_amop_temp(4033, 3802, 3802,  2, 's', 3244,  403, 0);

select Insert_pg_amop_temp(4033, 3802, 3802,  3, 's', 3240,  403, 0);

select Insert_pg_amop_temp(4033, 3802, 3802,  4, 's', 3245,  403, 0);

select Insert_pg_amop_temp(4033, 3802, 3802,  5, 's', 3243,  403, 0);

select Insert_pg_amop_temp(4034, 3802, 3802,  1, 's', 3240,  405, 0);

select Insert_pg_amop_temp(4036, 3802, 3802,  7, 's', 3246, 2742, 0);

select Insert_pg_amop_temp(4036, 3802,   25,  9, 's', 3247, 2742, 0);

select Insert_pg_amop_temp(4036, 3802, 1009, 10, 's', 3248, 2742, 0);

select Insert_pg_amop_temp(4036, 3802, 1009, 11, 's', 3249, 2742, 0);

select Insert_pg_amop_temp(4037, 3802, 3802,  7, 's', 3246, 2742, 0);

drop function Insert_pg_amop_temp();
SET search_path TO information_schema;

-- element_types is generated by data_type_privileges
DROP VIEW IF EXISTS information_schema.element_types CASCADE;

-- data_type_privileges is generated by columns
DROP VIEW IF EXISTS information_schema.data_type_privileges CASCADE;
-- data_type_privileges is generated by table_privileges
DROP VIEW IF EXISTS information_schema.role_column_grants CASCADE;
-- data_type_privileges is generated by column_privileges
DROP VIEW IF EXISTS information_schema.role_table_grants CASCADE;

-- other views need upgrade for matview
DROP VIEW IF EXISTS information_schema.column_domain_usage CASCADE;
DROP VIEW IF EXISTS information_schema.column_privileges CASCADE;
DROP VIEW IF EXISTS information_schema.column_udt_usage CASCADE;
DROP VIEW IF EXISTS information_schema.columns CASCADE;
DROP VIEW IF EXISTS information_schema.table_privileges CASCADE;
DROP VIEW IF EXISTS information_schema.tables CASCADE;
DROP VIEW IF EXISTS information_schema.view_column_usage CASCADE;
DROP VIEW IF EXISTS information_schema.view_table_usage CASCADE;

CREATE VIEW information_schema.column_domain_usage AS
    SELECT CAST(current_database() AS sql_identifier) AS domain_catalog,
           CAST(nt.nspname AS sql_identifier) AS domain_schema,
           CAST(t.typname AS sql_identifier) AS domain_name,
           CAST(current_database() AS sql_identifier) AS table_catalog,
           CAST(nc.nspname AS sql_identifier) AS table_schema,
           CAST(c.relname AS sql_identifier) AS table_name,
           CAST(a.attname AS sql_identifier) AS column_name

    FROM pg_type t, pg_namespace nt, pg_class c, pg_namespace nc,
         pg_attribute a

    WHERE t.typnamespace = nt.oid
          AND c.relnamespace = nc.oid
          AND a.attrelid = c.oid
          AND a.atttypid = t.oid
          AND t.typtype = 'd'
          AND c.relkind IN ('r', 'm', 'v', 'f')
          AND (c.relname not like 'mlog_%' AND c.relname not like 'matviewmap_%')
          AND a.attnum > 0
          AND NOT a.attisdropped
          AND pg_has_role(t.typowner, 'USAGE');

CREATE VIEW information_schema.column_privileges AS
    SELECT CAST(u_grantor.rolname AS sql_identifier) AS grantor,
           CAST(grantee.rolname AS sql_identifier) AS grantee,
           CAST(current_database() AS sql_identifier) AS table_catalog,
           CAST(nc.nspname AS sql_identifier) AS table_schema,
           CAST(x.relname AS sql_identifier) AS table_name,
           CAST(x.attname AS sql_identifier) AS column_name,
           CAST(x.prtype AS character_data) AS privilege_type,
           CAST(
             CASE WHEN
                  -- object owner always has grant options
                  pg_has_role(x.grantee, x.relowner, 'USAGE')
                  OR x.grantable
                  THEN 'YES' ELSE 'NO' END AS yes_or_no) AS is_grantable

    FROM (
           SELECT pr_c.grantor,
                  pr_c.grantee,
                  attname,
                  relname,
                  relnamespace,
                  pr_c.prtype,
                  pr_c.grantable,
                  pr_c.relowner
           FROM (SELECT oid, relname, relnamespace, relowner, (aclexplode(coalesce(relacl, acldefault('r', relowner)))).*
                 FROM pg_class
                 WHERE relkind IN ('r', 'm', 'v', 'f')
                ) pr_c (oid, relname, relnamespace, relowner, grantor, grantee, prtype, grantable),
                pg_attribute a
           WHERE a.attrelid = pr_c.oid
                 AND a.attnum > 0
                 AND NOT a.attisdropped
           UNION
           SELECT pr_a.grantor,
                  pr_a.grantee,
                  attname,
                  relname,
                  relnamespace,
                  pr_a.prtype,
                  pr_a.grantable,
                  c.relowner
           FROM (SELECT attrelid, attname, (aclexplode(coalesce(attacl, acldefault('c', relowner)))).*
                 FROM pg_attribute a JOIN pg_class cc ON (a.attrelid = cc.oid)
                 WHERE attnum > 0
                       AND NOT attisdropped
                ) pr_a (attrelid, attname, grantor, grantee, prtype, grantable),
                pg_class c
           WHERE pr_a.attrelid = c.oid
                 AND relkind IN ('r', 'm', 'v', 'f')
         ) x,
         pg_namespace nc,
         pg_authid u_grantor,
         (
           SELECT oid, rolname FROM pg_authid
           UNION ALL
           SELECT 0::oid, 'PUBLIC'
         ) AS grantee (oid, rolname)

    WHERE x.relnamespace = nc.oid
          AND x.grantee = grantee.oid
          AND x.grantor = u_grantor.oid
          AND x.prtype IN ('INSERT', 'SELECT', 'UPDATE', 'REFERENCES', 'COMMENT')
          AND (x.relname not like 'mlog_%' AND x.relname not like 'matviewmap_%')
          AND (pg_has_role(u_grantor.oid, 'USAGE')
               OR pg_has_role(grantee.oid, 'USAGE')
               OR grantee.rolname = 'PUBLIC');

CREATE VIEW information_schema.column_udt_usage AS
    SELECT CAST(current_database() AS sql_identifier) AS udt_catalog,
           CAST(coalesce(nbt.nspname, nt.nspname) AS sql_identifier) AS udt_schema,
           CAST(coalesce(bt.typname, t.typname) AS sql_identifier) AS udt_name,
           CAST(current_database() AS sql_identifier) AS table_catalog,
           CAST(nc.nspname AS sql_identifier) AS table_schema,
           CAST(c.relname AS sql_identifier) AS table_name,
           CAST(a.attname AS sql_identifier) AS column_name

    FROM pg_attribute a, pg_class c, pg_namespace nc,
         (pg_type t JOIN pg_namespace nt ON (t.typnamespace = nt.oid))
           LEFT JOIN (pg_type bt JOIN pg_namespace nbt ON (bt.typnamespace = nbt.oid))
           ON (t.typtype = 'd' AND t.typbasetype = bt.oid)

    WHERE a.attrelid = c.oid
          AND a.atttypid = t.oid
          AND nc.oid = c.relnamespace
          AND a.attnum > 0 AND NOT a.attisdropped AND c.relkind in ('r', 'm', 'v', 'f')
          AND (c.relname not like 'mlog_%' AND c.relname not like 'matviewmap_%')
          AND pg_has_role(coalesce(bt.typowner, t.typowner), 'USAGE');

CREATE VIEW information_schema.columns AS
    SELECT CAST(current_database() AS sql_identifier) AS table_catalog,
           CAST(nc.nspname AS sql_identifier) AS table_schema,
           CAST(c.relname AS sql_identifier) AS table_name,
           CAST(a.attname AS sql_identifier) AS column_name,
           CAST(a.attnum AS cardinal_number) AS ordinal_position,
           CAST(CASE WHEN ad.adgencol <> 's' THEN pg_get_expr(ad.adbin, ad.adrelid) END AS character_data) AS column_default,
           CAST(CASE WHEN a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) THEN 'NO' ELSE 'YES' END
             AS yes_or_no)
             AS is_nullable,

           CAST(
             CASE WHEN t.typtype = 'd' THEN
               CASE WHEN bt.typelem <> 0 AND bt.typlen = -1 THEN 'ARRAY'
                    WHEN nbt.nspname = 'pg_catalog' THEN format_type(t.typbasetype, null)
                    ELSE 'USER-DEFINED' END
             ELSE
               CASE WHEN t.typelem <> 0 AND t.typlen = -1 THEN 'ARRAY'
                    WHEN nt.nspname = 'pg_catalog' THEN format_type(a.atttypid, null)
                    ELSE 'USER-DEFINED' END
             END
             AS character_data)
             AS data_type,

           CAST(
             _pg_char_max_length(_pg_truetypid(a, t), _pg_truetypmod(a, t))
             AS cardinal_number)
             AS character_maximum_length,

           CAST(
             _pg_char_octet_length(_pg_truetypid(a, t), _pg_truetypmod(a, t))
             AS cardinal_number)
             AS character_octet_length,

           CAST(
             _pg_numeric_precision(_pg_truetypid(a, t), _pg_truetypmod(a, t))
             AS cardinal_number)
             AS numeric_precision,

           CAST(
             _pg_numeric_precision_radix(_pg_truetypid(a, t), _pg_truetypmod(a, t))
             AS cardinal_number)
             AS numeric_precision_radix,

           CAST(
             _pg_numeric_scale(_pg_truetypid(a, t), _pg_truetypmod(a, t))
             AS cardinal_number)
             AS numeric_scale,

           CAST(
             _pg_datetime_precision(_pg_truetypid(a, t), _pg_truetypmod(a, t))
             AS cardinal_number)
             AS datetime_precision,

           CAST(
             _pg_interval_type(_pg_truetypid(a, t), _pg_truetypmod(a, t))
             AS character_data)
             AS interval_type,
           CAST(null AS cardinal_number) AS interval_precision,

           CAST(null AS sql_identifier) AS character_set_catalog,
           CAST(null AS sql_identifier) AS character_set_schema,
           CAST(null AS sql_identifier) AS character_set_name,

           CAST(CASE WHEN nco.nspname IS NOT NULL THEN current_database() END AS sql_identifier) AS collation_catalog,
           CAST(nco.nspname AS sql_identifier) AS collation_schema,
           CAST(co.collname AS sql_identifier) AS collation_name,

           CAST(CASE WHEN t.typtype = 'd' THEN current_database() ELSE null END
             AS sql_identifier) AS domain_catalog,
           CAST(CASE WHEN t.typtype = 'd' THEN nt.nspname ELSE null END
             AS sql_identifier) AS domain_schema,
           CAST(CASE WHEN t.typtype = 'd' THEN t.typname ELSE null END
             AS sql_identifier) AS domain_name,

           CAST(current_database() AS sql_identifier) AS udt_catalog,
           CAST(coalesce(nbt.nspname, nt.nspname) AS sql_identifier) AS udt_schema,
           CAST(coalesce(bt.typname, t.typname) AS sql_identifier) AS udt_name,

           CAST(null AS sql_identifier) AS scope_catalog,
           CAST(null AS sql_identifier) AS scope_schema,
           CAST(null AS sql_identifier) AS scope_name,

           CAST(null AS cardinal_number) AS maximum_cardinality,
           CAST(a.attnum AS sql_identifier) AS dtd_identifier,
           CAST('NO' AS yes_or_no) AS is_self_referencing,

           CAST('NO' AS yes_or_no) AS is_identity,
           CAST(null AS character_data) AS identity_generation,
           CAST(null AS character_data) AS identity_start,
           CAST(null AS character_data) AS identity_increment,
           CAST(null AS character_data) AS identity_maximum,
           CAST(null AS character_data) AS identity_minimum,
           CAST(null AS yes_or_no) AS identity_cycle,

           CAST(CASE WHEN ad.adgencol = 's' THEN 'ALWAYS' ELSE 'NEVER' END AS character_data) AS is_generated,
           CAST(CASE WHEN ad.adgencol = 's' THEN pg_get_expr(ad.adbin, ad.adrelid) END AS character_data) AS generation_expression,

           CAST(CASE WHEN c.relkind = 'r'
                          OR (c.relkind = 'v'
                              AND EXISTS (SELECT 1 FROM pg_rewrite WHERE ev_class = c.oid AND ev_type = '2' AND is_instead)
                              AND EXISTS (SELECT 1 FROM pg_rewrite WHERE ev_class = c.oid AND ev_type = '4' AND is_instead))
                THEN 'YES' ELSE 'NO' END AS yes_or_no) AS is_updatable

    FROM (pg_attribute a LEFT JOIN pg_attrdef ad ON attrelid = adrelid AND attnum = adnum)
         JOIN (pg_class c JOIN pg_namespace nc ON (c.relnamespace = nc.oid)) ON a.attrelid = c.oid
         JOIN (pg_type t JOIN pg_namespace nt ON (t.typnamespace = nt.oid)) ON a.atttypid = t.oid
         LEFT JOIN (pg_type bt JOIN pg_namespace nbt ON (bt.typnamespace = nbt.oid))
           ON (t.typtype = 'd' AND t.typbasetype = bt.oid)
         LEFT JOIN (pg_collation co JOIN pg_namespace nco ON (co.collnamespace = nco.oid))
           ON a.attcollation = co.oid AND (nco.nspname, co.collname) <> ('pg_catalog', 'default')

    WHERE (NOT pg_is_other_temp_schema(nc.oid))

          AND a.attnum > 0 AND NOT a.attisdropped AND c.relkind in ('r', 'm', 'v', 'f')

          AND (c.relname not like 'mlog_%' AND c.relname not like 'matviewmap_%')

          AND (pg_has_role(c.relowner, 'USAGE')
               OR has_column_privilege(c.oid, a.attnum,
                                       'SELECT, INSERT, UPDATE, REFERENCES'));

CREATE VIEW information_schema.table_privileges AS
    SELECT CAST(u_grantor.rolname AS sql_identifier) AS grantor,
           CAST(grantee.rolname AS sql_identifier) AS grantee,
           CAST(current_database() AS sql_identifier) AS table_catalog,
           CAST(nc.nspname AS sql_identifier) AS table_schema,
           CAST(c.relname AS sql_identifier) AS table_name,
           CAST(c.prtype AS character_data) AS privilege_type,
           CAST(
             CASE WHEN
                  -- object owner always has grant options
                  pg_has_role(grantee.oid, c.relowner, 'USAGE')
                  OR c.grantable
                  THEN 'YES' ELSE 'NO' END AS yes_or_no) AS is_grantable,
           CAST(CASE WHEN c.prtype = 'SELECT' THEN 'YES' ELSE 'NO' END AS yes_or_no) AS with_hierarchy

    FROM (
            SELECT oid, relname, relnamespace, relkind, relowner, (aclexplode(coalesce(relacl, acldefault('r', relowner)))).* FROM pg_class
         ) AS c (oid, relname, relnamespace, relkind, relowner, grantor, grantee, prtype, grantable),
         pg_namespace nc,
         pg_authid u_grantor,
         (
           SELECT oid, rolname FROM pg_authid
           UNION ALL
           SELECT 0::oid, 'PUBLIC'
         ) AS grantee (oid, rolname)

    WHERE c.relnamespace = nc.oid
          AND c.relkind IN ('r', 'm', 'v')
          AND (c.relname not like 'mlog_%' AND c.relname not like 'matviewmap_%')
          AND c.grantee = grantee.oid
          AND c.grantor = u_grantor.oid
          AND (c.prtype IN ('INSERT', 'SELECT', 'UPDATE', 'DELETE', 'TRUNCATE', 'REFERENCES', 'TRIGGER')
               OR c.prtype IN ('ALTER', 'DROP', 'COMMENT', 'INDEX', 'VACUUM')
          )
          AND (pg_has_role(u_grantor.oid, 'USAGE')
               OR pg_has_role(grantee.oid, 'USAGE')
               OR grantee.rolname = 'PUBLIC');

CREATE VIEW information_schema.tables AS
    SELECT CAST(current_database() AS sql_identifier) AS table_catalog,
           CAST(nc.nspname AS sql_identifier) AS table_schema,
           CAST(c.relname AS sql_identifier) AS table_name,

           CAST(
             CASE WHEN nc.oid = pg_my_temp_schema() THEN 'LOCAL TEMPORARY'
                  WHEN c.relkind = 'r' THEN 'BASE TABLE'
                  WHEN c.relkind = 'm' THEN 'MATERIALIZED VIEW'
                  WHEN c.relkind = 'v' THEN 'VIEW'
                  WHEN c.relkind = 'f' THEN 'FOREIGN TABLE'
                  ELSE null END
             AS character_data) AS table_type,

           CAST(null AS sql_identifier) AS self_referencing_column_name,
           CAST(null AS character_data) AS reference_generation,

           CAST(CASE WHEN t.typname IS NOT NULL THEN current_database() ELSE null END AS sql_identifier) AS user_defined_type_catalog,
           CAST(nt.nspname AS sql_identifier) AS user_defined_type_schema,
           CAST(t.typname AS sql_identifier) AS user_defined_type_name,

           CAST(CASE WHEN c.relkind = 'r'
                          OR (c.relkind = 'v'
                              AND EXISTS (SELECT 1 FROM pg_rewrite WHERE ev_class = c.oid AND ev_type = '3' AND is_instead))
                THEN 'YES' ELSE 'NO' END AS yes_or_no) AS is_insertable_into,

           CAST(CASE WHEN t.typname IS NOT NULL THEN 'YES' ELSE 'NO' END AS yes_or_no) AS is_typed,
           CAST(null AS character_data) AS commit_action

    FROM pg_namespace nc JOIN pg_class c ON (nc.oid = c.relnamespace)
           LEFT JOIN (pg_type t JOIN pg_namespace nt ON (t.typnamespace = nt.oid)) ON (c.reloftype = t.oid)

    WHERE c.relkind IN ('r', 'm', 'v', 'f')
          AND (c.relname not like 'mlog_%' AND c.relname not like 'matviewmap_%')
          AND (NOT pg_is_other_temp_schema(nc.oid))
          AND (pg_has_role(c.relowner, 'USAGE')
               OR has_table_privilege(c.oid, 'SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER')
               OR has_any_column_privilege(c.oid, 'SELECT, INSERT, UPDATE, REFERENCES') );

CREATE VIEW information_schema.view_column_usage AS
    SELECT DISTINCT
           CAST(current_database() AS sql_identifier) AS view_catalog,
           CAST(nv.nspname AS sql_identifier) AS view_schema,
           CAST(v.relname AS sql_identifier) AS view_name,
           CAST(current_database() AS sql_identifier) AS table_catalog,
           CAST(nt.nspname AS sql_identifier) AS table_schema,
           CAST(t.relname AS sql_identifier) AS table_name,
           CAST(a.attname AS sql_identifier) AS column_name

    FROM pg_namespace nv, pg_class v, pg_depend dv,
         pg_depend dt, pg_class t, pg_namespace nt,
         pg_attribute a

    WHERE nv.oid = v.relnamespace
          AND v.relkind = 'v'
          AND v.oid = dv.refobjid
          AND dv.refclassid = 'pg_catalog.pg_class'::regclass
          AND dv.classid = 'pg_catalog.pg_rewrite'::regclass
          AND dv.deptype = 'i'
          AND dv.objid = dt.objid
          AND dv.refobjid <> dt.refobjid
          AND dt.classid = 'pg_catalog.pg_rewrite'::regclass
          AND dt.refclassid = 'pg_catalog.pg_class'::regclass
          AND dt.refobjid = t.oid
          AND t.relnamespace = nt.oid
          AND t.relkind IN ('r', 'm', 'v', 'f')
          AND (t.relname not like 'mlog_%' AND t.relname not like 'matviewmap_%')
          AND t.oid = a.attrelid
          AND dt.refobjsubid = a.attnum
          AND pg_has_role(t.relowner, 'USAGE');

CREATE VIEW information_schema.view_table_usage AS
    SELECT DISTINCT
           CAST(current_database() AS sql_identifier) AS view_catalog,
           CAST(nv.nspname AS sql_identifier) AS view_schema,
           CAST(v.relname AS sql_identifier) AS view_name,
           CAST(current_database() AS sql_identifier) AS table_catalog,
           CAST(nt.nspname AS sql_identifier) AS table_schema,
           CAST(t.relname AS sql_identifier) AS table_name

    FROM pg_namespace nv, pg_class v, pg_depend dv,
         pg_depend dt, pg_class t, pg_namespace nt

    WHERE nv.oid = v.relnamespace
          AND v.relkind = 'v'
          AND v.oid = dv.refobjid
          AND dv.refclassid = 'pg_catalog.pg_class'::regclass
          AND dv.classid = 'pg_catalog.pg_rewrite'::regclass
          AND dv.deptype = 'i'
          AND dv.objid = dt.objid
          AND dv.refobjid <> dt.refobjid
          AND dt.classid = 'pg_catalog.pg_rewrite'::regclass
          AND dt.refclassid = 'pg_catalog.pg_class'::regclass
          AND dt.refobjid = t.oid
          AND t.relnamespace = nt.oid
          AND t.relkind IN ('r', 'm', 'v', 'f')
          AND (t.relname not like 'mlog_%' AND t.relname not like 'matviewmap_%')
          AND pg_has_role(t.relowner, 'USAGE');

CREATE VIEW information_schema.data_type_privileges AS
    SELECT CAST(current_database() AS sql_identifier) AS object_catalog,
           CAST(x.objschema AS sql_identifier) AS object_schema,
           CAST(x.objname AS sql_identifier) AS object_name,
           CAST(x.objtype AS character_data) AS object_type,
           CAST(x.objdtdid AS sql_identifier) AS dtd_identifier

    FROM
      (
        SELECT udt_schema, udt_name, 'USER-DEFINED TYPE'::text, dtd_identifier FROM attributes
        UNION ALL
        SELECT table_schema, table_name, 'TABLE'::text, dtd_identifier FROM columns
        UNION ALL
        SELECT domain_schema, domain_name, 'DOMAIN'::text, dtd_identifier FROM domains
        UNION ALL
        SELECT specific_schema, specific_name, 'ROUTINE'::text, dtd_identifier FROM parameters
        UNION ALL
        SELECT specific_schema, specific_name, 'ROUTINE'::text, dtd_identifier FROM routines
      ) AS x (objschema, objname, objtype, objdtdid);

CREATE VIEW information_schema.role_column_grants AS
    SELECT grantor,
           grantee,
           table_catalog,
           table_schema,
           table_name,
           column_name,
           privilege_type,
           is_grantable
    FROM column_privileges
    WHERE grantor IN (SELECT role_name FROM enabled_roles)
          OR grantee IN (SELECT role_name FROM enabled_roles);

CREATE VIEW information_schema.role_table_grants AS
    SELECT grantor,
           grantee,
           table_catalog,
           table_schema,
           table_name,
           privilege_type,
           is_grantable,
           with_hierarchy
    FROM table_privileges
    WHERE grantor IN (SELECT role_name FROM enabled_roles)
          OR grantee IN (SELECT role_name FROM enabled_roles);

CREATE VIEW information_schema.element_types AS
    SELECT CAST(current_database() AS sql_identifier) AS object_catalog,
           CAST(n.nspname AS sql_identifier) AS object_schema,
           CAST(x.objname AS sql_identifier) AS object_name,
           CAST(x.objtype AS character_data) AS object_type,
           CAST(x.objdtdid AS sql_identifier) AS collection_type_identifier,
           CAST(
             CASE WHEN nbt.nspname = 'pg_catalog' THEN format_type(bt.oid, null)
                  ELSE 'USER-DEFINED' END AS character_data) AS data_type,

           CAST(null AS cardinal_number) AS character_maximum_length,
           CAST(null AS cardinal_number) AS character_octet_length,
           CAST(null AS sql_identifier) AS character_set_catalog,
           CAST(null AS sql_identifier) AS character_set_schema,
           CAST(null AS sql_identifier) AS character_set_name,
           CAST(CASE WHEN nco.nspname IS NOT NULL THEN current_database() END AS sql_identifier) AS collation_catalog,
           CAST(nco.nspname AS sql_identifier) AS collation_schema,
           CAST(co.collname AS sql_identifier) AS collation_name,
           CAST(null AS cardinal_number) AS numeric_precision,
           CAST(null AS cardinal_number) AS numeric_precision_radix,
           CAST(null AS cardinal_number) AS numeric_scale,
           CAST(null AS cardinal_number) AS datetime_precision,
           CAST(null AS character_data) AS interval_type,
           CAST(null AS cardinal_number) AS interval_precision,

           CAST(null AS character_data) AS domain_default, -- XXX maybe a bug in the standard

           CAST(current_database() AS sql_identifier) AS udt_catalog,
           CAST(nbt.nspname AS sql_identifier) AS udt_schema,
           CAST(bt.typname AS sql_identifier) AS udt_name,

           CAST(null AS sql_identifier) AS scope_catalog,
           CAST(null AS sql_identifier) AS scope_schema,
           CAST(null AS sql_identifier) AS scope_name,

           CAST(null AS cardinal_number) AS maximum_cardinality,
           CAST('a' || CAST(x.objdtdid AS text) AS sql_identifier) AS dtd_identifier

    FROM pg_namespace n, pg_type at, pg_namespace nbt, pg_type bt,
         (
           /* columns, attributes */
           SELECT c.relnamespace, CAST(c.relname AS sql_identifier),
                  CASE WHEN c.relkind = 'c' THEN 'USER-DEFINED TYPE'::text ELSE 'TABLE'::text END,
                  a.attnum, a.atttypid, a.attcollation
           FROM pg_class c, pg_attribute a
           WHERE c.oid = a.attrelid
                 AND c.relkind IN ('r', 'm', 'v', 'f', 'c')
                 AND (c.relname not like 'mlog_%' AND c.relname not like 'matviewmap_%')
                 AND attnum > 0 AND NOT attisdropped

           UNION ALL

           /* domains */
           SELECT t.typnamespace, CAST(t.typname AS sql_identifier),
                  'DOMAIN'::text, 1, t.typbasetype, t.typcollation
           FROM pg_type t
           WHERE t.typtype = 'd'

           UNION ALL

           /* parameters */
           SELECT pronamespace, CAST(proname || '_' || CAST(oid AS text) AS sql_identifier),
                  'ROUTINE'::text, (ss.x).n, (ss.x).x, 0
           FROM (SELECT p.pronamespace, p.proname, p.oid,
                        _pg_expandarray(coalesce(p.proallargtypes, p.proargtypes::oid[])) AS x
                 FROM pg_proc p) AS ss

           UNION ALL

           /* result types */
           SELECT p.pronamespace, CAST(p.proname || '_' || CAST(p.oid AS text) AS sql_identifier),
                  'ROUTINE'::text, 0, p.prorettype, 0
           FROM pg_proc p

         ) AS x (objschema, objname, objtype, objdtdid, objtypeid, objcollation)
         LEFT JOIN (pg_collation co JOIN pg_namespace nco ON (co.collnamespace = nco.oid))
           ON x.objcollation = co.oid AND (nco.nspname, co.collname) <> ('pg_catalog', 'default')

    WHERE n.oid = x.objschema
          AND at.oid = x.objtypeid
          AND (at.typelem <> 0 AND at.typlen = -1)
          AND at.typelem = bt.oid
          AND nbt.oid = bt.typnamespace

          AND (n.nspname, x.objname, x.objtype, CAST(x.objdtdid AS sql_identifier)) IN
              ( SELECT object_schema, object_name, object_type, dtd_identifier
                    FROM data_type_privileges );

GRANT SELECT ON information_schema.element_types TO PUBLIC;
GRANT SELECT ON information_schema.data_type_privileges TO PUBLIC;
GRANT SELECT ON information_schema.role_column_grants TO PUBLIC;
GRANT SELECT ON information_schema.role_table_grants TO PUBLIC;
GRANT SELECT ON information_schema.column_domain_usage TO PUBLIC;
GRANT SELECT ON information_schema.column_privileges TO PUBLIC;
GRANT SELECT ON information_schema.column_udt_usage TO PUBLIC;
GRANT SELECT ON information_schema.columns TO PUBLIC;
GRANT SELECT ON information_schema.table_privileges TO PUBLIC;
GRANT SELECT ON information_schema.tables TO PUBLIC;
GRANT SELECT ON information_schema.view_column_usage TO PUBLIC;
GRANT SELECT ON information_schema.view_table_usage TO PUBLIC;

RESET search_path;

DROP FUNCTION IF EXISTS pg_catalog.gs_decrypt(IN decryptstr text, IN keystr text, IN type text,OUT decrypt_result_str text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6322;
CREATE FUNCTION pg_catalog.gs_decrypt(IN decryptstr text, IN keystr text, IN type text, OUT decrypt_result_str text) RETURNS text  LANGUAGE INTERNAL  as 'gs_decrypt';

DROP FUNCTION IF EXISTS pg_catalog.gs_encrypt(IN encryptstr text, IN keystr text, IN type text,OUT decrypt_result_str text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6323;
CREATE FUNCTION pg_catalog.gs_encrypt(IN encryptstr text, IN keystr text, IN type text, OUT encrypt_result_str text) RETURNS text  LANGUAGE INTERNAL  as 'gs_encrypt';
DROP FUNCTION IF EXISTS pg_catalog.gs_decrypt(IN decryptstr text, IN keystr text, IN type text,OUT decrypt_result_str text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6322;
CREATE FUNCTION pg_catalog.gs_decrypt(IN decryptstr text, IN keystr text, IN type text, OUT decrypt_result_str text) RETURNS text  LANGUAGE INTERNAL  as 'gs_decrypt';

DROP FUNCTION IF EXISTS pg_catalog.gs_encrypt(IN encryptstr text, IN keystr text, IN type text,OUT decrypt_result_str text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6323;
CREATE FUNCTION pg_catalog.gs_encrypt(IN encryptstr text, IN keystr text, IN type text, OUT encrypt_result_str text) RETURNS text  LANGUAGE INTERNAL  as 'gs_encrypt';UPDATE pg_catalog.pg_am set amcanunique = TRUE where amname = 'cbtree';
CREATE OR REPLACE VIEW pg_catalog.gs_session_cpu_statistics AS
SELECT
        S.datid AS datid,
        S.usename,
        S.pid,
        S.query_start AS start_time,
        T.min_cpu_time,
        T.max_cpu_time,
        T.total_cpu_time,
        S.query,
        S.node_group,
        T.top_cpu_dn
FROM pg_stat_activity_ng AS S, pg_stat_get_wlm_realtime_session_info(NULL) AS T
WHERE S.sessionid = T.threadid;
