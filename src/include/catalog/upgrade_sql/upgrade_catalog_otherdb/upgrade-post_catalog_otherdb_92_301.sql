--------------------------------------------------------------
-- add new type  jsonb
--------------------------------------------------------------
DROP TYPE IF EXISTS pg_catalog.jsonb;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_TYPE, 3802, 0, b;
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
) RETURNS SETOF anyelement LANGUAGE INTERNAL ROWS 100 VOLATILE  as 'json_populate_recordset';

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
