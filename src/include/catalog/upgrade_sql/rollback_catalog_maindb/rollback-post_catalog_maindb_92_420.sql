--------------------------------------------------------------
-- delete pg_amop
--------------------------------------------------------------
CREATE OR REPLACE FUNCTION Delete_pg_amop_temp()
RETURNS void
AS $$
DECLARE
row_name record;
query_str text;
query_str_nodes text;
BEGIN
  query_str_nodes := 'select * from dbe_perf.node_name';
  FOR row_name IN EXECUTE(query_str_nodes) LOOP
    delete from pg_catalog.pg_amop where amopfamily in (4033, 4034, 4035, 4036, 4037);
  END LOOP;
return;
END;
$$ LANGUAGE 'plpgsql';

SELECT Delete_pg_amop_temp();
DROP FUNCTION Delete_pg_amop_temp();


--------------------------------------------------------------
-- delete pg_amproc
--------------------------------------------------------------
CREATE OR REPLACE FUNCTION Delete_pg_amproc_temp()
RETURNS void
AS $$
DECLARE
row_name record;
query_str text;
query_str_nodes text;
BEGIN
  query_str_nodes := 'select * from dbe_perf.node_name';
  FOR row_name IN EXECUTE(query_str_nodes) LOOP
    delete from pg_catalog.pg_amproc where amprocfamily in (4033, 4034, 4035, 4036, 4037);
  END LOOP;
return;
END;
$$ LANGUAGE 'plpgsql';

SELECT Delete_pg_amproc_temp();
DROP FUNCTION Delete_pg_amproc_temp();


--------------------------------------------------------------
-- delete pg_opclass
--------------------------------------------------------------
CREATE OR REPLACE FUNCTION Delete_pg_opclass_temp()
RETURNS void
AS $$
DECLARE
row_name record;
query_str text;
query_str_nodes text;
BEGIN
  query_str_nodes := 'select * from dbe_perf.node_name';
  FOR row_name IN EXECUTE(query_str_nodes) LOOP
    delete from pg_catalog.pg_opclass where opcfamily in (4033, 4034, 4035, 4036, 4037);
  END LOOP;
return;
END;
$$ LANGUAGE 'plpgsql';

SELECT Delete_pg_opclass_temp();
DROP FUNCTION Delete_pg_opclass_temp();


--------------------------------------------------------------
-- delete pg_cast
--------------------------------------------------------------
DO $$
DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select * from pg_type where typname = 'jsonb' limit 1) into ans;
    if ans = true then
        DROP CAST IF EXISTS (json AS jsonb) CASCADE;
        DROP CAST IF EXISTS (jsonb AS json) CASCADE;
    end if;
END$$;


--------------------------------------------------------------
-- delete pg_opfamily
--------------------------------------------------------------
CREATE OR REPLACE FUNCTION Delete_pg_opfamily_temp()
RETURNS void
AS $$
DECLARE
row_name record;
query_str text;
query_str_nodes text;
BEGIN
  query_str_nodes := 'select * from dbe_perf.node_name';
  FOR row_name IN EXECUTE(query_str_nodes) LOOP
    delete from pg_catalog.pg_opfamily where oid in (4033, 4034, 4035, 4036, 4037);
  END LOOP;
return;
END;
$$ LANGUAGE 'plpgsql';

SELECT Delete_pg_opfamily_temp();
DROP FUNCTION Delete_pg_opfamily_temp();


--------------------------------------------------------------
-- delete pg_operator
--------------------------------------------------------------
DROP OPERATOR IF EXISTS pg_catalog.->(json, text) cascade;
DROP OPERATOR IF EXISTS pg_catalog.->>(json, text) cascade;
DROP OPERATOR IF EXISTS pg_catalog.->(json, integer) cascade;
DROP OPERATOR IF EXISTS pg_catalog.->>(json, integer) cascade;
DROP OPERATOR IF EXISTS pg_catalog.#>(json, _text) cascade;
DROP OPERATOR IF EXISTS pg_catalog.#>>(json, _text) cascade;

DO $$
DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select * from pg_type where typname = 'jsonb' limit 1) into ans;
    if ans = true then
        DROP OPERATOR IF EXISTS pg_catalog.->(jsonb, text) cascade;
        DROP OPERATOR IF EXISTS pg_catalog.->>(jsonb, text) cascade;
        DROP OPERATOR IF EXISTS pg_catalog.->(jsonb, integer) cascade;
        DROP OPERATOR IF EXISTS pg_catalog.->>(jsonb, integer) cascade;
        DROP OPERATOR IF EXISTS pg_catalog.#>(jsonb, _text) cascade;
        DROP OPERATOR IF EXISTS pg_catalog.#>>(jsonb, _text) cascade;
        DROP OPERATOR IF EXISTS pg_catalog.=(jsonb, jsonb) cascade;
        DROP OPERATOR IF EXISTS pg_catalog.<>(jsonb, jsonb) cascade;
        DROP OPERATOR IF EXISTS pg_catalog.<(jsonb, jsonb) cascade;
        DROP OPERATOR IF EXISTS pg_catalog.>(jsonb, jsonb) cascade;
        DROP OPERATOR IF EXISTS pg_catalog.<=(jsonb, jsonb) cascade;
        DROP OPERATOR IF EXISTS pg_catalog.>=(jsonb, jsonb) cascade;
        DROP OPERATOR IF EXISTS pg_catalog.@>(jsonb, jsonb) cascade;
        DROP OPERATOR IF EXISTS pg_catalog.?(jsonb, text) cascade;
        DROP OPERATOR IF EXISTS pg_catalog.?|(jsonb, _text) cascade;
        DROP OPERATOR IF EXISTS pg_catalog.?&(jsonb, _text) cascade;
        DROP OPERATOR IF EXISTS pg_catalog.<@(jsonb, jsonb) cascade;
    end if;
END$$;


--------------------------------------------------------------
-- delete builtin funcs
--------------------------------------------------------------
DROP FUNCTION IF EXISTS pg_catalog.gin_compare_jsonb(text, text) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gin_consistent_jsonb(internal, smallint, anyarray, integer, internal, internal, internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gin_consistent_jsonb_hash(internal, smallint, anyarray, integer, internal, internal, internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gin_extract_jsonb(internal, internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gin_extract_jsonb_hash(internal, internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gin_extract_jsonb_query(anyarray, internal, smallint, internal, internal, internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gin_extract_jsonb_query_hash(anyarray, internal, smallint, internal, internal, internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gin_triconsistent_jsonb(internal, smallint, anyarray, integer, internal, internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gin_triconsistent_jsonb_hash(internal, smallint, anyarray, integer, internal, internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.json_array_element(json, integer) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.json_array_element_text(json, integer) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.json_array_elements(from_json json, OUT value json) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.json_array_elements_text(from_json json, OUT value text) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.json_array_length(json) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.json_build_array() CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.json_build_array(VARIADIC "any") CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.json_build_object() CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.json_build_object(VARIADIC "any") CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.json_each(from_json json, OUT key text, OUT value json) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.json_each_text(from_json json, OUT key text, OUT value text) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.json_extract_path(json, VARIADIC text[]) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.json_extract_path_op(json, text[]) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.json_extract_path_text(json, VARIADIC text[]) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.json_extract_path_text_op(json, text[]) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.json_object(text[], text[]) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.json_object(text[]) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.json_object_field(json, text) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.json_object_field_text(json, text) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.json_object_keys(json) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.json_populate_record(anyelement, json, boolean) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.json_populate_recordset(anyelement, json, boolean) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.json_to_record(json, boolean) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.json_to_recordset(json, boolean) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.to_json(anyelement) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.json_typeof(json) CASCADE;

DO $$
DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select * from pg_type where typname = 'jsonb' limit 1) into ans;
    if ans = true then
        DROP FUNCTION IF EXISTS pg_catalog.jsonb_array_element(jsonb, integer) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.jsonb_array_element_text(jsonb, integer) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.jsonb_array_elements(from_json jsonb, OUT value jsonb) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.jsonb_array_elements_text(from_json jsonb, OUT value text) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.jsonb_array_length(jsonb) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.jsonb_cmp(jsonb, jsonb) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.jsonb_contained(jsonb, jsonb) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.jsonb_contains(jsonb, jsonb) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.jsonb_each(from_json jsonb, OUT key text, OUT value jsonb) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.jsonb_each_text(from_json jsonb, OUT key text, OUT value text) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.jsonb_eq(jsonb, jsonb) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.jsonb_exists(jsonb, text) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.jsonb_exists_all(jsonb, text[]) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.jsonb_exists_any(jsonb, text[]) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.jsonb_extract_path(jsonb, VARIADIC text[]) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.jsonb_extract_path_op(jsonb, text[]) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.jsonb_extract_path_text(jsonb, VARIADIC text[]) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.jsonb_extract_path_text_op(jsonb, text[]) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.jsonb_ge(jsonb, jsonb) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.jsonb_gt(jsonb, jsonb) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.jsonb_hash(jsonb) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.jsonb_le(jsonb, jsonb) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.jsonb_lt(jsonb, jsonb) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.jsonb_ne(jsonb, jsonb) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.jsonb_object_field(jsonb, text) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.jsonb_object_field_text(jsonb, text) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.jsonb_object_keys(jsonb) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.jsonb_populate_record(anyelement, jsonb, boolean) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.jsonb_populate_recordset(anyelement, jsonb, boolean) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.jsonb_typeof(jsonb) CASCADE;
    end if;
END$$;
----------------------------------------------------------------
-- recover funcs
----------------------------------------------------------------
ALTER FUNCTION json_in(cstring) STABLE;
ALTER FUNCTION json_send(json) STABLE;
ALTER FUNCTION json_recv(internal) STABLE;

----------------------------------------------------------------
-- delete two agg funcs
----------------------------------------------------------------
drop aggregate if exists pg_catalog.json_object_agg("any", "any");
DROP FUNCTION IF EXISTS pg_catalog.json_object_agg_finalfn(internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.json_object_agg_transfn(internal, "any", "any") CASCADE;

drop aggregate if exists pg_catalog.json_agg(anyelement);
DROP FUNCTION IF EXISTS pg_catalog.json_agg_finalfn(internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.json_agg_transfn(internal, anyelement) CASCADE;


--------------------------------------------------------------
-- delete type jsonb
--------------------------------------------------------------
DROP FUNCTION IF EXISTS pg_catalog.jsonb_in(cstring) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.jsonb_recv(internal) CASCADE;
DO $$
DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select * from pg_type where typname = 'jsonb' limit 1) into ans;
    if ans = true then
        DROP FUNCTION IF EXISTS pg_catalog.jsonb_out(jsonb) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.jsonb_send(jsonb) CASCADE;
    end if;
END$$;
DROP TYPE IF EXISTS pg_catalog._jsonb;
DROP TYPE IF EXISTS pg_catalog.jsonb;

DROP FUNCTION IF EXISTS pg_catalog.gs_decrypt(IN decryptstr text, IN keystr text, IN type text,OUT decrypt_result_str text) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gs_encrypt(IN encryptstr text, IN keystr text, IN type text,OUT encrypt_result_str text) CASCADE;DROP FUNCTION IF EXISTS pg_catalog.gs_decrypt(IN decryptstr text, IN keystr text, IN type text,OUT decrypt_result_str text) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gs_encrypt(IN encryptstr text, IN keystr text, IN type text,OUT encrypt_result_str text) CASCADE;DROP FUNCTION IF EXISTS pg_catalog.gs_decrypt(IN decryptstr text, IN keystr text, IN type text,OUT decrypt_result_str text) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gs_encrypt(IN encryptstr text, IN keystr text, IN type text,OUT encrypt_result_str text) CASCADE;UPDATE pg_catalog.pg_am set amcanunique = FALSE, amhandler = 0 where amname = 'cbtree';
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
WHERE S.pid = T.threadid;
