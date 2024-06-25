CREATE OR REPLACE FUNCTION dbe_perf.get_global_record_reset_time(OUT node_name text, OUT reset_time timestamp with time zone)
RETURNS setof record
AS $$
DECLARE
  row_data record;
  row_name record;
  query_str text;
  query_str_nodes text;
  BEGIN
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'select * from pg_catalog.get_node_stat_reset_time()';
      FOR row_data IN EXECUTE(query_str) LOOP
        node_name := row_name.node_name;
        reset_time := row_data.get_node_stat_reset_time;
        return next;
      END LOOP;
    END LOOP;
    RETURN;
  END; $$
LANGUAGE 'plpgsql';