/* src/pl/plpgsql/src/plpgsql--1.0.sql */

/*
 * Currently, all the interesting stuff is done by CREATE LANGUAGE.
 * Later we will probably "dumb down" that command and put more of the
 * knowledge into this script.
 */

CREATE PROCEDURAL LANGUAGE plpgsql;

COMMENT ON PROCEDURAL LANGUAGE plpgsql IS 'PL/pgSQL procedural language';

/*
 * PGXC system view to look for prepared transaction GID list in a cluster
 */
CREATE FUNCTION pgxc_prepared_xact()
RETURNS setof text
AS $$
DECLARE
	text_output text;
	row_data record;
	row_name record;
	query_str text;
	query_str_nodes text;
	BEGIN
		--Get all the node names
		query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE nodeis_active = true';
		FOR row_name IN EXECUTE(query_str_nodes) LOOP
			query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT gid FROM pg_prepared_xact()''';
			FOR row_data IN EXECUTE(query_str) LOOP
				return next row_data.gid;
			END LOOP;
		END LOOP;
		return;
	END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE VIEW pgxc_prepared_xacts AS
    SELECT DISTINCT * from pgxc_prepared_xact();

/*
 * PGXC system view to export communication layer status of datanode into each log file, when using sctp communictaion mode.
 */
CREATE FUNCTION pgxc_log_comm_status()
RETURNS void 
AS $$
DECLARE
	row_name record;
	query_str text;
	query_str_nodes text;
	BEGIN
		--Get all the node names
		query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE nodeis_active = true';
		FOR row_name IN EXECUTE(query_str_nodes) LOOP
			query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT pg_log_comm_status()''';
			EXECUTE(query_str);
		END LOOP;
		return;
	END; $$
LANGUAGE 'plpgsql' NOT FENCED;;

