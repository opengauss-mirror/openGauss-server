analyze pg_catalog.pg_class;
analyze pg_catalog.pg_namespace;
analyze pg_catalog.pgxc_class;
analyze pg_catalog.pg_statistic;
--sqlblock
DROP FUNCTION IF EXISTS PUBLIC.pgxc_analyzed_tuples() CASCADE;
--sqlblock
CREATE OR REPLACE FUNCTION PUBLIC.pgxc_analyzed_tuples
(
    OUT schemaname text,
    OUT tablename text,
    OUT dn_name text,
    OUT tuples real
)
RETURNS SETOF record
AS $$
DECLARE
    datanode_rd     record;
    fetch_tuples    record;
    fetch_dn        text;
    fetch_tuple_str text;
    BEGIN
        fetch_dn := 'SELECT node_name FROM pg_catalog.pgxc_node WHERE node_type=''D'' order by node_name';
        FOR datanode_rd IN EXECUTE(fetch_dn) LOOP
            dn_name         :=  datanode_rd.node_name;
            fetch_tuple_str := 'EXECUTE DIRECT ON (' || dn_name || ') ''SELECT
                                                                            n.nspname,
                                                                            c.relname,
                                                                            c.reltuples
                                                                        FROM pg_catalog.pg_class c
                                                                        INNER JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                                                                        where c.oid >16384 AND c.relkind = ''''r'''' and n.nspname <> ''''cstore'''' and n.nspname <> ''''pmk'''' and n.nspname <> ''''pg_catalog''''
                                                                        ''';
            FOR fetch_tuples IN EXECUTE(fetch_tuple_str) LOOP
                tuples      :=  fetch_tuples.reltuples;
                schemaname  :=  fetch_tuples.nspname;
                tablename   :=  fetch_tuples.relname;
                return next;
            END LOOP;
			RAISE INFO 'Finished fetching stats info from DataNode % at %',dn_name, clock_timestamp();
        END LOOP;
        return;
    END; $$
LANGUAGE 'plpgsql'
ROWS 1000000;
--sqlblock
DROP VIEW IF EXISTS PUBLIC.pgxc_analyzed_skewness;
--sqlblock
CREATE VIEW PUBLIC.pgxc_analyzed_skewness
AS
SELECT
    schemaname,
    tablename,
    (min(ratio)::numeric(6,3)) AS ratio_min,
    (max(ratio)::numeric(6,3) ) AS ratio_max,
    (max(ratio) - min(ratio))::numeric(6,3) AS skewness_ratio,
    ((max(ratio) - min(ratio)) * total_tuples / 100)::numeric(35) as skewness_tuple,
    ((max(ratio) - min(ratio)) * relwidth * total_tuples / 100)::numeric(35) as skewness_size,
    (stddev_samp(ratio)::numeric(6,3)) AS skewness_stddev
FROM
(
    WITH udt AS
    (
        SELECT
            n.nspname AS schemaname,
            c.relname AS tablename,
            relwidth
        FROM pg_catalog.pg_class c
        INNER JOIN pg_catalog.pg_namespace n ON (n.oid = c.relnamespace)
        INNER JOIN (SELECT sum(stawidth) as relwidth, starelid FROM pg_catalog.pg_statistic GROUP BY starelid)s ON s.starelid = c.oid
		INNER JOIN pg_catalog.pgxc_class x ON c.oid = x.pcrelid
        WHERE x.pclocatortype = 'H' AND c.reltuples > 500
    )

    SELECT
        schemaname,
        tablename,
        total_tuples,
        relwidth,
        (round(tuples/total_tuples, 4) * 100)AS ratio
    FROM
    (
        SELECT
            t.schemaname,
            t.tablename,
            t.dn_name,
            t.tuples,
            relwidth,
            sum(tuples) OVER (PARTITION BY t.schemaname, t.tablename) AS total_tuples
        FROM PUBLIC.pgxc_analyzed_tuples() t
        INNER JOIN udt u on (u.schemaname = t.schemaname and u.tablename = t.tablename)
    )
)
GROUP BY schemaname, tablename, total_tuples, relwidth;
--sqlblock
SELECT * FROM PUBLIC.pgxc_analyzed_skewness
WHERE skewness_tuple > 100000
ORDER BY skewness_tuple DESC, skewness_ratio DESC, skewness_size DESC;