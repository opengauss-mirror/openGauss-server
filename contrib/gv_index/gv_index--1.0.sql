\echo Use "CREATE EXTENSION gv_index" to load this file. \quit

CREATE OR REPLACE FUNCTION gv_graph_index_handler(internal) RETURNS index_am_handler
    AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION gv_graph_ambuild_sql(internal, internal, internal) RETURNS internal
    AS 'MODULE_PATHNAME' LANGUAGE C;
CREATE FUNCTION gv_graph_ambuildempty_sql(internal) RETURNS void
    AS 'MODULE_PATHNAME' LANGUAGE C;
CREATE FUNCTION gv_graph_aminsert_sql(internal, internal, internal, internal, internal, internal) RETURNS boolean
    AS 'MODULE_PATHNAME' LANGUAGE C;
CREATE FUNCTION gv_graph_ambulkdelete_sql(internal, internal, internal, internal) RETURNS internal
    AS 'MODULE_PATHNAME' LANGUAGE C;
CREATE FUNCTION gv_graph_amvacuumcleanup_sql(internal, internal) RETURNS internal
    AS 'MODULE_PATHNAME' LANGUAGE C;
CREATE FUNCTION gv_graph_amcostestimate_sql(internal, internal, internal, internal, internal, internal, internal)
    RETURNS void AS 'MODULE_PATHNAME' LANGUAGE C;
CREATE FUNCTION gv_graph_amoptions_sql(internal, internal) RETURNS internal
    AS 'MODULE_PATHNAME' LANGUAGE C;
CREATE FUNCTION gv_graph_ambeginscan_sql(internal, internal, internal) RETURNS internal
    AS 'MODULE_PATHNAME' LANGUAGE C;
CREATE FUNCTION gv_graph_amrescan_sql(internal, internal, internal, internal, internal) RETURNS void
    AS 'MODULE_PATHNAME' LANGUAGE C;
CREATE FUNCTION gv_graph_amgettuple_sql(internal, internal) RETURNS boolean
    AS 'MODULE_PATHNAME' LANGUAGE C;
CREATE FUNCTION gv_graph_amendscan_sql(internal) RETURNS void
    AS 'MODULE_PATHNAME' LANGUAGE C;
CREATE FUNCTION gv_graph_amdelete_sql(internal, internal, internal, internal, internal) RETURNS boolean
    AS 'MODULE_PATHNAME' LANGUAGE C;

DROP ACCESS METHOD IF EXISTS gv_graph;
CREATE ACCESS METHOD gv_graph TYPE INDEX HANDLER gv_graph_index_handler;

CREATE OPERATOR CLASS vector_l2_ops
    FOR TYPE vector USING gv_graph AS
    OPERATOR 1 <-> (vector, vector) FOR ORDER BY float_ops,
    FUNCTION 1 vector_l2_squared_distance(vector, vector);

CREATE OPERATOR CLASS vector_cosine_ops
    FOR TYPE vector USING gv_graph AS
    OPERATOR 1 <=> (vector, vector) FOR ORDER BY float_ops,
    FUNCTION 1 vector_negative_inner_product(vector, vector),
    FUNCTION 2 vector_norm(vector);
