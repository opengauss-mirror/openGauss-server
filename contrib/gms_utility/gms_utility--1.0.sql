/* contrib/gms_utility/gms_utility--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION gms_utility" to load this file. \quit

-- gms_utility package begin
-- gms_utility schema
CREATE SCHEMA gms_utility;
GRANT USAGE ON SCHEMA gms_utility TO PUBLIC;


/*
 * ----------------------------
 * -- DB_VERSION 
 * ----------------------------
 */
CREATE OR REPLACE PROCEDURE GMS_UTILITY.DB_VERSION(
    version         OUT     varchar2,
    compatibility   OUT     varchar2
) 
AS 
BEGIN
    version := 'openGauss ' || opengauss_version();
    compatibility  := 'openGuass ' || opengauss_version();
END;


/*
* ----------------------------
* -- ANALYZE_SCHEMA 
* ----------------------------
*/
CREATE OR REPLACE FUNCTION GMS_UTILITY.ANALYZE_SCHEMA_C_FUN (
   schema               VARCHAR2,
   method               VARCHAR2,
   estimate_rows        NUMBER DEFAULT NULL,
   estimate_percent     NUMBER DEFAULT NULL,
   method_opt           VARCHAR2 DEFAULT NULL
) RETURNS void
AS 'MODULE_PATHNAME','gms_analyze_schema'
LANGUAGE C VOLATILE NOT FENCED;

CREATE OR REPLACE PROCEDURE GMS_UTILITY.ANALYZE_SCHEMA (
   schema             IN  VARCHAR2,
   method             IN  VARCHAR2,
   estimate_rows      IN  NUMBER DEFAULT NULL,
   estimate_percent   IN  NUMBER DEFAULT NULL,
   method_opt         IN  VARCHAR2 DEFAULT NULL
)
AS
BEGIN
    GMS_UTILITY.ANALYZE_SCHEMA_C_FUN(schema, method, estimate_rows, estimate_percent, method_opt);
END;

/*
* ----------------------------
* -- ANALYZE_DATABASE 
* ----------------------------
*/
CREATE OR REPLACE PROCEDURE GMS_UTILITY.ANALYZE_DATABASE (
   method             IN  VARCHAR2,
   estimate_rows      IN  NUMBER DEFAULT NULL,
   estimate_percent   IN  NUMBER DEFAULT NULL,
   method_opt         IN  VARCHAR2 DEFAULT NULL
)
AS
    schema_name text;
BEGIN
    FOR schema_name IN SELECT nspname FROM pg_namespace
    LOOP
        GMS_UTILITY.ANALYZE_SCHEMA_C_FUN(schema_name, method, estimate_rows, estimate_percent, method_opt);
    END LOOP;
END;

/*
* ----------------------------
* -- CANONICALIZE
* ----------------------------
*/
CREATE OR REPLACE FUNCTION GMS_UTILITY.CANONICALIZE_C_FUNC (
    name          VARCHAR2,
    canon_len     INTEGER
) RETURNS VARCHAR2
AS 'MODULE_PATHNAME','gms_canonicalize'
LANGUAGE C VOLATILE NOT FENCED;

CREATE OR REPLACE PROCEDURE GMS_UTILITY.CANONICALIZE (
    name        IN  VARCHAR2,
    canon_name  OUT VARCHAR2,
    canon_len   IN  INTEGER
)
AS
BEGIN
    if name IS NULL then
        canon_name := NULL;
    else
        canon_name := GMS_UTILITY.CANONICALIZE_C_FUNC(name, canon_len);
    end if;
END;

/*
* ----------------------------
* -- COMPILE_SCHEMA
* ----------------------------
*/
CREATE OR REPLACE FUNCTION GMS_UTILITY.COMPILE_SCHEMA_C_FUNC (
    schema          VARCHAR2,
    compile_all     BOOLEAN DEFAULT TRUE,
    reuse_settings  BOOLEAN DEFAULT FALSE
) RETURNS void
AS 'MODULE_PATHNAME','gms_compile_schema'
LANGUAGE C VOLATILE NOT FENCED;

CREATE OR REPLACE PROCEDURE GMS_UTILITY.COMPILE_SCHEMA (
    schema          IN  VARCHAR2,
    compile_all     IN  BOOLEAN DEFAULT TRUE,
    reuse_settings  IN  BOOLEAN DEFAULT FALSE
)
AS
BEGIN
    GMS_UTILITY.COMPILE_SCHEMA_C_FUNC(schema, compile_all, reuse_settings);
END;

/*
* ----------------------------
* -- EXPAND_SQL_TEXT
* ----------------------------
*/
CREATE OR REPLACE FUNCTION GMS_UTILITY.EXPAND_SQL_TEXT_C_FUNC (
    input_sql_text      IN  CLOB,
    output_sql_text     OUT CLOB
)
AS 'MODULE_PATHNAME','gms_expand_sql_text'
LANGUAGE C VOLATILE NOT FENCED;

CREATE OR REPLACE PROCEDURE GMS_UTILITY.EXPAND_SQL_TEXT (
    input_sql_text      IN  CLOB,
    output_sql_text     OUT CLOB
)
AS
BEGIN
    output_sql_text := GMS_UTILITY.EXPAND_SQL_TEXT_C_FUNC(input_sql_text);
END;

/*
* ----------------------------
* -- GET_CPU_TIME
* ----------------------------
*/
CREATE OR REPLACE FUNCTION GMS_UTILITY.GET_CPU_TIME
RETURNS NUMBER
AS 'MODULE_PATHNAME','gms_get_cpu_time'
LANGUAGE C VOLATILE NOT FENCED;

/*
* ----------------------------
* -- GET_ENDIANNESS
* ----------------------------
*/
CREATE OR REPLACE FUNCTION GMS_UTILITY.GET_ENDIANNESS
RETURNS integer
AS 'MODULE_PATHNAME','gms_get_endianness'
LANGUAGE C VOLATILE NOT FENCED;

/*
* ----------------------------
* -- GET_SQL_HASH
* ----------------------------
*/
CREATE OR REPLACE FUNCTION GMS_UTILITY.GET_SQL_HASH_C_FUNC (
    name        IN  VARCHAR2,
    hash        OUT RAW,
    pre10ihash  OUT NUMBER
)
AS 'MODULE_PATHNAME','gms_get_sql_hash'
LANGUAGE C VOLATILE NOT FENCED;

CREATE OR REPLACE PROCEDURE GMS_UTILITY.GET_SQL_HASH (
    name        IN  VARCHAR2,
    hash        OUT RAW,
    last4byte   OUT NUMBER
)
AS
    TYPE RET IS RECORD(hash RAW, last4byte NUMBER);
    rec RET;
BEGIN
    rec := GMS_UTILITY.GET_SQL_HASH_C_FUNC(name);
    hash := rec.hash;
    last4byte := rec.last4byte;
END;

/*
* ----------------------------
* -- NAME_TOKENIZE
* ----------------------------
*/
CREATE OR REPLACE FUNCTION GMS_UTILITY.NAME_TOKENIZE_C_FUNC (
    name        IN  VARCHAR2,
    a           OUT VARCHAR2,
    b           OUT VARCHAR2,
    c           OUT VARCHAR2,
    dblink      OUT VARCHAR2,
    nextpos     OUT BINARY_INTEGER
)
AS 'MODULE_PATHNAME','gms_name_tokenize'
LANGUAGE C VOLATILE NOT FENCED;

CREATE OR REPLACE PROCEDURE GMS_UTILITY.NAME_TOKENIZE (
    name        IN  VARCHAR2,
    a           OUT VARCHAR2,
    b           OUT VARCHAR2,
    c           OUT VARCHAR2,
    dblink      OUT VARCHAR2,
    nextpos     OUT BINARY_INTEGER
)
AS
    TYPE RET IS RECORD(a VARCHAR2, b VARCHAR2, c VARCHAR2, dblink VARCHAR2, nextpos INT);
    rec RET;
BEGIN
    rec := GMS_UTILITY.NAME_TOKENIZE_C_FUNC(name);
    a := rec.a;
    b := rec.b;
    c := rec.c;
    dblink := rec.dblink;
    nextpos := rec.nextpos;
END;

/*
* ----------------------------
* -- NAME_RESOLVE
* ----------------------------
*/
CREATE OR REPLACE FUNCTION GMS_UTILITY.NAME_RESOLVE_C_FUNC (
    name            IN  VARCHAR2,
    context         IN  NUMBER,
    schema          OUT VARCHAR2,
    part1           OUT VARCHAR2,
    part2           OUT VARCHAR2,
    dblink          OUT VARCHAR2,
    part1_type      OUT NUMBER,
    object_number   OUT NUMBER
)
AS 'MODULE_PATHNAME','gms_name_resolve'
LANGUAGE C VOLATILE NOT FENCED;

CREATE OR REPLACE PROCEDURE GMS_UTILITY.NAME_RESOLVE (
    name            IN  VARCHAR2,
    context         IN  NUMBER,
    schema          OUT VARCHAR2,
    part1           OUT VARCHAR2,
    part2           OUT VARCHAR2,
    dblink          OUT VARCHAR2,
    part1_type      OUT NUMBER,
    object_number   OUT NUMBER
)
AS
    TYPE RET IS RECORD(s VARCHAR2, p1 VARCHAR2, p2 VARCHAR2, dblink VARCHAR2, p1_type NUMBER, obj_num NUMBER);
    rec RET;
BEGIN
    rec := GMS_UTILITY.NAME_RESOLVE_C_FUNC(name, context);
    schema := rec.s;
    part1 := rec.p1;
    part2 := rec.p2;
    dblink := rec.dblink;
    part1_type := rec.p1_type;
    object_number := rec.obj_num;
END;

/*
* ----------------------------
* -- IS_BIT_SET
* ----------------------------
*/
CREATE OR REPLACE FUNCTION GMS_UTILITY.IS_BIT_SET (
    r   IN RAW,
    n   IN NUMBER
) RETURNS NUMBER
AS 'MODULE_PATHNAME','gms_is_bit_set'
LANGUAGE C VOLATILE NOT FENCED;

/*
* ----------------------------
* -- IS_CLUSTER_DATABASE
* ----------------------------
*/
CREATE OR REPLACE FUNCTION GMS_UTILITY.IS_CLUSTER_DATABASE ()
RETURNS BOOLEAN
AS $$ SELECT FALSE::BOOLEAN $$
LANGUAGE SQL IMMUTABLE STRICT;

/*
* ----------------------------
* -- OLD_CURRENT_SCHEMA
* ----------------------------
*/
CREATE OR REPLACE FUNCTION GMS_UTILITY.OLD_CURRENT_SCHEMA ()
RETURNS VARCHAR2
AS 'MODULE_PATHNAME','gms_old_current_schema'
LANGUAGE C VOLATILE NOT FENCED;

/*
* ----------------------------
* -- OLD_CURRENT_USER
* ----------------------------
*/
CREATE OR REPLACE FUNCTION GMS_UTILITY.OLD_CURRENT_USER ()
RETURNS VARCHAR2
AS $$ SELECT CURRENT_USER::VARCHAR2 $$
LANGUAGE SQL IMMUTABLE STRICT;