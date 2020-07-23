/*
 * psql - the PostgreSQL interactive terminal
 *
 * Copyright (c) 2000-2012, PostgreSQL Global Development Group
 *
 * src/bin/psql/tab-complete.c
 */

/* ----------------------------------------------------------------------
 * This file implements a somewhat more sophisticated readline "TAB
 * completion" in psql. It is not intended to be AI, to replace
 * learning SQL, or to relieve you from thinking about what you're
 * doing. Also it does not always give you all the syntactically legal
 * completions, only those that are the most common or the ones that
 * the programmer felt most like implementing.
 *
 * CAVEAT: Tab completion causes queries to be sent to the backend.
 * The number of tuples returned gets limited, in most default
 * installations to 1000, but if you still don't like this prospect,
 * you can turn off tab completion in your ~/.inputrc (or else
 * ${INPUTRC}) file so:
 *
 *	 $if psql
 *	 set disable-completion on
 *	 $endif
 *
 * See `man 3 readline' or `info readline' for the full details. Also,
 * hence the
 *
 * BUGS:
 *
 * - If you split your queries across lines, this whole thing gets
 *	 confused. (To fix this, one would have to read psql's query
 *	 buffer rather than readline's line buffer, which would require
 *	 some major revisions of things.)
 *
 * - Table or attribute names with spaces in it may confuse it.
 *
 * - Quotes, parenthesis, and other funny characters are not handled
 *	 all that gracefully.
 * ----------------------------------------------------------------------
 */
#include "settings.h"
#include "postgres_fe.h"
#include "tab-complete.h"
#include "input.h"

/* If we don't have this, we might as well forget about the whole thing: */
#ifdef USE_READLINE

#include <ctype.h>
#include "libpq/libpq-fe.h"
#include "libpq/pqexpbuffer.h"
#include "common.h"
#include "stringutils.h"

#ifndef WIN32
#include "libpq/libpq-int.h"
#endif

#define filename_completion_function rl_filename_completion_function

#define completion_matches rl_completion_matches

/* word break characters */
#define WORD_BREAKS "\t\n@$><=;|&{() "

/*
 * This struct is used to define "schema queries", which are custom-built
 * to obtain possibly-schema-qualified names of database objects.  There is
 * enough similarity in the structure that we don't want to repeat it each
 * time.  So we put the components of each query into this struct and
 * assemble them with the common boilerplate in _complete_from_query().
 */
typedef struct SchemaQuery {
    /*
     * Name of catalog or catalogs to be queried, with alias, eg.
     * "pg_catalog.pg_class c".  Note that "pg_namespace n" will be added.
     */
    const char* catname;

    /*
     * Selection condition --- only rows meeting this condition are candidates
     * to display.	If catname mentions multiple tables, include the necessary
     * join condition here.  For example, "c.relkind = 'r'". Write NULL (not
     * an empty string) if not needed.
     */
    const char* selcondition;

    /*
     * Visibility condition --- which rows are visible without schema
     * qualification?  For example, "pg_catalog.pg_table_is_visible(c.oid)".
     */
    const char* viscondition;

    /*
     * Namespace --- name of field to join to pg_namespace.oid. For example,
     * "c.relnamespace".
     */
    const char* nameSpace;

    /*
     * Result --- the appropriately-quoted name to return, in the case of an
     * unqualified name.  For example, "pg_catalog.quote_ident(c.relname)".
     */
    const char* result;

    /*
     * In some cases a different result must be used for qualified names.
     * Enter that here, or write NULL if result can be used.
     */
    const char* qualresult;
} SchemaQuery;

/* Store maximum number of records we want from database queries
 * (implemented via SELECT ... LIMIT xx).
 */
static int completion_max_records;

/*
 * Communication variables set by COMPLETE_WITH_FOO macros and then used by
 * the completion callback functions.  Ugly but there is no better way.
 */
static const char* const* completion_charpp; /* to pass a list of strings */
#ifdef NOT_USED
static const char* completion_charp;         /* to pass a string */
static const char* completion_info_charp;    /* to pass a second string */
static const char* completion_info_charp2;   /* to pass a third string */
static const SchemaQuery* completion_squery; /* to pass a SchemaQuery */
#endif
static bool completion_case_sensitive;       /* completion is case sensitive */

/*
 * A few macros to ease typing. You can use these to complete the given
 * string with
 * 1) The results from a query you pass it. (Perhaps one of those below?)
 * 2) The results from a schema query you pass it.
 * 3) The items from a null-pointer-terminated list.
 * 4) A string constant.
 * 5) The list of attributes of the given table (possibly schema-qualified).
 */
#define COMPLETE_WITH_QUERY(query)                               \
    do {                                                         \
        completion_charp = query;                                \
        matches = completion_matches(text, complete_from_query); \
    } while (0)

#define COMPLETE_WITH_SCHEMA_QUERY(query, addon)                        \
    do {                                                                \
        completion_squery = &(query);                                   \
        completion_charp = addon;                                       \
        matches = completion_matches(text, complete_from_schema_query); \
    } while (0)

#define COMPLETE_WITH_LIST_CS(list)                             \
    do {                                                        \
        completion_charpp = list;                               \
        completion_case_sensitive = true;                       \
        matches = completion_matches(text, complete_from_list); \
    } while (0)

#define COMPLETE_WITH_LIST(list)                                \
    do {                                                        \
        completion_charpp = list;                               \
        completion_case_sensitive = false;                      \
        matches = completion_matches(text, complete_from_list); \
    } while (0)

#define COMPLETE_WITH_CONST(string)                              \
    do {                                                         \
        completion_charp = string;                               \
        completion_case_sensitive = false;                       \
        matches = completion_matches(text, complete_from_const); \
    } while (0)

#define COMPLETE_WITH_ATTR(relation, addon)                                                           \
    do {                                                                                              \
        char* _completion_schema;                                                                     \
        char* _completion_table;                                                                      \
        _completion_schema = strtokx(relation, " \t\n\r", ".", "\"", 0, false, false, pset.encoding); \
        (void)strtokx(NULL, " \t\n\r", ".", "\"", 0, false, false, pset.encoding);                    \
        _completion_table = strtokx(NULL, " \t\n\r", ".", "\"", 0, false, false, pset.encoding);      \
        if (_completion_table == NULL) {                                                              \
            completion_charp = Query_for_list_of_attributes addon;                                    \
            completion_info_charp = relation;                                                         \
        } else {                                                                                      \
            completion_charp = Query_for_list_of_attributes_with_schema addon;                        \
            completion_info_charp = _completion_table;                                                \
            completion_info_charp2 = _completion_schema;                                              \
        }                                                                                             \
        matches = completion_matches(text, complete_from_query);                                      \
    } while (0)

/*
 * Assembly instructions for schema queries
 */
static const SchemaQuery Query_for_list_of_aggregates = {
    /* catname */
    "pg_catalog.pg_proc p",
    /* selcondition */
    "p.prokind",
    /* viscondition */
    "pg_catalog.pg_function_is_visible(p.oid)",
    /* namespace */
    "p.pronamespace",
    /* result */
    "pg_catalog.quote_ident(p.proname)",
    /* qualresult */
    NULL};

static const SchemaQuery Query_for_list_of_datatypes = {
    /* catname */
    "pg_catalog.pg_type t",
    /* selcondition --- ignore table rowtypes and array types */
    "(t.typrelid = 0 "
    " OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid)) "
    "AND t.typname !~ '^_'",
    /* viscondition */
    "pg_catalog.pg_type_is_visible(t.oid)",
    /* namespace */
    "t.typnamespace",
    /* result */
    "pg_catalog.format_type(t.oid, NULL)",
    /* qualresult */
    "pg_catalog.quote_ident(t.typname)"};

static const SchemaQuery Query_for_list_of_domains = {
    /* catname */
    "pg_catalog.pg_type t",
    /* selcondition */
    "t.typtype = 'd'",
    /* viscondition */
    "pg_catalog.pg_type_is_visible(t.oid)",
    /* namespace */
    "t.typnamespace",
    /* result */
    "pg_catalog.quote_ident(t.typname)",
    /* qualresult */
    NULL};

static const SchemaQuery Query_for_list_of_functions = {
    /* catname */
    "pg_catalog.pg_proc p",
    /* selcondition */
    NULL,
    /* viscondition */
    "pg_catalog.pg_function_is_visible(p.oid)",
    /* namespace */
    "p.pronamespace",
    /* result */
    "pg_catalog.quote_ident(p.proname)",
    /* qualresult */
    NULL};

static const SchemaQuery Query_for_list_of_indexes = {
    /* catname */
    "pg_catalog.pg_class c",
    /* selcondition */
    "c.relkind IN ('i')",
    /* viscondition */
    "pg_catalog.pg_table_is_visible(c.oid)",
    /* namespace */
    "c.relnamespace",
    /* result */
    "pg_catalog.quote_ident(c.relname)",
    /* qualresult */
    NULL};

static const SchemaQuery Query_for_list_of_sequences = {
    /* catname */
    "pg_catalog.pg_class c",
    /* selcondition */
    "c.relkind IN ('S')",
    /* viscondition */
    "pg_catalog.pg_table_is_visible(c.oid)",
    /* namespace */
    "c.relnamespace",
    /* result */
    "pg_catalog.quote_ident(c.relname)",
    /* qualresult */
    NULL};

static const SchemaQuery Query_for_list_of_foreign_tables = {
    /* catname */
    "pg_catalog.pg_class c",
    /* selcondition */
    "c.relkind IN ('f')",
    /* viscondition */
    "pg_catalog.pg_table_is_visible(c.oid)",
    /* namespace */
    "c.relnamespace",
    /* result */
    "pg_catalog.quote_ident(c.relname)",
    /* qualresult */
    NULL};

static const SchemaQuery Query_for_list_of_tables = {
    /* catname */
    "pg_catalog.pg_class c",
    /* selcondition */
    "c.relkind IN ('r')",
    /* viscondition */
    "pg_catalog.pg_table_is_visible(c.oid)",
    /* namespace */
    "c.relnamespace",
    /* result */
    "pg_catalog.quote_ident(c.relname)",
    /* qualresult */
    NULL};

/* The bit masks for the following three functions come from
 * src/include/catalog/pg_trigger.h.
 */
static const SchemaQuery Query_for_list_of_insertables = {
    /* catname */
    "pg_catalog.pg_class c",
    /* selcondition */
    "(c.relkind = 'r' OR (c.relkind = 'v' AND c.relhastriggers AND EXISTS "
    "(SELECT 1 FROM pg_catalog.pg_trigger t WHERE t.tgrelid = c.oid AND t.tgtype & (1 << 2) <> 0)))",
    /* viscondition */
    "pg_catalog.pg_table_is_visible(c.oid)",
    /* namespace */
    "c.relnamespace",
    /* result */
    "pg_catalog.quote_ident(c.relname)",
    /* qualresult */
    NULL};

static const SchemaQuery Query_for_list_of_deletables = {
    /* catname */
    "pg_catalog.pg_class c",
    /* selcondition */
    "(c.relkind = 'r' OR (c.relkind = 'v' AND c.relhastriggers AND EXISTS "
    "(SELECT 1 FROM pg_catalog.pg_trigger t WHERE t.tgrelid = c.oid AND t.tgtype & (1 << 3) <> 0)))",
    /* viscondition */
    "pg_catalog.pg_table_is_visible(c.oid)",
    /* namespace */
    "c.relnamespace",
    /* result */
    "pg_catalog.quote_ident(c.relname)",
    /* qualresult */
    NULL};

static const SchemaQuery Query_for_list_of_updatables = {
    /* catname */
    "pg_catalog.pg_class c",
    /* selcondition */
    "(c.relkind = 'r' OR (c.relkind = 'v' AND c.relhastriggers AND EXISTS "
    "(SELECT 1 FROM pg_catalog.pg_trigger t WHERE t.tgrelid = c.oid AND t.tgtype & (1 << 4) <> 0)))",
    /* viscondition */
    "pg_catalog.pg_table_is_visible(c.oid)",
    /* namespace */
    "c.relnamespace",
    /* result */
    "pg_catalog.quote_ident(c.relname)",
    /* qualresult */
    NULL};

static const SchemaQuery Query_for_list_of_relations = {
    /* catname */
    "pg_catalog.pg_class c",
    /* selcondition */
    NULL,
    /* viscondition */
    "pg_catalog.pg_table_is_visible(c.oid)",
    /* namespace */
    "c.relnamespace",
    /* result */
    "pg_catalog.quote_ident(c.relname)",
    /* qualresult */
    NULL};

static const SchemaQuery Query_for_list_of_tsvf = {
    /* catname */
    "pg_catalog.pg_class c",
    /* selcondition */
    "c.relkind IN ('r', 'S', 'v', 'f')",
    /* viscondition */
    "pg_catalog.pg_table_is_visible(c.oid)",
    /* namespace */
    "c.relnamespace",
    /* result */
    "pg_catalog.quote_ident(c.relname)",
    /* qualresult */
    NULL};

static const SchemaQuery Query_for_list_of_tf = {
    /* catname */
    "pg_catalog.pg_class c",
    /* selcondition */
    "c.relkind IN ('r', 'f')",
    /* viscondition */
    "pg_catalog.pg_table_is_visible(c.oid)",
    /* namespace */
    "c.relnamespace",
    /* result */
    "pg_catalog.quote_ident(c.relname)",
    /* qualresult */
    NULL};

static const SchemaQuery Query_for_list_of_views = {
    /* catname */
    "pg_catalog.pg_class c",
    /* selcondition */
    "c.relkind IN ('v')",
    /* viscondition */
    "pg_catalog.pg_table_is_visible(c.oid)",
    /* namespace */
    "c.relnamespace",
    /* result */
    "pg_catalog.quote_ident(c.relname)",
    /* qualresult */
    NULL};

/*
 * Queries to get lists of names of various kinds of things, possibly
 * restricted to names matching a partially entered name.  In these queries,
 * the first %s will be replaced by the text entered so far (suitably escaped
 * to become a SQL literal string).  %d will be replaced by the length of the
 * string (in unescaped form).	A second and third %s, if present, will be
 * replaced by a suitably-escaped version of the string provided in
 * completion_info_charp.  A fourth and fifth %s are similarly replaced by
 * completion_info_charp2.
 *
 * Beware that the allowed sequences of %s and %d are determined by
 * _complete_from_query().
 */
#define Query_for_list_of_attributes                               \
    "SELECT pg_catalog.quote_ident(attname) "                      \
    "  FROM pg_catalog.pg_attribute a, pg_catalog.pg_class c "     \
    " WHERE c.oid = a.attrelid "                                   \
    "   AND a.attnum > 0 "                                         \
    "   AND NOT a.attisdropped "                                   \
    "   AND substring(pg_catalog.quote_ident(attname),1,%d)='%s' " \
    "   AND (pg_catalog.quote_ident(relname)='%s' "                \
    "        OR '\"' || relname || '\"'='%s') "                    \
    "   AND pg_catalog.pg_table_is_visible(c.oid)"

#define Query_for_list_of_attributes_with_schema                                          \
    "SELECT pg_catalog.quote_ident(attname) "                                             \
    "  FROM pg_catalog.pg_attribute a, pg_catalog.pg_class c, pg_catalog.pg_namespace n " \
    " WHERE c.oid = a.attrelid "                                                          \
    "   AND n.oid = c.relnamespace "                                                      \
    "   AND a.attnum > 0 "                                                                \
    "   AND NOT a.attisdropped "                                                          \
    "   AND substring(pg_catalog.quote_ident(attname),1,%d)='%s' "                        \
    "   AND (pg_catalog.quote_ident(relname)='%s' "                                       \
    "        OR '\"' || relname || '\"' ='%s') "                                          \
    "   AND (pg_catalog.quote_ident(nspname)='%s' "                                       \
    "        OR '\"' || nspname || '\"' ='%s') "

#define Query_for_list_of_template_databases                              \
    "SELECT pg_catalog.quote_ident(datname) FROM pg_catalog.pg_database " \
    " WHERE substring(pg_catalog.quote_ident(datname),1,%d)='%s' AND datistemplate"

#define Query_for_list_of_databases                                       \
    "SELECT pg_catalog.quote_ident(datname) FROM pg_catalog.pg_database " \
    " WHERE substring(pg_catalog.quote_ident(datname),1,%d)='%s'"

#define Query_for_list_of_tablespaces                                       \
    "SELECT pg_catalog.quote_ident(spcname) FROM pg_catalog.pg_tablespace " \
    " WHERE substring(pg_catalog.quote_ident(spcname),1,%d)='%s'"

#define Query_for_list_of_encodings                                    \
    " SELECT DISTINCT pg_catalog.pg_encoding_to_char(conforencoding) " \
    "   FROM pg_catalog.pg_conversion "                                \
    "  WHERE substring(pg_catalog.pg_encoding_to_char(conforencoding),1,%d)=UPPER('%s')"

#define Query_for_list_of_languages           \
    "SELECT pg_catalog.quote_ident(lanname) " \
    "  FROM pg_catalog.pg_language "          \
    " WHERE lanname != 'internal' "           \
    "   AND substring(pg_catalog.quote_ident(lanname),1,%d)='%s'"

#define Query_for_list_of_schemas                                          \
    "SELECT pg_catalog.quote_ident(nspname) FROM pg_catalog.pg_namespace " \
    " WHERE substring(pg_catalog.quote_ident(nspname),1,%d)='%s'"

#define Query_for_list_of_set_vars                                         \
    "SELECT name FROM "                                                    \
    " (SELECT pg_catalog.lower(name) AS name FROM pg_catalog.pg_settings " \
    "  WHERE context IN ('user', 'superuser') "                            \
    "  UNION ALL SELECT 'constraints' "                                    \
    "  UNION ALL SELECT 'transaction' "                                    \
    "  UNION ALL SELECT 'session' "                                        \
    "  UNION ALL SELECT 'role' "                                           \
    "  UNION ALL SELECT 'tablespace' "                                     \
    "  UNION ALL SELECT 'all') ss "                                        \
    " WHERE substring(name,1,%d)='%s'"

#define Query_for_list_of_show_vars                                        \
    "SELECT name FROM "                                                    \
    " (SELECT pg_catalog.lower(name) AS name FROM pg_catalog.pg_settings " \
    "  UNION ALL SELECT 'session authorization' "                          \
    "  UNION ALL SELECT 'all') ss "                                        \
    " WHERE substring(name,1,%d)='%s'"

#define Query_for_list_of_roles                \
    " SELECT pg_catalog.quote_ident(rolname) " \
    "   FROM pg_catalog.pg_roles "             \
    "  WHERE substring(pg_catalog.quote_ident(rolname),1,%d)='%s'"

#define Query_for_list_of_grant_roles                              \
    " SELECT pg_catalog.quote_ident(rolname) "                     \
    "   FROM pg_catalog.pg_roles "                                 \
    "  WHERE substring(pg_catalog.quote_ident(rolname),1,%d)='%s'" \
    " UNION ALL SELECT 'PUBLIC'"

/* the silly-looking length condition is just to eat up the current word */
#define Query_for_table_owning_index                                               \
    "SELECT pg_catalog.quote_ident(c1.relname) "                                   \
    "  FROM pg_catalog.pg_class c1, pg_catalog.pg_class c2, pg_catalog.pg_index i" \
    " WHERE c1.oid=i.indrelid and i.indexrelid=c2.oid"                             \
    "       and (%d = pg_catalog.length('%s'))"                                    \
    "       and pg_catalog.quote_ident(c2.relname)='%s'"                           \
    "       and pg_catalog.pg_table_is_visible(c2.oid)"

/* the silly-looking length condition is just to eat up the current word */
#define Query_for_index_of_table                                                   \
    "SELECT pg_catalog.quote_ident(c2.relname) "                                   \
    "  FROM pg_catalog.pg_class c1, pg_catalog.pg_class c2, pg_catalog.pg_index i" \
    " WHERE c1.oid=i.indrelid and i.indexrelid=c2.oid"                             \
    "       and (%d = pg_catalog.length('%s'))"                                    \
    "       and pg_catalog.quote_ident(c1.relname)='%s'"                           \
    "       and pg_catalog.pg_table_is_visible(c2.oid)"

/* the silly-looking length condition is just to eat up the current word */
#define Query_for_list_of_tables_for_trigger             \
    "SELECT pg_catalog.quote_ident(relname) "            \
    "  FROM pg_catalog.pg_class"                         \
    " WHERE (%d = pg_catalog.length('%s'))"              \
    "   AND oid IN "                                     \
    "       (SELECT tgrelid FROM pg_catalog.pg_trigger " \
    "         WHERE pg_catalog.quote_ident(tgname)='%s')"

#define Query_for_list_of_ts_configurations                                \
    "SELECT pg_catalog.quote_ident(cfgname) FROM pg_catalog.pg_ts_config " \
    " WHERE substring(pg_catalog.quote_ident(cfgname),1,%d)='%s'"

#define Query_for_list_of_ts_dictionaries                                 \
    "SELECT pg_catalog.quote_ident(dictname) FROM pg_catalog.pg_ts_dict " \
    " WHERE substring(pg_catalog.quote_ident(dictname),1,%d)='%s'"

#define Query_for_list_of_ts_parsers                                       \
    "SELECT pg_catalog.quote_ident(prsname) FROM pg_catalog.pg_ts_parser " \
    " WHERE substring(pg_catalog.quote_ident(prsname),1,%d)='%s'"

#define Query_for_list_of_ts_templates                                        \
    "SELECT pg_catalog.quote_ident(tmplname) FROM pg_catalog.pg_ts_template " \
    " WHERE substring(pg_catalog.quote_ident(tmplname),1,%d)='%s'"

#define Query_for_list_of_fdws                    \
    " SELECT pg_catalog.quote_ident(fdwname) "    \
    "   FROM pg_catalog.pg_foreign_data_wrapper " \
    "  WHERE substring(pg_catalog.quote_ident(fdwname),1,%d)='%s'"

#define Query_for_list_of_servers              \
    " SELECT pg_catalog.quote_ident(srvname) " \
    "   FROM pg_catalog.pg_foreign_server "    \
    "  WHERE substring(pg_catalog.quote_ident(srvname),1,%d)='%s'"

#define Query_for_list_of_user_mappings        \
    " SELECT pg_catalog.quote_ident(usename) " \
    "   FROM pg_catalog.pg_user_mappings "     \
    "  WHERE substring(pg_catalog.quote_ident(usename),1,%d)='%s'"

#define Query_for_list_of_access_methods      \
    " SELECT pg_catalog.quote_ident(amname) " \
    "   FROM pg_catalog.pg_am "               \
    "  WHERE substring(pg_catalog.quote_ident(amname),1,%d)='%s'"

#define Query_for_list_of_arguments                        \
    " SELECT pg_catalog.oidvectortypes(proargtypes)||')' " \
    "   FROM pg_catalog.pg_proc "                          \
    "  WHERE proname='%s'"

#define Query_for_list_of_extensions           \
    " SELECT pg_catalog.quote_ident(extname) " \
    "   FROM pg_catalog.pg_extension "         \
    "  WHERE substring(pg_catalog.quote_ident(extname),1,%d)='%s'"

#define Query_for_list_of_available_extensions    \
    " SELECT pg_catalog.quote_ident(name) "       \
    "   FROM pg_catalog.pg_available_extensions " \
    "  WHERE substring(pg_catalog.quote_ident(name),1,%d)='%s' AND installed_version IS NULL"

#define Query_for_list_of_prepared_statements    \
    " SELECT pg_catalog.quote_ident(name) "      \
    "   FROM pg_catalog.pg_prepared_statements " \
    "  WHERE substring(pg_catalog.quote_ident(name),1,%d)='%s'"

#ifdef PGXC
#define Query_for_list_of_available_nodenames \
    " SELECT NODE_NAME "                      \
    "  FROM PGXC_NODE"
#define Query_for_list_of_available_coordinators \
    " SELECT NODE_NAME "                         \
    "  FROM PGXC_NODE"                           \
    "   WHERE NODE_TYPE = 'C'"
#define Query_for_list_of_available_datanodes \
    " SELECT NODE_NAME "                      \
    "  FROM PGXC_NODE"                        \
    "   WHERE NODE_TYPE = 'D'"
#define Query_for_list_of_available_nodegroup_names \
    " SELECT GROUP_NAME "                           \
    "  FROM PGXC_GROUP"
#endif

/*
 * This is a list of all "things" in Pgsql, which can show up after CREATE or
 * DROP; and there is also a query to get a list of them.
 */
typedef struct {
    const char* name;
    const char* query;         /* simple query, or NULL */
    const SchemaQuery* squery; /* schema query, or NULL */
    const bits32 flags;        /* visibility flags, see below */
} pgsql_thing_t;

#define THING_NO_CREATE (1 << 0) /* should not show up after CREATE */
#define THING_NO_DROP (1 << 1)   /* should not show up after DROP */
#define THING_NO_SHOW (THING_NO_CREATE | THING_NO_DROP)

static const pgsql_thing_t words_after_create[] = {
    {"AGGREGATE", NULL, &Query_for_list_of_aggregates, 0},
#ifdef PGXC
    {"BARRIER", NULL, NULL, 0}, /* Comes barrier name next, so skip it */
#endif
    {"CAST", NULL, NULL, 0}, /* Casts have complex structures for names, so
    * skip it */
    {"COLLATION",
        "SELECT pg_catalog.quote_ident(collname) FROM pg_catalog.pg_collation WHERE collencoding IN (-1, "
        "pg_catalog.pg_char_to_encoding(pg_catalog.getdatabaseencoding())) AND "
        "substring(pg_catalog.quote_ident(collname),1,%d)='%s'",
        NULL,
        0},

    /*
     * CREATE CONSTRAINT TRIGGER is not supported here because it is designed
     * to be used only by pg_dump.
     */
    {"CONFIGURATION", Query_for_list_of_ts_configurations, NULL, THING_NO_SHOW},
    {"CONVERSION",
        "SELECT pg_catalog.quote_ident(conname) FROM pg_catalog.pg_conversion WHERE "
        "substring(pg_catalog.quote_ident(conname),1,%d)='%s'",
        NULL,
        0},
    {"DATABASE", Query_for_list_of_databases, NULL, 0},
    {"DICTIONARY", Query_for_list_of_ts_dictionaries, NULL, THING_NO_SHOW},
    {"DOMAIN", NULL, &Query_for_list_of_domains, 0},
    {"EXTENSION", Query_for_list_of_extensions, NULL, 0},
#ifndef PGXC
    {"FOREIGN DATA WRAPPER", NULL, NULL, 0},
    {"FOREIGN TABLE", NULL, NULL, 0},
#endif
    {"FUNCTION", NULL, &Query_for_list_of_functions, 0},
    {"GROUP", Query_for_list_of_roles, NULL, 0},
    {"LANGUAGE", Query_for_list_of_languages, NULL, 0},
    {"INDEX", NULL, &Query_for_list_of_indexes, 0},
#ifdef PGXC
    {"NODE", Query_for_list_of_available_nodenames, NULL, 0},
    {"NODE GROUP", Query_for_list_of_available_nodegroup_names, NULL, 0},
#endif
    {"OPERATOR", NULL, NULL, 0},            /* Querying for this is probably not such a
    * good idea. */
    {"OWNED", NULL, NULL, THING_NO_CREATE}, /* for DROP OWNED BY ... */
    {"PARSER", Query_for_list_of_ts_parsers, NULL, THING_NO_SHOW},
    {"ROLE", Query_for_list_of_roles, NULL, 0},
    {"RULE",
        "SELECT pg_catalog.quote_ident(rulename) FROM pg_catalog.pg_rules WHERE "
        "substring(pg_catalog.quote_ident(rulename),1,%d)='%s'",
        NULL,
        0},
    {"SCHEMA", Query_for_list_of_schemas, NULL, 0},
    {"SEQUENCE", NULL, &Query_for_list_of_sequences, 0},
#ifndef PGXC
    {"SERVER", Query_for_list_of_servers, NULL, 0},
#endif
    {"TABLE", NULL, &Query_for_list_of_tables, 0},
    {"TABLESPACE", Query_for_list_of_tablespaces, NULL, 0},
    {"TEMP", NULL, NULL, THING_NO_DROP}, /* for CREATE TEMP TABLE ... */
    {"TEMPLATE", Query_for_list_of_ts_templates, NULL, THING_NO_SHOW},
    {"TEXT SEARCH", NULL, NULL, 0},
    {"TRIGGER",
        "SELECT pg_catalog.quote_ident(tgname) FROM pg_catalog.pg_trigger WHERE "
        "substring(pg_catalog.quote_ident(tgname),1,%d)='%s'",
        NULL,
        0},
    {"TYPE", NULL, &Query_for_list_of_datatypes, 0},
    {"UNIQUE", NULL, NULL, THING_NO_DROP},   /* for CREATE UNIQUE INDEX ... */
    {"UNLOGGED", NULL, NULL, THING_NO_DROP}, /* for CREATE UNLOGGED TABLE
    * ... */
    {"USER", Query_for_list_of_roles, NULL, 0},
#ifndef PGXC
    {"USER MAPPING FOR", NULL, NULL, 0},
#endif
    {"VIEW", NULL, &Query_for_list_of_views, 0},
    {NULL, NULL, NULL, 0} /* end of list */
};

#ifdef NOT_USED
/* Forward declaration of functions */
/*psql_completion is deleted because it's too complex and not be used at all. */
static char* create_command_generator(const char* text, int state);
static char* drop_command_generator(const char* text, int state);
static char* complete_from_query(const char* text, int state);
static char* complete_from_schema_query(const char* text, int state);
static char* _complete_from_query(int is_schema_query, const char* text, int state);
static char* complete_from_list(const char* text, int state);
static char* complete_from_const(const char* text, int state);
static char** complete_from_variables(char* text, const char* prefix, const char* suffix);
static char* complete_from_files(const char* text, int state);
#endif

static char* pg_strdup_keyword_case(const char* s, const char* ref);

#ifdef NOT_USED
static PGresult* exec_query(const char* query);
static void get_previous_words(int point, char** previous_words, int nwords);

static char* quote_file_name(char* text, int match_type, char* quote_pointer);
static char* dequote_file_name(char* text, char quote_char);
#endif

/*
 * Initialize the readline library for our purposes.
 */
void initialize_readline(void)
{
    rl_readline_name = (char*)pset.progname;

    /* psql_completion is deleted because it's too complex and not be used at all. */
    rl_attempted_completion_function = NULL;

    rl_basic_word_break_characters = WORD_BREAKS;

    completion_max_records = 1000;

    /*
     * There is a variable rl_completion_query_items for this but apparently
     * it's not defined everywhere.
     */
}

#ifdef NOT_USED
/*
 * GENERATOR FUNCTIONS
 *
 * These functions do all the actual work of completing the input. They get
 * passed the text so far and the count how many times they have been called
 * so far with the same text.
 * If you read the above carefully, you'll see that these don't get called
 * directly but through the readline interface.
 * The return value is expected to be the full completion of the text, going
 * through a list each time, or NULL if there are no more matches. The string
 * will be free()'d by readline, so you must run it through strdup() or
 * something of that sort.
 */
/*
 * Common routine for create_command_generator and drop_command_generator.
 * Entries that have 'excluded' flags are not returned.
 */
static char* create_or_drop_command_generator(const char* text, int state, bits32 excluded)
{
    static int list_index, string_length;
    const char* name = NULL;

    /* If this is the first time for this completion, init some values */
    if (state == 0) {
        list_index = 0;
        string_length = strlen(text);
    }

    /* find something that matches */
    while ((name = words_after_create[list_index++].name)) {
        if ((pg_strncasecmp(name, text, string_length) == 0) && !(words_after_create[list_index - 1].flags & excluded))
            return pg_strdup_keyword_case(name, text);
    }
    /* if nothing matches, return NULL */
    return NULL;
}

/*
 * This one gives you one from a list of things you can put after CREATE
 * as defined above.
 */
static char* create_command_generator(const char* text, int state)
{
    return create_or_drop_command_generator(text, state, THING_NO_CREATE);
}

/*
 * This function gives you a list of things you can put after a DROP command.
 */
static char* drop_command_generator(const char* text, int state)
{
    return create_or_drop_command_generator(text, state, THING_NO_DROP);
}

/* The following two functions are wrappers for _complete_from_query */
static char* complete_from_query(const char* text, int state)
{
    return _complete_from_query(0, text, state);
}

static char* complete_from_schema_query(const char* text, int state)
{
    return _complete_from_query(1, text, state);
}

/*
 * This creates a list of matching things, according to a query pointed to
 * by completion_charp.
 * The query can be one of two kinds:
 *
 * 1. A simple query which must contain a %d and a %s, which will be replaced
 * by the string length of the text and the text itself. The query may also
 * have up to four more %s in it; the first two such will be replaced by the
 * value of completion_info_charp, the next two by the value of
 * completion_info_charp2.
 *
 * 2. A schema query used for completion of both schema and relation names.
 * These are more complex and must contain in the following order:
 * %d %s %d %s %d %s %s %d %s
 * where %d is the string length of the text and %s the text itself.
 *
 * It is assumed that strings should be escaped to become SQL literals
 * (that is, what is in the query is actually ... '%s' ...)
 *
 * See top of file for examples of both kinds of query.
 */
static char* _complete_from_query(int is_schema_query, const char* text, int state)
{
    static int list_index, string_length;
    static PGresult* result = NULL;

    /*
     * If this is the first time for this completion, we fetch a list of our
     * "things" from the backend.
     */
    if (state == 0) {
        PQExpBufferData query_buffer;
        char* e_text = NULL;
        char* e_info_charp = NULL;
        char* e_info_charp2 = NULL;

        list_index = 0;
        string_length = strlen(text);

        /* Free any prior result */
        PQclear(result);
        result = NULL;

        /* Set up suitably-escaped copies of textual inputs */
        e_text = (char*)pg_malloc(string_length * 2 + 1);
        PQescapeString(e_text, text, string_length);

        if (NULL != completion_info_charp) {
            size_t charp_len;

            charp_len = strlen(completion_info_charp);
            e_info_charp = (char*)pg_malloc(charp_len * 2 + 1);
            PQescapeString(e_info_charp, completion_info_charp, charp_len);
        } else
            e_info_charp = NULL;

        if (NULL != completion_info_charp2) {
            size_t charp_len;

            charp_len = strlen(completion_info_charp2);
            e_info_charp2 = (char*)pg_malloc(charp_len * 2 + 1);
            PQescapeString(e_info_charp2, completion_info_charp2, charp_len);
        } else
            e_info_charp2 = NULL;

        initPQExpBuffer(&query_buffer);

        if (is_schema_query) {
            /* completion_squery gives us the pieces to assemble */
            const char* qualresult = completion_squery->qualresult;

            if (qualresult == NULL)
                qualresult = completion_squery->result;

            /* Get unqualified names matching the input-so-far */
            appendPQExpBuffer(
                &query_buffer, "SELECT %s FROM %s WHERE ", completion_squery->result, completion_squery->catname);
            if (NULL != completion_squery->selcondition)
                appendPQExpBuffer(&query_buffer, "%s AND ", completion_squery->selcondition);
            appendPQExpBuffer(
                &query_buffer, "substring(%s,1,%d)='%s'", completion_squery->result, string_length, e_text);
            appendPQExpBuffer(&query_buffer, " AND %s", completion_squery->viscondition);

            /*
             * When fetching relation names, suppress system catalogs unless
             * the input-so-far begins with "pg_".	This is a compromise
             * between not offering system catalogs for completion at all, and
             * having them swamp the result when the input is just "p".
             */
            if (strcmp(completion_squery->catname, "pg_catalog.pg_class c") == 0 && strncmp(text, "pg_", 3) != 0) {
                appendPQExpBuffer(&query_buffer,
                    " AND c.relnamespace <> (SELECT oid FROM"
                    " pg_catalog.pg_namespace WHERE nspname = 'pg_catalog')");
            }

            /*
             * Add in matching schema names, but only if there is more than
             * one potential match among schema names.
             */
            appendPQExpBuffer(&query_buffer,
                "\nUNION\n"
                "SELECT pg_catalog.quote_ident(n.nspname) || '.' "
                "FROM pg_catalog.pg_namespace n "
                "WHERE substring(pg_catalog.quote_ident(n.nspname) || '.',1,%d)='%s'",
                string_length,
                e_text);
            appendPQExpBuffer(&query_buffer,
                " AND (SELECT pg_catalog.count(*)"
                " FROM pg_catalog.pg_namespace"
                " WHERE substring(pg_catalog.quote_ident(nspname) || '.',1,%d) ="
                " substring('%s',1,pg_catalog.length(pg_catalog.quote_ident(nspname))+1)) > 1",
                string_length,
                e_text);

            /*
             * Add in matching qualified names, but only if there is exactly
             * one schema matching the input-so-far.
             */
            appendPQExpBuffer(&query_buffer,
                "\nUNION\n"
                "SELECT pg_catalog.quote_ident(n.nspname) || '.' || %s "
                "FROM %s, pg_catalog.pg_namespace n "
                "WHERE %s = n.oid AND ",
                qualresult,
                completion_squery->catname,
                completion_squery->nameSpace);
            if (completion_squery->selcondition != NULL)
                appendPQExpBuffer(&query_buffer, "%s AND ", completion_squery->selcondition);
            appendPQExpBuffer(&query_buffer,
                "substring(pg_catalog.quote_ident(n.nspname) || '.' || %s,1,%d)='%s'",
                qualresult,
                string_length,
                e_text);

            /*
             * This condition exploits the single-matching-schema rule to
             * speed up the query
             */
            appendPQExpBuffer(&query_buffer,
                " AND substring(pg_catalog.quote_ident(n.nspname) || '.',1,%d) ="
                " substring('%s',1,pg_catalog.length(pg_catalog.quote_ident(n.nspname))+1)",
                string_length,
                e_text);
            appendPQExpBuffer(&query_buffer,
                " AND (SELECT pg_catalog.count(*)"
                " FROM pg_catalog.pg_namespace"
                " WHERE substring(pg_catalog.quote_ident(nspname) || '.',1,%d) ="
                " substring('%s',1,pg_catalog.length(pg_catalog.quote_ident(nspname))+1)) = 1",
                string_length,
                e_text);

            /* If an addon query was provided, use it */
            if (NULL != completion_charp)
                appendPQExpBuffer(&query_buffer, "\n%s", completion_charp);
        } else {
            /* completion_charp is an sprintf-style format string */
            appendPQExpBuffer(&query_buffer,
                completion_charp,
                string_length,
                e_text,
                e_info_charp,
                e_info_charp,
                e_info_charp2,
                e_info_charp2);
        }

        /* Limit the number of records in the result */
        appendPQExpBuffer(&query_buffer, "\nLIMIT %d", completion_max_records);

        result = exec_query(query_buffer.data);

        termPQExpBuffer(&query_buffer);
        free(e_text);
        e_text = NULL;
        if (e_info_charp != NULL) {
            free(e_info_charp);
            e_info_charp = NULL;
        }
        if (e_info_charp2 != NULL) {
            free(e_info_charp2);
            e_info_charp2 = NULL;
        }
    }

    /* Find something that matches */
    if (result && PQresultStatus(result) == PGRES_TUPLES_OK) {
        const char* item = NULL;

        while (list_index < PQntuples(result) && (item = PQgetvalue(result, list_index++, 0)))
            if (pg_strncasecmp(text, item, string_length) == 0)
                return pg_strdup(item);
    }

    /* If nothing matches, free the db structure and return null */
    PQclear(result);
    result = NULL;
    return NULL;
}
#endif

/*
 * This function returns in order one of a fixed, NULL pointer terminated list
 * of strings (if matching). This can be used if there are only a fixed number
 * SQL words that can appear at certain spot.
 */
static char* complete_from_list(const char* text, int state)
{
    static int string_length, list_index, matches;
    static bool casesensitive = false;
    const char* item = NULL;

    /* need to have a list */
    psql_assert(completion_charpp);

    /* Initialization */
    if (state == 0) {
        list_index = 0;
        string_length = strlen(text);
        casesensitive = completion_case_sensitive;
        matches = 0;
    }

    while ((item = completion_charpp[list_index++])) {
        /* First pass is case sensitive */
        if (casesensitive && strncmp(text, item, string_length) == 0) {
            matches++;
            return pg_strdup(item);
        }

        /* Second pass is case insensitive, don't bother counting matches */
        if (!casesensitive && pg_strncasecmp(text, item, string_length) == 0) {
            if (completion_case_sensitive)
                return pg_strdup(item);
            else

                /*
                 * If case insensitive matching was requested initially,
                 * adjust the case according to setting.
                 */
                return pg_strdup_keyword_case(item, text);
        }
    }

    /*
     * No matches found. If we're not case insensitive already, lets switch to
     * being case insensitive and try again
     */
    if (casesensitive && matches == 0) {
        casesensitive = false;
        list_index = 0;
        state++;
        return complete_from_list(text, state);
    }

    /* If no more matches, return null. */
    return NULL;
}

#ifdef NOT_USED
/*
 * This function returns one fixed string the first time even if it doesn't
 * match what's there, and nothing the second time. This should be used if
 * there is only one possibility that can appear at a certain spot, so
 * misspellings will be overwritten.  The string to be passed must be in
 * completion_charp.
 */
static char* complete_from_const(const char* text, int state)
{
    psql_assert(completion_charp);
    if (state == 0) {
        if (completion_case_sensitive)
            return pg_strdup(completion_charp);
        else

            /*
             * If case insensitive matching was requested initially, adjust
             * the case according to setting.
             */
            return pg_strdup_keyword_case(completion_charp, text);
    } else
        return NULL;
}

/*
 * This function supports completion with the name of a psql variable.
 * The variable names can be prefixed and suffixed with additional text
 * to support quoting usages.
 */
static char** complete_from_variables(char* text, const char* prefix, const char* suffix)
{
    char** matches = NULL;
    int overhead = strlen(prefix) + strlen(suffix) + 1;
    char** varnames = NULL;
    int nvars = 0;
    int maxvars = 100;
    int i;
    struct _variable* ptr = NULL;
    int rc;
    size_t sz = 0;

    varnames = (char**)pg_malloc((maxvars + 1) * sizeof(char*));

    ptr = (pset.vars != NULL) ? pset.vars->next : NULL;
    for (;ptr != NULL; ptr = ptr->next) {
        char* buffer = NULL;

        if (nvars >= maxvars) {
            maxvars *= 2;
            varnames = (char**)pg_realloc(varnames, (maxvars + 1) * sizeof(char*));
            if (varnames == NULL) {
                psql_error("out of memory\n");
                exit(EXIT_FAILURE);
            }
        }

        sz = strlen(ptr->name) + overhead;
        buffer = (char*)pg_malloc(sz);
        rc = sprintf_s(buffer, sz, "%s%s%s", prefix, ptr->name, suffix);
        check_sprintf_s(rc);
        varnames[nvars++] = buffer;
    }

    varnames[nvars] = NULL;
    COMPLETE_WITH_LIST_CS((const char* const*)varnames);

    for (i = 0; i < nvars; i++) {
        free(varnames[i]);
        varnames[i] = NULL;
    }
    free(varnames);
    varnames = NULL;

    return matches;
}

/*
 * This function wraps rl_filename_completion_function() to strip quotes from
 * the input before searching for matches and to quote any matches for which
 * the consuming command will require it.
 */
static char* complete_from_files(const char* text, int state)
{
    static const char* unquoted_text = NULL;
    char* unquoted_match = NULL;
    char* ret = NULL;

    if (state == 0) {
        /* Initialization: stash the unquoted input. */
        unquoted_text = strtokx(text, "", NULL, "'", *completion_charp, false, true, pset.encoding);
        /* expect a NULL return for the empty string only */
        if (NULL == unquoted_text) {
            psql_assert(!*text);
            unquoted_text = text;
        }
    }

    unquoted_match = filename_completion_function(unquoted_text, state);
    if (unquoted_match != NULL) {
        /*
         * Caller sets completion_charp to a zero- or one-character string
         * containing the escape character.  This is necessary since \copy has
         * no escape character, but every other backslash command recognizes
         * "\" as an escape character.  Since we have only two callers, don't
         * bother providing a macro to simplify this.
         */
        ret = quote_if_needed(unquoted_match, " \t\r\n\"`", '\'', *completion_charp, pset.encoding);
        if (ret != NULL) {
            free(unquoted_match);
            unquoted_match = NULL;
        } else
            ret = unquoted_match;
    }

    return ret;
}
#endif

/* HELPER FUNCTIONS */
/*
 * Make a pg_strdup copy of s and convert the case according to
 * COMP_KEYWORD_CASE variable, using ref as the text that was already entered.
 */
static char* pg_strdup_keyword_case(const char* s, const char* ref)
{
    char *ret = NULL;
    char *p = NULL;
    unsigned char first = ref[0];
    int tocase;
    const char* varval = NULL;

    varval = GetVariable(pset.vars, "COMP_KEYWORD_CASE");
    if (varval == NULL)
        tocase = 0;
    else if (strcmp(varval, "lower") == 0)
        tocase = -2;
    else if (strcmp(varval, "preserve-lower") == 0)
        tocase = -1;
    else if (strcmp(varval, "preserve-upper") == 0)
        tocase = +1;
    else if (strcmp(varval, "upper") == 0)
        tocase = +2;
    else
        tocase = 0;

    /* default */
    if (tocase == 0)
        tocase = +1;

    ret = pg_strdup(s);

    if (tocase == -2 || ((tocase == -1 || tocase == +1) && islower(first)) || (tocase == -1 && !isalpha(first)))
        for (p = ret; *p; p++)
            *p = pg_tolower((unsigned char)*p);
    else
        for (p = ret; *p; p++)
            *p = pg_toupper((unsigned char)*p);

    return ret;
}

#ifdef NOT_USED
/*
 * Execute a query and report any errors. This should be the preferred way of
 * talking to the database in this file.
 */
static PGresult* exec_query(const char* query)
{
    PGresult* result = NULL;

    if (query == NULL || pset.db == NULL || PQstatus(pset.db) != CONNECTION_OK)
        return NULL;

    result = PQexec(pset.db, query);
    if (PQresultStatus(result) != PGRES_TUPLES_OK) {
#ifdef NOT_USED
        psql_error("tab completion query failed: %s\nQuery was:\n%s\n", PQerrorMessage(pset.db), query);
#endif
        PQclear(result);
        result = NULL;
    }

    return result;
}

/*
 * Return the nwords word(s) before point.	Words are returned right to left,
 * that is, previous_words[0] gets the last word before point.
 * If we run out of words, remaining array elements are set to empty strings.
 * Each array element is filled with a malloc'd string.
 */
static void get_previous_words(int point, char** previous_words, int nwords)
{
    const char* buf = rl_line_buffer; /* alias */
    int i;
    errno_t rc = EOK;

    /* first we look for a non-word char before the current point */
    for (i = point - 1; i >= 0; i--)
        if (strchr(WORD_BREAKS, buf[i]))
            break;
    point = i;

    while (nwords-- > 0) {
        int start, end;
        char* s = NULL;

        /* now find the first non-space which then constitutes the end */
        end = -1;
        for (i = point; i >= 0; i--) {
            if (!isspace((unsigned char)buf[i])) {
                end = i;
                break;
            }
        }

        /*
         * If no end found we return an empty string, because there is no word
         * before the point
         */
        if (end < 0) {
            point = end;
            s = pg_strdup("");
        } else {
            /*
             * Otherwise we now look for the start. The start is either the
             * last character before any word-break character going backwards
             * from the end, or it's simply character 0. We also handle open
             * quotes and parentheses.
             */
            bool inquotes = false;
            int parentheses = 0;

            for (start = end; start > 0; start--) {
                if (buf[start] == '"')
                    inquotes = !inquotes;
                if (!inquotes) {
                    if (buf[start] == ')')
                        parentheses++;
                    else if (buf[start] == '(') {
                        if (--parentheses <= 0)
                            break;
                    } else if (parentheses == 0 && strchr(WORD_BREAKS, buf[start - 1]))
                        break;
                }
            }

            point = start - 1;

            /* make a copy of chars from start to end inclusive */
            s = (char*)pg_malloc(end - start + 2);
            rc = strncpy_s(s, end - start + 2, &buf[start], end - start + 2);
            securec_check_c(rc, "\0", "\0");
        }

        *previous_words++ = s;
    }
}
#endif

#ifdef NOT_USED

/*
 * Surround a string with single quotes. This works for both SQL and
 * psql internal. Currently disabled because it is reported not to
 * cooperate with certain versions of readline.
 */
static char* quote_file_name(char* text, int match_type, char* quote_pointer)
{
    char* s = NULL;
    size_t length;
    int rc;

    (void)quote_pointer; /* not used */

    length = strlen(text) + (match_type == SINGLE_MATCH ? 3 : 2);
    s = pg_malloc(length);
    s[0] = '\'';
    rc = strcpy_s(s + 1, length - 1, text);
    check_strcpy_s(rc);
    if (match_type == SINGLE_MATCH)
        s[length - 2] = '\'';
    s[length - 1] = '\0';
    return s;
}

static char* dequote_file_name(char* text, char quote_char)
{
    char* s = NULL;
    size_t length;
    errno_t rc = EOK;

    if (!quote_char)
        return pg_strdup(text);

    length = strlen(text);
    s = pg_malloc(length - 2 + 1);
    rc = strncpy_s(s, length - 2 + 1, text + 1, length - 2 + 1);
    securec_check_c(rc, "\0", "\0");

    return s;
}
#endif /* NOT_USED */

#endif /* USE_READLINE */
