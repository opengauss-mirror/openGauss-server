/*
 * psql - the openGauss interactive terminal
 *
 * Copyright (c) 2000-2012, PostgreSQL Global Development Group
 *
 * src/bin/psql/tab-complete.h
 */
#ifndef TAB_COMPLETE_H
#define TAB_COMPLETE_H

#include "postgres_fe.h"

/* word break characters */
#define WORD_BREAKS "\t\n@$><=;|&{() "

#ifdef HAVE_READLINE_READLINE_H
#define filename_completion_function rl_filename_completion_function

#define completion_matches rl_completion_matches

/*
 * This struct is used to define "schema queries", which are custom-built
 * to obtain possibly-schema-qualified names of database objects.  There is
 * enough similarity in the structure that we don't want to repeat it each
 * time.  So we put the components of each query into this struct and
 * assemble them with the common boilerplate in _CompleteFromQuery().
 */
typedef struct SchemaQuery {
    /*
     * Name of catalog or catalogs to be queried, with alias, eg.
     * "pg_catalog.pg_class c".  Note that "pg_namespace n" will be added.
     */
    const char *catname;

    /*
     * Selection condition --- only rows meeting this condition are candidates
     * to display.	If catname mentions multiple tables, include the necessary
     * join condition here.  For example, "c.relkind = 'r'". Write NULL (not
     * an empty string) if not needed.
     */
    const char *selcondition;

    /*
     * Visibility condition --- which rows are visible without schema
     * qualification?  For example, "pg_catalog.pg_table_is_visible(c.oid)".
     */
    const char *viscondition;

    /*
     * Namespace --- name of field to join to pg_namespace.oid. For example,
     * "c.relnamespace".
     */
    const char *nameSpace;

    /*
     * Result --- the appropriately-quoted name to return, in the case of an
     * unqualified name.  For example, "pg_catalog.quote_ident(c.relname)".
     */
    const char *result;

    /*
     * In some cases a different result must be used for qualified names.
     * Enter that here, or write NULL if result can be used.
     */
    const char *qualresult;
} SchemaQuery;

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
 * _CompleteFromQuery().
 */
#define Query_for_list_of_attributes "SELECT pg_catalog.quote_ident(attname) "                          \
        "  FROM pg_catalog.pg_attribute a, pg_catalog.pg_class c "     \
        " WHERE c.oid = a.attrelid "                                   \
        "   AND a.attnum > 0 "                                         \
        "   AND NOT a.attisdropped "                                   \
        "   AND substring(pg_catalog.quote_ident(attname),1,%d)='%s' " \
        "   AND (pg_catalog.quote_ident(relname)='%s' "                \
        "        OR '\"' || relname || '\"'='%s') "                    \
        "   AND pg_catalog.pg_table_is_visible(c.oid)"

#define Query_for_list_of_attributes_with_schema                                              \
    "SELECT pg_catalog.quote_ident(attname) "                                                 \
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

#define Query_for_list_of_template_databases "SELECT pg_catalog.quote_ident(datname) FROM pg_catalog.pg_database " \
        " WHERE substring(pg_catalog.quote_ident(datname),1,%d)='%s' AND datistemplate"

#define Query_for_list_of_databases "SELECT pg_catalog.quote_ident(datname) FROM pg_catalog.pg_database " \
        " WHERE substring(pg_catalog.quote_ident(datname),1,%d)='%s'"

#define Query_for_list_of_tablespaces "SELECT pg_catalog.quote_ident(spcname) FROM pg_catalog.pg_tablespace " \
        " WHERE substring(pg_catalog.quote_ident(spcname),1,%d)='%s'"

#define Query_for_list_of_encodings " SELECT DISTINCT pg_catalog.pg_encoding_to_char(conforencoding) " \
        "   FROM pg_catalog.pg_conversion "                            \
        "  WHERE substring(pg_catalog.pg_encoding_to_char(conforencoding),1,%d)=UPPER('%s')"

#define Query_for_list_of_languages "SELECT pg_catalog.quote_ident(lanname) " \
        "  FROM pg_catalog.pg_language "      \
        " WHERE lanname != 'internal' "       \
        "   AND substring(pg_catalog.quote_ident(lanname),1,%d)='%s'"

#define Query_for_list_of_schemas "SELECT pg_catalog.quote_ident(nspname) FROM pg_catalog.pg_namespace " \
        " WHERE substring(pg_catalog.quote_ident(nspname),1,%d)='%s'"

#define Query_for_list_of_alter_system_set_vars "SELECT name FROM " \
                " (SELECT pg_catalog.lower(name) AS name FROM pg_catalog.pg_settings "\
                "  WHERE context != 'internal' "\
                "  UNION ALL SELECT 'all') ss "\
                " WHERE substring(name,1,%d)='%s'"

#define Query_for_list_of_set_vars "SELECT name FROM "                                                        \
        " (SELECT pg_catalog.lower(name) AS name FROM pg_catalog.pg_settings " \
        "  WHERE context IN ('user', 'superuser') "                            \
        "  UNION ALL SELECT 'constraints' "                                    \
        "  UNION ALL SELECT 'transaction' "                                    \
        "  UNION ALL SELECT 'session' "                                        \
        "  UNION ALL SELECT 'local' "                                          \
        "  UNION ALL SELECT 'role' "                                           \
        "  UNION ALL SELECT 'tablespace' "                                     \
        "  UNION ALL SELECT 'all') ss "                                        \
        " WHERE substring(name,1,%d)='%s'"

#define Query_for_list_of_show_vars "SELECT name FROM "                                                        \
        " (SELECT pg_catalog.lower(name) AS name FROM pg_catalog.pg_settings " \
        "  UNION ALL SELECT 'session authorization' "                          \
        "  UNION ALL SELECT 'all') ss "                                        \
        " WHERE substring(name,1,%d)='%s'"

#define Query_for_list_of_roles " SELECT pg_catalog.quote_ident(rolname) " \
        "   FROM pg_catalog.pg_roles "         \
        "  WHERE substring(pg_catalog.quote_ident(rolname),1,%d)='%s'"

#define Query_for_list_of_grant_roles " SELECT pg_catalog.quote_ident(rolname) "                         \
        "   FROM pg_catalog.pg_roles "                                 \
        "  WHERE substring(pg_catalog.quote_ident(rolname),1,%d)='%s'" \
        " UNION ALL SELECT 'PUBLIC'"

/* the silly-looking length condition is just to eat up the current word */
#define Query_for_table_owning_index                                                   \
    "SELECT pg_catalog.quote_ident(c1.relname) "                                       \
        "  FROM pg_catalog.pg_class c1, pg_catalog.pg_class c2, pg_catalog.pg_index i" \
        " WHERE c1.oid=i.indrelid and i.indexrelid=c2.oid"                             \
        "       and (%d = pg_catalog.length('%s'))"                                    \
        "       and pg_catalog.quote_ident(c2.relname)='%s'"                           \
        "       and pg_catalog.pg_table_is_visible(c2.oid)"

/* the silly-looking length condition is just to eat up the current word */
#define Query_for_index_of_table "SELECT pg_catalog.quote_ident(c2.relname) "                                       \
        "  FROM pg_catalog.pg_class c1, pg_catalog.pg_class c2, pg_catalog.pg_index i" \
        " WHERE c1.oid=i.indrelid and i.indexrelid=c2.oid"                             \
        "       and (%d = pg_catalog.length('%s'))"                                    \
        "       and pg_catalog.quote_ident(c1.relname)='%s'"                           \
        "       and pg_catalog.pg_table_is_visible(c2.oid)"

/* the silly-looking length condition is just to eat up the current word */
#define Query_for_list_of_tables_for_trigger "SELECT pg_catalog.quote_ident(relname) "                \
        "  FROM pg_catalog.pg_class"                         \
        " WHERE (%d = pg_catalog.length('%s'))"              \
        "   AND oid IN "                                     \
        "       (SELECT tgrelid FROM pg_catalog.pg_trigger " \
        "         WHERE pg_catalog.quote_ident(tgname)='%s')"

#define Query_for_list_of_ts_configurations "SELECT pg_catalog.quote_ident(cfgname) FROM pg_catalog.pg_ts_config " \
        " WHERE substring(pg_catalog.quote_ident(cfgname),1,%d)='%s'"

#define Query_for_list_of_ts_dictionaries "SELECT pg_catalog.quote_ident(dictname) FROM pg_catalog.pg_ts_dict " \
        " WHERE substring(pg_catalog.quote_ident(dictname),1,%d)='%s'"

#define Query_for_list_of_ts_parsers "SELECT pg_catalog.quote_ident(prsname) FROM pg_catalog.pg_ts_parser " \
        " WHERE substring(pg_catalog.quote_ident(prsname),1,%d)='%s'"

#define Query_for_list_of_ts_templates "SELECT pg_catalog.quote_ident(tmplname) FROM pg_catalog.pg_ts_template " \
        " WHERE substring(pg_catalog.quote_ident(tmplname),1,%d)='%s'"

#define Query_for_list_of_fdws " SELECT pg_catalog.quote_ident(fdwname) "        \
        "   FROM pg_catalog.pg_foreign_data_wrapper " \
        "  WHERE substring(pg_catalog.quote_ident(fdwname),1,%d)='%s'"

#define Query_for_list_of_servers " SELECT pg_catalog.quote_ident(srvname) "  \
        "   FROM pg_catalog.pg_foreign_server " \
        "  WHERE substring(pg_catalog.quote_ident(srvname),1,%d)='%s'"

#define Query_for_list_of_user_mappings " SELECT pg_catalog.quote_ident(usename) " \
        "   FROM pg_catalog.pg_user_mappings " \
        "  WHERE substring(pg_catalog.quote_ident(usename),1,%d)='%s'"

#define Query_for_list_of_access_methods " SELECT pg_catalog.quote_ident(amname) " \
        "   FROM pg_catalog.pg_am "           \
        "  WHERE substring(pg_catalog.quote_ident(amname),1,%d)='%s'"

#define Query_for_list_of_arguments " SELECT pg_catalog.oidvectortypes(proargtypes)||')' " \
        "   FROM pg_catalog.pg_proc "                      \
        "  WHERE proname='%s'"

#define Query_for_list_of_extensions " SELECT pg_catalog.quote_ident(extname) " \
        "   FROM pg_catalog.pg_extension "     \
        "  WHERE substring(pg_catalog.quote_ident(extname),1,%d)='%s'"

#define Query_for_list_of_available_extensions " SELECT pg_catalog.quote_ident(name) "           \
        "   FROM pg_catalog.pg_available_extensions " \
        "  WHERE substring(pg_catalog.quote_ident(name),1,%d)='%s' AND installed_version IS NULL"

#define Query_for_list_of_available_extension_versions \
" SELECT pg_catalog.quote_ident(version) "\
"   FROM pg_catalog.pg_available_extension_versions "\
"  WHERE (%d = pg_catalog.length('%s'))"\
"    AND pg_catalog.quote_ident(name)='%s'"

#define Query_for_list_of_prepared_statements " SELECT pg_catalog.quote_ident(name) "          \
        "   FROM pg_catalog.pg_prepared_statements " \
        "  WHERE substring(pg_catalog.quote_ident(name),1,%d)='%s'"

#ifdef PGXC
#define Query_for_list_of_available_nodenames " SELECT NODE_NAME "                      \
        "  FROM PGXC_NODE"
#define Query_for_list_of_available_coordinators " SELECT NODE_NAME "                         \
        "  FROM PGXC_NODE"                       \
        "   WHERE NODE_TYPE = 'C'"
#define Query_for_list_of_available_datanodes " SELECT NODE_NAME "                      \
        "  FROM PGXC_NODE"                    \
        "   WHERE NODE_TYPE = 'D'"
#define Query_for_list_of_available_nodegroup_names " SELECT GROUP_NAME "                           \
        "  FROM PGXC_GROUP"
#endif

/*
 * This is a list of all "things" in Pgsql, which can show up after CREATE or
 * DROP; and there is also a query to get a list of them.
 */
typedef struct {
    const char *name;
    const char *query;         /* simple query, or NULL */
    const SchemaQuery *squery; /* schema query, or NULL */
    const bits32 flags;        /* visibility flags, see below */
} pgsql_thing_t;

#define THING_NO_CREATE (1 << 0) /* should not show up after CREATE */
#define THING_NO_DROP (1 << 1)   /* should not show up after DROP */
#define THING_NO_SHOW (THING_NO_CREATE | THING_NO_DROP)

#endif /* HAVE_READLINE_READLINE_H */

void initialize_readline(void);

#endif

