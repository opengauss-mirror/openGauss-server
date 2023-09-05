/*
 * psql - the openGauss interactive terminal
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
 * 	 $if psql
 * 	 set disable-completion on
 * 	 $endif
 *
 * See `man 3 readline' or `info readline' for the full details. Also,
 * hence the
 *
 * BUGS:
 *
 * - If you split your queries across lines, this whole thing gets
 * 	 confused. (To fix this, one would have to read psql's query
 * 	 buffer rather than readline's line buffer, which would require
 * 	 some major revisions of things.)
 *
 * - Table or attribute names with spaces in it may confuse it.
 *
 * - Quotes, parenthesis, and other funny characters are not handled
 * 	 all that gracefully.
 * ----------------------------------------------------------------------
 */
#include "settings.h"
#include "postgres_fe.h"
#include "input.h"
#include "tab-complete.h"

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

/* Store maximum number of records we want from database queries
 * (implemented via SELECT ... LIMIT xx).
 */
static int completion_max_records;
#ifdef HAVE_READLINE_READLINE_H

/*
 * Communication variables set by COMPLETE_WITH_FOO macros and then used by
 * the completion callback functions.  Ugly but there is no better way.
 */
static const char* completion_charp;         /* to pass a string */
static const char* const* completion_charpp; /* to pass a list of strings */
static const char* completion_info_charp;    /* to pass a second string */
static const char* completion_info_charp2;   /* to pass a third string */
static const SchemaQuery* completion_squery; /* to pass a SchemaQuery */
static bool completion_case_sensitive;       /* completion is case sensitive */

static char** PsqlCompletion(const char *text, int start, int end);
static char* CreateCommandGenerator(const char* text, int state);
static char* DropCommandGenerator(const char* text, int state);
static char* CompleteFromQuery(const char* text, int state);
static char* CompleteFromSchemaQuery(const char* text, int state);
static char* _CompleteFromQuery(int isSchemaQuery, const char* text, int state);
static char* CompleteFromList(const char* text, int state);
static char* CompleteFromConst(const char* text, int state);
static char** CompleteFromVariables(const char* text, const char* prefix, const char* suffix);
static char* CompleteFromFiles(const char* text, int state);

static char* pg_strdup_keyword_case(const char* s, const char* ref);
static PGresult* ExecQuery(const char* query);
static void GetPreviousWords(int point, char** previousWords, int nwords);

/*
 * A few macros to ease typing. You can use these to complete the given
 * string with
 * 1) The results from a query you pass it. (Perhaps one of those below?)
 * 2) The results from a schema query you pass it.
 * 3) The items from a null-pointer-terminated list.
 * 4) A string constant.
 * 5) The list of attributes of the given table (possibly schema-qualified).
 */
#define COMPLETE_WITH_QUERY(query) do {                                                         \
        completion_charp = query;                                \
        matches = completion_matches(text, CompleteFromQuery); \
    } while (0)

#define COMPLETE_WITH_SCHEMA_QUERY(query, addon) do {                                                                \
        completion_squery = &(query);                                   \
        completion_charp = addon;                                       \
        matches = completion_matches(text, CompleteFromSchemaQuery); \
    } while (0)

#define COMPLETE_WITH_LIST_CS(list) do {                                                        \
        completion_charpp = list;                               \
        completion_case_sensitive = true;                       \
        matches = completion_matches(text, CompleteFromList); \
    } while (0)

#define COMPLETE_WITH_LIST(list) do {                                                        \
        completion_charpp = list;                               \
        completion_case_sensitive = false;                      \
        matches = completion_matches(text, CompleteFromList); \
    } while (0)

#define COMPLETE_WITH_CONST(string) do {                                                         \
        completion_charp = string;                               \
        completion_case_sensitive = false;                       \
        matches = completion_matches(text, CompleteFromConst); \
    } while (0)

#define COMPLETE_WITH_ATTR(relation, addon)                                                           \
    do {                                                                                              \
        char *_completion_schema;                                                                     \
        char *_completion_table;                                                                      \
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
        matches = completion_matches(text, CompleteFromQuery);                                      \
    } while (0)

/*
 * Assembly instructions for schema queries
 */
static const SchemaQuery Query_for_list_of_aggregates = {
    /* catname */
    "pg_catalog.pg_proc p",
    /* selcondition */
    "p.proisagg",
    /* viscondition */
    "pg_catalog.pg_function_is_visible(p.oid)",
    /* namespace */
    "p.pronamespace",
    /* result */
    "pg_catalog.quote_ident(p.proname)",
    /* qualresult */
    NULL
};

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
    "pg_catalog.quote_ident(t.typname)"
};

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
    NULL
};

static const SchemaQuery Query_for_list_of_functions = {
    /* catname */
    "pg_catalog.pg_proc p",
    /* selcondition */
    "p.prokind != 'p'",
    /* viscondition */
    "pg_catalog.pg_function_is_visible(p.oid)",
    /* namespace */
    "p.pronamespace",
    /* result */
    "pg_catalog.quote_ident(p.proname)",
    /* qualresult */
    NULL
};

static const SchemaQuery Query_for_list_of_procedures[] = {
    /* catname */
    "pg_catalog.pg_proc p",
    /* selcondition */
    "p.prokind = 'p'",
    /* viscondition */
    "pg_catalog.pg_function_is_visible(p.oid)",
    /* namespace */
    "p.pronamespace",
    /* result */
    "pg_catalog.quote_ident(p.proname)",
    /* qualresult */
    NULL
};

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
    NULL
};

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
    NULL
};

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
    NULL
};

static const SchemaQuery Query_for_list_of_tables = {
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
    NULL
};

/* The bit masks for the following three functions come from
 * src/include/catalog/pg_trigger.h.
 */
static const SchemaQuery Query_for_list_of_insertables = {
    /* catname */
    "pg_catalog.pg_class c",
    /* selcondition */
    "(c.relkind IN ('r', 'f') OR (c.relkind = 'v' AND c.relhastriggers AND EXISTS "
    "(SELECT 1 FROM pg_catalog.pg_trigger t WHERE t.tgrelid = c.oid AND t.tgtype & (1 << 2) <> 0)))",
    /* viscondition */
    "pg_catalog.pg_table_is_visible(c.oid)",
    /* namespace */
    "c.relnamespace",
    /* result */
    "pg_catalog.quote_ident(c.relname)",
    /* qualresult */
    NULL
};

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
    NULL
};

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
    NULL
};

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
    NULL
};

static const SchemaQuery Query_for_list_of_tsvmf = {
    /* catname */
    "pg_catalog.pg_class c",
    /* selcondition */
    "c.relkind IN ('r', 'S', 'v', 'm', 'f')",
    /* viscondition */
    "pg_catalog.pg_table_is_visible(c.oid)",
    /* namespace */
    "c.relnamespace",
    /* result */
    "pg_catalog.quote_ident(c.relname)",
    /* qualresult */
    NULL
};

static const SchemaQuery Query_for_list_of_tmf = {
    /* catname */
    "pg_catalog.pg_class c",
    /* selcondition */
    "c.relkind IN ('r', 'm', 'f')",
    /* viscondition */
    "pg_catalog.pg_table_is_visible(c.oid)",
    /* namespace */
    "c.relnamespace",
    /* result */
    "pg_catalog.quote_ident(c.relname)",
    /* qualresult */
    NULL
};

static const SchemaQuery Query_for_list_of_tm = {
    /* catname */
    "pg_catalog.pg_class c",
    /* selcondition */
    "c.relkind IN ('r', 'm')",
    /* viscondition */
    "pg_catalog.pg_table_is_visible(c.oid)",
    /* namespace */
    "c.relnamespace",
    /* result */
    "pg_catalog.quote_ident(c.relname)",
    /* qualresult */
    NULL
};

static const SchemaQuery Query_for_list_of_tmi = {
    /* catname */
    "pg_catalog.pg_class c",
    /* selcondition */
    "c.relkind IN ('r', 'm', 'i')",
    /* viscondition */
    "pg_catalog.pg_table_is_visible(c.oid)",
    /* namespace */
    "c.relnamespace",
    /* result */
    "pg_catalog.quote_ident(c.relname)",
    /* qualresult */
    NULL
};

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
    NULL
};

static const SchemaQuery Query_for_list_of_matviews = {
    /* catname */
    "pg_catalog.pg_class c",
    /* selcondition */
    "c.relkind IN ('m')",
    /* viscondition */
    "pg_catalog.pg_table_is_visible(c.oid)",
    /* namespace */
    "c.relnamespace",
    /* result */
    "pg_catalog.quote_ident(c.relname)",
    /* qualresult */
    NULL
};

static const SchemaQuery Query_for_list_of_constraints_with_schema = {
    /* catname */
    "pg_catalog.pg_constraint c",
    /* selcondition */
    "c.conrelid <> 0",
    /* viscondition */
    "true",
    /* namespace */
    "c.connamespace",
    /* result */
    "pg_catalog.quote_ident(c.conname)",
    /* qualresult */
    NULL
};

static const pgsql_thing_t words_after_create[] = {
    {"AGGREGATE", NULL, &Query_for_list_of_aggregates, 0},
    {"APP WORKLOAD GROUP", NULL, NULL, 0},
    {"APP WORKLOAD GROUP MAPPING", NULL, NULL, 0},
#ifdef PGXC
    {"BARRIER", NULL, NULL, 0}, /* Comes barrier name next, so skip it */
#endif
    {"CAST", NULL, NULL, 0}, /* Casts have complex structures for names, so
                              * skip it */
    {"CLIENT MASTER KEY", NULL, NULL, 0},
    {"COLLATION",
     "SELECT pg_catalog.quote_ident(collname) FROM pg_catalog.pg_collation WHERE collencoding IN (-1, "
     "pg_catalog.pg_char_to_encoding(pg_catalog.getdatabaseencoding())) AND "
     "substring(pg_catalog.quote_ident(collname),1,%d)='%s'",
     NULL,
     0},

    {"COLUMN ENCRYPTION KEY", NULL, NULL, 0},
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
    {"FOREIGN DATA WRAPPER", NULL, NULL, 0},
    {"FOREIGN TABLE", NULL, NULL, 0},
    {"FUNCTION", NULL, &Query_for_list_of_functions, 0},
    {"GLOBAL", NULL, NULL, 0},
    {"GROUP", Query_for_list_of_roles, NULL, 0},
    {"INCREMENTAL MATERIALIZED VIEW", NULL, NULL, 0},
    {"INDEX", NULL, &Query_for_list_of_indexes, 0},
    {"LANGUAGE", Query_for_list_of_languages, NULL, 0},
    {"LOCAL", NULL, NULL, 0},
    {"MATERIALIZED VIEW", NULL, &Query_for_list_of_matviews, 0},
#ifdef PGXC
    {"NODE", Query_for_list_of_available_nodenames, NULL, 0},
    {"NODE GROUP", Query_for_list_of_available_nodegroup_names, NULL, 0},
#endif
    {"OPERATOR", NULL, NULL, 0},            /* Querying for this is probably not such a good idea. */
    {"OR REPLACE", NULL, NULL, THING_NO_DROP},
    {"OWNED", NULL, NULL, THING_NO_CREATE}, /* for DROP OWNED BY ... */
    {"PARSER", Query_for_list_of_ts_parsers, NULL, THING_NO_SHOW},
    {"POLICY", NULL, NULL, 0},
    {"PROCEDURAL", NULL, NULL, 0},
    {"PROCEDURE", NULL, Query_for_list_of_procedures, 0},
    {"PUBLICATION", NULL, NULL, 0},
    {"ROLE", Query_for_list_of_roles, NULL, 0},
    {"RULE",
        "SELECT pg_catalog.quote_ident(rulename) FROM pg_catalog.pg_rules WHERE "
        "substring(pg_catalog.quote_ident(rulename),1,%d)='%s'",
        NULL,
        0},
    {"ROW LEVEL SECURITY POLICY", NULL, NULL, 0},
    {"SCHEMA", Query_for_list_of_schemas, NULL, 0},
    {"SEQUENCE", NULL, &Query_for_list_of_sequences, 0},
    {"SERVER", Query_for_list_of_servers, NULL, 0},
    {"SUBSCRIPTION", NULL, NULL, 0},
    {"TABLE", NULL, &Query_for_list_of_tables, 0},
    {"TABLESPACE", Query_for_list_of_tablespaces, NULL, 0},
    {"TEMP", NULL, NULL, THING_NO_DROP}, /* for CREATE TEMP TABLE ... */
    {"TEMPLATE", Query_for_list_of_ts_templates, NULL, THING_NO_SHOW},
    {"TEMPORARY", NULL, NULL, THING_NO_DROP},
    {"TEXT SEARCH", NULL, NULL, 0},
    {"TRIGGER",
        "SELECT pg_catalog.quote_ident(tgname) FROM pg_catalog.pg_trigger WHERE "
        "substring(pg_catalog.quote_ident(tgname),1,%d)='%s'",
        NULL,
        0},
    {"TRUSTED", NULL, NULL, THING_NO_DROP},
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
 * Common routine for CreateCommandGenerator and DropCommandGenerator.
 * Entries that have 'excluded' flags are not returned.
 */
static char *create_or_DropCommandGenerator(const char *text, int state, bits32 excluded)
{
    static int listIndex, stringLength;
    const char *name = NULL;

    /* If this is the first time for this completion, init some values */
    if (state == 0) {
        listIndex = 0;
        stringLength = strlen(text);
    }

    /* find something that matches */
    while ((name = words_after_create[listIndex++].name)) {
        if ((pg_strncasecmp(name, text, stringLength) == 0) && !(words_after_create[listIndex - 1].flags & excluded))
            return pg_strdup_keyword_case(name, text);
    }
    /* if nothing matches, return NULL */
    return NULL;
}

/*
 * This one gives you one from a list of things you can put after CREATE
 * as defined above.
 */
static char *CreateCommandGenerator(const char *text, int state)
{
    return create_or_DropCommandGenerator(text, state, THING_NO_CREATE);
}

/*
 * This function gives you a list of things you can put after a DROP command.
 */
static char *DropCommandGenerator(const char *text, int state)
{
    return create_or_DropCommandGenerator(text, state, THING_NO_DROP);
}

/* The following two functions are wrappers for _CompleteFromQuery */
static char *CompleteFromQuery(const char *text, int state)
{
    return _CompleteFromQuery(0, text, state);
}

static char *CompleteFromSchemaQuery(const char *text, int state)
{
    return _CompleteFromQuery(1, text, state);
}
/*
 * The completion function.
 *
 * According to readline spec this gets passed the text entered so far and its
 * start and end positions in the readline buffer. The return value is some
 * partially obscure list format that can be generated by readline's
 * completion_matches() function, so we don't have to worry about it.
 */
static char** PsqlCompletion(const char *text, int start, int end)
{
    /* This is the variable we'll return. */
    char **matches = NULL;

    /* This array will contain some scannage of the input line. */
    char *previousWords[8];

    /* For compactness, we use these macros to reference previous_words[]. */
#define PREV_WD (previousWords[0])
#define PREV2_WD (previousWords[1])
#define PREV3_WD (previousWords[2])
#define PREV4_WD (previousWords[3])
#define PREV5_WD (previousWords[4])
#define PREV6_WD (previousWords[5])
#define PREV7_WD (previousWords[6])
#define PREV8_WD (previousWords[7])

    static const char* const sqlCommands[] = {
        "ABORT", "ALTER", "ANALYSE", "ANALYZE", "BEGIN", "CALL", "CHECKPOINT", "CLOSE", "CLUSTER",
        "COMMENT", "COMMIT", "COPY", "CREATE", "CURSOR", "DEALLOCATE", "DECLARE",
        "DELETE FROM", "DISCARD", "DO", "DROP", "END", "EXECUTE", "EXPLAIN", "FETCH",
        "GRANT", "INSERT", "LISTEN", "LOAD", "LOCK", "MOVE", "NOTIFY", "PREPARE",
        "REASSIGN", "REFRESH", "REINDEX", "RELEASE", "RESET", "REVOKE", "ROLLBACK",
        "SAVEPOINT", "SECURITY LABEL", "SELECT", "SET", "SHOW", "START",
        "TABLE", "TRUNCATE", "UNLISTEN", "UPDATE", "VACUUM", "VALUES", "WITH",
        NULL
    };

    static const char* const backslashCommands[] = {
        "\\a",
        "\\connect", "\\conninfo", "\\C", "\\cd", "\\copy", "\\copyright",
        "\\d", "\\da", "\\db", "\\dc", "\\dC", "\\dd", "\\ddp", "\\dD", "\\ded",
        "\\des", "\\det", "\\deu", "\\dew", "\\dE", "\\df", "\\dF", "\\dFd", 
        "\\dFp", "\\dFt", "\\dg", "\\di", "\\dl", "\\dL", "\\dn", "\\do", "\\dO",
        "\\dp", "\\drds", "\\ds", "\\dt", "\\dT", "\\dv", "\\du", "\\dx",
        "\\e", "\\echo", "\\ef", "\\encoding",
        "\\f",
        "\\g",
        "\\h", "\\help", "\\H",
        "\\i", "\\i+", "\\ir", "\\ir+",
        "\\l", "\\lo_import", "\\lo_export", "\\lo_list", "\\lo_unlink",
        "\\o",
        "\\p", "\\parallel", "\\prompt", "\\pset",
        "\\q", "\\qecho",
        "\\r",
        "\\set", "\\setenv", "\\sf",
        "\\t", "\\T", "\\timing",
        "\\unset",
        "\\x",
        "\\w",
        "\\z",
        "\\!", "\\?",
        NULL
    };

    (void)end; /* not used */

#ifdef HAVE_RL_COMPLETION_APPEND_CHARACTER
    rl_completion_append_character = ' ';
#endif

    /* Clear a few things. */
    completion_charp = NULL;
    completion_charpp = NULL;
    completion_info_charp = NULL;
    completion_info_charp2 = NULL;

    /*
     * Scan the input line before our current position for the last few words.
     * According to those we'll make some smart decisions on what the user is
     * probably intending to type.
     */
    GetPreviousWords(start, previousWords, lengthof(previousWords));

    /* If a backslash command was started, continue */
    if (text[0] == '\\') {
        COMPLETE_WITH_LIST_CS(backslashCommands);
    }

    /* Variable interpolation */
    else if (text[0] == ':' && text[1] != ':') {
        if (text[1] == '\'') {
            matches = CompleteFromVariables(text, ":'", "'");
        }
        else if (text[1] == '"') {
            matches = CompleteFromVariables(text, ":\"", "\"");
        }
        else {
            matches = CompleteFromVariables(text, ":", "");
        }
    }

    /* If no previous word, suggest one of the basic sql commands */
    else if (PREV_WD[0] == '\0') {
        COMPLETE_WITH_LIST(sqlCommands);
    }

    /* CREATE */
    /* complete with something you can create */
    else if (pg_strcasecmp(PREV_WD, "CREATE") == 0 && pg_strcasecmp(PREV2_WD, "GRANT") != 0 &&
             pg_strcasecmp(PREV2_WD, "REVOKE") != 0)
        matches = completion_matches(text, CreateCommandGenerator);

    /* complete with something you can create or replace */
    else if (pg_strcasecmp(PREV3_WD, "CREATE") == 0 && pg_strcasecmp(PREV2_WD, "OR") == 0 &&
             pg_strcasecmp(PREV_WD, "REPLACE") == 0) {
        static const char* const listCreateOrReplace[] = {"FUNCTION", "PROCEDURE", "LANGUAGE", "RULE", "VIEW",
                                                          "AGGREGATE", "TRANSFORM", "TRIGGER", NULL};
        COMPLETE_WITH_LIST(listCreateOrReplace);
    }

    else if ((pg_strcasecmp(PREV4_WD, "CREATE") == 0 && pg_strcasecmp(PREV3_WD, "DATABASE") == 0 &&
             pg_strcasecmp(PREV_WD, "OWNER") == 0) || (pg_strcasecmp(PREV5_WD, "CREATE") == 0 &&
             pg_strcasecmp(PREV4_WD, "DATABASE") == 0 && pg_strcasecmp(PREV2_WD, "OWNER") == 0 && 
             pg_strcasecmp(PREV_WD, "=") == 0)) {
        COMPLETE_WITH_QUERY(Query_for_list_of_roles);
    }

    /* complete CREATE COLUMN ENCRYPTION KEY <name> with WITH VALUES ( */
    else if (pg_strcasecmp(PREV5_WD, "CREATE") == 0 && pg_strcasecmp(PREV4_WD, "COLUMN") == 0 &&
             pg_strcasecmp(PREV3_WD, "ENCRYPTION") == 0 && pg_strcasecmp(PREV2_WD, "KEY") == 0) {
        COMPLETE_WITH_CONST("WITH VALUES (");
    }

    /* complete CREATE COLUMN ENCRYPTION KEY <name> WITH VALUES ( with CLIENT_MASTER_KEY = */
    else if (pg_strcasecmp(PREV8_WD, "CREATE") == 0 && pg_strcasecmp(PREV7_WD, "COLUMN") == 0 &&
             pg_strcasecmp(PREV6_WD, "ENCRYPTION") == 0 && pg_strcasecmp(PREV5_WD, "KEY") == 0 &&
             pg_strcasecmp(PREV3_WD, "WITH") == 0 && pg_strcasecmp(PREV2_WD, "VALUES") == 0 &&
             pg_strcasecmp(PREV_WD, "(") == 0) {
        COMPLETE_WITH_CONST("CLIENT_MASTER_KEY =");
    }

    /* complete CREATE COLUMN ENCRYPTION KEY <name> WITH VALUES ( CLIENT_MASTER_KEY = <name>, with ALGORITHM  = */
    else if (pg_strcasecmp(PREV4_WD, "CLIENT_MASTER_KEY") == 0 && pg_strcasecmp(PREV3_WD, "=") == 0 &&
             pg_strcasecmp(PREV_WD, ",") == 0) {
        COMPLETE_WITH_CONST("ALGORITHM  =");
    }
    else if (pg_strcasecmp(PREV8_WD, "CLIENT_MASTER_KEY") == 0 && pg_strcasecmp(PREV4_WD, "ALGORITHM") == 0 &&
             pg_strcasecmp(PREV3_WD, "=") == 0 && pg_strcasecmp(PREV_WD, ",") == 0) {
        COMPLETE_WITH_CONST("ENCRYPTED_VALUE =");
    }

    /* complete CREATE CLIENT MASTER KEY <name> with WITH ( */
    else if (pg_strcasecmp(PREV5_WD, "CREATE") == 0 && pg_strcasecmp(PREV4_WD, "CLIENT") == 0 &&
             pg_strcasecmp(PREV3_WD, "MASTER") == 0 && pg_strcasecmp(PREV2_WD, "KEY") == 0) {
        COMPLETE_WITH_CONST("WITH (");
    }

    /* complete CREATE CLIENT MASTER KEY <name> WITH ( with KEY_STORE = */
    else if (pg_strcasecmp(PREV7_WD, "CREATE") == 0 && pg_strcasecmp(PREV6_WD, "CLIENT") == 0 &&
             pg_strcasecmp(PREV5_WD, "MASTER") == 0 && pg_strcasecmp(PREV4_WD, "KEY") == 0 &&
             pg_strcasecmp(PREV2_WD, "WITH") == 0 && pg_strcasecmp(PREV_WD, "(") == 0) {
        COMPLETE_WITH_CONST("KEY_STORE =");
    }
    else if (pg_strcasecmp(PREV4_WD, "KEY_STORE") == 0 && pg_strcasecmp(PREV3_WD, "=") == 0 &&
             pg_strcasecmp(PREV_WD, ",") == 0) {
        COMPLETE_WITH_CONST("KEY_PATH =");
    }
    else if (pg_strcasecmp(PREV4_WD, "KEY_PATH") == 0 && pg_strcasecmp(PREV3_WD, "=") == 0 &&
             pg_strcasecmp(PREV_WD, ",") == 0) {
        COMPLETE_WITH_CONST("ALGORITHM =");
    }

    /* complete with AUTHORIZATION in ALTER SCHEMA xxx */
    else if (pg_strcasecmp(PREV3_WD, "CREATE") == 0 && pg_strcasecmp(PREV2_WD, "SCHEMA") == 0 &&
             pg_strcasecmp(PREV_WD, "AUTHORIZATION") != 0)
        COMPLETE_WITH_CONST("AUTHORIZATION");

    else if (pg_strcasecmp(PREV2_WD, "CREATE") == 0 && pg_strcasecmp(PREV_WD, "SCHEMA") == 0)
        COMPLETE_WITH_QUERY(Query_for_list_of_schemas "UNION SELECT 'AUTHORIZATION'");

    /* DROP, but not DROP embedded in other commands */
    /* complete with something you can drop */
    else if (pg_strcasecmp(PREV_WD, "DROP") == 0 && PREV2_WD[0] == '\0')
        matches = completion_matches(text, DropCommandGenerator);

    else if (pg_strcasecmp(PREV3_WD, "PARTITION") == 0 && pg_strcasecmp(PREV_WD, "VALUES"))
        COMPLETE_WITH_CONST("LESS THAN");

    /* ALTER */

    /*
     * complete with what you can alter (TABLE, GROUP, USER, ...) unless we're
     * in ALTER TABLE sth ALTER
     */
    else if (pg_strcasecmp(PREV_WD, "ALTER") == 0 && pg_strcasecmp(PREV3_WD, "TABLE") != 0) {
        static const char* const listAlter[] = {
            "AGGREGATE", "APP WORKLOAD GROUP", "APP WORKLOAD GROUP MAPPING", "COLLATION", "CONVERSION",
            "DATABASE", "DEFAULT PRIVILEGES", "DOMAIN", "EXTENSION", "FOREIGN DATA WRAPPER",
            "FOREIGN TABLE", "FUNCTION", "GROUP", "INDEX", "LANGUAGE", "LARGE OBJECT",
            "MATERIALIZED VIEW", "OPERATOR", "POLICY", "PROCEDURE", "PUBLICATION", "ROLE", "ROW LEVEL SECURITY POLICY",
            "RULE", "SCHEMA", "SERVER", "SESSION", "SEQUENCE", "SUBSCRIPTION", "SYSTEM", "TABLE", "TABLESPACE",
            "TEXT SEARCH", "TRIGGER", "TYPE", "USER", "USER MAPPING FOR", "VIEW", NULL
        };

        COMPLETE_WITH_LIST(listAlter);
    }
    /* ALTER AGGREGATE,FUNCTION,PROCEDURE <name> */
    else if (pg_strcasecmp(PREV3_WD, "ALTER") == 0 && (pg_strcasecmp(PREV2_WD, "AGGREGATE") == 0 ||
             pg_strcasecmp(PREV2_WD, "FUNCTION") == 0 || pg_strcasecmp(PREV2_WD, "PROCEDURE") == 0))
        COMPLETE_WITH_CONST("(");
    /* ALTER AGGREGATE,FUNCTION <name> (...) */
    else if (pg_strcasecmp(PREV4_WD, "ALTER") == 0 && (pg_strcasecmp(PREV3_WD, "AGGREGATE") == 0 ||
             pg_strcasecmp(PREV3_WD, "FUNCTION") == 0 || pg_strcasecmp(PREV2_WD, "PROCEDURE") == 0)) {
        if (PREV_WD[strlen(PREV_WD) - 1] == ')') {
            static const char* const listAlterAgg[] = {"OWNER TO", "RENAME TO", "SET SCHEMA", NULL};

            COMPLETE_WITH_LIST(listAlterAgg);
        } else {
            size_t tmpLength = strlen(Query_for_list_of_arguments) + strlen(PREV2_WD);
            char *tmpBuf = (char *)pg_malloc(tmpLength);

            int rc = sprintf_s(tmpBuf, tmpLength, Query_for_list_of_arguments, PREV2_WD);
            securec_check_ss_c(rc, "", "");
            COMPLETE_WITH_QUERY(tmpBuf);
            free(tmpBuf);
        }
    }
    /* ALTER PUBLICATION <name> ... */
    else if (pg_strcasecmp(PREV2_WD, "ALTER") == 0 && pg_strcasecmp(PREV_WD, "PUBLICATION") == 0) {
        static const char * const listAlterPub[] = {"ADD TABLE", "DROP TABLE", "OWNER TO", "RENAME TO", "SET", NULL};
        COMPLETE_WITH_LIST(listAlterPub);
    }
    /* ALTER PUBLICATION <name> SET */
    else if (pg_strcasecmp(PREV4_WD, "ALTER") == 0 && pg_strcasecmp(PREV3_WD, "PUBLICATION") == 0 &&
        pg_strcasecmp(PREV_WD, "SET") == 0) {
        static const char * const listAlterPubSet[] = {"(", "TABLE", NULL};
        COMPLETE_WITH_LIST(listAlterPubSet);
    }
    /* ALTER PUBLICATION <name> SET ( */
    else if (pg_strcasecmp(PREV5_WD, "ALTER") == 0 && pg_strcasecmp(PREV4_WD, "PUBLICATION") == 0 &&
        pg_strcasecmp(PREV2_WD, "SET") == 0 && pg_strcasecmp(PREV_WD, "(") == 0) {
        static const char * const listAlterPubSet2[] = {"publish", NULL};
        COMPLETE_WITH_LIST(listAlterPubSet2);
    }
    /* ALTER SUBSCRIPTION <name> */
    else if (pg_strcasecmp(PREV2_WD, "ALTER") == 0 && pg_strcasecmp(PREV_WD, "SUBSCRIPTION") == 0) {
        static const char * const listAlterSub[] = {"CONNECTION", "ENABLE", "DISABLE", "OWNER TO", "RENAME TO",
            "SET", NULL};
        COMPLETE_WITH_LIST(listAlterSub);
    }
    /* ALTER SUBSCRIPTION <name> SET */
    else if (pg_strcasecmp(PREV4_WD, "ALTER") == 0 && pg_strcasecmp(PREV3_WD, "SUBSCRIPTION") == 0 &&
        pg_strcasecmp(PREV_WD, "SET") == 0) {
        static const char * const listAlterSubSet[] = {"(", "PUBLICATION", NULL};
        COMPLETE_WITH_LIST(listAlterSubSet);
    }
    /* ALTER SUBSCRIPTION <name> SET ( */
    else if (pg_strcasecmp(PREV5_WD, "ALTER") == 0 && pg_strcasecmp(PREV4_WD, "SUBSCRIPTION") == 0 &&
        pg_strcasecmp(PREV2_WD, "SET") == 0 && pg_strcasecmp(PREV_WD, "(") == 0) {
        static const char * const listAlterSubSet2[] = {"slot_name", "synchronous_commit", NULL};
        COMPLETE_WITH_LIST(listAlterSubSet2);
    }

    /* ALTER SESSION */
    else if (pg_strcasecmp(PREV2_WD, "ALTER") == 0 && pg_strcasecmp(PREV_WD, "SESSION") == 0)
        COMPLETE_WITH_CONST("SET");

    /* ALTER SYSTEM */
    else if (pg_strcasecmp(PREV2_WD, "ALTER") == 0 && pg_strcasecmp(PREV_WD, "SYSTEM") == 0) {
        static const char* const listAlterSystem[] = {"KILL SESSION", "SET", NULL};

        COMPLETE_WITH_LIST(listAlterSystem);
    }

    else if (pg_strcasecmp(PREV3_WD, "ALTER") == 0 && pg_strcasecmp(PREV2_WD, "SYSTEM") == 0 &&
             pg_strcasecmp(PREV_WD, "SET") == 0)
        COMPLETE_WITH_QUERY(Query_for_list_of_alter_system_set_vars);

    /* ALTER SCHEMA <name> */
    else if (pg_strcasecmp(PREV3_WD, "ALTER") == 0 && pg_strcasecmp(PREV2_WD, "SCHEMA") == 0) {
        static const char* const listAlterGen[] = {"OWNER TO", "RENAME TO", NULL};

        COMPLETE_WITH_LIST(listAlterGen);
    }

    /* ALTER COLLATION <name> */
    else if (pg_strcasecmp(PREV3_WD, "ALTER") == 0 && pg_strcasecmp(PREV2_WD, "COLLATION") == 0) {
        static const char* const listAlterGen[] = {"OWNER TO", "RENAME TO", "SET SCHEMA", NULL};

        COMPLETE_WITH_LIST(listAlterGen);
    }

    /* ALTER CONVERSION <name> */
    else if (pg_strcasecmp(PREV3_WD, "ALTER") == 0 && pg_strcasecmp(PREV2_WD, "CONVERSION") == 0) {
        static const char* const listAlterGen[] = {"OWNER TO", "RENAME TO", "SET SCHEMA", NULL};

        COMPLETE_WITH_LIST(listAlterGen);
    }

    /* ALTER DATABASE <name> */
    else if (pg_strcasecmp(PREV3_WD, "ALTER") == 0 && pg_strcasecmp(PREV2_WD, "DATABASE") == 0) {
        static const char* const listAlterDatabase[] = {
            "WITH", "RESET", "SET", "OWNER TO", "RENAME TO", "CONNECTION LIMIT", "ENABLE PRIVATE OBJECT",
            "DISABLE PRIVATE OBJECT", NULL
        };

        COMPLETE_WITH_LIST(listAlterDatabase);
    } else if (pg_strcasecmp(PREV4_WD, "ALTER") == 0 && pg_strcasecmp(PREV3_WD, "DATABASE") == 0 &&
               pg_strcasecmp(PREV_WD, "WITH") == 0) {
        static const char* const listAlterDatabaseWith[] = {
            "CONNECTION LIMIT", "ENABLE PRIVATE OBJECT", "DISABLE PRIVATE OBJECT", NULL
        };

        COMPLETE_WITH_LIST(listAlterDatabaseWith);
    }

    /* ALTER EXTENSION <name> */
    else if (pg_strcasecmp(PREV3_WD, "ALTER") == 0 && pg_strcasecmp(PREV2_WD, "EXTENSION") == 0) {
        static const char* const listAlterExtension[] = {"ADD", "DROP", "UPDATE", "SET SCHEMA", NULL};

        COMPLETE_WITH_LIST(listAlterExtension);
    }

    /* ALTER EXTENSION <name> UPDATE */
    else if (pg_strcasecmp(PREV4_WD, "ALTER") == 0 && pg_strcasecmp(PREV3_WD, "EXTENSION") == 0 &&
            pg_strcasecmp(PREV_WD, "UPDATE") == 0) {
        COMPLETE_WITH_CONST("TO");
    }

    /* ALTER EXTENSION <name> UPDATE TO */
    else if (pg_strcasecmp(PREV5_WD, "ALTER") == 0 && pg_strcasecmp(PREV4_WD, "EXTENSION") == 0 &&
            pg_strcasecmp(PREV2_WD, "UPDATE") == 0 && pg_strcasecmp(PREV_WD, "TO") == 0) {
        completion_info_charp = PREV3_WD;
        COMPLETE_WITH_QUERY(Query_for_list_of_available_extension_versions);
    }

    /* ALTER EXTENSION <name> ADD/DROP */
    else if (pg_strcasecmp(PREV4_WD, "ALTER") == 0 && pg_strcasecmp(PREV3_WD, "EXTENSION") == 0 &&
            (pg_strcasecmp(PREV_WD, "ADD") == 0 || pg_strcasecmp(PREV_WD, "DROP"))) {
        static const char* const listAlterExtensionMemberObject[] = {
            "AGGREGATE", "CAST", "COLLATION", "CONVERSION", "DOMAIN", "EVENT TRIGGER", "FOREIGN DATA WRAPPER",
            "FOREIGN TABLE", "FUNCTION", "MATERIALIZED VIEW", "OPERATOR", "OPERATOR CLASS", "OPERATOR FAMILY ",
            "PROCEDURAL LANGUAGE", "LANGUAGE", "SCHEMA", "SEQUENCE", "SERVER", "TABLE", "TEXT SEARCH CONFIGURATION",
            "TEXT SEARCH DICTIONARY", "TEXT SEARCH PARSER", "TEXT SEARCH TEMPLATE", "TYPE", "VIEW", NULL};

        COMPLETE_WITH_LIST(listAlterExtensionMemberObject);
    }

    /* ALTER FOREIGN */
    else if (pg_strcasecmp(PREV2_WD, "ALTER") == 0 && pg_strcasecmp(PREV_WD, "FOREIGN") == 0) {
        static const char* const listAlterForeign[] = {"DATA WRAPPER", "TABLE", NULL};

        COMPLETE_WITH_LIST(listAlterForeign);
    }

    /* ALTER FOREIGN DATA WRAPPER <name> */
    else if (pg_strcasecmp(PREV5_WD, "ALTER") == 0 && pg_strcasecmp(PREV4_WD, "FOREIGN") == 0 &&
             pg_strcasecmp(PREV3_WD, "DATA") == 0 && pg_strcasecmp(PREV2_WD, "WRAPPER") == 0) {
        static const char* const listAlterFdw[] = {"HANDLER", "VALIDATOR", "OPTIONS", "OWNER TO", NULL};

        COMPLETE_WITH_LIST(listAlterFdw);
    }

    /* ALTER FOREIGN TABLE <name> */
    else if (pg_strcasecmp(PREV4_WD, "ALTER") == 0 && pg_strcasecmp(PREV3_WD, "FOREIGN") == 0 &&
             pg_strcasecmp(PREV2_WD, "TABLE") == 0) {
        static const char* const listAlterForeignTable[] = {"ALTER", "DROP", "RENAME", "OWNER TO", "SET SCHEMA", NULL};

        COMPLETE_WITH_LIST(listAlterForeignTable);
    }

    /* ALTER INDEX <name> */
    else if (pg_strcasecmp(PREV3_WD, "ALTER") == 0 && pg_strcasecmp(PREV2_WD, "INDEX") == 0) {
        static const char* const listAlterindex[] = {"OWNER TO", "RENAME TO", "SET", "RESET", NULL};

        COMPLETE_WITH_LIST(listAlterindex);
    }
    /* ALTER INDEX <name> SET */
    else if (pg_strcasecmp(PREV4_WD, "ALTER") == 0 && pg_strcasecmp(PREV3_WD, "INDEX") == 0 &&
             pg_strcasecmp(PREV_WD, "SET") == 0) {
        static const char* const listAlterindexset[] = {"(", "TABLESPACE", NULL};

        COMPLETE_WITH_LIST(listAlterindexset);
    }
    /* ALTER INDEX <name> RESET */
    else if (pg_strcasecmp(PREV4_WD, "ALTER") == 0 && pg_strcasecmp(PREV3_WD, "INDEX") == 0 &&
             pg_strcasecmp(PREV_WD, "RESET") == 0)
        COMPLETE_WITH_CONST("(");
    /* ALTER INDEX <foo> SET|RESET ( */
    else if (pg_strcasecmp(PREV5_WD, "ALTER") == 0 && pg_strcasecmp(PREV4_WD, "INDEX") == 0 &&
             (pg_strcasecmp(PREV2_WD, "SET") == 0 || pg_strcasecmp(PREV2_WD, "RESET") == 0) &&
             pg_strcasecmp(PREV_WD, "(") == 0) {
        static const char* const listIndexOptions[] = {"fillfactor", "fastupdate", "deduplication", NULL};

        COMPLETE_WITH_LIST(listIndexOptions);
    }

    /* ALTER LANGUAGE <name> */
    else if (pg_strcasecmp(PREV3_WD, "ALTER") == 0 && pg_strcasecmp(PREV2_WD, "LANGUAGE") == 0) {
        static const char* const listAlterLanguage[] = {"OWNER TO", "RENAME TO", NULL};

        COMPLETE_WITH_LIST(listAlterLanguage);
    }

    /* ALTER LARGE OBJECT <oid> */
    else if (pg_strcasecmp(PREV4_WD, "ALTER") == 0 && pg_strcasecmp(PREV3_WD, "LARGE") == 0 &&
             pg_strcasecmp(PREV2_WD, "OBJECT") == 0) {
        static const char* const listAlterLargeObject[] = {"OWNER TO", NULL};

        COMPLETE_WITH_LIST(listAlterLargeObject);
    }

    /* ALTER MATERIALIZED VIEW */
    else if (pg_strcasecmp(PREV3_WD, "ALTER") == 0 && pg_strcasecmp(PREV2_WD, "MATERIALIZED") == 0 &&
             pg_strcasecmp(PREV2_WD, "VIEW") == 0) {
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_matviews, NULL);
    }

    /* ALTER USER,ROLE <name> */
    else if (pg_strcasecmp(PREV3_WD, "ALTER") == 0 &&
             !(pg_strcasecmp(PREV2_WD, "USER") == 0 && pg_strcasecmp(PREV_WD, "MAPPING") == 0) &&
             (pg_strcasecmp(PREV2_WD, "USER") == 0 || pg_strcasecmp(PREV2_WD, "ROLE") == 0)) {
        static const char* const listAlterUser[] = {
            "ACCOUNT", "AUDITADMIN", "CONNECTION LIMIT", "CREATEDB",
            "CREATEROLE", "CREATEUSER", "ENCRYPTED", "IDENT", "IDENTIFIED BY",
            "INDEPENDENT", "INHERIT", "LOGIN", "MONADMIN", "NOAUDITADMIN",
            "NOCREATEDB", "NOCREATEROLE", "NOCREATEUSER", "NODE GROUP",
            "NOINDEPENDENT", "NOINHERIT", "NOLOGIN", "NOMONADMIN", "NOOPRADMIN",
            "NOPERSISTENCE", "NOPOLADMIN", "NOREPLICATION",
            "NOSYSADMIN", "NOUSEFT", "NOVCADMIN", "OPRADMIN", "PASSWORD",
            "PERM SPACE", "PERSISTENCE", "PGUSER", "POLADMIN", "RENAME TO",
            "REPLICATION", "RESET", "RESOURCE POOL", "SET", "SPILL SPACE",
            "SYSADMIN", "TEMP SPACE", "UNENCRYPTED", "USEFT",
            "USER GROUP", "VALID", "VCADMIN", "WITH", NULL
        };

        COMPLETE_WITH_LIST(listAlterUser);
    }

    /* ALTER USER,ROLE <name> VALID*/
    else if (pg_strcasecmp(PREV4_WD, "ALTER") == 0 &&
             !(pg_strcasecmp(PREV3_WD, "USER") == 0 && pg_strcasecmp(PREV2_WD, "MAPPING") == 0) &&
             (pg_strcasecmp(PREV3_WD, "USER") == 0 || pg_strcasecmp(PREV3_WD, "ROLE") == 0) &&
             pg_strcasecmp(PREV_WD, "VALID") == 0) {
        static const char* const listAlterUserValid[] = {"BEGIN", "UNTIL", NULL};

        COMPLETE_WITH_LIST(listAlterUserValid);
    }

    /* ALTER USER,ROLE <name> WITH */
    else if ((pg_strcasecmp(PREV4_WD, "ALTER") == 0 &&
             (pg_strcasecmp(PREV3_WD, "USER") == 0 || pg_strcasecmp(PREV3_WD, "ROLE") == 0) &&
             pg_strcasecmp(PREV_WD, "WITH") == 0)) {
        /* Similar to the above, but don't complete "WITH" again. */
        static const char* const listAlterUserWith[] = {
            "ACCOUNT", "AUDITADMIN", "CONNECTION LIMIT", "CREATEDB",
            "CREATEROLE", "CREATEUSER", "ENCRYPTED", "IDENT", "IDENTIFIED BY",
            "INDEPENDENT", "INHERIT", "LOGIN", "MONADMIN", "NOAUDITADMIN",
            "NOCREATEDB", "NOCREATEROLE", "NOCREATEUSER", "NODE GROUP",
            "NOINDEPENDENT", "NOINHERIT", "NOLOGIN", "NOMONADMIN", "NOOPRADMIN",
            "NOPERSISTENCE", "NOPOLADMIN", "NOREPLICATION",
            "NOSYSADMIN", "NOUSEFT", "NOVCADMIN", "OPRADMIN", "PASSWORD",
            "PERM SPACE", "PERSISTENCE", "PGUSER", "POLADMIN", "RENAME TO",
            "REPLICATION", "RESET", "RESOURCE POOL", "SET", "SPILL SPACE",
            "SYSADMIN", "TEMP SPACE", "UNENCRYPTED", "USEFT",
            "USER GROUP", "VALID", "VCADMIN", NULL
        };

        COMPLETE_WITH_LIST(listAlterUserWith);
    }

    /* ALTER USER,ROLE <name> WITH VALID*/
    else if ((pg_strcasecmp(PREV5_WD, "ALTER") == 0 &&
             (pg_strcasecmp(PREV4_WD, "USER") == 0 || pg_strcasecmp(PREV4_WD, "ROLE") == 0) &&
             pg_strcasecmp(PREV2_WD, "WITH") == 0) && pg_strcasecmp(PREV_WD, "VALID") == 0) {
        /* Similar to the above, but don't complete "WITH" again. */
        static const char* const listAlterUserWithValid[] = {"BEGIN", "UNTIL", NULL};

        COMPLETE_WITH_LIST(listAlterUserWithValid);
    }

    /* ALTER USER,ROLE <name> ENCRYPTED,UNENCRYPTED */
    else if (pg_strcasecmp(PREV4_WD, "ALTER") == 0 &&
             (pg_strcasecmp(PREV3_WD, "ROLE") == 0 || pg_strcasecmp(PREV3_WD, "USER") == 0) &&
             (pg_strcasecmp(PREV_WD, "ENCRYPTED") == 0 || pg_strcasecmp(PREV_WD, "UNENCRYPTED") == 0)) {
        static const char* const listAlterUserEncrypted[] = {"IDENTIFIED BY", "PASSWORD", NULL};
        COMPLETE_WITH_LIST(listAlterUserEncrypted);
    }

    /* ALTER USER,ROLE <name> WITH ENCRYPTED,UNENCRYPTED */
    else if (pg_strcasecmp(PREV5_WD, "ALTER") == 0 && (pg_strcasecmp(PREV4_WD, "ROLE") == 0 ||
             pg_strcasecmp(PREV4_WD, "USER") == 0) && pg_strcasecmp(PREV2_WD, "WITH") == 0 &&
             (pg_strcasecmp(PREV_WD, "ENCRYPTED") == 0 || pg_strcasecmp(PREV_WD, "UNENCRYPTED") == 0)) {
        static const char* const listAlterUserWithEncrypted[] = {"IDENTIFIED BY", "PASSWORD", NULL};
        COMPLETE_WITH_LIST(listAlterUserWithEncrypted);
    }

    /* ALTER DEFAULT PRIVILEGES */
    else if (pg_strcasecmp(PREV3_WD, "ALTER") == 0 && pg_strcasecmp(PREV2_WD, "DEFAULT") == 0 &&
             pg_strcasecmp(PREV_WD, "PRIVILEGES") == 0) {
        static const char* const listAlterDefaultPrivileges[] = {"FOR ROLE", "FOR USER", "IN SCHEMA", NULL};

        COMPLETE_WITH_LIST(listAlterDefaultPrivileges);
    }
    /* ALTER DEFAULT PRIVILEGES FOR */
    else if (pg_strcasecmp(PREV4_WD, "ALTER") == 0 && pg_strcasecmp(PREV3_WD, "DEFAULT") == 0 &&
             pg_strcasecmp(PREV2_WD, "PRIVILEGES") == 0 && pg_strcasecmp(PREV_WD, "FOR") == 0) {
        static const char* const listAlterDefaultPrivileges_FOR[] = {"ROLE", "USER", NULL};

        COMPLETE_WITH_LIST(listAlterDefaultPrivileges_FOR);
    }
    /* ALTER DEFAULT PRIVILEGES { FOR ROLE ... | IN SCHEMA ... } */
    else if (pg_strcasecmp(PREV5_WD, "DEFAULT") == 0 && pg_strcasecmp(PREV4_WD, "PRIVILEGES") == 0 &&
             (pg_strcasecmp(PREV3_WD, "FOR") == 0 || pg_strcasecmp(PREV3_WD, "IN") == 0)) {
        static const char* const listAlterDefaultPrivilegesRest[] = {"GRANT", "REVOKE", NULL};

        COMPLETE_WITH_LIST(listAlterDefaultPrivilegesRest);
    }
    /* ALTER DOMAIN <name> */
    else if (pg_strcasecmp(PREV3_WD, "ALTER") == 0 && pg_strcasecmp(PREV2_WD, "DOMAIN") == 0) {
        static const char* const listAlterDomain[] = {
            "ADD", "DROP", "OWNER TO", "RENAME", "SET", "VALIDATE CONSTRAINT", NULL
        };

        COMPLETE_WITH_LIST(listAlterDomain);
    }
    /* ALTER DOMAIN <sth> DROP */
    else if (pg_strcasecmp(PREV4_WD, "ALTER") == 0 && pg_strcasecmp(PREV3_WD, "DOMAIN") == 0 &&
             pg_strcasecmp(PREV_WD, "DROP") == 0) {
        static const char* const listAlterDomain2[] = {"CONSTRAINT", "DEFAULT", "NOT NULL", NULL};

        COMPLETE_WITH_LIST(listAlterDomain2);
    }
    /* ALTER DOMAIN <sth> RENAME */
    else if (pg_strcasecmp(PREV4_WD, "ALTER") == 0 && pg_strcasecmp(PREV3_WD, "DOMAIN") == 0 &&
             pg_strcasecmp(PREV_WD, "RENAME") == 0) {
        static const char* const listAlterDomain[] = {"CONSTRAINT", "TO", NULL};

        COMPLETE_WITH_LIST(listAlterDomain);
    }
    /* ALTER DOMAIN <sth> RENAME CONSTRAINT <sth> */
    else if (pg_strcasecmp(PREV5_WD, "DOMAIN") == 0 && pg_strcasecmp(PREV3_WD, "RENAME") == 0 &&
             pg_strcasecmp(PREV2_WD, "CONSTRAINT") == 0)
        COMPLETE_WITH_CONST("TO");

    /* ALTER DOMAIN <sth> SET */
    else if (pg_strcasecmp(PREV4_WD, "ALTER") == 0 && pg_strcasecmp(PREV3_WD, "DOMAIN") == 0 &&
             pg_strcasecmp(PREV_WD, "SET") == 0) {
        static const char* const listAlterDomain3[] = {"DEFAULT", "NOT NULL", "SCHEMA", NULL};

        COMPLETE_WITH_LIST(listAlterDomain3);
    }
    /* ALTER SEQUENCE <name> */
    else if (pg_strcasecmp(PREV3_WD, "ALTER") == 0 && pg_strcasecmp(PREV2_WD, "SEQUENCE") == 0) {
        static const char* const listAlterSequence[] = {
            "MAXVALUE", "NO MAXVALUE", "NOMAXVALUE",  "OWNED BY", "OWNER TO", NULL
        };

        COMPLETE_WITH_LIST(listAlterSequence);
    }

    /* ALTER SERVER <name> */
    else if (pg_strcasecmp(PREV3_WD, "ALTER") == 0 && pg_strcasecmp(PREV2_WD, "SERVER") == 0) {
        static const char* const listAlterServer[] = {
            "VERSION", "OPTIONS", "OWNER TO", "RENAME TO", NULL
        };

        COMPLETE_WITH_LIST(listAlterServer);
    }

    /* ALTER MATERIALIZED VIEW <name> */
    else if (pg_strcasecmp(PREV4_WD, "ALTER") == 0 && pg_strcasecmp(PREV3_WD, "MATERIALIZED") == 0 &&
             pg_strcasecmp(PREV2_WD, "VIEW") == 0) {
        static const char *const listAlterMatview[] = {
            "ALTER COLUMN", "OWNER TO", "RENAME", "RESET (", "SET", NULL};

        COMPLETE_WITH_LIST(listAlterMatview);
    }
    /* ALTER MATERIALIZED VIEW xxx RENAME */
    else if (pg_strcasecmp(PREV5_WD, "ALTER") == 0 && pg_strcasecmp(PREV4_WD, "MATERIALIZED") == 0 &&
             pg_strcasecmp(PREV3_WD, "VIEW") == 0 && pg_strcasecmp(PREV_WD, "RENAME") == 0)
        COMPLETE_WITH_ATTR(PREV2_WD, " UNION SELECT 'COLUMN' UNION SELECT 'TO'");
    else if (pg_strcasecmp(PREV6_WD, "ALTER") == 0 && pg_strcasecmp(PREV5_WD, "MATERIALIZED") == 0 &&
             pg_strcasecmp(PREV4_WD, "VIEW") == 0 && (pg_strcasecmp(PREV2_WD, "ALTER") == 0 || 
             pg_strcasecmp(PREV2_WD, "RENAME") == 0) && pg_strcasecmp(PREV_WD, "COLUMN") == 0)
        COMPLETE_WITH_ATTR(PREV3_WD, "");
    /* ALTER MATERIALIZED VIEW xxx RENAME yyy */
    else if (pg_strcasecmp(PREV6_WD, "ALTER") == 0 && pg_strcasecmp(PREV5_WD, "MATERIALIZED") == 0 &&
             pg_strcasecmp(PREV4_WD, "VIEW") == 0 && pg_strcasecmp(PREV2_WD, "RENAME") == 0 && 
             pg_strcasecmp(PREV_WD, "TO") != 0)
        COMPLETE_WITH_CONST("TO");
    /* ALTER MATERIALIZED VIEW xxx RENAME COLUMN yyy */
    else if (pg_strcasecmp(PREV7_WD, "ALTER") == 0 && pg_strcasecmp(PREV6_WD, "MATERIALIZED") == 0 &&
             pg_strcasecmp(PREV5_WD, "VIEW") == 0 && pg_strcasecmp(PREV3_WD, "RENAME") == 0 && 
             pg_strcasecmp(PREV2_WD, "COLUMN") == 0 && pg_strcasecmp(PREV_WD, "TO") != 0)
        COMPLETE_WITH_CONST("TO");
    /* ALTER MATERIALIZED VIEW xxx SET */
    else if (pg_strcasecmp(PREV5_WD, "ALTER") == 0 && pg_strcasecmp(PREV4_WD, "MATERIALIZED") == 0 &&
             pg_strcasecmp(PREV3_WD, "VIEW") == 0 && pg_strcasecmp(PREV_WD, "SET") == 0) {
        static const char *const listAlterS[] = {
            "(", "SCHEMA", "TABLESPACE", "WITHOUT CLUSTER", NULL};

        COMPLETE_WITH_LIST(listAlterS);
    }

    /* ALTER VIEW <name> */
    else if (pg_strcasecmp(PREV3_WD, "ALTER") == 0 && pg_strcasecmp(PREV2_WD, "VIEW") == 0) {
        static const char* const listAlterview[] = {"ALTER COLUMN", "OWNER TO", "RENAME TO", "SET SCHEMA", NULL};

        COMPLETE_WITH_LIST(listAlterview);
    }

    /* ALTER [ROW LEVEL SECURITY] POLICY <name> on <table> */
    else if ((pg_strcasecmp(PREV8_WD, "ALTER") == 0 || pg_strcasecmp(PREV5_WD, "ALTER") == 0) &&
             pg_strcasecmp(PREV4_WD, "POLICY") == 0 && pg_strcasecmp(PREV2_WD, "ON") == 0) {
        static const char* const listAlterPolicy[] = {"RENAME TO", "TO", "USING (", NULL};

        COMPLETE_WITH_LIST(listAlterPolicy);
    }

    /* ALTER TRIGGER <name>, add ON */
    else if (pg_strcasecmp(PREV3_WD, "ALTER") == 0 && pg_strcasecmp(PREV2_WD, "TRIGGER") == 0)
        COMPLETE_WITH_CONST("ON");

    else if (pg_strcasecmp(PREV4_WD, "ALTER") == 0 && pg_strcasecmp(PREV3_WD, "TRIGGER") == 0) {
        completion_info_charp = PREV2_WD;
        COMPLETE_WITH_QUERY(Query_for_list_of_tables_for_trigger);
    }

    /*
     * If we have ALTER TRIGGER <sth> ON, then add the correct tablename
     */
    else if (pg_strcasecmp(PREV4_WD, "ALTER") == 0 && pg_strcasecmp(PREV3_WD, "TRIGGER") == 0 &&
             pg_strcasecmp(PREV_WD, "ON") == 0)
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_tables, NULL);

    /* ALTER TRIGGER <name> ON <name> */
    else if (pg_strcasecmp(PREV5_WD, "ALTER") == 0 && pg_strcasecmp(PREV4_WD, "TRIGGER") == 0 &&
             pg_strcasecmp(PREV2_WD, "ON") == 0)
        COMPLETE_WITH_CONST("RENAME TO");

    /*
     * If we detect ALTER TABLE <name>, suggest sub commands
     */
    else if (pg_strcasecmp(PREV3_WD, "ALTER") == 0 && pg_strcasecmp(PREV2_WD, "TABLE") == 0) {
        static const char* const listAlteR2[] = {
            "ADD", "ALTER", "CLUSTER ON", "DISABLE", "DROP", "ENABLE", "INHERIT",
            "NO INHERIT", "RENAME", "RESET", "OWNER TO", "SET",
            "VALIDATE CONSTRAINT", NULL
        };

        COMPLETE_WITH_LIST(listAlteR2);
    }
    /* ALTER TABLE xxx ENABLE */
    else if (pg_strcasecmp(PREV4_WD, "ALTER") == 0 && pg_strcasecmp(PREV3_WD, "TABLE") == 0 &&
             pg_strcasecmp(PREV_WD, "ENABLE") == 0) {
        static const char* const listAlterEnable[] = {"ALWAYS", "REPLICA", "RULE", "TRIGGER", NULL};

        COMPLETE_WITH_LIST(listAlterEnable);
    } else if (pg_strcasecmp(PREV4_WD, "TABLE") == 0 && pg_strcasecmp(PREV2_WD, "ENABLE") == 0 &&
               (pg_strcasecmp(PREV_WD, "REPLICA") == 0 || pg_strcasecmp(PREV_WD, "ALWAYS") == 0)) {
        static const char* const listAlterEnablE2[] = {"RULE", "TRIGGER", NULL};

        COMPLETE_WITH_LIST(listAlterEnablE2);
    } else if (pg_strcasecmp(PREV4_WD, "ALTER") == 0 && pg_strcasecmp(PREV3_WD, "TABLE") == 0 &&
               pg_strcasecmp(PREV_WD, "DISABLE") == 0) {
        static const char* const listAlterDisable[] = {"RULE", "TRIGGER", NULL};

        COMPLETE_WITH_LIST(listAlterDisable);
    }

    /* ALTER TABLE xxx ALTER */
    else if (pg_strcasecmp(PREV4_WD, "ALTER") == 0 && pg_strcasecmp(PREV3_WD, "TABLE") == 0 &&
             pg_strcasecmp(PREV_WD, "ALTER") == 0)
        COMPLETE_WITH_ATTR(PREV2_WD, " UNION SELECT 'COLUMN'");

    /* ALTER TABLE xxx RENAME */
    else if (pg_strcasecmp(PREV4_WD, "ALTER") == 0 && pg_strcasecmp(PREV3_WD, "TABLE") == 0 &&
             pg_strcasecmp(PREV_WD, "RENAME") == 0)
        COMPLETE_WITH_ATTR(PREV2_WD, " UNION SELECT 'COLUMN' UNION SELECT 'CONSTRAINT' UNION SELECT 'TO'");

    /*
     * If we have TABLE <sth> ALTER COLUMN|RENAME COLUMN, provide list of
     * columns
     */
    else if (pg_strcasecmp(PREV4_WD, "TABLE") == 0 &&
             (pg_strcasecmp(PREV2_WD, "ALTER") == 0 || pg_strcasecmp(PREV2_WD, "RENAME") == 0) &&
             pg_strcasecmp(PREV_WD, "COLUMN") == 0)
        COMPLETE_WITH_ATTR(PREV3_WD, "");

    /* ALTER TABLE xxx RENAME yyy */
    else if (pg_strcasecmp(PREV4_WD, "TABLE") == 0 && pg_strcasecmp(PREV2_WD, "RENAME") == 0 &&
             pg_strcasecmp(PREV_WD, "CONSTRAINT") != 0 && pg_strcasecmp(PREV_WD, "TO") != 0)
        COMPLETE_WITH_CONST("TO");

    /* ALTER TABLE xxx RENAME COLUMN/CONSTRAINT yyy */
    else if (pg_strcasecmp(PREV5_WD, "TABLE") == 0 && pg_strcasecmp(PREV3_WD, "RENAME") == 0 &&
             (pg_strcasecmp(PREV2_WD, "COLUMN") == 0 || pg_strcasecmp(PREV2_WD, "CONSTRAINT") == 0) &&
             pg_strcasecmp(PREV_WD, "TO") != 0)
        COMPLETE_WITH_CONST("TO");

    /* If we have TABLE <sth> DROP, provide COLUMN or CONSTRAINT */
    else if (pg_strcasecmp(PREV3_WD, "TABLE") == 0 && pg_strcasecmp(PREV_WD, "DROP") == 0) {
        static const char* const listTableDrop[] = {"COLUMN", "CONSTRAINT", NULL};

        COMPLETE_WITH_LIST(listTableDrop);
    }
    /* If we have TABLE <sth> DROP COLUMN, provide list of columns */
    else if (pg_strcasecmp(PREV4_WD, "TABLE") == 0 && pg_strcasecmp(PREV2_WD, "DROP") == 0 &&
             pg_strcasecmp(PREV_WD, "COLUMN") == 0)
        COMPLETE_WITH_ATTR(PREV3_WD, "");
    /* ALTER TABLE ALTER [COLUMN] <foo> */
    else if ((pg_strcasecmp(PREV3_WD, "ALTER") == 0 && pg_strcasecmp(PREV2_WD, "COLUMN") == 0) ||
             (pg_strcasecmp(PREV4_WD, "TABLE") == 0 && pg_strcasecmp(PREV2_WD, "ALTER") == 0)) {
        static const char* const listColumnAlter[] = {"TYPE", "SET", "RESET", "DROP", NULL};

        COMPLETE_WITH_LIST(listColumnAlter);
    }
    /* ALTER TABLE ALTER [COLUMN] <foo> SET */
    else if (((pg_strcasecmp(PREV4_WD, "ALTER") == 0 && pg_strcasecmp(PREV3_WD, "COLUMN") == 0) ||
             (pg_strcasecmp(PREV5_WD, "TABLE") == 0 && pg_strcasecmp(PREV3_WD, "ALTER") == 0)) &&
             pg_strcasecmp(PREV_WD, "SET") == 0) {
        static const char* const listColumnSet[] = {"(", "DEFAULT", "NOT NULL", "STATISTICS", "STORAGE", NULL};

        COMPLETE_WITH_LIST(listColumnSet);
    }
    /* ALTER TABLE ALTER [COLUMN] <foo> SET ( */
    else if (((pg_strcasecmp(PREV5_WD, "ALTER") == 0 && pg_strcasecmp(PREV4_WD, "COLUMN") == 0) ||
             pg_strcasecmp(PREV4_WD, "ALTER") == 0) &&
             pg_strcasecmp(PREV2_WD, "SET") == 0 && pg_strcasecmp(PREV_WD, "(") == 0) {
        static const char* const listColumnOptions[] = {"n_distinct", "n_distinct_inherited", NULL};

        COMPLETE_WITH_LIST(listColumnOptions);
    }
    /* ALTER TABLE ALTER [COLUMN] <foo> SET STORAGE */
    else if (((pg_strcasecmp(PREV5_WD, "ALTER") == 0 && pg_strcasecmp(PREV4_WD, "COLUMN") == 0) ||
             pg_strcasecmp(PREV4_WD, "ALTER") == 0) &&
             pg_strcasecmp(PREV2_WD, "SET") == 0 && pg_strcasecmp(PREV_WD, "STORAGE") == 0) {
        static const char* const listColumnStorage[] = {"PLAIN", "EXTERNAL", "EXTENDED", "MAIN", NULL};

        COMPLETE_WITH_LIST(listColumnStorage);
    }
    /* ALTER TABLE ALTER [COLUMN] <foo> DROP */
    else if (((pg_strcasecmp(PREV4_WD, "ALTER") == 0 && pg_strcasecmp(PREV3_WD, "COLUMN") == 0) ||
             (pg_strcasecmp(PREV5_WD, "TABLE") == 0 && pg_strcasecmp(PREV3_WD, "ALTER") == 0)) &&
             pg_strcasecmp(PREV_WD, "DROP") == 0) {
        static const char* const listColumnDrop[] = {"DEFAULT", "NOT NULL", NULL};

        COMPLETE_WITH_LIST(listColumnDrop);
    } else if (pg_strcasecmp(PREV3_WD, "TABLE") == 0 && pg_strcasecmp(PREV_WD, "CLUSTER") == 0)
        COMPLETE_WITH_CONST("ON");
    else if (pg_strcasecmp(PREV4_WD, "TABLE") == 0 && pg_strcasecmp(PREV2_WD, "CLUSTER") == 0 &&
             pg_strcasecmp(PREV_WD, "ON") == 0) {
        completion_info_charp = PREV3_WD;
        COMPLETE_WITH_QUERY(Query_for_index_of_table);
    }
    /* If we have TABLE <sth> SET, provide WITHOUT,TABLESPACE and SCHEMA */
    else if (pg_strcasecmp(PREV3_WD, "TABLE") == 0 && pg_strcasecmp(PREV_WD, "SET") == 0) {
        static const char* const listTableSet[] = {"(", "WITHOUT", "TABLESPACE", "SCHEMA", NULL};

        COMPLETE_WITH_LIST(listTableSet);
    }
    /* If we have TABLE <sth> SET TABLESPACE provide a list of tablespaces */
    else if (pg_strcasecmp(PREV4_WD, "TABLE") == 0 && pg_strcasecmp(PREV2_WD, "SET") == 0 &&
             pg_strcasecmp(PREV_WD, "TABLESPACE") == 0)
        COMPLETE_WITH_QUERY(Query_for_list_of_tablespaces);
    /* If we have TABLE <sth> SET WITHOUT provide CLUSTER or OIDS */
    else if (pg_strcasecmp(PREV4_WD, "TABLE") == 0 && pg_strcasecmp(PREV2_WD, "SET") == 0 &&
             pg_strcasecmp(PREV_WD, "WITHOUT") == 0) {
        static const char* const listTableSet2[] = {"CLUSTER", "OIDS", NULL};

        COMPLETE_WITH_LIST(listTableSet2);
    }
    /* ALTER TABLE <foo> RESET */
    else if (pg_strcasecmp(PREV3_WD, "TABLE") == 0 && pg_strcasecmp(PREV_WD, "RESET") == 0)
        COMPLETE_WITH_CONST("(");
    /* ALTER TABLE <foo> SET|RESET ( */
    else if (pg_strcasecmp(PREV4_WD, "TABLE") == 0 &&
             (pg_strcasecmp(PREV2_WD, "SET") == 0 || pg_strcasecmp(PREV2_WD, "RESET") == 0) &&
             pg_strcasecmp(PREV_WD, "(") == 0) {
        static const char* const listTableOptions[] = {
            "autovacuum_analyze_scale_factor",
            "autovacuum_analyze_threshold",
            "autovacuum_enabled",
            "autovacuum_freeze_max_age",
            "autovacuum_freeze_min_age",
            "autovacuum_freeze_table_age",
            "autovacuum_vacuum_cost_delay",
            "autovacuum_vacuum_cost_limit",
            "autovacuum_vacuum_scale_factor",
            "autovacuum_vacuum_threshold",
            "fillfactor",
            "toast.autovacuum_enabled",
            "toast.autovacuum_freeze_max_age",
            "toast.autovacuum_freeze_min_age",
            "toast.autovacuum_freeze_table_age",
            "toast.autovacuum_vacuum_cost_delay",
            "toast.autovacuum_vacuum_cost_limit",
            "toast.autovacuum_vacuum_scale_factor",
            "toast.autovacuum_vacuum_threshold",
            NULL
        };

        COMPLETE_WITH_LIST(listTableOptions);
    }

    /* ALTER TABLESPACE <foo> with RENAME TO, OWNER TO, SET, RESET, RESIZE MAXSIZE */
    else if (pg_strcasecmp(PREV3_WD, "ALTER") == 0 && pg_strcasecmp(PREV2_WD, "TABLESPACE") == 0) {
        static const char* const listAlterTspc[] = {"RENAME TO", "OWNER TO", "SET", "RESET", "RESIZE MAXSIZE", NULL};

        COMPLETE_WITH_LIST(listAlterTspc);
    }
    /* ALTER TABLESPACE <foo> SET|RESET */
    else if (pg_strcasecmp(PREV4_WD, "ALTER") == 0 && pg_strcasecmp(PREV3_WD, "TABLESPACE") == 0 &&
             (pg_strcasecmp(PREV_WD, "SET") == 0 || pg_strcasecmp(PREV_WD, "RESET") == 0))
        COMPLETE_WITH_CONST("(");
    /* ALTER TABLESPACE <foo> SET|RESET ( */
    else if (pg_strcasecmp(PREV5_WD, "ALTER") == 0 && pg_strcasecmp(PREV4_WD, "TABLESPACE") == 0 &&
             (pg_strcasecmp(PREV2_WD, "SET") == 0 || pg_strcasecmp(PREV2_WD, "RESET") == 0) &&
             pg_strcasecmp(PREV_WD, "(") == 0) {
        static const char* const listTablespaceOptions[] = {"seq_page_cost", "random_page_cost", NULL};

        COMPLETE_WITH_LIST(listTablespaceOptions);
    }

    /* ALTER TEXT SEARCH */
    else if (pg_strcasecmp(PREV3_WD, "ALTER") == 0 && pg_strcasecmp(PREV2_WD, "TEXT") == 0 &&
             pg_strcasecmp(PREV_WD, "SEARCH") == 0) {
        static const char* const listAlterTextSearch[] = {"CONFIGURATION", "DICTIONARY", "PARSER", "TEMPLATE", NULL};

        COMPLETE_WITH_LIST(listAlterTextSearch);
    } else if (pg_strcasecmp(PREV5_WD, "ALTER") == 0 && pg_strcasecmp(PREV4_WD, "TEXT") == 0 &&
               pg_strcasecmp(PREV3_WD, "SEARCH") == 0 &&
               (pg_strcasecmp(PREV2_WD, "TEMPLATE") == 0 || pg_strcasecmp(PREV2_WD, "PARSER") == 0)) {
        static const char* const listAlterTextSearcH2[] = {"RENAME TO", "SET SCHEMA", NULL};

        COMPLETE_WITH_LIST(listAlterTextSearcH2);
    } else if (pg_strcasecmp(PREV5_WD, "ALTER") == 0 && pg_strcasecmp(PREV4_WD, "TEXT") == 0 &&
               pg_strcasecmp(PREV3_WD, "SEARCH") == 0 && pg_strcasecmp(PREV2_WD, "DICTIONARY") == 0) {
        static const char* const listAlterTextSearch3[] = {"OWNER TO", "RENAME TO", "SET SCHEMA", NULL};

        COMPLETE_WITH_LIST(listAlterTextSearch3);
    } else if (pg_strcasecmp(PREV5_WD, "ALTER") == 0 && pg_strcasecmp(PREV4_WD, "TEXT") == 0 &&
               pg_strcasecmp(PREV3_WD, "SEARCH") == 0 && pg_strcasecmp(PREV2_WD, "CONFIGURATION") == 0) {
        static const char* const listAlterTextSearch4[] = {
            "ADD MAPPING FOR", "ALTER MAPPING", "DROP MAPPING FOR", "OWNER TO", "RENAME TO", "SET SCHEMA", NULL
        };

        COMPLETE_WITH_LIST(listAlterTextSearch4);
    }

    /* complete ALTER TYPE <foo> with actions */
    else if (pg_strcasecmp(PREV3_WD, "ALTER") == 0 && pg_strcasecmp(PREV2_WD, "TYPE") == 0) {
        static const char* const listAlterType[] = {
            "ADD ATTRIBUTE", "ADD VALUE", "ALTER ATTRIBUTE", "DROP ATTRIBUTE",
            "OWNER TO", "RENAME", "SET SCHEMA", NULL
        };

        COMPLETE_WITH_LIST(listAlterType);
    }
    /* complete ALTER TYPE <foo> ADD with actions */
    else if (pg_strcasecmp(PREV4_WD, "ALTER") == 0 && pg_strcasecmp(PREV3_WD, "TYPE") == 0 &&
             pg_strcasecmp(PREV_WD, "ADD") == 0) {
        static const char* const listAlterType[] = {"ATTRIBUTE", "VALUE", NULL};

        COMPLETE_WITH_LIST(listAlterType);
    }
    /* ALTER TYPE <foo> RENAME	*/
    else if (pg_strcasecmp(PREV4_WD, "ALTER") == 0 && pg_strcasecmp(PREV3_WD, "TYPE") == 0 &&
             pg_strcasecmp(PREV_WD, "RENAME") == 0) {
        static const char* const listAlterType[] = {"ATTRIBUTE", "TO", NULL};

        COMPLETE_WITH_LIST(listAlterType);
    }
    /* ALTER TYPE xxx RENAME ATTRIBUTE yyy */
    else if (pg_strcasecmp(PREV5_WD, "TYPE") == 0 && pg_strcasecmp(PREV3_WD, "RENAME") == 0 &&
             pg_strcasecmp(PREV2_WD, "ATTRIBUTE") == 0)
        COMPLETE_WITH_CONST("TO");

    /*
     * If we have TYPE <sth> ALTER/DROP/RENAME ATTRIBUTE, provide list of
     * attributes
     */
    else if (pg_strcasecmp(PREV4_WD, "TYPE") == 0 &&
             (pg_strcasecmp(PREV2_WD, "ALTER") == 0 || pg_strcasecmp(PREV2_WD, "DROP") == 0 ||
             pg_strcasecmp(PREV2_WD, "RENAME") == 0) &&
             pg_strcasecmp(PREV_WD, "ATTRIBUTE") == 0)
        COMPLETE_WITH_ATTR(PREV3_WD, "");
    /* ALTER TYPE ALTER ATTRIBUTE <foo> */
    else if ((pg_strcasecmp(PREV3_WD, "ALTER") == 0 && pg_strcasecmp(PREV2_WD, "ATTRIBUTE") == 0)) {
        COMPLETE_WITH_CONST("TYPE");
    }
    /* complete ALTER GROUP <foo> */
    else if (pg_strcasecmp(PREV3_WD, "ALTER") == 0 && pg_strcasecmp(PREV2_WD, "GROUP") == 0) {
        static const char* const listAlterGroup[] = {"ADD USER", "DROP USER", "RENAME TO", NULL};

        COMPLETE_WITH_LIST(listAlterGroup);
    }
    /* complete ALTER GROUP <foo> ADD|DROP with USER */
    else if (pg_strcasecmp(PREV4_WD, "ALTER") == 0 && pg_strcasecmp(PREV3_WD, "GROUP") == 0 &&
             (pg_strcasecmp(PREV_WD, "ADD") == 0 || pg_strcasecmp(PREV_WD, "DROP") == 0))
        COMPLETE_WITH_CONST("USER");
    /* complete {ALTER} GROUP <foo> ADD|DROP USER with a user name */
    else if (pg_strcasecmp(PREV4_WD, "GROUP") == 0 &&
             (pg_strcasecmp(PREV2_WD, "ADD") == 0 || pg_strcasecmp(PREV2_WD, "DROP") == 0) &&
             pg_strcasecmp(PREV_WD, "USER") == 0)
        COMPLETE_WITH_QUERY(Query_for_list_of_roles);

    /* BEGIN, END, ABORT */
    else if (pg_strcasecmp(PREV_WD, "BEGIN") == 0 || pg_strcasecmp(PREV_WD, "END") == 0 ||
             pg_strcasecmp(PREV_WD, "ABORT") == 0) {
        static const char* const listTrans[] = {"WORK", "TRANSACTION", NULL};

        COMPLETE_WITH_LIST(listTrans);
    }
    /* COMMIT */
    else if (pg_strcasecmp(PREV_WD, "COMMIT") == 0) {
        static const char* const listCommit[] = {"WORK", "TRANSACTION", "PREPARED", NULL};

        COMPLETE_WITH_LIST(listCommit);
    }
    /* RELEASE SAVEPOINT */
    else if (pg_strcasecmp(PREV_WD, "RELEASE") == 0)
        COMPLETE_WITH_CONST("SAVEPOINT");
    /* ROLLBACK */
    else if (pg_strcasecmp(PREV_WD, "ROLLBACK") == 0) {
        static const char* const listTrans[] = {"WORK", "TRANSACTION", "TO SAVEPOINT", "PREPARED", NULL};

        COMPLETE_WITH_LIST(listTrans);
    }
    /* CLUSTER */

    /*
     * If the previous word is CLUSTER and not without produce list of tables
     */
    else if (pg_strcasecmp(PREV_WD, "CLUSTER") == 0 && pg_strcasecmp(PREV2_WD, "WITHOUT") != 0)
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_tm, NULL);
    /* If we have CLUSTER <sth>, then add "USING" */
    else if (pg_strcasecmp(PREV2_WD, "CLUSTER") == 0 && pg_strcasecmp(PREV_WD, "ON") != 0) {
        COMPLETE_WITH_CONST("USING");
    }

    /*
     * If we have CLUSTER <sth> USING, then add the index as well.
     */
    else if (pg_strcasecmp(PREV3_WD, "CLUSTER") == 0 && pg_strcasecmp(PREV_WD, "USING") == 0) {
        completion_info_charp = PREV2_WD;
        COMPLETE_WITH_QUERY(Query_for_index_of_table);
    }

    /* COMMENT */
    else if (pg_strcasecmp(PREV_WD, "COMMENT") == 0)
        COMPLETE_WITH_CONST("ON");
    else if (pg_strcasecmp(PREV2_WD, "COMMENT") == 0 && pg_strcasecmp(PREV_WD, "ON") == 0) {
        static const char* const listComment[] = {
            "CAST", "COLLATION", "CONVERSION", "DATABASE", "EXTENSION",
            "FOREIGN DATA WRAPPER", "FOREIGN TABLE",
            "SERVER", "INDEX", "LANGUAGE", "RULE", "SCHEMA", "SEQUENCE",
            "TABLE", "TYPE", "VIEW", "MATERIALIZED VIEW", "COLUMN", "AGGREGATE", "FUNCTION",
            "OPERATOR", "TRIGGER", "CONSTRAINT", "DOMAIN", "LARGE OBJECT",
            "TABLESPACE", "TEXT SEARCH", "ROLE", NULL
        };

        COMPLETE_WITH_LIST(listComment);
    } else if (pg_strcasecmp(PREV3_WD, "COMMENT") == 0 && pg_strcasecmp(PREV2_WD, "ON") == 0 &&
               pg_strcasecmp(PREV_WD, "FOREIGN") == 0) {
        static const char* const listTrans2[] = {"DATA WRAPPER", "TABLE", NULL};

        COMPLETE_WITH_LIST(listTrans2);
    } else if (pg_strcasecmp(PREV4_WD, "COMMENT") == 0 && pg_strcasecmp(PREV3_WD, "ON") == 0 &&
               pg_strcasecmp(PREV2_WD, "TEXT") == 0 && pg_strcasecmp(PREV_WD, "SEARCH") == 0) {
        static const char* const listTrans2[] = {"CONFIGURATION", "DICTIONARY", "PARSER", "TEMPLATE", NULL};

        COMPLETE_WITH_LIST(listTrans2);
    } else if (pg_strcasecmp(PREV4_WD, "COMMENT") == 0 && pg_strcasecmp(PREV3_WD, "ON") == 0 &&
               pg_strcasecmp(PREV2_WD, "MATERIALIZED") == 0 && pg_strcasecmp(PREV_WD, "VIEW") == 0) {
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_matviews, NULL);
    } else if ((pg_strcasecmp(PREV4_WD, "COMMENT") == 0 && pg_strcasecmp(PREV3_WD, "ON") == 0) ||
               (pg_strcasecmp(PREV5_WD, "COMMENT") == 0 && pg_strcasecmp(PREV4_WD, "ON") == 0) ||
               (pg_strcasecmp(PREV6_WD, "COMMENT") == 0 && pg_strcasecmp(PREV5_WD, "ON") == 0))
        COMPLETE_WITH_CONST("IS");

    /* COPY */

    /*
     * If we have COPY [BINARY] (which you'd have to type yourself), offer
     * list of tables (Also cover the analogous backslash command)
     */
    else if (pg_strcasecmp(PREV_WD, "COPY") == 0 || pg_strcasecmp(PREV_WD, "\\copy") == 0 ||
             (pg_strcasecmp(PREV2_WD, "COPY") == 0 && pg_strcasecmp(PREV_WD, "BINARY") == 0))
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_tables, NULL);
    /* If we have COPY|BINARY <sth>, complete it with "TO" or "FROM" */
    else if ((pg_strcasecmp(PREV2_WD, "COPY") == 0 || pg_strcasecmp(PREV2_WD, "\\copy") == 0 ||
             pg_strcasecmp(PREV2_WD, "BINARY") == 0) || ((pg_strcasecmp(PREV3_WD, "COPY") == 0 ||
             pg_strcasecmp(PREV3_WD, "\\copy") == 0 || pg_strcasecmp(PREV3_WD, "BINARY") == 0) &&
             PREV_WD[strlen(PREV_WD) - 1] == ')')) {
        static const char* const listFromTo[] = {"FROM", "TO", NULL};

        COMPLETE_WITH_LIST(listFromTo);
    }
    /* If we have COPY|BINARY <sth> FROM|TO, complete with filename */
    else if ((pg_strcasecmp(PREV3_WD, "COPY") == 0 || pg_strcasecmp(PREV3_WD, "\\copy") == 0 ||
             pg_strcasecmp(PREV3_WD, "BINARY") == 0) &&
             (pg_strcasecmp(PREV_WD, "FROM") == 0 || pg_strcasecmp(PREV_WD, "TO") == 0)) {
        completion_charp = "";
        matches = completion_matches(text, CompleteFromFiles);
    }

    else if ((pg_strcasecmp(PREV6_WD, "COPY") == 0 || pg_strcasecmp(PREV6_WD, "\\copy") == 0) &&
             (pg_strcasecmp(PREV4_WD, "TO") == 0 || pg_strcasecmp(PREV4_WD, "FROM") == 0) &&
             pg_strcasecmp(PREV2_WD, "WITH") == 0 && pg_strcasecmp(PREV_WD, "(") == 0) {
        static const char* const listCopyFromToWith[] = {
            "FORMAT", "OIDS", "DELIMITER", "NULL", "HEADER", "FILEHEADER",
            "FREEZE", "QUOTE", "ESCAPE", "EOL", "NOESCAPING", "FORCE_QUOTE",
            "FORCE_NOT_NULL", "ENCODING", "IGNORE_EXTRA_DATA", "FILL_MISSING_FIELDS",
            "COMPATIBLE_ILLEGAL_CHARS", "DATE_FORMAT", "TIME_FORMAT", "TIMESTAMP_FORMAT",
            "SMALLDATETIME_FORMAT", NULL
        };
        COMPLETE_WITH_LIST(listCopyFromToWith);
    }

    /* Handle COPY|BINARY <sth> FROM|TO filename */
    else if ((pg_strcasecmp(PREV4_WD, "COPY") == 0 || pg_strcasecmp(PREV4_WD, "\\copy") == 0 ||
             pg_strcasecmp(PREV4_WD, "BINARY") == 0) &&
             (pg_strcasecmp(PREV2_WD, "FROM") == 0 || pg_strcasecmp(PREV2_WD, "TO") == 0)) {
        static const char* const listCopy[] = {
            "BINARY", "OIDS", "NULL", "CSV", "ENCODING", "HEADER", "FILEHEADER",
            "FREEZE", "FORCE NOT NULL", "FORCE QUOTE", "QUOTE", "ESCAPE",
            "EOL", "IGNORE_EXTRA_DATA", "FILL_MISSING_FIELDS", "FIXED FORMATTER",
            "COMPATIBLE_ILLEGAL_CHARS", "DATE_FORMAT", "TIME_FORMAT", "TIMESTAMP_FORMAT",
            "SMALLDATETIME_FORMAT", "WITH (", "WITHOUT ESCAPING", NULL
        };

        COMPLETE_WITH_LIST(listCopy);
    }

    /* Handle COPY|BINARY <sth> FROM|TO filename CSV */
    else if (pg_strcasecmp(PREV_WD, "CSV") == 0 &&
             (pg_strcasecmp(PREV3_WD, "FROM") == 0 || pg_strcasecmp(PREV3_WD, "TO") == 0)) {
        static const char* const listCsv[] = {"HEADER", "QUOTE", "ESCAPE", "FORCE QUOTE", "FORCE NOT NULL", NULL};

        COMPLETE_WITH_LIST(listCsv);
    }

    /* CREATE CAST */
    else if (pg_strcasecmp(PREV2_WD, "CREATE") == 0 && pg_strcasecmp(PREV_WD, "CAST") == 0) {
        COMPLETE_WITH_CONST("(");
    }
    else if (pg_strcasecmp(PREV4_WD, "CREATE") == 0 && pg_strcasecmp(PREV3_WD, "CAST") == 0 &&
            pg_strcasecmp(PREV2_WD, "(") == 0) {
        COMPLETE_WITH_CONST("AS");
    }
    else if (pg_strcasecmp(PREV6_WD, "CREATE") == 0 && pg_strcasecmp(PREV5_WD, "CAST") == 0 &&
        pg_strcasecmp(PREV4_WD, "(") == 0 && pg_strcasecmp(PREV2_WD, "AS") == 0) {
        COMPLETE_WITH_CONST(")");
    }
    else if (pg_strcasecmp(PREV3_WD, "CREATE") == 0 && pg_strcasecmp(PREV2_WD, "CAST") == 0 &&
            pg_strcasecmp(PREV_WD, "(") != 0) {
        static const char* const listCreateCastOptions[] = {"WITH FUNCTION", "WITHOUT FUNCTION", "WITH INOUT", NULL};
        COMPLETE_WITH_LIST(listCreateCastOptions);
    }

    /* CREATE DATABASE */
    else if (pg_strcasecmp(PREV3_WD, "CREATE") == 0 && pg_strcasecmp(PREV2_WD, "DATABASE") == 0) {
        static const char* const listDatabase[] = {
            "WITH", "OWNER", "TEMPLATE", "ENCODING", "LC_COLLATE", "LC_CTYPE",
            "DBCOMPATIBILITY", "TABLESPACE", "CONNECTION LIMIT", NULL
        };

        COMPLETE_WITH_LIST(listDatabase);
    } else if (pg_strcasecmp(PREV4_WD, "CREATE") == 0 && pg_strcasecmp(PREV3_WD, "DATABASE") == 0 &&
               pg_strcasecmp(PREV_WD, "WITH") == 0) {
        static const char* const listDatabaseWith[] = {
            "OWNER", "TEMPLATE", "ENCODING", "LC_COLLATE", "LC_CTYPE", "DBCOMPATIBILITY", "TABLESPACE",
            "CONNECTION LIMIT", NULL
        };

        COMPLETE_WITH_LIST(listDatabaseWith);
    } else if (pg_strcasecmp(PREV4_WD, "CREATE") == 0 && pg_strcasecmp(PREV3_WD, "DATABASE") == 0 &&
               pg_strcasecmp(PREV_WD, "TEMPLATE") == 0)
        COMPLETE_WITH_QUERY(Query_for_list_of_template_databases);

    /* CREATE EXTENSION */
    /* Complete with available extensions rather than installed ones. */
    else if (pg_strcasecmp(PREV2_WD, "CREATE") == 0 && pg_strcasecmp(PREV_WD, "EXTENSION") == 0)
        COMPLETE_WITH_QUERY(Query_for_list_of_available_extensions);
    /* CREATE EXTENSION <name> */
    else if (pg_strcasecmp(PREV3_WD, "CREATE") == 0 && pg_strcasecmp(PREV2_WD, "EXTENSION") == 0) {
        static const char* const listCreateExtension[] = {"WITH", "SCHEMA", "VERSION", "FROM", NULL};
        COMPLETE_WITH_LIST(listCreateExtension);
    }

    else if (pg_strcasecmp(PREV4_WD, "CREATE") == 0 && pg_strcasecmp(PREV3_WD, "EXTENSION") == 0 &&
             pg_strcasecmp(PREV_WD, "WITH") == 0) {
        static const char* const listCreateExtensionWith[] = {"SCHEMA", "VERSION", "FROM", NULL};
        COMPLETE_WITH_LIST(listCreateExtensionWith);
    }

    /* CREATE FOREIGN */
    else if (pg_strcasecmp(PREV2_WD, "CREATE") == 0 && pg_strcasecmp(PREV_WD, "FOREIGN") == 0) {
        static const char* const listCreateForeign[] = {"DATA WRAPPER", "TABLE", NULL};

        COMPLETE_WITH_LIST(listCreateForeign);
    }

    /* CREATE FOREIGN DATA WRAPPER */
    else if (pg_strcasecmp(PREV5_WD, "CREATE") == 0 && pg_strcasecmp(PREV4_WD, "FOREIGN") == 0 &&
             pg_strcasecmp(PREV3_WD, "DATA") == 0 && pg_strcasecmp(PREV2_WD, "WRAPPER") == 0) {
        static const char* const listCreateForeignDataWrapper[] = {"HANDLER", "VALIDATOR", NULL};

        COMPLETE_WITH_LIST(listCreateForeignDataWrapper);
    }

    /* CREATE INDEX */
    /* First off we complete CREATE UNIQUE with "INDEX" */
    else if (pg_strcasecmp(PREV2_WD, "CREATE") == 0 && pg_strcasecmp(PREV_WD, "UNIQUE") == 0)
        COMPLETE_WITH_CONST("INDEX");
    /* If we have CREATE|UNIQUE INDEX, then add "ON" and existing indexes */
    else if (pg_strcasecmp(PREV_WD, "INDEX") == 0 &&
             (pg_strcasecmp(PREV2_WD, "CREATE") == 0 || pg_strcasecmp(PREV2_WD, "UNIQUE") == 0))
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_indexes, " UNION SELECT 'ON'"
            " UNION SELECT 'CONCURRENTLY'");
    /* Complete ... INDEX [<name>] ON with a list of tables  */
    else if ((pg_strcasecmp(PREV3_WD, "INDEX") == 0 || pg_strcasecmp(PREV2_WD, "INDEX") == 0 ||
             pg_strcasecmp(PREV2_WD, "CONCURRENTLY") == 0) &&
             pg_strcasecmp(PREV_WD, "ON") == 0)
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_tmf, NULL);
    /* If we have CREATE|UNIQUE INDEX <sth> CONCURRENTLY, then add "ON" */
    else if ((pg_strcasecmp(PREV3_WD, "INDEX") == 0 || pg_strcasecmp(PREV2_WD, "INDEX") == 0) &&
             pg_strcasecmp(PREV_WD, "CONCURRENTLY") == 0)
        COMPLETE_WITH_CONST("ON");
    /* If we have CREATE|UNIQUE INDEX <sth>, then add "ON" or "CONCURRENTLY" */
    else if ((pg_strcasecmp(PREV3_WD, "CREATE") == 0 || pg_strcasecmp(PREV3_WD, "UNIQUE") == 0) &&
             pg_strcasecmp(PREV2_WD, "INDEX") == 0) {
        static const char* const listCreateIndex[] = {"CONCURRENTLY", "ON", NULL};

        COMPLETE_WITH_LIST(listCreateIndex);
    }

    /*
     * Complete INDEX <name> ON <table> with a list of table columns (which
     * should really be in parens)
     */
    else if ((pg_strcasecmp(PREV4_WD, "INDEX") == 0 || pg_strcasecmp(PREV3_WD, "INDEX") == 0 ||
             pg_strcasecmp(PREV3_WD, "CONCURRENTLY") == 0) &&
             pg_strcasecmp(PREV2_WD, "ON") == 0) {
        static const char* const listCreateIndex2[] = {"(", "USING", NULL};

        COMPLETE_WITH_LIST(listCreateIndex2);
    } else if ((pg_strcasecmp(PREV5_WD, "INDEX") == 0 || pg_strcasecmp(PREV4_WD, "INDEX") == 0 ||
               pg_strcasecmp(PREV4_WD, "CONCURRENTLY") == 0) &&
               pg_strcasecmp(PREV3_WD, "ON") == 0 && pg_strcasecmp(PREV_WD, "(") == 0)
        COMPLETE_WITH_ATTR(PREV2_WD, "");
    /* same if you put in USING */
    else if (pg_strcasecmp(PREV5_WD, "ON") == 0 && pg_strcasecmp(PREV3_WD, "USING") == 0 &&
             pg_strcasecmp(PREV_WD, "(") == 0)
        COMPLETE_WITH_ATTR(PREV4_WD, "");
    /* Complete USING with an index method */
    else if (pg_strcasecmp(PREV_WD, "USING") == 0)
        COMPLETE_WITH_QUERY(Query_for_list_of_access_methods);
    else if (pg_strcasecmp(PREV4_WD, "ON") == 0 && pg_strcasecmp(PREV2_WD, "USING") == 0 &&
             pg_strcasecmp(PREV6_WD, "POLICY") != 0)
        COMPLETE_WITH_CONST("(");

    /* CREATE OR REPLACE */
    else if (pg_strcasecmp(PREV2_WD, "CREATE") == 0 && pg_strcasecmp(PREV_WD, "OR") == 0)
        COMPLETE_WITH_CONST("REPLACE");

    /* CREATE|DROP|ALTER PROCEDURAL|TRUSTED LANGUAGE */
    else if ((pg_strcasecmp(PREV2_WD, "CREATE") == 0 || pg_strcasecmp(PREV2_WD, "DROP") == 0 ||
             pg_strcasecmp(PREV2_WD, "ALTER") == 0) && (pg_strcasecmp(PREV_WD, "PROCEDURAL") == 0 ||
             pg_strcasecmp(PREV_WD, "TRUSTED") == 0))
        COMPLETE_WITH_CONST("LANGUAGE");

    else if (pg_strcasecmp(PREV3_WD, "CREATE") == 0 && pg_strcasecmp(PREV2_WD, "LANGUAGE") == 0) {
        static const char* const createLanguage[] = {"HANDLER", "INLINE", "VALIDATOR", NULL};
        COMPLETE_WITH_LIST(createLanguage);
    }

    /* CREATE AGGREGATE xxx (input_data_type) ( */
    else if (pg_strcasecmp(PREV5_WD, "CREATE") == 0 && pg_strcasecmp(PREV4_WD, "AGGREGATE") == 0 &&
             PREV2_WD[strlen(PREV2_WD) - 1] == ')' && pg_strcasecmp(PREV_WD, "(") == 0) {
        static const char* const listCreateAggregate[] = {"SFUNC", "STYPE", "FINALFUNC", "INITCOND", "SORTOP", NULL};
        COMPLETE_WITH_LIST(listCreateAggregate);
    }

    /* CREATE AGGREGATE xxx ( */
    else if (pg_strcasecmp(PREV4_WD, "CREATE") == 0 && pg_strcasecmp(PREV3_WD, "AGGREGATE") == 0 &&
             pg_strcasecmp(PREV_WD, "(") == 0) {
        static const char* const listCreateAggregateOld[] = {
            "BASETYPE", "SFUNC", "STYPE", "FINALFUNC", "INITCOND", "SORTOP", NULL
        };
        COMPLETE_WITH_LIST(listCreateAggregateOld);
    }

    /* CREATE TYPE xxx */
    else if (pg_strcasecmp(PREV3_WD, "CREATE") == 0 && pg_strcasecmp(PREV2_WD, "TYPE") == 0) {
        static const char* const listCreateType[] = {"AS", "AS ENUM", NULL};
        COMPLETE_WITH_LIST(listCreateType);
    }

    /* CREATE TYPE xxx ( */
    else if (pg_strcasecmp(PREV4_WD, "CREATE") == 0 && pg_strcasecmp(PREV3_WD, "TYPE") == 0 &&
             pg_strcasecmp(PREV_WD, "(") == 0) {
        static const char* const listCreateTypeArgs[] = {
            "INPUT", "OUTPUT", "RECEIVE", "SEND", "TYPMOD_IN", "TYPMOD_OUT",
            "ANALYZE", "INTERNALLENGTH", "PASSEDBYVALUE", "ALIGNMENT", "STORAGE", 
            "LIKE", "CATEGORY", "PREFERRED", "DEFAULT", "ELEMENT", "DELIMITER",
            "COLLATABLE", NULL
        };
        COMPLETE_WITH_LIST(listCreateTypeArgs);
    }

    /* CREATE OPERATOR xxx ( */
    else if (pg_strcasecmp(PREV4_WD, "CREATE") == 0 && pg_strcasecmp(PREV3_WD, "OPERATOR") == 0 &&
             pg_strcasecmp(PREV_WD, "(") == 0) {
        static const char* const listCreateTypeArgs[] = {
            "PROCEDURE", "LEFTARG", "RIGHTARG", "COMMUTATOR", "NEGATOR", "RESTRICT",
            "JOIN", "HASHES", "MERGES", NULL
        };
        COMPLETE_WITH_LIST(listCreateTypeArgs);
    }

    /* ALTER OPERATOR xxx (...)  */
    else if (pg_strcasecmp(PREV4_WD, "ALTER") == 0 && pg_strcasecmp(PREV3_WD, "OPERATOR") == 0 &&
             PREV_WD[strlen(PREV_WD) - 1] == ')') {
        static const char* const listAlterOperator[] = {"OWNER TO", "SET SCHEMA", NULL};
        COMPLETE_WITH_LIST(listAlterOperator);
    }

    /* CREATE POLICY */
    /* Complete "CREATE ROW LEVEL SECURITY POLICY */
    else if (pg_strcasecmp(PREV4_WD, "CREATE") == 0 && pg_strcasecmp(PREV3_WD, "ROW") == 0 &&
             pg_strcasecmp(PREV2_WD, "LEVEL") == 0 && pg_strcasecmp(PREV_WD, "SECURITY") == 0)
        COMPLETE_WITH_CONST("POLICY");
    /* Complete "CREATE POLICY <name> ON" */
    else if (pg_strcasecmp(PREV3_WD, "CREATE") == 0 && pg_strcasecmp(PREV2_WD, "POLICY") == 0)
        COMPLETE_WITH_CONST("ON");
    /* Complete "CREATE POLICY <name> ON <table>" */
    else if (pg_strcasecmp(PREV4_WD, "CREATE") == 0 && pg_strcasecmp(PREV3_WD, "POLICY") == 0 &&
             pg_strcasecmp(PREV_WD, "ON") == 0)
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_tables, NULL);
    /* Complete "CREATE POLICY <name> ON <table> AS|FOR|TO|USING" */
    else if ((pg_strcasecmp(PREV5_WD, "CREATE") == 0 || pg_strcasecmp(PREV8_WD, "CREATE") == 0) &&
             pg_strcasecmp(PREV4_WD, "POLICY") == 0 && pg_strcasecmp(PREV2_WD, "ON") == 0) {
        static const char* const createPolicyOn[] = {"AS", "FOR", "TO", "USING (", NULL};
        COMPLETE_WITH_LIST(createPolicyOn);
    }
    /* CREATE POLICY <name> ON <table> AS PERMISSIVE|RESTRICTIVE */
    else if (pg_strcasecmp(PREV6_WD, "CREATE") == 0 && pg_strcasecmp(PREV5_WD, "POLICY") == 0 &&
             pg_strcasecmp(PREV3_WD, "ON") == 0 && pg_strcasecmp(PREV_WD, "AS") == 0) {
        static const char* const createPolicyOnAs[] = {"PERMISSIVE", "RESTRICTIVE", NULL};
        COMPLETE_WITH_LIST(createPolicyOnAs);
    }

    /*
     * CREATE POLICY <name> ON <table> AS PERMISSIVE|RESTRICTIVE
     * FOR|TO|USING
     */
    else if (pg_strcasecmp(PREV7_WD, "CREATE") == 0 && pg_strcasecmp(PREV6_WD, "POLICY") == 0 &&
             pg_strcasecmp(PREV4_WD, "ON") == 0 && pg_strcasecmp(PREV2_WD, "AS") == 0) {
        static const char* const createPolicyOnAsSth[] = {"FOR", "TO", "USING", NULL};
        COMPLETE_WITH_LIST(createPolicyOnAsSth);
    }
    /* CREATE POLICY <name> ON <table> FOR ALL|SELECT|INSERT|UPDATE|DELETE */
    else if (pg_strcasecmp(PREV6_WD, "CREATE") == 0 && pg_strcasecmp(PREV5_WD, "POLICY") == 0 &&
             pg_strcasecmp(PREV3_WD, "ON") == 0 && pg_strcasecmp(PREV_WD, "FOR") == 0) {
        static const char* const createPolicyOnFor[] = {"ALL", "SELECT", "INSERT", "UPDATE", "DELETE", NULL};
        COMPLETE_WITH_LIST(createPolicyOnFor);
    }
    /* Complete "CREATE POLICY <name> ON <table> FOR INSERT TO" */
    else if (pg_strcasecmp(PREV7_WD, "CREATE") == 0 && pg_strcasecmp(PREV6_WD, "POLICY") == 0 &&
             pg_strcasecmp(PREV4_WD, "ON") == 0 && pg_strcasecmp(PREV2_WD, "FOR") == 0 &&
             pg_strcasecmp(PREV_WD, "INSERT") == 0)
        COMPLETE_WITH_CONST("TO");
    /* Complete "CREATE POLICY <name> ON <table> FOR SELECT|DELETE|ALL|UPDATE TO|USING" */
    else if (pg_strcasecmp(PREV7_WD, "CREATE") == 0 && pg_strcasecmp(PREV6_WD, "POLICY") == 0 &&
             pg_strcasecmp(PREV4_WD, "ON") == 0 && pg_strcasecmp(PREV2_WD, "FOR") == 0 &&
             (pg_strcasecmp(PREV_WD, "SELECT") == 0 || pg_strcasecmp(PREV_WD, "DELETE") == 0 ||
             pg_strcasecmp(PREV_WD, "ALL") == 0 || pg_strcasecmp(PREV_WD, "UPDATE") == 0)) {
        static const char* const createPolicyOnForSd[] = {"TO", "USING (", NULL};
        COMPLETE_WITH_LIST(createPolicyOnForSd);
    }
    /* Complete "CREATE POLICY <name> ON <table> TO <role>" */
    else if (pg_strcasecmp(PREV6_WD, "CREATE") == 0 && pg_strcasecmp(PREV5_WD, "POLICY") == 0 &&
             pg_strcasecmp(PREV3_WD, "ON") == 0 && pg_strcasecmp(PREV_WD, "TO") == 0) {
        COMPLETE_WITH_QUERY(Query_for_list_of_grant_roles);
    }
    /* Complete "CREATE POLICY <name> ON <table> USING (" */
    else if (pg_strcasecmp(PREV6_WD, "CREATE") == 0 && pg_strcasecmp(PREV5_WD, "POLICY") == 0 &&
             pg_strcasecmp(PREV3_WD, "ON") == 0 && pg_strcasecmp(PREV_WD, "USING") == 0) {
        COMPLETE_WITH_CONST("(");
    }

    /* CREATE PUBLICATION */
    else if (pg_strcasecmp(PREV3_WD, "CREATE") == 0 && pg_strcasecmp(PREV2_WD, "PUBLICATION") == 0) {
        static const char * const createPub[] = {"FOR TABLE", "FOR ALL TABLES", "WITH (", NULL};
        COMPLETE_WITH_LIST(createPub);
    }
    /* CREATE PUBLICATION <name> FOR */
    else if (pg_strcasecmp(PREV4_WD, "CREATE") == 0 && pg_strcasecmp(PREV3_WD, "PUBLICATION") == 0 &&
        pg_strcasecmp(PREV_WD, "FOR") == 0) {
        static const char * const createPub2[] = {"TABLE", "ALL TABLES", NULL};
        COMPLETE_WITH_LIST(createPub2);
    }
    /* CREATE PUBLICATION <name> FOR TABLE <table>, ... */
    else if (pg_strcasecmp(PREV5_WD, "CREATE") == 0 && pg_strcasecmp(PREV4_WD, "PUBLICATION") == 0 &&
        pg_strcasecmp(PREV2_WD, "FOR") == 0 && pg_strcasecmp(PREV_WD, "TABLE") == 0) {
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_tables, NULL);
    }
    /* CREATE SUBSCRIPTION */
    else if (pg_strcasecmp(PREV3_WD, "CREATE") == 0 && pg_strcasecmp(PREV2_WD, "SUBSCRIPTION") == 0) {
        COMPLETE_WITH_CONST("CONNECTION");
    }

    /* CREATE RULE */
    /* Complete "CREATE RULE <sth>" with "AS" */
    else if (pg_strcasecmp(PREV3_WD, "CREATE") == 0 && pg_strcasecmp(PREV2_WD, "RULE") == 0)
        COMPLETE_WITH_CONST("AS");
    /* Complete "CREATE RULE <sth> AS with "ON" */
    else if (pg_strcasecmp(PREV4_WD, "CREATE") == 0 && pg_strcasecmp(PREV3_WD, "RULE") == 0 &&
             pg_strcasecmp(PREV_WD, "AS") == 0)
        COMPLETE_WITH_CONST("ON");
    /* Complete "RULE * AS ON" with SELECT|UPDATE|DELETE|INSERT */
    else if (pg_strcasecmp(PREV4_WD, "RULE") == 0 && pg_strcasecmp(PREV2_WD, "AS") == 0 &&
             pg_strcasecmp(PREV_WD, "ON") == 0) {
        static const char* const ruleEvents[] = {"SELECT", "UPDATE", "INSERT", "DELETE", NULL};

        COMPLETE_WITH_LIST(ruleEvents);
    }
    /* Complete "AS ON <sth with a 'T' :)>" with a "TO" */
    else if (pg_strcasecmp(PREV3_WD, "AS") == 0 && pg_strcasecmp(PREV2_WD, "ON") == 0 &&
             (pg_toupper((unsigned char)PREV_WD[4]) == 'T' || pg_toupper((unsigned char)PREV_WD[5]) == 'T'))
        COMPLETE_WITH_CONST("TO");
    /* Complete "AS ON <sth> TO" with a table name */
    else if (pg_strcasecmp(PREV4_WD, "AS") == 0 && pg_strcasecmp(PREV3_WD, "ON") == 0 &&
             pg_strcasecmp(PREV_WD, "TO") == 0)
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_tables, NULL);

    else if ((pg_strcasecmp(PREV4_WD, "CREATE") == 0 || pg_strcasecmp(PREV3_WD, "CREATE") == 0) &&
             pg_strcasecmp(PREV2_WD, "SEQUENCE") == 0) {
        static const char* const listCreateSequence[] = {
            "INCREMENT", "MINVALUE", "NOMINVALUE", "MAXVALUE", "NOMAXVALUE",
            "START WITH", "NO", "CACHE", "CYCLE", "NOCYCLE", "OWNED BY", NULL
        };

        COMPLETE_WITH_LIST(listCreateSequence);
    }
    else if ((pg_strcasecmp(PREV4_WD, "CREATE") == 0 || pg_strcasecmp(PREV3_WD, "CREATE") == 0) &&
             pg_strcasecmp(PREV3_WD, "SEQUENCE") == 0 && pg_strcasecmp(PREV_WD, "NO") == 0) {
        static const char* const listCreateSequencE2[] = {"MINVALUE", "MAXVALUE", "CYCLE", NULL};

        COMPLETE_WITH_LIST(listCreateSequencE2);
    }

    /* CREATE SERVER <name> */
    else if (pg_strcasecmp(PREV3_WD, "CREATE") == 0 && pg_strcasecmp(PREV2_WD, "SERVER") == 0) {
        static const char* const listCreateServer[] = {"TYPE", "VERSION", "FOREIGN DATA WRAPPER", NULL};

        COMPLETE_WITH_LIST(listCreateServer);
    }

    else if (pg_strcasecmp(PREV7_WD, "CREATE") == 0 && pg_strcasecmp(PREV6_WD, "SERVER") == 0 &&
             pg_strcasecmp(PREV4_WD, "foreign") == 0 && pg_strcasecmp(PREV3_WD, "data") == 0 &&
             pg_strcasecmp(PREV2_WD, "wrapper") == 0)
        COMPLETE_WITH_CONST("OPTIONS");

    /* CREATE TABLE */
    /* Complete "CREATE TEMP/TEMPORARY" with the possible temp objects */
    else if (pg_strcasecmp(PREV2_WD, "CREATE") == 0 &&
             (pg_strcasecmp(PREV_WD, "TEMP") == 0 || pg_strcasecmp(PREV_WD, "TEMPORARY") == 0)) {
        static const char* const listTemp[] = {"SEQUENCE", "TABLE", "VIEW", NULL};

        COMPLETE_WITH_LIST(listTemp);
    }
    /* Complete "CREATE UNLOGGED" with TABLE */
    else if (pg_strcasecmp(PREV2_WD, "CREATE") == 0 && pg_strcasecmp(PREV_WD, "UNLOGGED") == 0) {
        COMPLETE_WITH_CONST("TABLE");
    }

    else if ((pg_strcasecmp(PREV5_WD, "CREATE") == 0 || pg_strcasecmp(PREV4_WD, "CREATE") == 0) &&
             pg_strcasecmp(PREV3_WD, "TABLE") == 0 && PREV_WD[strlen(PREV_WD) - 1] == ')') {
        static const char* const listCreateTable[] = {
            "WITH (", "ON COMMIT", "COMPRESS", "NOCOMPRESS", "TABLESPACE", "PARTITION BY", NULL
        };

        COMPLETE_WITH_LIST(listCreateTable);
    }

    else if ((pg_strcasecmp(PREV7_WD, "CREATE") == 0 || pg_strcasecmp(PREV6_WD, "CREATE") == 0) &&
             pg_strcasecmp(PREV5_WD, "TABLE") == 0 && PREV3_WD[strlen(PREV3_WD) - 1] == ')' &&
             pg_strcasecmp(PREV2_WD, "WITH") == 0 && pg_strcasecmp(PREV_WD, "(") == 0 ) {
        static const char* const listCreateTableWith[] = {
            "FILLFACTOR", "ORIENTATION", "COMPRESSION", "MAX_BATCHROW", "PARTIAL_CLUSTER_ROWS",
            "DELTAROW_THRESHOLD", NULL
        };

        COMPLETE_WITH_LIST(listCreateTableWith);
    }

    /* CREATE TABLESPACE */
    else if (pg_strcasecmp(PREV3_WD, "CREATE") == 0 && pg_strcasecmp(PREV2_WD, "TABLESPACE") == 0) {
        static const char* const listCreateTablespace[] = {"OWNER", "LOCATION", "RELATIVE LOCATION", NULL};

        COMPLETE_WITH_LIST(listCreateTablespace);
    }
    /* Complete CREATE TABLESPACE name OWNER name with "LOCATION" */
    else if (pg_strcasecmp(PREV5_WD, "CREATE") == 0 && pg_strcasecmp(PREV4_WD, "TABLESPACE") == 0 &&
             pg_strcasecmp(PREV2_WD, "OWNER") == 0) {
        static const char* const listCreateTablespaceOwner[] = {"LOCATION", "RELATIVE LOCATION", NULL};
        COMPLETE_WITH_LIST(listCreateTablespaceOwner);
    }

    /* CREATE TEXT SEARCH */
    else if (pg_strcasecmp(PREV3_WD, "CREATE") == 0 && pg_strcasecmp(PREV2_WD, "TEXT") == 0 &&
             pg_strcasecmp(PREV_WD, "SEARCH") == 0) {
        static const char* const listCreateTextSearch[] = {"CONFIGURATION", "DICTIONARY", "PARSER", "TEMPLATE", NULL};

        COMPLETE_WITH_LIST(listCreateTextSearch);
    } else if (pg_strcasecmp(PREV4_WD, "TEXT") == 0 && pg_strcasecmp(PREV3_WD, "SEARCH") == 0 &&
               pg_strcasecmp(PREV2_WD, "CONFIGURATION") == 0)
        COMPLETE_WITH_CONST("(");

    /* CREATE TRIGGER */
    /* complete CREATE TRIGGER <name> with BEFORE,AFTER */
    else if (pg_strcasecmp(PREV3_WD, "CREATE") == 0 && pg_strcasecmp(PREV2_WD, "TRIGGER") == 0) {
        static const char* const listCreateTrigger[] = {"BEFORE", "AFTER", "INSTEAD OF", NULL};

        COMPLETE_WITH_LIST(listCreateTrigger);
    }
    /* complete CREATE TRIGGER <name> BEFORE,AFTER with an event */
    else if (pg_strcasecmp(PREV4_WD, "CREATE") == 0 && pg_strcasecmp(PREV3_WD, "TRIGGER") == 0 &&
             (pg_strcasecmp(PREV_WD, "BEFORE") == 0 || pg_strcasecmp(PREV_WD, "AFTER") == 0)) {
        static const char* const listCreateTriggerEvents[] = {"INSERT", "DELETE", "UPDATE", "TRUNCATE", NULL};

        COMPLETE_WITH_LIST(listCreateTriggerEvents);
    }
    /* complete CREATE TRIGGER <name> INSTEAD OF with an event */
    else if (pg_strcasecmp(PREV5_WD, "CREATE") == 0 && pg_strcasecmp(PREV4_WD, "TRIGGER") == 0 &&
             pg_strcasecmp(PREV2_WD, "INSTEAD") == 0 && pg_strcasecmp(PREV_WD, "OF") == 0) {
        static const char* const listCreateTriggerEvents[] = {"INSERT", "DELETE", "UPDATE", NULL};

        COMPLETE_WITH_LIST(listCreateTriggerEvents);
    }
    /* complete CREATE TRIGGER <name> BEFORE,AFTER sth with OR,ON */
    else if ((pg_strcasecmp(PREV5_WD, "CREATE") == 0 && pg_strcasecmp(PREV4_WD, "TRIGGER") == 0 &&
             (pg_strcasecmp(PREV2_WD, "BEFORE") == 0 || pg_strcasecmp(PREV2_WD, "AFTER") == 0)) ||
             (pg_strcasecmp(PREV5_WD, "TRIGGER") == 0 && pg_strcasecmp(PREV3_WD, "INSTEAD") == 0 &&
             pg_strcasecmp(PREV2_WD, "OF") == 0)) {
        static const char* const listCreateTrigger2[] = {"ON", "OR", NULL};

        COMPLETE_WITH_LIST(listCreateTrigger2);
    }

    /*
     * complete CREATE TRIGGER <name> BEFORE,AFTER event ON with a list of
     * tables
     */
    else if (pg_strcasecmp(PREV5_WD, "TRIGGER") == 0 &&
             (pg_strcasecmp(PREV3_WD, "BEFORE") == 0 || pg_strcasecmp(PREV3_WD, "AFTER") == 0) &&
             pg_strcasecmp(PREV_WD, "ON") == 0)
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_tables, NULL);
    /* complete CREATE TRIGGER ... INSTEAD OF event ON with a list of views */
    else if (pg_strcasecmp(PREV4_WD, "INSTEAD") == 0 && pg_strcasecmp(PREV3_WD, "OF") == 0 &&
             pg_strcasecmp(PREV_WD, "ON") == 0)
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_views, NULL);
    /* complete CREATE TRIGGER ... EXECUTE with PROCEDURE */
    else if (pg_strcasecmp(PREV_WD, "EXECUTE") == 0 && PREV2_WD[0] != '\0')
        COMPLETE_WITH_CONST("DIRECT");

    /* CREATE ROLE,USER,GROUP <name> */
    else if (pg_strcasecmp(PREV3_WD, "CREATE") == 0 &&
             !(pg_strcasecmp(PREV2_WD, "USER") == 0 && pg_strcasecmp(PREV_WD, "MAPPING") == 0) &&
             (pg_strcasecmp(PREV2_WD, "ROLE") == 0 || pg_strcasecmp(PREV2_WD, "GROUP") == 0 ||
             pg_strcasecmp(PREV2_WD, "USER") == 0)) {
        static const char* const listCreateRole[] = {
            "ADMIN", "AUDITADMIN", "CONNECTION LIMIT", "CREATEDB", "CREATEROLE",
            "CREATEUSER", "DEFAULT TABLESPACE", "ENCRYPTED", "IDENTIFIED BY", "IN",
            "IN GROUP", "IN ROLE", "INDEPENDENT", "INHERIT", "LOGIN", "MONADMIN",
            "NOAUDITADMIN", "NOCREATEDB", "NOCREATEROLE", "NOCREATEUSER", "NODE GROUP",
            "NOINDEPENDENT", "NOINHERIT", "NOLOGIN", "NOMONADMIN", "NOOPRADMIN",
            "NOPERSISTENCE", "NOPOLADMIN", "NOREPLICATION", "NOSYSADMIN",
            "NOUSEFT", "NOVCADMIN", "OPRADMIN", "PASSWORD", "PERM SPACE", "PERSISTENCE",
            "PGUSER", "POLADMIN", "PROFILE", "PROFILE DEFAULT", "REPLICATION",
            "RESOURCE POOL", "ROLE", "SPILL SPACE", "SYSADMIN", "SYSID",
            "TEMP SPACE", "UNENCRYPTED", "USEFT", "USER", "USER GROUP", "VALID",
            "VCADMIN", "WITH", NULL
        };

        COMPLETE_WITH_LIST(listCreateRole);
    }

    /* CREATE ROLE,USER,GROUP <name> VALID*/
    else if (pg_strcasecmp(PREV4_WD, "CREATE") == 0 &&
             !(pg_strcasecmp(PREV3_WD, "USER") == 0 && pg_strcasecmp(PREV2_WD, "MAPPING") == 0) &&
             (pg_strcasecmp(PREV3_WD, "ROLE") == 0 || pg_strcasecmp(PREV3_WD, "GROUP") == 0 ||
             pg_strcasecmp(PREV3_WD, "USER") == 0) && pg_strcasecmp(PREV_WD, "VALID") == 0) {
        static const char* const listCreateRoleValid[] = {"BEGIN", "UNTIL", NULL};

        COMPLETE_WITH_LIST(listCreateRoleValid);
    }

    /* CREATE ROLE,USER,GROUP <name> WITH */
    else if ((pg_strcasecmp(PREV4_WD, "CREATE") == 0 &&
             (pg_strcasecmp(PREV3_WD, "ROLE") == 0 || pg_strcasecmp(PREV3_WD, "GROUP") == 0 ||
             pg_strcasecmp(PREV3_WD, "USER") == 0) &&
             pg_strcasecmp(PREV_WD, "WITH") == 0)) {
        /* Similar to the above, but don't complete "WITH" again. */
        static const char* const listCreateRoleWith[] = {
            "ADMIN", "AUDITADMIN", "CONNECTION LIMIT", "CREATEDB", "CREATEROLE",
            "CREATEUSER", "DEFAULT TABLESPACE", "ENCRYPTED", "IDENTIFIED BY", "IN",
            "IN GROUP", "IN ROLE", "INDEPENDENT", "INHERIT", "LOGIN", "MONADMIN",
            "NOAUDITADMIN", "NOCREATEDB", "NOCREATEROLE", "NOCREATEUSER", "NODE GROUP",
            "NOINDEPENDENT", "NOINHERIT", "NOLOGIN", "NOMONADMIN", "NOOPRADMIN",
            "NOPERSISTENCE", "NOPOLADMIN", "NOREPLICATION", "NOSYSADMIN",
            "NOUSEFT", "NOVCADMIN", "OPRADMIN", "PASSWORD", "PERM SPACE", "PERSISTENCE",
            "PGUSER", "POLADMIN", "PROFILE", "PROFILE DEFAULT", "REPLICATION",
            "RESOURCE POOL", "ROLE", "SPILL SPACE", "SYSADMIN", "SYSID",
            "TEMP SPACE", "UNENCRYPTED", "USEFT", "USER", "USER GROUP", "VALID",
            "VCADMIN", NULL
        };

        COMPLETE_WITH_LIST(listCreateRoleWith);
    }

    /* CREATE ROLE,USER,GROUP <name> WITH */
    else if ((pg_strcasecmp(PREV5_WD, "CREATE") == 0 &&
             (pg_strcasecmp(PREV4_WD, "ROLE") == 0 || pg_strcasecmp(PREV4_WD, "GROUP") == 0 ||
             pg_strcasecmp(PREV4_WD, "USER") == 0) &&
             pg_strcasecmp(PREV2_WD, "WITH") == 0) && pg_strcasecmp(PREV_WD, "VALID") == 0) {
        /* Similar to the above, but don't complete "WITH" again. */
        static const char* const listCreateRoleWithValid[] = {"BEGIN", "UNTIL", NULL};

        COMPLETE_WITH_LIST(listCreateRoleWithValid);
    }

    /*
     * complete CREATE ROLE,USER,GROUP <name> ENCRYPTED,UNENCRYPTED with
     * PASSWORD
     */
    else if (pg_strcasecmp(PREV4_WD, "CREATE") == 0 &&
             (pg_strcasecmp(PREV3_WD, "ROLE") == 0 || pg_strcasecmp(PREV3_WD, "GROUP") == 0 ||
             pg_strcasecmp(PREV3_WD, "USER") == 0) &&
             (pg_strcasecmp(PREV_WD, "ENCRYPTED") == 0 || pg_strcasecmp(PREV_WD, "UNENCRYPTED") == 0)) {
        static const char* const listCreateRoleWithEncrypted[] = {"PASSWORD", "IDENTIFIED BY", NULL};
        COMPLETE_WITH_LIST(listCreateRoleWithEncrypted);
    }
    /* complete CREATE ROLE,USER,GROUP <name> IN with ROLE,GROUP */
    else if (pg_strcasecmp(PREV4_WD, "CREATE") == 0 &&
             (pg_strcasecmp(PREV3_WD, "ROLE") == 0 || pg_strcasecmp(PREV3_WD, "GROUP") == 0 ||
             pg_strcasecmp(PREV3_WD, "USER") == 0) &&
             pg_strcasecmp(PREV_WD, "IN") == 0) {
        static const char* const listCreateRole3[] = {"GROUP", "ROLE", NULL};

        COMPLETE_WITH_LIST(listCreateRole3);
    }

    /* CREATE VIEW */
    /* Complete CREATE VIEW <name> with AS */
    else if (pg_strcasecmp(PREV3_WD, "CREATE") == 0 && pg_strcasecmp(PREV2_WD, "VIEW") == 0)
        COMPLETE_WITH_CONST("AS");
    /* Complete "CREATE VIEW <sth> AS with "SELECT" */
    else if (pg_strcasecmp(PREV4_WD, "CREATE") == 0 && pg_strcasecmp(PREV3_WD, "VIEW") == 0 &&
             pg_strcasecmp(PREV_WD, "AS") == 0)
        COMPLETE_WITH_CONST("SELECT");

    /* CREATE MATERIALIZED VIEW */
    else if (pg_strcasecmp(PREV2_WD, "CREATE") == 0 && pg_strcasecmp(PREV_WD, "MATERIALIZED") == 0)
        COMPLETE_WITH_CONST("VIEW");
    /* Complete CREATE MATERIALIZED VIEW <name> with AS|TABLESPACE */
    else if ((pg_strcasecmp(PREV5_WD, "CREATE") == 0 || pg_strcasecmp(PREV4_WD, "CREATE") == 0) &&
             pg_strcasecmp(PREV3_WD, "MATERIALIZED") == 0 && pg_strcasecmp(PREV2_WD, "VIEW") == 0) {
        static const char* const listCreateMaterializedView[] = {"AS", "TABLESPACE", NULL};
        COMPLETE_WITH_LIST(listCreateMaterializedView);
    }
    /* Complete "CREATE MATERIALIZED VIEW <sth> AS with "SELECT" */
    else if (pg_strcasecmp(PREV5_WD, "CREATE") == 0 && pg_strcasecmp(PREV4_WD, "MATERIALIZED") == 0 &&
             pg_strcasecmp(PREV3_WD, "VIEW") == 0 && pg_strcasecmp(PREV_WD, "AS") == 0)
        COMPLETE_WITH_CONST("SELECT");

    /* DECLARE */
    else if (pg_strcasecmp(PREV2_WD, "DECLARE") == 0) {
        static const char* const listDeclare[] = {"BINARY", "INSENSITIVE", "SCROLL", "NO SCROLL", "CURSOR", NULL};

        COMPLETE_WITH_LIST(listDeclare);
    }

    /* CURSOR */
    else if (pg_strcasecmp(PREV2_WD, "CURSOR") == 0) {
        static const char* const listDeclareCursor[] = {
            "BINARY", "SCROLL", "NO SCROLL","INSENSITIVE","FOR", NULL
        };

        COMPLETE_WITH_LIST(listDeclareCursor);
    }

    /* DELETE */

    /*
     * Complete DELETE with FROM (only if the word before that is not "ON"
     * (cf. rules) or "BEFORE" or "AFTER" (cf. triggers) or GRANT)
     */
    else if (pg_strcasecmp(PREV_WD, "DELETE") == 0 &&
             !(pg_strcasecmp(PREV2_WD, "ON") == 0 || pg_strcasecmp(PREV2_WD, "GRANT") == 0 ||
             pg_strcasecmp(PREV2_WD, "BEFORE") == 0 || pg_strcasecmp(PREV2_WD, "AFTER") == 0))
        COMPLETE_WITH_CONST("FROM");
    /* Complete DELETE FROM with a list of tables */
    else if (pg_strcasecmp(PREV2_WD, "DELETE") == 0 && pg_strcasecmp(PREV_WD, "FROM") == 0)
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_deletables, NULL);
    /* Complete DELETE FROM <table> */
    else if (pg_strcasecmp(PREV3_WD, "DELETE") == 0 && pg_strcasecmp(PREV2_WD, "FROM") == 0) {
        static const char* const listDelete[] = {"USING", "WHERE", "SET", NULL};

        COMPLETE_WITH_LIST(listDelete);
    }
    /* XXX: implement tab completion for DELETE ... USING */

    /* DISCARD */
    else if (pg_strcasecmp(PREV_WD, "DISCARD") == 0) {
        static const char* const listDiscard[] = {"ALL", "PLANS", "TEMP", NULL};

        COMPLETE_WITH_LIST(listDiscard);
    }

    /* DO */

    /*
     * Complete DO with LANGUAGE.
     */
    else if (pg_strcasecmp(PREV_WD, "DO") == 0) {
        static const char* const listDo[] = {"LANGUAGE", NULL};

        COMPLETE_WITH_LIST(listDo);
    }

    /* DROP (when not the previous word) */
    /* DROP AGGREGATE */
    else if (pg_strcasecmp(PREV3_WD, "DROP") == 0 && pg_strcasecmp(PREV2_WD, "AGGREGATE") == 0)
        COMPLETE_WITH_CONST("(");

    /* DROP object with CASCADE / RESTRICT */
    else if ((pg_strcasecmp(PREV3_WD, "DROP") == 0 && (pg_strcasecmp(PREV2_WD, "COLLATION") == 0 ||
             pg_strcasecmp(PREV2_WD, "CONVERSION") == 0 || pg_strcasecmp(PREV2_WD, "DOMAIN") == 0 ||
             pg_strcasecmp(PREV2_WD, "EXTENSION") == 0 || pg_strcasecmp(PREV2_WD, "FUNCTION") == 0 ||
             pg_strcasecmp(PREV2_WD, "INDEX") == 0 || pg_strcasecmp(PREV2_WD, "LANGUAGE") == 0 ||
             pg_strcasecmp(PREV2_WD, "PUBLICATION") == 0 ||
             pg_strcasecmp(PREV2_WD, "SCHEMA") == 0 || pg_strcasecmp(PREV2_WD, "SEQUENCE") == 0 ||
             pg_strcasecmp(PREV2_WD, "SERVER") == 0 || pg_strcasecmp(PREV2_WD, "SUBSCRIPTION") == 0 ||
             pg_strcasecmp(PREV2_WD, "TABLE") == 0 ||
             pg_strcasecmp(PREV2_WD, "TYPE") == 0 || pg_strcasecmp(PREV2_WD, "VIEW") == 0 ||
             pg_strcasecmp(PREV2_WD, "USER") == 0)) ||
             (pg_strcasecmp(PREV4_WD, "DROP") == 0 && pg_strcasecmp(PREV3_WD, "AGGREGATE") == 0 &&
             PREV_WD[strlen(PREV_WD) - 1] == ')') ||
             (pg_strcasecmp(PREV5_WD, "DROP") == 0 && pg_strcasecmp(PREV4_WD, "FOREIGN") == 0 &&
             pg_strcasecmp(PREV3_WD, "DATA") == 0 && pg_strcasecmp(PREV2_WD, "WRAPPER") == 0) ||
             (pg_strcasecmp(PREV5_WD, "DROP") == 0 && pg_strcasecmp(PREV4_WD, "TEXT") == 0 &&
             pg_strcasecmp(PREV3_WD, "SEARCH") == 0 &&
             (pg_strcasecmp(PREV2_WD, "CONFIGURATION") == 0 || pg_strcasecmp(PREV2_WD, "DICTIONARY") == 0 ||
             pg_strcasecmp(PREV2_WD, "PARSER") == 0 || pg_strcasecmp(PREV2_WD, "TEMPLATE") == 0))) {
             if (pg_strcasecmp(PREV3_WD, "DROP") == 0 && pg_strcasecmp(PREV2_WD, "FUNCTION") == 0) {
            COMPLETE_WITH_CONST("(");
        } else {
            static const char* const listDropCr[] = {"CASCADE", "RESTRICT", NULL};

            COMPLETE_WITH_LIST(listDropCr);
        }
    } else if (pg_strcasecmp(PREV2_WD, "DROP") == 0 && pg_strcasecmp(PREV_WD, "FOREIGN") == 0) {
        static const char* const dropCreateForeign[] = {"DATA WRAPPER", "TABLE", NULL};

        COMPLETE_WITH_LIST(dropCreateForeign);
    /* DROP MATERIALIZED VIEW */
    } else if (pg_strcasecmp(PREV2_WD, "DROP") == 0 && pg_strcasecmp(PREV_WD, "MATERIALIZED") == 0) {
        COMPLETE_WITH_CONST("VIEW");
    } else if (pg_strcasecmp(PREV3_WD, "DROP") == 0 && pg_strcasecmp(PREV2_WD, "MATERIALIZED") == 0 &&
               pg_strcasecmp(PREV_WD, "VIEW") == 0) {
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_matviews, NULL);
    } else if (pg_strcasecmp(PREV4_WD, "DROP") == 0 &&
               (pg_strcasecmp(PREV3_WD, "AGGREGATE") == 0 || pg_strcasecmp(PREV3_WD, "FUNCTION") == 0) &&
               pg_strcasecmp(PREV_WD, "(") == 0) {
        size_t tmpLength = strlen(Query_for_list_of_arguments) + strlen(PREV2_WD);
        char *tmpBuf = (char *)pg_malloc(tmpLength);

        int rc = sprintf_s(tmpBuf, tmpLength, Query_for_list_of_arguments, PREV2_WD);
        securec_check_ss_c(rc,"","");
        COMPLETE_WITH_QUERY(tmpBuf);
        free(tmpBuf);
    }
    /* DROP OWNED BY */
    else if (pg_strcasecmp(PREV2_WD, "DROP") == 0 && pg_strcasecmp(PREV_WD, "OWNED") == 0)
        COMPLETE_WITH_CONST("BY");
    else if (pg_strcasecmp(PREV3_WD, "DROP") == 0 && pg_strcasecmp(PREV2_WD, "OWNED") == 0 &&
             pg_strcasecmp(PREV_WD, "BY") == 0)
        COMPLETE_WITH_QUERY(Query_for_list_of_roles);
    else if (pg_strcasecmp(PREV3_WD, "DROP") == 0 && pg_strcasecmp(PREV2_WD, "TEXT") == 0 &&
             pg_strcasecmp(PREV_WD, "SEARCH") == 0) {
        static const char* const listAlterTextSearch[] = {"CONFIGURATION", "DICTIONARY", "PARSER", "TEMPLATE", NULL};

        COMPLETE_WITH_LIST(listAlterTextSearch);
    }

    /* DROP TRIGGER */
    else if (pg_strcasecmp(PREV3_WD, "DROP") == 0 && pg_strcasecmp(PREV2_WD, "TRIGGER") == 0)
        COMPLETE_WITH_CONST("ON");
    else if (pg_strcasecmp(PREV4_WD, "DROP") == 0 && pg_strcasecmp(PREV3_WD, "TRIGGER") == 0 &&
             pg_strcasecmp(PREV_WD, "ON") == 0) {
        completion_info_charp = PREV2_WD;
        COMPLETE_WITH_QUERY(Query_for_list_of_tables_for_trigger);
    }
    else if (pg_strcasecmp(PREV5_WD, "DROP") == 0 && pg_strcasecmp(PREV4_WD, "TRIGGER") == 0 &&
             pg_strcasecmp(PREV2_WD, "ON") == 0) {
        static const char* const listDropTriggerOn[] = {"CASCADE", "RESTRICT", NULL};

        COMPLETE_WITH_LIST(listDropTriggerOn);
    }

    /* EXECUTE, but not EXECUTE embedded in other commands */
    else if (pg_strcasecmp(PREV_WD, "EXECUTE") == 0 && PREV2_WD[0] == '\0')
        COMPLETE_WITH_QUERY(Query_for_list_of_prepared_statements);

    /* EXPLAIN */

    /*
     * Complete EXPLAIN [ANALYZE] [VERBOSE] with list of EXPLAIN-able commands
     */
    else if (pg_strcasecmp(PREV_WD, "EXPLAIN") == 0) {
        static const char* const listExplain[] = {
            "SELECT", "INSERT", "DELETE", "UPDATE", "DECLARE", "ANALYZE", "VERBOSE",
            "COSTS", "CPU", "DETAIL", "NODES", "NUM_NODES", "BUFFERS", "TIMING",
            "PLAN", "FORMAT", "ANALYSE", "PERFORMANCE", NULL
        };

        COMPLETE_WITH_LIST(listExplain);
    } else if (pg_strcasecmp(PREV2_WD, "EXPLAIN") == 0 && pg_strcasecmp(PREV_WD, "ANALYZE") == 0) {
        static const char* const listExplain[] = {"SELECT", "INSERT", "DELETE", "UPDATE", "DECLARE", "VERBOSE", NULL};

        COMPLETE_WITH_LIST(listExplain);
    } else if ((pg_strcasecmp(PREV2_WD, "EXPLAIN") == 0 && pg_strcasecmp(PREV_WD, "VERBOSE") == 0) ||
               (pg_strcasecmp(PREV3_WD, "EXPLAIN") == 0 && pg_strcasecmp(PREV2_WD, "ANALYZE") == 0 &&
               pg_strcasecmp(PREV_WD, "VERBOSE") == 0)) {
        static const char* const listExplain[] = {"SELECT", "INSERT", "DELETE", "UPDATE", "DECLARE", NULL};

        COMPLETE_WITH_LIST(listExplain);
    }

    /* FETCH && MOVE */
    /* Complete FETCH with one of FORWARD, BACKWARD, RELATIVE */
    else if (pg_strcasecmp(PREV_WD, "FETCH") == 0 || pg_strcasecmp(PREV_WD, "MOVE") == 0) {
        static const char* const listFetch1[] = {"ABSOLUTE", "BACKWARD", "FORWARD", "RELATIVE", NULL};

        COMPLETE_WITH_LIST(listFetch1);
    }
    /* Complete FETCH <sth> with one of ALL, NEXT, PRIOR */
    else if (pg_strcasecmp(PREV2_WD, "FETCH") == 0 || pg_strcasecmp(PREV2_WD, "MOVE") == 0) {
        static const char* const list_FETCH2[] = {"ALL", "NEXT", "PRIOR", NULL};

        COMPLETE_WITH_LIST(list_FETCH2);
    }

    /*
     * Complete FETCH <sth1> <sth2> with "FROM" or "IN". These are equivalent,
     * but we may as well tab-complete both: perhaps some users prefer one
     * variant or the other.
     */
    else if (pg_strcasecmp(PREV3_WD, "FETCH") == 0 || pg_strcasecmp(PREV3_WD, "MOVE") == 0) {
        static const char* const listFromin[] = {"FROM", "IN", NULL};

        COMPLETE_WITH_LIST(listFromin);
    }

    /* FOREIGN DATA WRAPPER */
    /* applies in ALTER/DROP FDW and in CREATE SERVER */
    else if (pg_strcasecmp(PREV4_WD, "CREATE") != 0 && pg_strcasecmp(PREV3_WD, "FOREIGN") == 0 &&
             pg_strcasecmp(PREV2_WD, "DATA") == 0 && pg_strcasecmp(PREV_WD, "WRAPPER") == 0)
        COMPLETE_WITH_QUERY(Query_for_list_of_fdws);

    /* FOREIGN TABLE */
    else if (pg_strcasecmp(PREV3_WD, "CREATE") != 0 && pg_strcasecmp(PREV2_WD, "FOREIGN") == 0 &&
             pg_strcasecmp(PREV_WD, "TABLE") == 0)
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_foreign_tables, NULL);

    /* GRANT && REVOKE */
    /* Complete GRANT/REVOKE with a list of roles and privileges */
    else if (pg_strcasecmp(PREV_WD, "GRANT") == 0 || pg_strcasecmp(PREV_WD, "REVOKE") == 0) {
        COMPLETE_WITH_QUERY(Query_for_list_of_roles " UNION SELECT 'SELECT'"
            " UNION SELECT 'INSERT'"
            " UNION SELECT 'UPDATE'"
            " UNION SELECT 'DELETE'"
            " UNION SELECT 'TRUNCATE'"
            " UNION SELECT 'REFERENCES'"
            " UNION SELECT 'TRIGGER'"
            " UNION SELECT 'CREATE'"
            " UNION SELECT 'CONNECT'"
            " UNION SELECT 'TEMPORARY'"
            " UNION SELECT 'EXECUTE'"
            " UNION SELECT 'USAGE'"
            " UNION SELECT 'ALL'"
            " UNION SELECT 'ALTER'"
            " UNION SELECT 'DROP'"
            " UNION SELECT 'COMMENT'"
            " UNION SELECT 'INDEX'"
            " UNION SELECT 'VACUUM'"
            " UNION SELECT 'READ'"
            " UNION SELECT 'WRITE'");
    }

    /*
     * Complete GRANT/REVOKE <privilege> with "ON", GRANT/REVOKE <role> with
     * TO/FROM
     */
    else if (pg_strcasecmp(PREV2_WD, "GRANT") == 0 || pg_strcasecmp(PREV2_WD, "REVOKE") == 0) {
        if (pg_strcasecmp(PREV_WD, "SELECT") == 0 || pg_strcasecmp(PREV_WD, "INSERT") == 0 ||
            pg_strcasecmp(PREV_WD, "UPDATE") == 0 || pg_strcasecmp(PREV_WD, "DELETE") == 0 ||
            pg_strcasecmp(PREV_WD, "TRUNCATE") == 0 || pg_strcasecmp(PREV_WD, "REFERENCES") == 0 ||
            pg_strcasecmp(PREV_WD, "TRIGGER") == 0 || pg_strcasecmp(PREV_WD, "CREATE") == 0 ||
            pg_strcasecmp(PREV_WD, "CONNECT") == 0 || pg_strcasecmp(PREV_WD, "TEMPORARY") == 0 ||
            pg_strcasecmp(PREV_WD, "TEMP") == 0 || pg_strcasecmp(PREV_WD, "EXECUTE") == 0 ||
            pg_strcasecmp(PREV_WD, "USAGE") == 0)
            COMPLETE_WITH_CONST("ON");
        else if (pg_strcasecmp(PREV_WD, "ALL") == 0){
            static const char* const listGrantAll[] = {"ON", "PRIVILEGES", NULL};
            COMPLETE_WITH_LIST(listGrantAll);
        }else {
            if (pg_strcasecmp(PREV2_WD, "GRANT") == 0)
                COMPLETE_WITH_CONST("TO");
            else
                COMPLETE_WITH_CONST("FROM");
        }
    }

    else if ((pg_strcasecmp(PREV3_WD, "GRANT") == 0 || pg_strcasecmp(PREV3_WD, "REVOKE") == 0) &&
             pg_strcasecmp(PREV2_WD, "ALL") == 0 && pg_strcasecmp(PREV_WD, "PRIVILEGES") == 0) {
        COMPLETE_WITH_CONST("ON");
    }
    /*
     * Complete GRANT/REVOKE <sth> ON with a list of tables, views, sequences,
     * and indexes
     *
     * keywords DATABASE, FUNCTION, LANGUAGE, SCHEMA added to query result via
     * UNION; seems to work intuitively
     *
     * Note: GRANT/REVOKE can get quite complex; tab-completion as implemented
     * here will only work if the privilege list contains exactly one
     * privilege
     */
    else if ((pg_strcasecmp(PREV3_WD, "GRANT") == 0 || pg_strcasecmp(PREV3_WD, "REVOKE") == 0) &&
             pg_strcasecmp(PREV_WD, "ON") == 0)
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_tsvmf, " UNION SELECT 'DATABASE'"
            " UNION SELECT 'DOMAIN'"
            " UNION SELECT 'FOREIGN DATA WRAPPER'"
            " UNION SELECT 'FOREIGN SERVER'"
            " UNION SELECT 'FUNCTION'"
            " UNION SELECT 'LANGUAGE'"
            " UNION SELECT 'LARGE OBJECT'"
            " UNION SELECT 'SCHEMA'"
            " UNION SELECT 'TABLESPACE'"
            " UNION SELECT 'TYPE'"
            " UNION SELECT 'TABLE'"
            " UNION SELECT 'DIRECTORY'"
            " UNION SELECT 'NODE GROUP'"
            " UNION SELECT 'DATA SOURCE'");
    else if ((pg_strcasecmp(PREV4_WD, "GRANT") == 0 || pg_strcasecmp(PREV4_WD, "REVOKE") == 0) &&
             pg_strcasecmp(PREV2_WD, "ON") == 0 && pg_strcasecmp(PREV_WD, "FOREIGN") == 0) {
        static const char* const listPrivilegeForeign[] = {"DATA WRAPPER", "SERVER", NULL};

        COMPLETE_WITH_LIST(listPrivilegeForeign);
    }

    /* Complete "GRANT/REVOKE * ON * " with "TO/FROM" */
    else if ((pg_strcasecmp(PREV4_WD, "GRANT") == 0 || pg_strcasecmp(PREV4_WD, "REVOKE") == 0) &&
             pg_strcasecmp(PREV2_WD, "ON") == 0) {
        if (pg_strcasecmp(PREV_WD, "DATABASE") == 0)
            COMPLETE_WITH_QUERY(Query_for_list_of_databases);
        else if (pg_strcasecmp(PREV_WD, "DOMAIN") == 0)
            COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_domains, NULL);
        else if (pg_strcasecmp(PREV_WD, "FUNCTION") == 0)
            COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_functions, NULL);
        else if (pg_strcasecmp(PREV_WD, "LANGUAGE") == 0)
            COMPLETE_WITH_QUERY(Query_for_list_of_languages);
        else if (pg_strcasecmp(PREV_WD, "SCHEMA") == 0)
            COMPLETE_WITH_QUERY(Query_for_list_of_schemas);
        else if (pg_strcasecmp(PREV_WD, "TABLESPACE") == 0)
            COMPLETE_WITH_QUERY(Query_for_list_of_tablespaces);
        else if (pg_strcasecmp(PREV_WD, "TYPE") == 0)
            COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_datatypes, NULL);
        else if (pg_strcasecmp(PREV_WD, "DATA") == 0)
            COMPLETE_WITH_CONST("SOURCE");
        else if (pg_strcasecmp(PREV4_WD, "GRANT") == 0)
            COMPLETE_WITH_CONST("TO");
        else
            COMPLETE_WITH_CONST("FROM");
    }

    else if ((pg_strcasecmp(PREV2_WD, "tablespace") == 0 && pg_strcasecmp(PREV_WD, "=") == 0) ||
             pg_strcasecmp(PREV_WD, "tablespace") == 0)
        COMPLETE_WITH_QUERY(Query_for_list_of_tablespaces);

    /* Complete "GRANT/REVOKE * ON * TO/FROM" with username, GROUP, or PUBLIC */
    else if (pg_strcasecmp(PREV5_WD, "GRANT") == 0 && pg_strcasecmp(PREV3_WD, "ON") == 0) {
        if (pg_strcasecmp(PREV_WD, "TO") == 0)
            COMPLETE_WITH_QUERY(Query_for_list_of_grant_roles);
        else
            COMPLETE_WITH_CONST("TO");
    } else if (pg_strcasecmp(PREV5_WD, "REVOKE") == 0 && pg_strcasecmp(PREV3_WD, "ON") == 0) {
        if (pg_strcasecmp(PREV_WD, "FROM") == 0)
            COMPLETE_WITH_QUERY(Query_for_list_of_grant_roles);
        else
            COMPLETE_WITH_CONST("FROM");
    }

    /* Complete "GRANT/REVOKE * ON SCHEMA/.. * TO" with username, GROUP, or PUBLIC */
    else if (pg_strcasecmp(PREV6_WD, "GRANT") == 0 && pg_strcasecmp(PREV4_WD, "ON") == 0) {
        if (pg_strcasecmp(PREV_WD, "TO") == 0)
            COMPLETE_WITH_QUERY(Query_for_list_of_grant_roles);
        else
            COMPLETE_WITH_CONST("TO");
    }

    /* Complete "GRANT/REVOKE * TO/FROM" with username, GROUP, or PUBLIC */
    else if (pg_strcasecmp(PREV3_WD, "GRANT") == 0 && pg_strcasecmp(PREV_WD, "TO") == 0) {
        COMPLETE_WITH_QUERY(Query_for_list_of_grant_roles);
    } else if (pg_strcasecmp(PREV3_WD, "REVOKE") == 0 && pg_strcasecmp(PREV_WD, "FROM") == 0) {
        COMPLETE_WITH_QUERY(Query_for_list_of_grant_roles);
    }

    /* GROUP BY */
    else if (pg_strcasecmp(PREV3_WD, "FROM") == 0 && pg_strcasecmp(PREV_WD, "GROUP") == 0)
        COMPLETE_WITH_CONST("BY");

    /* INSERT */
    /* Complete INSERT with "INTO" */
    else if (pg_strcasecmp(PREV_WD, "INSERT") == 0)
        COMPLETE_WITH_CONST("INTO");
    /* Complete INSERT INTO with table names */
    else if (pg_strcasecmp(PREV2_WD, "INSERT") == 0 && pg_strcasecmp(PREV_WD, "INTO") == 0)
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_insertables, NULL);
    /* Complete "INSERT INTO <table> (" with attribute names */
    else if (pg_strcasecmp(PREV4_WD, "INSERT") == 0 && pg_strcasecmp(PREV3_WD, "INTO") == 0 &&
        pg_strcasecmp(PREV_WD, "(") == 0)
        COMPLETE_WITH_ATTR(PREV2_WD, "");

    /*
     * Complete INSERT INTO <table> with "(" or "VALUES" or "SELECT" or
     * "TABLE" or "DEFAULT VALUES"
     */
    else if (pg_strcasecmp(PREV3_WD, "INSERT") == 0 && pg_strcasecmp(PREV2_WD, "INTO") == 0) {
        static const char* const listInsert[] = {"(", "DEFAULT VALUES", "SELECT", "TABLE", "VALUES", NULL};

        COMPLETE_WITH_LIST(listInsert);
    }

    /*
     * Complete INSERT INTO <table> (attribs) with "VALUES" or "SELECT" or
     * "TABLE"
     */
    else if (pg_strcasecmp(PREV4_WD, "INSERT") == 0 && pg_strcasecmp(PREV3_WD, "INTO") == 0 &&
             PREV_WD[strlen(PREV_WD) - 1] == ')') {
        static const char* const listInsert[] = {"SELECT", "TABLE", "VALUES", NULL};

        COMPLETE_WITH_LIST(listInsert);
    }

    /* Insert an open parenthesis after "VALUES" */
    else if (pg_strcasecmp(PREV_WD, "VALUES") == 0 && pg_strcasecmp(PREV2_WD, "DEFAULT") != 0)
        COMPLETE_WITH_CONST("(");

    /* LOCK */
    /* Complete LOCK [TABLE] with a list of tables */
    else if (pg_strcasecmp(PREV_WD, "LOCK") == 0)
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_tables, " UNION SELECT 'TABLE'");
    else if (pg_strcasecmp(PREV_WD, "TABLE") == 0 && pg_strcasecmp(PREV2_WD, "LOCK") == 0)
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_tables, " UNION ALL SELECT 'ONLY'");

    /* For the following, handle the case of a single table only for now */

    /* Complete LOCK [TABLE] <table> with "IN" */
    else if ((pg_strcasecmp(PREV2_WD, "LOCK") == 0 && pg_strcasecmp(PREV_WD, "TABLE") != 0) ||
             (pg_strcasecmp(PREV2_WD, "TABLE") == 0 && pg_strcasecmp(PREV3_WD, "LOCK") == 0)) {
        static const char* const lockTableList[] = {"IN", "NOWAIT", NULL};
        
        COMPLETE_WITH_LIST(lockTableList);
    }

    /* Complete LOCK [TABLE] <table> IN with a lock mode */
    else if (pg_strcasecmp(PREV_WD, "IN") == 0 && (pg_strcasecmp(PREV3_WD, "LOCK") == 0 ||
             (pg_strcasecmp(PREV3_WD, "TABLE") == 0 && pg_strcasecmp(PREV4_WD, "LOCK") == 0))) {
        static const char* const lockModes[] = {
            "ACCESS SHARE MODE",
            "ROW SHARE MODE", "ROW EXCLUSIVE MODE",
            "SHARE UPDATE EXCLUSIVE MODE", "SHARE MODE",
            "SHARE ROW EXCLUSIVE MODE",
            "EXCLUSIVE MODE", "ACCESS EXCLUSIVE MODE", NULL
        };

        COMPLETE_WITH_LIST(lockModes);
    }

    else if ((pg_strcasecmp(PREV_WD, "ACCESS") == 0 || pg_strcasecmp(PREV_WD, "ROW") == 0) &&
             pg_strcasecmp(PREV2_WD, "IN") == 0 && (pg_strcasecmp(PREV4_WD, "LOCK") == 0 ||
             (pg_strcasecmp(PREV4_WD, "TABLE") == 0 && pg_strcasecmp(PREV5_WD, "LOCK") == 0))) {
        static const char* const lockModesAccess[] = {"SHARE MODE", "EXCLUSIVE MODE", NULL};
        COMPLETE_WITH_LIST(lockModesAccess);
    }

    else if (pg_strcasecmp(PREV_WD, "SHARE") == 0&& pg_strcasecmp(PREV2_WD, "IN") == 0 &&
             (pg_strcasecmp(PREV4_WD, "LOCK") == 0 || (pg_strcasecmp(PREV4_WD, "TABLE") == 0 &&
             pg_strcasecmp(PREV5_WD, "LOCK") == 0))) {
        static const char* const lockModesAccess[] = {"UPDATE EXCLUSIVE MODE", "ROW EXCLUSIVE MODE", "MODE", NULL};
        COMPLETE_WITH_LIST(lockModesAccess);
    }

    /* NOTIFY */
    else if (pg_strcasecmp(PREV_WD, "NOTIFY") == 0)
        COMPLETE_WITH_QUERY("SELECT pg_catalog.quote_ident(channel) FROM pg_catalog.pg_listening_channels() AS channel "
                            "WHERE substring(pg_catalog.quote_ident(channel),1,%d)='%s'");

    /* OPTIONS */
    else if (pg_strcasecmp(PREV_WD, "OPTIONS") == 0)
        COMPLETE_WITH_CONST("(");

    /* OWNER TO  - complete with available roles */
    else if (pg_strcasecmp(PREV2_WD, "OWNER") == 0 && pg_strcasecmp(PREV_WD, "TO") == 0)
        COMPLETE_WITH_QUERY(Query_for_list_of_roles);

    /* ORDER BY */
    else if (pg_strcasecmp(PREV3_WD, "FROM") == 0 && pg_strcasecmp(PREV_WD, "ORDER") == 0)
        COMPLETE_WITH_CONST("BY");
    else if (pg_strcasecmp(PREV4_WD, "FROM") == 0 && pg_strcasecmp(PREV2_WD, "ORDER") == 0 &&
             pg_strcasecmp(PREV_WD, "BY") == 0)
        COMPLETE_WITH_ATTR(PREV3_WD, "");

    /* PREPARE xx AS */
    else if (pg_strcasecmp(PREV_WD, "AS") == 0 && pg_strcasecmp(PREV3_WD, "PREPARE") == 0) {
        static const char* const listPrepare[] = {"SELECT", "UPDATE", "INSERT", "DELETE", NULL};

        COMPLETE_WITH_LIST(listPrepare);
    }

    /*
     * PREPARE TRANSACTION is missing on purpose. It's intended for transaction
     * managers, not for manual use in interactive sessions.
     */

    /* REASSIGN OWNED BY xxx TO yyy */
    else if (pg_strcasecmp(PREV_WD, "REASSIGN") == 0)
        COMPLETE_WITH_CONST("OWNED");
    else if (pg_strcasecmp(PREV_WD, "OWNED") == 0 && pg_strcasecmp(PREV2_WD, "REASSIGN") == 0)
        COMPLETE_WITH_CONST("BY");
    else if (pg_strcasecmp(PREV_WD, "BY") == 0 && pg_strcasecmp(PREV2_WD, "OWNED") == 0 &&
             pg_strcasecmp(PREV3_WD, "REASSIGN") == 0)
        COMPLETE_WITH_QUERY(Query_for_list_of_roles);
    else if (pg_strcasecmp(PREV2_WD, "BY") == 0 && pg_strcasecmp(PREV3_WD, "OWNED") == 0 &&
             pg_strcasecmp(PREV4_WD, "REASSIGN") == 0)
        COMPLETE_WITH_CONST("TO");
    else if (pg_strcasecmp(PREV_WD, "TO") == 0 && pg_strcasecmp(PREV3_WD, "BY") == 0 &&
             pg_strcasecmp(PREV4_WD, "OWNED") == 0 && pg_strcasecmp(PREV5_WD, "REASSIGN") == 0)
        COMPLETE_WITH_QUERY(Query_for_list_of_roles);

    /* REFRESH MATERIALIZED VIEW */
    else if (pg_strcasecmp(PREV_WD, "REFRESH") == 0) {
        static const char* const refreshObject[] = {"MATERIALIZED VIEW", "INCREMENTAL MATERIALIZED VIEW", NULL};
        COMPLETE_WITH_LIST(refreshObject);
    }
    else if (pg_strcasecmp(PREV2_WD, "REFRESH") == 0 && pg_strcasecmp(PREV_WD, "MATERIALIZED") == 0)
        COMPLETE_WITH_CONST("VIEW");
    else if (pg_strcasecmp(PREV3_WD, "REFRESH") == 0 && pg_strcasecmp(PREV2_WD, "MATERIALIZED") == 0 &&
             pg_strcasecmp(PREV_WD, "VIEW") == 0)
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_matviews, NULL);
    else if (pg_strcasecmp(PREV4_WD, "REFRESH") == 0 && pg_strcasecmp(PREV3_WD, "MATERIALIZED") == 0 &&
             pg_strcasecmp(PREV2_WD, "VIEW") == 0)
        COMPLETE_WITH_CONST("WITH");
    else if (pg_strcasecmp(PREV5_WD, "REFRESH") == 0 && pg_strcasecmp(PREV4_WD, "MATERIALIZED") == 0 &&
             pg_strcasecmp(PREV3_WD, "VIEW") == 0 && pg_strcasecmp(PREV_WD, "WITH") == 0) {
        static const char *const listWithData[] = {
            "NO DATA", "DATA", NULL};

        COMPLETE_WITH_LIST(listWithData);
    } else if (pg_strcasecmp(PREV6_WD, "REFRESH") == 0 && pg_strcasecmp(PREV5_WD, "MATERIALIZED") == 0 &&
        pg_strcasecmp(PREV4_WD, "VIEW") == 0 && pg_strcasecmp(PREV2_WD, "WITH") == 0 &&
        pg_strcasecmp(PREV_WD, "NO") == 0)
        COMPLETE_WITH_CONST("DATA");

    /* REINDEX */
    else if (pg_strcasecmp(PREV_WD, "REINDEX") == 0) {
        static const char* const listReindex[] = {"TABLE", "INDEX", "SYSTEM", "DATABASE", NULL};

        COMPLETE_WITH_LIST(listReindex);
    } else if (pg_strcasecmp(PREV2_WD, "REINDEX") == 0) {
        if (pg_strcasecmp(PREV_WD, "TABLE") == 0)
            COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_tm, " UNION SELECT 'CONCURRENTLY'");
        else if (pg_strcasecmp(PREV_WD, "INDEX") == 0)
            COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_indexes, " UNION SELECT 'CONCURRENTLY'");
        else if (pg_strcasecmp(PREV_WD, "SYSTEM") == 0 || pg_strcasecmp(PREV_WD, "DATABASE") == 0)
            COMPLETE_WITH_QUERY(Query_for_list_of_databases " UNION SELECT 'CONCURRENTLY'");
    } else if (pg_strcasecmp(PREV3_WD, "REINDEX") == 0) {
        if (pg_strcasecmp(PREV2_WD, "TABLE") == 0 && pg_strcasecmp(PREV_WD, "CONCURRENTLY") == 0)
            COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_tm, NULL);
        else if (pg_strcasecmp(PREV2_WD, "INDEX") == 0 && pg_strcasecmp(PREV_WD, "CONCURRENTLY") == 0)
            COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_indexes, NULL);
        else if ((pg_strcasecmp(PREV2_WD, "SYSTEM") == 0 || pg_strcasecmp(PREV2_WD, "DATABASE") == 0) && 
            pg_strcasecmp(PREV_WD, "CONCURRENTLY") == 0)
            COMPLETE_WITH_QUERY(Query_for_list_of_databases);
    }

    /* SECURITY LABEL */
    else if (pg_strcasecmp(PREV_WD, "SECURITY") == 0 && PREV2_WD[0] == '\0')
        COMPLETE_WITH_CONST("LABEL");
    else if (pg_strcasecmp(PREV2_WD, "SECURITY") == 0 && pg_strcasecmp(PREV_WD, "LABEL") == 0) {
        static const char* const listSecurityLabelPreposition[] = {"ON", "FOR"};

        COMPLETE_WITH_LIST(listSecurityLabelPreposition);
    } else if (pg_strcasecmp(PREV4_WD, "SECURITY") == 0 && pg_strcasecmp(PREV3_WD, "LABEL") == 0 &&
               pg_strcasecmp(PREV2_WD, "FOR") == 0)
        COMPLETE_WITH_CONST("ON");
    else if ((pg_strcasecmp(PREV3_WD, "SECURITY") == 0 && pg_strcasecmp(PREV2_WD, "LABEL") == 0 &&
             pg_strcasecmp(PREV_WD, "ON") == 0) ||
             (pg_strcasecmp(PREV5_WD, "SECURITY") == 0 && pg_strcasecmp(PREV4_WD, "LABEL") == 0 &&
             pg_strcasecmp(PREV3_WD, "FOR") == 0 && pg_strcasecmp(PREV_WD, "ON") == 0)) {
        static const char* const listSecurityLabel[] = {
            "LANGUAGE", "SCHEMA", "SEQUENCE", "TABLE", "TYPE", "VIEW", "MATERIALIZED VIEW", "COLUMN",
            "AGGREGATE", "FUNCTION", "DOMAIN", "LARGE OBJECT", NULL
        };

        COMPLETE_WITH_LIST(listSecurityLabel);
    } else if (pg_strcasecmp(PREV5_WD, "SECURITY") == 0 && pg_strcasecmp(PREV4_WD, "LABEL") == 0 &&
               pg_strcasecmp(PREV3_WD, "ON") == 0)
        COMPLETE_WITH_CONST("IS");

    /* SELECT */
    /* naah . . . */

    /* SET, RESET, SHOW */
    /* Complete with a variable name */
    else if ((pg_strcasecmp(PREV_WD, "SET") == 0 && pg_strcasecmp(PREV3_WD, "UPDATE") != 0) ||
             pg_strcasecmp(PREV_WD, "RESET") == 0)
        COMPLETE_WITH_QUERY(Query_for_list_of_set_vars);
    else if (pg_strcasecmp(PREV_WD, "SHOW") == 0)
        COMPLETE_WITH_QUERY(Query_for_list_of_show_vars);
    /* Complete "SET TRANSACTION" */
    else if ((pg_strcasecmp(PREV2_WD, "SET") == 0 && pg_strcasecmp(PREV_WD, "TRANSACTION") == 0) ||
             (pg_strcasecmp(PREV2_WD, "START") == 0 && pg_strcasecmp(PREV_WD, "TRANSACTION") == 0) ||
             (pg_strcasecmp(PREV2_WD, "BEGIN") == 0 && pg_strcasecmp(PREV_WD, "WORK") == 0) ||
             (pg_strcasecmp(PREV2_WD, "BEGIN") == 0 && pg_strcasecmp(PREV_WD, "TRANSACTION") == 0) ||
             (pg_strcasecmp(PREV4_WD, "SESSION") == 0 && pg_strcasecmp(PREV3_WD, "CHARACTERISTICS") == 0 &&
             pg_strcasecmp(PREV2_WD, "AS") == 0 && pg_strcasecmp(PREV_WD, "TRANSACTION") == 0)) {
        static const char* const myList[] = {"ISOLATION LEVEL", "READ", NULL};

        COMPLETE_WITH_LIST(myList);
    } else if ((pg_strcasecmp(PREV3_WD, "SET") == 0 || pg_strcasecmp(PREV3_WD, "BEGIN") == 0 ||
               pg_strcasecmp(PREV3_WD, "START") == 0 ||
               (pg_strcasecmp(PREV4_WD, "CHARACTERISTICS") == 0 && pg_strcasecmp(PREV3_WD, "AS") == 0)) &&
               (pg_strcasecmp(PREV2_WD, "TRANSACTION") == 0 || pg_strcasecmp(PREV2_WD, "WORK") == 0) &&
               pg_strcasecmp(PREV_WD, "ISOLATION") == 0)
        COMPLETE_WITH_CONST("LEVEL");
    else if ((pg_strcasecmp(PREV4_WD, "SET") == 0 || pg_strcasecmp(PREV4_WD, "BEGIN") == 0 ||
             pg_strcasecmp(PREV4_WD, "START") == 0 || pg_strcasecmp(PREV4_WD, "AS") == 0) &&
             (pg_strcasecmp(PREV3_WD, "TRANSACTION") == 0 || pg_strcasecmp(PREV3_WD, "WORK") == 0) &&
             pg_strcasecmp(PREV2_WD, "ISOLATION") == 0 && pg_strcasecmp(PREV_WD, "LEVEL") == 0) {
        static const char* const myList[] = {"READ", "REPEATABLE", "SERIALIZABLE", NULL};

        COMPLETE_WITH_LIST(myList);
    } else if ((pg_strcasecmp(PREV4_WD, "TRANSACTION") == 0 || pg_strcasecmp(PREV4_WD, "WORK") == 0) &&
               pg_strcasecmp(PREV3_WD, "ISOLATION") == 0 && pg_strcasecmp(PREV2_WD, "LEVEL") == 0 &&
               pg_strcasecmp(PREV_WD, "READ") == 0) {
        static const char* const myList[] = {"UNCOMMITTED", "COMMITTED", NULL};

        COMPLETE_WITH_LIST(myList);
    } else if ((pg_strcasecmp(PREV4_WD, "TRANSACTION") == 0 || pg_strcasecmp(PREV4_WD, "WORK") == 0) &&
               pg_strcasecmp(PREV3_WD, "ISOLATION") == 0 && pg_strcasecmp(PREV2_WD, "LEVEL") == 0 &&
               pg_strcasecmp(PREV_WD, "REPEATABLE") == 0)
        COMPLETE_WITH_CONST("READ");
    else if ((pg_strcasecmp(PREV3_WD, "SET") == 0 || pg_strcasecmp(PREV3_WD, "BEGIN") == 0 ||
             pg_strcasecmp(PREV3_WD, "START") == 0 || pg_strcasecmp(PREV3_WD, "AS") == 0) &&
             (pg_strcasecmp(PREV2_WD, "TRANSACTION") == 0 || pg_strcasecmp(PREV2_WD, "WORK") == 0) &&
             pg_strcasecmp(PREV_WD, "READ") == 0) {
        static const char* const myList[] = {"ONLY", "WRITE", NULL};

        COMPLETE_WITH_LIST(myList);
    }
    else if (pg_strcasecmp(PREV2_WD, "SET") == 0 && pg_strcasecmp(PREV_WD, "CONSTRAINTS") == 0) {
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_constraints_with_schema, " UNION SELECT 'ALL'");
    }
    /* Complete SET CONSTRAINTS <foo> with DEFERRED|IMMEDIATE */
    else if (pg_strcasecmp(PREV3_WD, "SET") == 0 && pg_strcasecmp(PREV2_WD, "CONSTRAINTS") == 0) {
        static const char* const constraintList[] = {"DEFERRED", "IMMEDIATE", NULL};

        COMPLETE_WITH_LIST(constraintList);
    }
    /* Complete SET ROLE */
    else if (pg_strcasecmp(PREV2_WD, "SET") == 0 && pg_strcasecmp(PREV_WD, "ROLE") == 0)
        COMPLETE_WITH_QUERY(Query_for_list_of_roles);
    /* Complete SET SESSION with AUTHORIZATION or CHARACTERISTICS... */
    else if (pg_strcasecmp(PREV2_WD, "SET") == 0 &&
            (pg_strcasecmp(PREV_WD, "SESSION") == 0 || pg_strcasecmp(PREV_WD, "LOCAL") == 0)) {
        static const char* const myList[] = {"AUTHORIZATION", "CHARACTERISTICS AS TRANSACTION", NULL};

        COMPLETE_WITH_LIST(myList);
    }
    /* Complete SET SESSION AUTHORIZATION with username */
    else if (pg_strcasecmp(PREV3_WD, "SET") == 0 && pg_strcasecmp(PREV2_WD, "SESSION") == 0 &&
             pg_strcasecmp(PREV_WD, "AUTHORIZATION") == 0)
        COMPLETE_WITH_QUERY(Query_for_list_of_roles " UNION SELECT 'DEFAULT'");
    /* Complete RESET SESSION with AUTHORIZATION */
    else if (pg_strcasecmp(PREV2_WD, "RESET") == 0 && pg_strcasecmp(PREV_WD, "SESSION") == 0)
        COMPLETE_WITH_CONST("AUTHORIZATION");
    /* Complete SET <var> with "TO" */
    else if (pg_strcasecmp(PREV2_WD, "SET") == 0 && pg_strcasecmp(PREV4_WD, "UPDATE") != 0 &&
             pg_strcasecmp(PREV_WD, "TABLESPACE") != 0 && pg_strcasecmp(PREV_WD, "SCHEMA") != 0 &&
             PREV_WD[strlen(PREV_WD) - 1] != ')' && pg_strcasecmp(PREV4_WD, "DOMAIN") != 0 &&
             pg_strcasecmp(PREV_WD, "CONSTRAINTS") != 0)
        COMPLETE_WITH_CONST("TO");
    /* Suggest possible variable values */
    else if (pg_strcasecmp(PREV3_WD, "SET") == 0 && (pg_strcasecmp(PREV_WD, "TO") == 0 || strcmp(PREV_WD, "=") == 0)) {
        if (pg_strcasecmp(PREV2_WD, "DateStyle") == 0) {
            static const char* const myList[] = {
                "ISO", "SQL", "Postgres", "German",
                "YMD", "DMY", "MDY",
                "US", "European", "NonEuropean",
                "DEFAULT", NULL
            };

            COMPLETE_WITH_LIST(myList);
        } else if (pg_strcasecmp(PREV2_WD, "IntervalStyle") == 0) {
            static const char* const myList[] = {"postgres", "postgres_verbose", "sql_standard", "iso_8601", NULL};

            COMPLETE_WITH_LIST(myList);
        } else if (pg_strcasecmp(PREV2_WD, "GEQO") == 0) {
            static const char* const myList[] = {"ON", "OFF", "DEFAULT", NULL};

            COMPLETE_WITH_LIST(myList);
        } else {
            static const char* const myList[] = {"DEFAULT", NULL};

            COMPLETE_WITH_LIST(myList);
        }
    }

    /* START TRANSACTION */
    else if (pg_strcasecmp(PREV_WD, "START") == 0)
        COMPLETE_WITH_CONST("TRANSACTION");

    /* TABLE, but not TABLE embedded in other commands */
    else if (pg_strcasecmp(PREV_WD, "TABLE") == 0 && PREV2_WD[0] == '\0')
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_relations, NULL);

    /* TRUNCATE */
    else if (pg_strcasecmp(PREV_WD, "TRUNCATE") == 0)
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_tables,
                                   " UNION SELECT 'TABLE'"
                                   " UNION SELECT 'ONLY'");

    else if (pg_strcasecmp(PREV2_WD, "TRUNCATE") == 0 && pg_strcasecmp(PREV_WD, "TABLE") == 0)
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_tables,
                                   " UNION SELECT 'ONLY'");

    else if (((pg_strcasecmp(PREV3_WD, "TRUNCATE") == 0 && pg_strcasecmp(PREV2_WD, "TABLE")) ||
             (pg_strcasecmp(PREV2_WD, "TRUNCATE") == 0)) && pg_strcasecmp(PREV_WD, "ONLY") == 0)
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_tables, NULL);

    /* UNLISTEN */
    else if (pg_strcasecmp(PREV_WD, "UNLISTEN") == 0)
        COMPLETE_WITH_QUERY("SELECT pg_catalog.quote_ident(channel) FROM pg_catalog.pg_listening_channels() AS channel "
                            "WHERE substring(pg_catalog.quote_ident(channel),1,%d)='%s' UNION SELECT '*'");

    /* UPDATE */
    /* If prev. word is UPDATE suggest a list of tables */
    else if (pg_strcasecmp(PREV_WD, "UPDATE") == 0)
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_updatables, NULL);
    /* Complete UPDATE <table> with "SET" */
    else if (pg_strcasecmp(PREV2_WD, "UPDATE") == 0)
        COMPLETE_WITH_CONST("SET");

    /*
     * If the previous word is SET (and it wasn't caught above as the _first_
     * word) the word before it was (hopefully) a table name and we'll now
     * make a list of attributes.
     */
    else if (pg_strcasecmp(PREV_WD, "SET") == 0)
        COMPLETE_WITH_ATTR(PREV2_WD, "");

    /* UPDATE xx SET yy = */
    else if (pg_strcasecmp(PREV2_WD, "SET") == 0 && pg_strcasecmp(PREV4_WD, "UPDATE") == 0)
        COMPLETE_WITH_CONST("=");

    /* USER MAPPING */
    else if ((pg_strcasecmp(PREV3_WD, "ALTER") == 0 || pg_strcasecmp(PREV3_WD, "CREATE") == 0 ||
             pg_strcasecmp(PREV3_WD, "DROP") == 0) &&
             pg_strcasecmp(PREV2_WD, "USER") == 0 && pg_strcasecmp(PREV_WD, "MAPPING") == 0)
        COMPLETE_WITH_CONST("FOR");
    else if (pg_strcasecmp(PREV4_WD, "CREATE") == 0 && pg_strcasecmp(PREV3_WD, "USER") == 0 &&
             pg_strcasecmp(PREV2_WD, "MAPPING") == 0 && pg_strcasecmp(PREV_WD, "FOR") == 0)
        COMPLETE_WITH_QUERY(Query_for_list_of_roles " UNION SELECT 'CURRENT_USER'"
            " UNION SELECT 'PUBLIC'"
            " UNION SELECT 'USER'");
    else if ((pg_strcasecmp(PREV4_WD, "ALTER") == 0 || pg_strcasecmp(PREV4_WD, "DROP") == 0) &&
             pg_strcasecmp(PREV3_WD, "USER") == 0 && pg_strcasecmp(PREV2_WD, "MAPPING") == 0 &&
             pg_strcasecmp(PREV_WD, "FOR") == 0)
        COMPLETE_WITH_QUERY(Query_for_list_of_user_mappings);
    else if ((pg_strcasecmp(PREV5_WD, "CREATE") == 0 || pg_strcasecmp(PREV5_WD, "ALTER") == 0 ||
             pg_strcasecmp(PREV5_WD, "DROP") == 0) &&
             pg_strcasecmp(PREV4_WD, "USER") == 0 && pg_strcasecmp(PREV3_WD, "MAPPING") == 0 &&
             pg_strcasecmp(PREV2_WD, "FOR") == 0)
        COMPLETE_WITH_CONST("SERVER");

    /*
     * VACUUM [ FULL | FREEZE ] [ VERBOSE ] [ table ]
     * VACUUM [ FULL | FREEZE ] [ VERBOSE ] ANALYZE [ table [ (column [, ...] ) ] ]
     */
    else if (pg_strcasecmp(PREV_WD, "VACUUM") == 0)
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_tm, " UNION SELECT 'FULL'"
            " UNION SELECT 'FREEZE'"
            " UNION SELECT 'ANALYZE'"
            " UNION SELECT 'VERBOSE'"
            " UNION SELECT 'DELTAMERGE'"
            " UNION SELECT 'HDFSDIRECTORY'"
            " UNION SELECT 'COMPACT'"
            " UNION SELECT 'ANALYSE'"
            " UNION SELECT 'PARTITION'");
    else if (pg_strcasecmp(PREV2_WD, "VACUUM") == 0 && pg_strcasecmp(PREV_WD, "FULL") == 0)
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_tm, " UNION SELECT 'ANALYZE'"
            " UNION SELECT 'VERBOSE'"
            " UNION SELECT 'COMPACT'");
    else if (pg_strcasecmp(PREV2_WD, "VACUUM") == 0 && pg_strcasecmp(PREV_WD, "FREEZE") == 0)
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_tm, " UNION SELECT 'ANALYZE'"
            " UNION SELECT 'VERBOSE'");
    else if (pg_strcasecmp(PREV3_WD, "VACUUM") == 0 && pg_strcasecmp(PREV_WD, "ANALYZE") == 0 &&
             (pg_strcasecmp(PREV2_WD, "FULL") == 0 || pg_strcasecmp(PREV2_WD, "FREEZE") == 0))
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_tm, " UNION SELECT 'VERBOSE'");
    else if (pg_strcasecmp(PREV3_WD, "VACUUM") == 0 && pg_strcasecmp(PREV_WD, "VERBOSE") == 0 &&
             (pg_strcasecmp(PREV2_WD, "FULL") == 0 || pg_strcasecmp(PREV2_WD, "FREEZE") == 0))
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_tm, " UNION SELECT 'ANALYZE'");
    else if (pg_strcasecmp(PREV2_WD, "VACUUM") == 0 && pg_strcasecmp(PREV_WD, "VERBOSE") == 0)
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_tm, " UNION SELECT 'ANALYZE'");
    else if (pg_strcasecmp(PREV2_WD, "VACUUM") == 0 && pg_strcasecmp(PREV_WD, "ANALYZE") == 0)
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_tm, " UNION SELECT 'VERBOSE'");
    else if ((pg_strcasecmp(PREV_WD, "ANALYZE") == 0 && pg_strcasecmp(PREV2_WD, "VERBOSE") == 0) ||
             (pg_strcasecmp(PREV_WD, "VERBOSE") == 0 && pg_strcasecmp(PREV2_WD, "ANALYZE") == 0))
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_tm, NULL);

    /* WITH [RECURSIVE] */

    /*
     * Only match when WITH is the first word, as WITH may appear in many
     * other contexts.
     */
    else if (pg_strcasecmp(PREV_WD, "WITH") == 0 && PREV2_WD[0] == '\0')
        COMPLETE_WITH_CONST("RECURSIVE");

    /* ANALYZE */
    /* If the previous word is ANALYZE, produce list of tables */
    else if (pg_strcasecmp(PREV_WD, "ANALYZE") == 0)
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_tmf, " UNION SELECT 'VERBOSE'"
            " UNION SELECT 'VERIFY'"
            " UNION SELECT 'PARTITION'");
    else if (pg_strcasecmp(PREV2_WD, "ANALYZE") == 0 && pg_strcasecmp(PREV_WD, "VERIFY") == 0) {
        static const char* const listAnalizeVerify[] = {"FAST", "COMPLETE", NULL};

        COMPLETE_WITH_LIST_CS(listAnalizeVerify);
    }
    else if (pg_strcasecmp(PREV3_WD, "ANALYZE") == 0 && pg_strcasecmp(PREV2_WD, "VERIFY") == 0 &&
             (pg_strcasecmp(PREV_WD, "FAST") == 0 || pg_strcasecmp(PREV_WD, "COMPLETE") == 0)) {
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_tmi, NULL);
    }

    /* WHERE */
    /* Simple case of the word before the where being the table name */
    else if (pg_strcasecmp(PREV_WD, "WHERE") == 0)
        COMPLETE_WITH_ATTR(PREV2_WD, "");

    /* ... FROM ... */
    else if (pg_strcasecmp(PREV_WD, "FROM") == 0 && pg_strcasecmp(PREV3_WD, "COPY") != 0 &&
             pg_strcasecmp(PREV3_WD, "\\copy") != 0)
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_tsvmf, NULL);

    /* ... JOIN ... */
    else if (pg_strcasecmp(PREV_WD, "JOIN") == 0)
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_tsvmf, NULL);

    /* Backslash commands */
    else if (strcmp(PREV_WD, "\\connect") == 0 || strcmp(PREV_WD, "\\c") == 0)
        COMPLETE_WITH_QUERY(Query_for_list_of_databases);

    else if (strncmp(PREV_WD, "\\da", strlen("\\da")) == 0)
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_aggregates, NULL);
    else if (strncmp(PREV_WD, "\\db", strlen("\\db")) == 0)
        COMPLETE_WITH_QUERY(Query_for_list_of_tablespaces);
    else if (strncmp(PREV_WD, "\\dD", strlen("\\dD")) == 0)
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_domains, NULL);
    else if (strncmp(PREV_WD, "\\des", strlen("\\des")) == 0)
        COMPLETE_WITH_QUERY(Query_for_list_of_servers);
    else if (strncmp(PREV_WD, "\\deu", strlen("\\deu")) == 0)
        COMPLETE_WITH_QUERY(Query_for_list_of_user_mappings);
    else if (strncmp(PREV_WD, "\\dew", strlen("\\dew")) == 0)
        COMPLETE_WITH_QUERY(Query_for_list_of_fdws);

    else if (strncmp(PREV_WD, "\\df", strlen("\\df")) == 0)
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_functions, NULL);
    else if (strncmp(PREV_WD, "\\dFd", strlen("\\dFd")) == 0)
        COMPLETE_WITH_QUERY(Query_for_list_of_ts_dictionaries);
    else if (strncmp(PREV_WD, "\\dFp", strlen("\\dFp")) == 0)
        COMPLETE_WITH_QUERY(Query_for_list_of_ts_parsers);
    else if (strncmp(PREV_WD, "\\dFt", strlen("\\dFt")) == 0)
        COMPLETE_WITH_QUERY(Query_for_list_of_ts_templates);
    /* must be at end of \dF */
    else if (strncmp(PREV_WD, "\\dF", strlen("\\dF")) == 0)
        COMPLETE_WITH_QUERY(Query_for_list_of_ts_configurations);

    else if (strncmp(PREV_WD, "\\di", strlen("\\di")) == 0)
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_indexes, NULL);
    else if (strncmp(PREV_WD, "\\dL", strlen("\\dL")) == 0)
        COMPLETE_WITH_QUERY(Query_for_list_of_languages);
    else if (strncmp(PREV_WD, "\\dn", strlen("\\dn")) == 0)
        COMPLETE_WITH_QUERY(Query_for_list_of_schemas);
    else if (strncmp(PREV_WD, "\\dp", strlen("\\dp")) == 0 || strncmp(PREV_WD, "\\z", strlen("\\z")) == 0)
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_tsvmf, NULL);
    else if (strncmp(PREV_WD, "\\ds", strlen("\\ds")) == 0)
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_sequences, NULL);
    else if (strncmp(PREV_WD, "\\dt", strlen("\\dt")) == 0)
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_tables, NULL);
    else if (strncmp(PREV_WD, "\\dT", strlen("\\dT")) == 0)
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_datatypes, NULL);
    else if (strncmp(PREV_WD, "\\du", strlen("\\du")) == 0 || (strncmp(PREV_WD, "\\dg", strlen("\\dg")) == 0))
        COMPLETE_WITH_QUERY(Query_for_list_of_roles);
    else if (strncmp(PREV_WD, "\\dv", strlen("\\dv")) == 0)
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_views, NULL);
    else if (strncmp(PREV_WD, "\\dm", strlen("\\dm")) == 0)
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_matviews, NULL);

    /* must be at end of \d list */
    else if (strncmp(PREV_WD, "\\d", strlen("\\d")) == 0)
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_relations, NULL);

    else if (strcmp(PREV_WD, "\\ef") == 0)
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_functions, NULL);

    else if (strcmp(PREV_WD, "\\encoding") == 0)
        COMPLETE_WITH_QUERY(Query_for_list_of_encodings);
    else if (strcmp(PREV_WD, "\\h") == 0 || strcmp(PREV_WD, "\\help") == 0)
        COMPLETE_WITH_LIST(sqlCommands);
    else if ((strcmp(PREV2_WD, "\\h") == 0 || strcmp(PREV2_WD, "\\help") == 0) &&
             pg_strcasecmp(PREV_WD, "DROP") == 0) {
        matches = completion_matches(text, DropCommandGenerator);
    }
    else if (strcmp(PREV_WD, "\\password") == 0)
        COMPLETE_WITH_QUERY(Query_for_list_of_roles);
    else if (strcmp(PREV_WD, "\\pset") == 0) {
        static const char* const myList[] = {
            "format", "border", "expanded",
            "null", "fieldsep", "tuples_only", "title", "tableattr",
            "linestyle", "pager", "recordsep", NULL
        };

        COMPLETE_WITH_LIST_CS(myList);
    } else if (strcmp(PREV2_WD, "\\pset") == 0) {
        if (strcmp(PREV_WD, "format") == 0) {
            static const char* const myList[] = {"unaligned", "aligned", "wrapped", "html", "latex", "troff-ms", NULL};

            COMPLETE_WITH_LIST_CS(myList);
        } else if (strcmp(PREV_WD, "linestyle") == 0) {
            static const char* const myList[] = {"ascii", "old-ascii", "unicode", NULL};

            COMPLETE_WITH_LIST_CS(myList);
        }
    } else if (strcmp(PREV_WD, "\\set") == 0) {
        matches = CompleteFromVariables(text, "", "");
    } else if (strcmp(PREV_WD, "\\sf") == 0 || strcmp(PREV_WD, "\\sf+") == 0)
        COMPLETE_WITH_SCHEMA_QUERY(Query_for_list_of_functions, NULL);
    else if (strcmp(PREV_WD, "\\cd") == 0 || strcmp(PREV_WD, "\\e") == 0 || strcmp(PREV_WD, "\\edit") == 0 ||
             strcmp(PREV_WD, "\\g") == 0 || strcmp(PREV_WD, "\\i") == 0 || strcmp(PREV_WD, "\\include") == 0 ||
             strcmp(PREV_WD, "\\ir") == 0 || strcmp(PREV_WD, "\\include_relative") == 0 || strcmp(PREV_WD, "\\o") == 0 ||
             strcmp(PREV_WD, "\\out") == 0 || strcmp(PREV_WD, "\\s") == 0 || strcmp(PREV_WD, "\\w") == 0 ||
             strcmp(PREV_WD, "\\write") == 0) {
        completion_charp = "\\";
        matches = completion_matches(text, CompleteFromFiles);
    }

    /*
     * Finally, we look through the list of "things", such as TABLE, INDEX and
     * check if that was the previous word. If so, execute the query to get a
     * list of them.
     */
    else {
        int i;

        for (i = 0; words_after_create[i].name; i++) {
            if (pg_strcasecmp(PREV_WD, words_after_create[i].name) == 0) {
                if (words_after_create[i].query) {
                    COMPLETE_WITH_QUERY(words_after_create[i].query);
                }
                else if (words_after_create[i].squery) {
                    COMPLETE_WITH_SCHEMA_QUERY(*words_after_create[i].squery, NULL);
                }
                break;
            }
        }
    }

    /*
     * If we still don't have anything to match we have to fabricate some sort
     * of default list. If we were to just return NULL, readline automatically
     * attempts filename completion, and that's usually no good.
     */
    if (matches == NULL) {
        COMPLETE_WITH_CONST("");
#ifdef HAVE_RL_COMPLETION_APPEND_CHARACTER
        rl_completion_append_character = '\0';
#endif
    }

    /* free storage */
    {
        int i;

        for (i = 0; i < (int)lengthof(previousWords); i++)
            free(previousWords[i]);
    }

    /* Return our Grand List O' Matches */
    return matches;
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
static char *_CompleteFromQuery(int isSchemaQuery, const char *text, int state)
{
    static int listIndex, stringLength;
    static PGresult *result = NULL;

    /*
     * If this is the first time for this completion, we fetch a list of our
     * "things" from the backend.
     */
    if (state == 0) {
        PQExpBufferData queryBuffer;
        char *eText = NULL;
        char *eInfoCharp = NULL;
        char *eInfoCharp2 = NULL;

        listIndex = 0;
        stringLength = strlen(text);

        /* Free any prior result */
        PQclear(result);
        result = NULL;

        /* Set up suitably-escaped copies of textual inputs */
        eText = (char *)pg_malloc(stringLength * 2 + 1);
        PQescapeString(eText, text, stringLength);

        if (NULL != completion_info_charp) {
            size_t charpLen;

            charpLen = strlen(completion_info_charp);
            eInfoCharp = (char *)pg_malloc(charpLen * 2 + 1);
            PQescapeString(eInfoCharp, completion_info_charp, charpLen);
        } else
            eInfoCharp = NULL;

        if (NULL != completion_info_charp2) {
            size_t charpLen;

            charpLen = strlen(completion_info_charp2);
            eInfoCharp2 = (char *)pg_malloc(charpLen * 2 + 1);
            PQescapeString(eInfoCharp2, completion_info_charp2, charpLen);
        } else
            eInfoCharp2 = NULL;

        initPQExpBuffer(&queryBuffer);

        if (isSchemaQuery) {
            /* completion_squery gives us the pieces to assemble */
            const char *qualresult = completion_squery->qualresult;

            if (qualresult == NULL)
                qualresult = completion_squery->result;

            /* Get unqualified names matching the input-so-far */
            appendPQExpBuffer(&queryBuffer, "SELECT %s FROM %s WHERE ", completion_squery->result,
                completion_squery->catname);
            if (NULL != completion_squery->selcondition)
                appendPQExpBuffer(&queryBuffer, "%s AND ", completion_squery->selcondition);
            appendPQExpBuffer(&queryBuffer, "substring(%s,1,%d)='%s'", completion_squery->result, stringLength,
                eText);
            appendPQExpBuffer(&queryBuffer, " AND %s", completion_squery->viscondition);

            /*
             * When fetching relation names, suppress system catalogs unless
             * the input-so-far begins with "pg_".	This is a compromise
             * between not offering system catalogs for completion at all, and
             * having them swamp the result when the input is just "p".
             */
            if (strcmp(completion_squery->catname, "pg_catalog.pg_class c") == 0 &&
                strncmp(text, "pg_", 3) != 0 && strncmp(text, "gs_", 3) != 0) {
                appendPQExpBuffer(&queryBuffer, " AND c.relnamespace <> (SELECT oid FROM"
                    " pg_catalog.pg_namespace WHERE nspname = 'pg_catalog')");
            }

            /*
             * Add in matching schema names, but only if there is more than
             * one potential match among schema names.
             */
            appendPQExpBuffer(&queryBuffer,
                "\nUNION\n"
                "SELECT pg_catalog.quote_ident(n.nspname) || '.' "
                "FROM pg_catalog.pg_namespace n "
                "WHERE substring(pg_catalog.quote_ident(n.nspname) || '.',1,%d)='%s'",
                stringLength, eText);
            appendPQExpBuffer(&queryBuffer,
                " AND (SELECT pg_catalog.count(*)"
                " FROM pg_catalog.pg_namespace"
                " WHERE substring(pg_catalog.quote_ident(nspname) || '.',1,%d) ="
                " substring('%s',1,pg_catalog.length(pg_catalog.quote_ident(nspname))+1)) > 1",
                stringLength, eText);

            /*
             * Add in matching qualified names, but only if there is exactly
             * one schema matching the input-so-far.
             */
            appendPQExpBuffer(&queryBuffer,
                "\nUNION\n"
                "SELECT pg_catalog.quote_ident(n.nspname) || '.' || %s "
                "FROM %s, pg_catalog.pg_namespace n "
                "WHERE %s = n.oid AND ",
                qualresult, completion_squery->catname, completion_squery->nameSpace);
            if (completion_squery->selcondition != NULL)
                appendPQExpBuffer(&queryBuffer, "%s AND ", completion_squery->selcondition);
            appendPQExpBuffer(&queryBuffer, "substring(pg_catalog.quote_ident(n.nspname) || '.' || %s,1,%d)='%s'",
                qualresult, stringLength, eText);

            /*
             * This condition exploits the single-matching-schema rule to
             * speed up the query
             */
            appendPQExpBuffer(&queryBuffer,
                " AND substring(pg_catalog.quote_ident(n.nspname) || '.',1,%d) ="
                " substring('%s',1,pg_catalog.length(pg_catalog.quote_ident(n.nspname))+1)",
                stringLength, eText);
            appendPQExpBuffer(&queryBuffer,
                " AND (SELECT pg_catalog.count(*)"
                " FROM pg_catalog.pg_namespace"
                " WHERE substring(pg_catalog.quote_ident(nspname) || '.',1,%d) ="
                " substring('%s',1,pg_catalog.length(pg_catalog.quote_ident(nspname))+1)) = 1",
                stringLength, eText);

            /* If an addon query was provided, use it */
            if (NULL != completion_charp)
                appendPQExpBuffer(&queryBuffer, "\n%s", completion_charp);
        } else {
            /* completion_charp is an sprintf-style format string */
            appendPQExpBuffer(&queryBuffer, completion_charp, stringLength, eText, eInfoCharp, eInfoCharp,
                eInfoCharp2, eInfoCharp2);
        }

        /* Limit the number of records in the result */
        appendPQExpBuffer(&queryBuffer, "\nLIMIT %d", completion_max_records);

        result = ExecQuery(queryBuffer.data);

        termPQExpBuffer(&queryBuffer);
        free(eText);
        eText = NULL;
        if (eInfoCharp != NULL) {
            free(eInfoCharp);
            eInfoCharp = NULL;
        }
        if (eInfoCharp2 != NULL) {
            free(eInfoCharp2);
            eInfoCharp2 = NULL;
        }
    }

    /* Find something that matches */
    if (result && PQresultStatus(result) == PGRES_TUPLES_OK) {
        const char *item = NULL;

        while (listIndex < PQntuples(result) && (item = PQgetvalue(result, listIndex++, 0)))
            if (pg_strncasecmp(text, item, stringLength) == 0)
                return pg_strdup(item);
    }

    /* If nothing matches, free the db structure and return null */
    PQclear(result);
    result = NULL;
    return NULL;
}

/*
 * This function returns in order one of a fixed, NULL pointer terminated list
 * of strings (if matching). This can be used if there are only a fixed number
 * SQL words that can appear at certain spot.
 */
static char *CompleteFromList(const char *text, int state)
{
    static int stringLength, listIndex, matches;
    static bool casesensitive = false;
    const char *item = NULL;

    /* need to have a list */
    psql_assert(completion_charpp);

    /* Initialization */
    if (state == 0) {
        listIndex = 0;
        stringLength = strlen(text);
        casesensitive = completion_case_sensitive;
        matches = 0;
    }

    while ((item = completion_charpp[listIndex++])) {
        /* First pass is case sensitive */
        if (casesensitive && strncmp(text, item, stringLength) == 0) {
            matches++;
            return pg_strdup(item);
        }

        /* Second pass is case insensitive, don't bother counting matches */
        if (!casesensitive && pg_strncasecmp(text, item, stringLength) == 0) {
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
        listIndex = 0;
        state++;
        return CompleteFromList(text, state);
    }

    /* If no more matches, return null. */
    return NULL;
}

/*
 * This function returns one fixed string the first time even if it doesn't
 * match what's there, and nothing the second time. This should be used if
 * there is only one possibility that can appear at a certain spot, so
 * misspellings will be overwritten.  The string to be passed must be in
 * completion_charp.
 */
static char *CompleteFromConst(const char *text, int state)
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
static char **CompleteFromVariables(const char *text, const char *prefix, const char *suffix)
{
    char **matches = NULL;
    int overhead = strlen(prefix) + strlen(suffix) + 1;
    char **varnames = NULL;
    int nvars = 0;
    int maxvars = 100;
    int i;
    struct _variable *ptr = NULL;
    int rc;
    size_t sz = 0;

    varnames = (char **)pg_malloc((maxvars + 1) * sizeof(char *));

    ptr = (pset.vars != NULL) ? pset.vars->next : NULL;
    for (; ptr != NULL; ptr = ptr->next) {
        char *buffer = NULL;

        if (nvars >= maxvars) {
            maxvars *= 2;
            varnames = (char **)psql_realloc(varnames, (maxvars + 1) * sizeof(char *), (maxvars + 1) * sizeof(char *));
            if (varnames == NULL) {
                psql_error("out of memory\n");
                exit(EXIT_FAILURE);
            }
        }

        sz = strlen(ptr->name) + overhead;
        buffer = (char *)pg_malloc(sz);
        rc = sprintf_s(buffer, sz, "%s%s%s", prefix, ptr->name, suffix);
        check_sprintf_s(rc);
        varnames[nvars++] = buffer;
    }

    varnames[nvars] = NULL;
    COMPLETE_WITH_LIST_CS((const char * const *)varnames);

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
static char *CompleteFromFiles(const char *text, int state)
{
    static const char *unquotedText = NULL;
    char *unquotedMatch = NULL;
    char *ret = NULL;

    if (state == 0) {
        /* Initialization: stash the unquoted input. */
        unquotedText = strtokx(text, "", NULL, "'", *completion_charp, false, true, pset.encoding);
        /* expect a NULL return for the empty string only */
        if (NULL == unquotedText) {
            psql_assert(!*text);
            unquotedText = text;
        }
    }

    unquotedMatch = filename_completion_function(unquotedText, state);
    if (unquotedMatch != NULL) {
        /*
         * Caller sets completion_charp to a zero- or one-character string
         * containing the escape character.  This is necessary since \copy has
         * no escape character, but every other backslash command recognizes
         * "\" as an escape character.  Since we have only two callers, don't
         * bother providing a macro to simplify this.
         */
        ret = quote_if_needed(unquotedMatch, " \t\r\n\"`", '\'', *completion_charp, pset.encoding);
        if (ret != NULL) {
            free(unquotedMatch);
            unquotedMatch = NULL;
        } else
            ret = unquotedMatch;
    }
    return ret;
}

/* HELPER FUNCTIONS */
/*
 * Make a pg_strdup copy of s and convert the case according to
 * COMP_KEYWORD_CASE variable, using ref as the text that was already entered.
 */
static char *pg_strdup_keyword_case(const char *s, const char *ref)
{
    char *ret = NULL;
    char *p = NULL;
    unsigned char first = ref[0];
    int tocase;
    const char *varval = NULL;

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

/*
 * Execute a query and report any errors. This should be the preferred way of
 * talking to the database in this file.
 */
static PGresult *ExecQuery(const char *query)
{
    PGresult *result = NULL;

    if (query == NULL || pset.db == NULL || PQstatus(pset.db) != CONNECTION_OK)
        return NULL;

    result = PQexec(pset.db, query);
    if (PQresultStatus(result) != PGRES_TUPLES_OK) {
        PQclear(result);
        result = NULL;
    }

    return result;
}

/*
 * Return the nwords word(s) before point.	Words are returned right to left,
 * that is, previousWords[0] gets the last word before point.
 * If we run out of words, remaining array elements are set to empty strings.
 * Each array element is filled with a malloc'd string.
 */
static void GetPreviousWords(int point, char **previousWords, int nwords)
{
    char *buf = NULL; /* alias */
    int i;
    errno_t rc = EOK;

    /*
     * If we have anything in tab_completion_query_buf, paste it together with
     * rl_line_buffer to construct the full query.  Otherwise we can just use
     * rl_line_buffer as the input string.
     */
    if (tab_completion_query_buf && tab_completion_query_buf->len > 0) {
        i = tab_completion_query_buf->len;
        const int bufLen = point + i + 2;
        buf = (char*)pg_malloc(bufLen);
        rc = memcpy_s(buf, bufLen, tab_completion_query_buf->data, i);
        securec_check_c(rc, "\0", "\0");

        buf[i++] = '\n';
        rc = memcpy_s(buf + i, bufLen, rl_line_buffer, point);
        securec_check_c(rc, "\0", "\0");
        i += point;
        buf[i] = '\0';
        /* Readjust point to reference appropriate offset in buf */
        point = i;
    } else {
        buf = rl_line_buffer;
    }

    /* first we look for a non-word char before the current point */
    for (i = point - 1; i >= 0; i--)
        if (strchr(WORD_BREAKS, buf[i]))
            break;
    point = i;

    while (nwords-- > 0) {
        int start, end;
        char *s = NULL;

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
            s = (char *)pg_malloc(end - start + 2);
            rc = strncpy_s(s, end - start + 2, &buf[start], end - start + 1);
            s[end - start + 1] = '\0';
            securec_check_c(rc, "\0", "\0");
        }

        *previousWords++ = s;
    }

    if (buf != rl_line_buffer) {
        free(buf);
    }
}
#endif /* HAVE_READLINE_READLINE_H */

/*
 * Since readline doesn't let us pass any state through to the tab completion
 * callback, we have to use this global variable to let GetPreviousWords()
 * get at the previous lines of the current command.
 */
PQExpBuffer tab_completion_query_buf = NULL;

/*
 * Initialize the readline library for our purposes.
 */
void initialize_readline(void)
{
    rl_readline_name = (char *)pset.progname;

#ifdef HAVE_READLINE_READLINE_H
    /* PsqlCompletion is deleted because it's too complex and not be used at all. */
    rl_attempted_completion_function = PsqlCompletion;
#else
    rl_attempted_completion_function = NULL;
#endif

    rl_basic_word_break_characters = WORD_BREAKS;

    completion_max_records = 1000;

    /*
     * There is a variable rl_completion_query_items for this but apparently
     * it's not defined everywhere.
     */
}

#endif /* USE_READLINE */
