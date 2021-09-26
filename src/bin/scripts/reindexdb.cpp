/* -------------------------------------------------------------------------
 *
 * reindexdb
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *
 * src/bin/scripts/reindexdb.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres_fe.h"
#include "common.h"
#include "dumputils.h"

static void reindex_one_database(const char* name, const char* dbname_tem, const char* type, const char* host,
    const char* port, const char* username, enum trivalue prompt_password, const char* progname_tem, bool echo);
static void reindex_all_databases(const char* maintenance_db, const char* host, const char* port, const char* username,
    enum trivalue prompt_password, const char* progname_tem, bool echo, bool quiet);
static void reindex_system_catalogs(const char* dbname_tem, const char* host, const char* port, const char* username,
    enum trivalue prompt_password, const char* progname_tem, bool echo);
static void help(const char* progname_tem);

int main(int argc, char* argv[])
{
    static struct option long_options[] = {{"host", required_argument, NULL, 'h'},
        {"port", required_argument, NULL, 'p'},
        {"username", required_argument, NULL, 'U'},
        {"no-password", no_argument, NULL, 'w'},
        {"password", no_argument, NULL, 'W'},
        {"echo", no_argument, NULL, 'e'},
        {"quiet", no_argument, NULL, 'q'},
        {"dbname", required_argument, NULL, 'd'},
        {"all", no_argument, NULL, 'a'},
        {"system", no_argument, NULL, 's'},
        {"table", required_argument, NULL, 't'},
        {"index", required_argument, NULL, 'i'},
        {"maintenance-db", required_argument, NULL, 2},
        {NULL, 0, NULL, 0}};

    const char* progname_tem = NULL;
    int optindex;
    int c;

    const char* dbname_tem = NULL;
    const char* maintenance_db = NULL;
    const char* host = NULL;
    const char* port = NULL;
    const char* username = NULL;
    enum trivalue prompt_password = TRI_DEFAULT;
    bool syscatalog = false;
    bool alldb = false;
    bool echo = false;
    bool quiet = false;
    const char* table = NULL;
    const char* index = NULL;

    progname_tem = get_progname(argv[0]);
    set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("pgscripts"));

    handle_help_version_opts(argc, argv, "reindexdb", help);

    /* process command-line options */
    while ((c = getopt_long(argc, argv, "h:p:U:wWeqd:ast:i:", long_options, &optindex)) != -1) {
        switch (c) {
            case 'h':
                host = optarg;
                break;
            case 'p':
                port = optarg;
                break;
            case 'U':
                username = optarg;
                break;
            case 'w':
                prompt_password = TRI_NO;
                break;
            case 'W':
                prompt_password = TRI_YES;
                break;
            case 'e':
                echo = true;
                break;
            case 'q':
                quiet = true;
                break;
            case 'd':
                dbname_tem = optarg;
                break;
            case 'a':
                alldb = true;
                break;
            case 's':
                syscatalog = true;
                break;
            case 't':
                table = optarg;
                break;
            case 'i':
                index = optarg;
                break;
            case 2:
                maintenance_db = optarg;
                break;
            default:
                fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname_tem);
                exit(1);
        }
    }

    /*
     * Non-option argument specifies database name as long as it wasn't
     * already specified with -d / --dbname
     */
    if (optind < argc && dbname_tem == NULL) {
        dbname_tem = argv[optind];
        optind++;
    }

    if (optind < argc) {
        fprintf(stderr, _("%s: too many command-line arguments (first is \"%s\")\n"), progname_tem, argv[optind]);
        fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname_tem);
        exit(1);
    }

    setup_cancel_handler();

    if (alldb) {
        if (dbname_tem != NULL) {
            fprintf(stderr, _("%s: cannot reindex all databases and a specific one at the same time\n"), progname_tem);
            exit(1);
        }
        if (syscatalog) {
            fprintf(stderr, _("%s: cannot reindex all databases and system catalogs at the same time\n"), progname_tem);
            exit(1);
        }
        if (table != NULL) {
            fprintf(stderr, _("%s: cannot reindex a specific table in all databases\n"), progname_tem);
            exit(1);
        }
        if (index != NULL) {
            fprintf(stderr, _("%s: cannot reindex a specific index in all databases\n"), progname_tem);
            exit(1);
        }

        reindex_all_databases(maintenance_db, host, port, username, prompt_password, progname_tem, echo, quiet);
    } else if (syscatalog) {
        if (table != NULL) {
            fprintf(stderr, _("%s: cannot reindex a specific table and system catalogs at the same time\n"), progname_tem);
            exit(1);
        }
        if (index != NULL) {
            fprintf(stderr, _("%s: cannot reindex a specific index and system catalogs at the same time\n"), progname_tem);
            exit(1);
        }

        if (dbname_tem == NULL) {
            if (getenv("PGDATABASE") != NULL)
                dbname_tem = getenv("PGDATABASE");
            else if (getenv("PGUSER") != NULL)
                dbname_tem = getenv("PGUSER");
            else
                dbname_tem = get_user_name(progname_tem);
        }

        reindex_system_catalogs(dbname_tem, host, port, username, prompt_password, progname_tem, echo);
    } else {
        if (dbname_tem == NULL) {
            if (getenv("PGDATABASE") != NULL)
                dbname_tem = getenv("PGDATABASE");
            else if (getenv("PGUSER") != NULL)
                dbname_tem = getenv("PGUSER");
            else
                dbname_tem = get_user_name(progname_tem);
        }

        if (index != NULL)
            reindex_one_database(index, dbname_tem, "INDEX", host, port, username, prompt_password, progname_tem, echo);
        if (table != NULL)
            reindex_one_database(table, dbname_tem, "TABLE", host, port, username, prompt_password, progname_tem, echo);
        /* reindex database only if index or table is not specified */
        if (index == NULL && table == NULL)
            reindex_one_database(dbname_tem, dbname_tem, "DATABASE", host, port, username, prompt_password, progname_tem, echo);
    }

    exit(0);
}

static void reindex_one_database(const char* name, const char* dbname_tem, const char* type, const char* host,
    const char* port, const char* username, enum trivalue prompt_password, const char* progname_tem, bool echo)
{
    PQExpBufferData sql;

    PGconn* conn = NULL;

    initPQExpBuffer(&sql);

    appendPQExpBuffer(&sql, "REINDEX");
    if (strcmp(type, "TABLE") == 0)
        appendPQExpBuffer(&sql, " TABLE %s", name);
    else if (strcmp(type, "INDEX") == 0)
        appendPQExpBuffer(&sql, " INDEX %s", name);
    else if (strcmp(type, "DATABASE") == 0)
        appendPQExpBuffer(&sql, " DATABASE %s", fmtId(name));
    appendPQExpBuffer(&sql, ";\n");

    conn = connectDatabase(dbname_tem, host, port, username, prompt_password, progname_tem, false);
    if (!executeMaintenanceCommand(conn, sql.data, echo)) {
        if (strcmp(type, "TABLE") == 0)
            fprintf(stderr,
                _("%s: reindexing of table \"%s\" in database \"%s\" failed: %s"),
                progname_tem,
                name,
                dbname_tem,
                PQerrorMessage(conn));
        if (strcmp(type, "INDEX") == 0)
            fprintf(stderr,
                _("%s: reindexing of index \"%s\" in database \"%s\" failed: %s"),
                progname_tem,
                name,
                dbname_tem,
                PQerrorMessage(conn));
        else
            fprintf(stderr, _("%s: reindexing of database \"%s\" failed: %s"), progname_tem, dbname_tem, PQerrorMessage(conn));
        PQfinish(conn);
        exit(1);
    }

    PQfinish(conn);
    termPQExpBuffer(&sql);
}

static void reindex_all_databases(const char* maintenance_db, const char* host, const char* port, const char* username,
    enum trivalue prompt_password, const char* progname_tem, bool echo, bool quiet)
{
    PGconn* conn = NULL;
    PGresult* result = NULL;
    int i;

    conn = connectMaintenanceDatabase(maintenance_db, host, port, username, prompt_password, progname_tem);
    result = executeQuery(conn, "SELECT datname FROM pg_database WHERE datallowconn ORDER BY 1;", progname_tem, echo);
    PQfinish(conn);

    for (i = 0; i < PQntuples(result); i++) {
        char* dbname_tem = PQgetvalue(result, i, 0);

        if (!quiet) {
            printf(_("%s: reindexing database \"%s\"\n"), progname_tem, dbname_tem);
            fflush(stdout);
        }

        reindex_one_database(dbname_tem, dbname_tem, "DATABASE", host, port, username, prompt_password, progname_tem, echo);
    }

    PQclear(result);
}

static void reindex_system_catalogs(const char* dbname_tem, const char* host, const char* port, const char* username,
    enum trivalue prompt_password, const char* progname_tem, bool echo)
{
    PQExpBufferData sql;

    PGconn* conn = NULL;

    initPQExpBuffer(&sql);

    appendPQExpBuffer(&sql, "REINDEX SYSTEM %s;\n", dbname_tem);

    conn = connectDatabase(dbname_tem, host, port, username, prompt_password, progname_tem, false);
    if (!executeMaintenanceCommand(conn, sql.data, echo)) {
        fprintf(stderr, _("%s: reindexing of system catalogs failed: %s"), progname_tem, PQerrorMessage(conn));
        PQfinish(conn);
        exit(1);
    }
    PQfinish(conn);
    termPQExpBuffer(&sql);
}

static void help(const char* progname_tem)
{
    printf(_("%s reindexes a openGauss database.\n\n"), progname_tem);
    printf(_("Usage:\n"));
    printf(_("  %s [OPTION]... [DBNAME]\n"), progname_tem);
    printf(_("\nOptions:\n"));
    printf(_("  -a, --all                 reindex all databases\n"));
    printf(_("  -d, --dbname=DBNAME       database to reindex\n"));
    printf(_("  -e, --echo                show the commands being sent to the server\n"));
    printf(_("  -i, --index=INDEX         recreate specific index only\n"));
    printf(_("  -q, --quiet               don't write any messages\n"));
    printf(_("  -s, --system              reindex system catalogs\n"));
    printf(_("  -t, --table=TABLE         reindex specific table only\n"));
    printf(_("  -V, --version             output version information, then exit\n"));
    printf(_("  -?, --help                show this help, then exit\n"));
    printf(_("\nConnection options:\n"));
    printf(_("  -h, --host=HOSTNAME       database server host or socket directory\n"));
    printf(_("  -p, --port=PORT           database server port\n"));
    printf(_("  -U, --username=USERNAME   user name to connect as\n"));
    printf(_("  -w, --no-password         never prompt for password\n"));
    printf(_("  -W, --password            force password prompt\n"));
    printf(_("  --maintenance-db=DBNAME   alternate maintenance database\n"));
    printf(_("\nRead the description of the SQL command REINDEX for details.\n"));
#if ((defined(ENABLE_MULTIPLE_NODES)) || (defined(ENABLE_PRIVATEGAUSS)))
    printf(_("\nReport bugs to GaussDB support.\n"));
#else
    printf(_("\nReport bugs to openGauss community by raising an issue.\n"));
#endif
}

