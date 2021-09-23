/* -------------------------------------------------------------------------
 *
 * createuser
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/bin/scripts/createuser.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres_fe.h"
#include "common.h"
#include "dumputils.h"

static void help(const char* progname);
/* Database Security: Support password complexity */
static bool IsSpecialCharacter(char ch);
static bool CheckUserPasswd(const char* username, const char* passwd);
int main(int argc, char* argv[])
{
    static struct option long_options[] = {{"host", required_argument, NULL, 'h'},
        {"port", required_argument, NULL, 'p'},
        {"username", required_argument, NULL, 'U'},
        {"no-password", no_argument, NULL, 'w'},
        {"password", no_argument, NULL, 'W'},
        {"echo", no_argument, NULL, 'e'},
        {"createdb", no_argument, NULL, 'd'},
        {"no-createdb", no_argument, NULL, 'D'},
        {"superuser", no_argument, NULL, 's'},
        {"no-superuser", no_argument, NULL, 'S'},
        {"createrole", no_argument, NULL, 'r'},
        {"no-createrole", no_argument, NULL, 'R'},
        {"inherit", no_argument, NULL, 'i'},
        {"no-inherit", no_argument, NULL, 'I'},
        {"login", no_argument, NULL, 'l'},
        {"no-login", no_argument, NULL, 'L'},
        {"replication", no_argument, NULL, 1},
        {"no-replication", no_argument, NULL, 2},
        /* add audit admin privilege */
        {"auditadmin", no_argument, NULL, 3},
        {"no-auditadmin", no_argument, NULL, 4},
        {"interactive", no_argument, NULL, 5},
        /* adduser is obsolete, undocumented spelling of superuser */
        {"adduser", no_argument, NULL, 'a'},
        {"no-adduser", no_argument, NULL, 'A'},
        {"connection-limit", required_argument, NULL, 'c'},
        {"pwprompt", no_argument, NULL, 'P'},
        {"encrypted", no_argument, NULL, 'E'},
        {"unencrypted", no_argument, NULL, 'N'},
        {NULL, 0, NULL, 0}};

    const char* progname_tem = NULL;
    int optindex;
    int c;
    const char* newuser = NULL;
    char* host = NULL;
    char* port = NULL;
    char* username = NULL;
    enum trivalue prompt_password = TRI_DEFAULT;
    bool echo = false;
    bool interactive = false;
    char* conn_limit = NULL;
    bool pwprompt = false;
    char* newpassword = NULL;

    /* Tri-valued variables.  */
    enum trivalue createdb = TRI_DEFAULT;
    enum trivalue superuser = TRI_DEFAULT;
    enum trivalue createrole = TRI_DEFAULT;
    enum trivalue inherit = TRI_DEFAULT;
    enum trivalue login = TRI_DEFAULT;
    enum trivalue replication = TRI_DEFAULT;
    enum trivalue auditadmin = TRI_DEFAULT; /* add audit admin privilege */
    enum trivalue encrypted = TRI_DEFAULT;

    PQExpBufferData sql;

    PGconn* conn = NULL;
    PGresult* result = NULL;

    progname_tem = get_progname(argv[0]);
    set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("pgscripts"));

    handle_help_version_opts(argc, argv, "createuser", help);

    while ((c = getopt_long(argc, argv, "h:p:U:wWedDsSaArRiIlLc:PEN", long_options, &optindex)) != -1) {
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
            case 'd':
                createdb = TRI_YES;
                break;
            case 'D':
                createdb = TRI_NO;
                break;
            case 's':
            case 'a':
                superuser = TRI_YES;
                break;
            case 'S':
            case 'A':
                superuser = TRI_NO;
                break;
            case 'r':
                createrole = TRI_YES;
                break;
            case 'R':
                createrole = TRI_NO;
                break;
            case 'i':
                inherit = TRI_YES;
                break;
            case 'I':
                inherit = TRI_NO;
                break;
            case 'l':
                login = TRI_YES;
                break;
            case 'L':
                login = TRI_NO;
                break;
            case 'c':
                conn_limit = optarg;
                break;
            case 'P':
                pwprompt = true;
                break;
            case 'E':
                encrypted = TRI_YES;
                break;
            case 'N':
                encrypted = TRI_NO;
                break;
            case 1:
                replication = TRI_YES;
                break;
            case 2:
                replication = TRI_NO;
                break;
            /* add audit admin privilege */
            case 3:
                auditadmin = TRI_YES;
                break;
            case 4:
                auditadmin = TRI_NO;
                break;
            case 5:
                interactive = true;
                break;
            default:
                fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname_tem);
                exit(1);
        }
    }

    switch (argc - optind) {
        case 0:
            break;
        case 1:
            newuser = argv[optind];
            break;
        default:
            fprintf(stderr, _("%s: too many command-line arguments (first is \"%s\")\n"), progname_tem, argv[optind + 1]);
            fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname_tem);
            exit(1);
    }

    if (newuser == NULL) {
        if (interactive) {
            newuser = simple_prompt("Enter name of role to add: ", 128, true);
        } else {
            if (getenv("PGUSER") != NULL) {
                newuser = getenv("PGUSER");
            } else {
                newuser = get_user_name(progname_tem);
            }
        }
    }

    if (pwprompt) {
        char *pw1 = NULL;
        char *pw2 = NULL;
        int i = 0;
        for (i = 0; i != 3; ++i) {
            /* Database Security: Support password complexity */
            pw1 = simple_prompt("Enter new system admin password: ", 1024, false);
            pw2 = simple_prompt("Enter it again: ", 1024, false);
            /* if pwd1 equal to pwd2, try new password again */
            if (strcmp(pw1, pw2) != 0) {
                fprintf(stderr, _("Passwords didn't match.\n"));
                fprintf(stderr, _("Please try new password again.\n"));
                free(pw1);
                free(pw2);
                continue;
            }
            free(pw2);
            pw2 = NULL;
            /* if the pwd1 does not match, try it again*/
            if (CheckUserPasswd(newuser, pw1)) {
                break;
            }
        }
        if (i == 3) {
            free(pw1);
            pw1 = NULL;
            exit(1);
        }
        newpassword = pw1;
    }

    if (superuser == 0) {
        if (interactive && yesno_prompt("Shall the new role be a system admin?")) {
            superuser = TRI_YES;
        } else {
            superuser = TRI_NO;
        }
    }

    if (superuser == TRI_YES) {
        /* Not much point in trying to restrict a superuser */
        createdb = TRI_YES;
        createrole = TRI_YES;
    }

    if (createdb == 0) {
        if (interactive && yesno_prompt("Shall the new role be allowed to create databases?")) {
            createdb = TRI_YES;
        } else {
            createdb = TRI_NO;
        }
    }

    if (createrole == 0) {
        if (interactive && yesno_prompt("Shall the new role be allowed to create more new roles?")) {
            createrole = TRI_YES;
        } else {
            createrole = TRI_NO;
        }
    }

    if (inherit == 0) {
        inherit = TRI_YES;
    }
    if (login == 0) {
        login = TRI_YES;
    }
    conn = connectDatabase("postgres", host, port, username, prompt_password, progname_tem, false);

    initPQExpBuffer(&sql);

    printfPQExpBuffer(&sql, "CREATE ROLE %s", fmtId(newuser));
    if (newpassword != NULL) {
        if (encrypted == TRI_YES) {
            appendPQExpBuffer(&sql, " ENCRYPTED");
        }
        if (encrypted == TRI_NO) {
            appendPQExpBuffer(&sql, " UNENCRYPTED");
        }
        appendPQExpBuffer(&sql, " PASSWORD ");
        appendPQExpBuffer(&sql, "'%s'", newpassword);
        
    }
    if (superuser == TRI_YES) {
        appendPQExpBuffer(&sql, " SUPERUSER");
    }
    if (superuser == TRI_NO) {
        appendPQExpBuffer(&sql, " NOSUPERUSER");
    }
    if (createdb == TRI_YES) {
        appendPQExpBuffer(&sql, " CREATEDB");
    }
    if (createdb == TRI_NO) {
        appendPQExpBuffer(&sql, " NOCREATEDB");
    }
    if (createrole == TRI_YES) {
        appendPQExpBuffer(&sql, " CREATEROLE");
    }
    if (createrole == TRI_NO) {
        appendPQExpBuffer(&sql, " NOCREATEROLE");
    }
    if (inherit == TRI_YES) {
        appendPQExpBuffer(&sql, " INHERIT");
    }
    if (inherit == TRI_NO) {
        appendPQExpBuffer(&sql, " NOINHERIT");
    }
    if (login == TRI_YES) {
        appendPQExpBuffer(&sql, " LOGIN");
    }
    if (login == TRI_NO) {
        appendPQExpBuffer(&sql, " NOLOGIN");
    }
    if (replication == TRI_YES) {
        appendPQExpBuffer(&sql, " REPLICATION");
    }
    if (replication == TRI_NO) {
        appendPQExpBuffer(&sql, " NOREPLICATION");
    }
    /* add audit admin privilege */
    if (auditadmin == TRI_YES) {
        appendPQExpBuffer(&sql, " AUDITADMIN");
    }
    if (auditadmin == TRI_NO) {
        appendPQExpBuffer(&sql, " NOAUDITADMIN");
    }
    if (conn_limit != NULL) {
        appendPQExpBuffer(&sql, " CONNECTION LIMIT %s", conn_limit);
    }
    appendPQExpBuffer(&sql, ";\n");

    if (echo)
        printf("%s", sql.data);
    result = PQexec(conn, sql.data);
    if (PQresultStatus(result) != PGRES_COMMAND_OK) {
        fprintf(stderr, _("%s: creation of new role failed: %s"), progname_tem, PQerrorMessage(conn));
        PQfinish(conn);
        exit(1);
    }

    PQclear(result);
    PQfinish(conn);
    exit(0);
}

static void help(const char* progname_tem)
{
    printf(_("%s creates a new openGauss role.\n\n"), progname_tem);
    printf(_("Usage:\n"));
    printf(_("  %s [OPTION]... [ROLENAME]\n"), progname_tem);
    printf(_("\nOptions:\n"));
    printf(_("  -c, --connection-limit=N  connection limit for role (default: no limit)\n"));
    printf(_("  -d, --createdb            role can create new databases\n"));
    printf(_("  -D, --no-createdb         role cannot create databases (default)\n"));
    printf(_("  -e, --echo                show the commands being sent to the server\n"));
    printf(_("  -E, --encrypted           encrypt stored password\n"));
    printf(_("  -i, --inherit             role inherits privileges of roles it is a\n"
             "                            member of (default)\n"));
    printf(_("  -I, --no-inherit          role does not inherit privileges\n"));
    printf(_("  -l, --login               role can login (default)\n"));
    printf(_("  -L, --no-login            role cannot login\n"));
    printf(_("  -N, --unencrypted         do not encrypt stored password\n"));
    printf(_("  -P, --pwprompt            assign a password to new role\n"));
    printf(_("  -r, --createrole          role can create new roles\n"));
    printf(_("  -R, --no-createrole       role cannot create roles (default)\n"));
    printf(_("  -s, --superuser           role will be system admin\n"));
    printf(_("  -S, --no-superuser        role will not be system admin (default)\n"));
    printf(_("  -V, --version             output version information, then exit\n"));
    printf(_("  --interactive             prompt for missing role name and attributes rather\n"
             "                            than using defaults\n"));
    printf(_("  --replication             role can initiate replication\n"));
    printf(_("  --no-replication          role cannot initiate replication\n"));
    /* add audit admin privilege */
    printf(_("  --auditadmin              role can administer audit data\n"));
    printf(_("  --no-auditadmin           role cannot administer audit data\n"));
    printf(_("  -?, --help                show this help, then exit\n"));
    printf(_("\nConnection options:\n"));
    printf(_("  -h, --host=HOSTNAME       database server host or socket directory\n"));
    printf(_("  -p, --port=PORT           database server port\n"));
    printf(_("  -U, --username=USERNAME   user name to connect as (not the one to create)\n"));
    printf(_("  -w, --no-password         never prompt for password\n"));
    printf(_("  -W, --password            force password prompt\n"));
#if ((defined(ENABLE_MULTIPLE_NODES)) || (defined(ENABLE_PRIVATEGAUSS)))
    printf(_("\nReport bugs to GaussDB support.\n"));
#else
    printf(_("\nReport bugs to openGauss community by raising an issue.\n"));
#endif
}

/*
 * Brief			: whether the ch is one of the specifically letters
 * Description		:
 * Notes			:
 */
static bool IsSpecialCharacter(char ch)
{
    char* ptr = "~!@#$%^&*()-_=+\\|[{}];:,<.>/?";
    while (*ptr != '\0') {
        if (*ptr == ch) {
            return true;
        }
        ptr++;
    }
    return false;
}

/*
 * Brief			: Whether the initial password satisfy the complexity requirement.
 * Description		:
 * Notes			:
 */
static bool CheckUserPasswd(const char* username, const char* passwd)
{
    unsigned int slen = 0;
    int kinds[4] = {0};
    int kinds_num = 0;
    const char* ptr = NULL;
    int i = 0;
    if (username == NULL) {
        fprintf(stderr, _("The parameter username of CheckUserPasswd is NULL\n"));
        return false;
    }

    if (passwd == NULL) {
        fprintf(stderr, _("The parameter passwd of CheckUserPasswd is NULL\n"));
        return false;
    }

    /* passwd must be ascii code*/
    ptr = passwd;
    while (*ptr != '\0') {
        if (*ptr < 0 || *ptr > 127) {
            fprintf(stderr, _("The passwd must be ascii code."));
            return false;
        }
        ptr++;
    }

    /* passwd must contain at least eight characters*/
    slen = (unsigned int)strlen(passwd);
    if (slen < 8) {
        fprintf(stderr, _("Passwd must contain at least eight characters.\n"));
        return false;
    }

    /* passwd should not equal to the rolname*/
    if (0 == pg_strcasecmp(passwd, username)) {
        fprintf(stderr, _("Password should not equal to the rolname.\n"));
        return false;
    }

    /* passwd must contain at least three kinds of characters*/
    ptr = passwd;
    while (*ptr != '\0') {
        if (*ptr >= 'A' && *ptr <= 'Z') {
            kinds[0]++;
        } else if (*ptr >= 'a' && *ptr <= 'z') {
            kinds[1]++;
        } else if (*ptr >= '0' && *ptr <= '9') {
            kinds[2]++;
        } else if (IsSpecialCharacter(*ptr)) {
            kinds[3]++;
        }
        ptr++;
    }
    for (i = 0; i != 4; ++i) {
        if (kinds[i] > 0) {
            kinds_num++;
        }
    }
    if (kinds_num < 3) {
        fprintf(stderr, _("Password must contain at least three kinds of characters.\n"));
        return false;
    }
    return true;
}

